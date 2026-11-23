/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/


package rice.p2p.past;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import rice.Continuation;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.past.messaging.MessageLostMessage;
import rice.p2p.past.messaging.PastMessage;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * The messages of a {@link PastImpl} that go over a socket of their own: it reads the messages that arrive on the
 * sockets the endpoint accepts and writes a message to a new socket to another node.
 */
final class PastSocketTransport {
  private static final int SOCKET_TIMEOUT_MILLIS = 10000;

  private final PastImpl past;
  private final Endpoint endpoint;
  private final PastPendingRequests pending;
  private final Logger logger;

  /**
   * AppSocket -> ByteBuffer[]
   *
   * Used for receiving the objects.
   */
  private final PastSocketReader socketReader;

  PastSocketTransport(PastImpl past, Endpoint endpoint, PastPendingRequests pending, Logger logger, int maxSocketMessageBytes) {
    this.past = past;
    this.endpoint = endpoint;
    this.pending = pending;
    this.logger = logger;
    this.socketReader = new PastSocketReader(maxSocketMessageBytes);
  }

  /** Starts to accept the sockets of other nodes, one after the other, and to deliver the messages they carry. */
  void acceptSockets() {
    endpoint.accept(new AppSocketReceiver() {

      @Override
      public void receiveSocket(AppSocket socket) {
        logger.debug("Received Socket from {}", socket);
        socket.register(true, false, SOCKET_TIMEOUT_MILLIS, this);
        endpoint.accept(this);
      }

      @Override
      public void receiveSelectResult(AppSocket socket, boolean canRead,
          boolean canWrite) {
        logger.trace("Reading from {}", socket);
        try {
          byte[] message = socketReader.read(socket);
          if (message != null) {
            // deserialize the object
            SimpleInputBuffer inputBuffer = new SimpleInputBuffer(message);

            short type = inputBuffer.readShort();

            PastMessage result = (PastMessage)endpoint.getDeserializer().deserialize(inputBuffer,type,(byte)0,null);
            past.deliver(null,result);
          }

          // there will be more data on the socket if we haven't received everything yet
          // need to register either way to be able to read from the sockets when they are closed remotely, could alternatively close early
          // cause we are currently only sending 1 message per socket, but it's better to just keep reading in case we one day reuse sockets
            socket.register(true, false, SOCKET_TIMEOUT_MILLIS, this);

            // no recursive call to read the next object: calling read when not ready throws an exception
        } catch (EOFException e) {
          close(socket);
        } catch (IOException e) {
          receiveException(socket, e);
        }
      }

      @Override
      public void receiveException(AppSocket socket, Exception e) {
        logger.warn("Error receiving message", e);
        close(socket);
      }

      void close(AppSocket socket) {
        if (socket == null) return;
        socketReader.forget(socket);
        socket.close();
      }

    });  }

  void send(final NodeHandle handle, final PastMessage msg, final Continuation<?, Exception> continuation) {
    if (continuation != null) {
      CancellableTask timer = endpoint.scheduleMessage(new MessageLostMessage(msg.getUID(), past.getLocalNodeHandle(), null, msg, handle), past.MESSAGE_TIMEOUT);
      pending.insert(msg.getUID(), timer, continuation);
    }

    // create a bb[] to be written
    SimpleOutputBuffer outputBuffer = new SimpleOutputBuffer();
    try {
      outputBuffer.writeInt(0); // place holder for size...
      outputBuffer.writeShort(msg.getType());
      msg.serialize(outputBuffer);
    } catch (IOException e) {
      if (continuation != null) continuation.receiveException(e);
    }

    // add the size back to the beginning...
    int size = outputBuffer.getWritten()-Integer.BYTES; // remove the size of the size :)
    logger.trace("Sending size of {} to {} to send {}", size, handle, msg);
    byte[] bytes = outputBuffer.getBytes();
    MathUtils.intToByteArray(size,bytes,0);

    // prepare the bytes for writing
    final ByteBuffer[] buffers = new ByteBuffer[1];
    buffers[0] = ByteBuffer.wrap(bytes, 0, outputBuffer.getWritten()); // the whole thing


    logger.debug("Opening socket to {} to send {}", handle, msg);
    endpoint.connect(handle, new AppSocketReceiver() {

      @Override
      public void receiveSocket(AppSocket socket) {
        logger.trace("Opened socket to {}:{} to send {}", handle, socket, msg);
        socket.register(false, true, SOCKET_TIMEOUT_MILLIS, this);
      }

      @Override
      public void receiveSelectResult(AppSocket socket, boolean canRead,
          boolean canWrite) {
        logger.trace("Writing to {}:{} to send {}", handle, socket, msg);


        try {
          socket.write(buffers[0]);
        } catch (IOException e) {
          if (continuation != null)
            continuation.receiveException(e);
          else
            logger.warn("Error sending {}", msg, e);
          return; // don't continue to try to send
        }
        if (buffers[0].remaining() > 0) {
          socket.register(false, true, SOCKET_TIMEOUT_MILLIS, this);
        } else {
          socket.close();
        }
      }

      @Override
      public void receiveException(AppSocket socket, Exception e) {
        if (continuation != null) continuation.receiveException(e);
      }
    },
    SOCKET_TIMEOUT_MILLIS);
  }
}
