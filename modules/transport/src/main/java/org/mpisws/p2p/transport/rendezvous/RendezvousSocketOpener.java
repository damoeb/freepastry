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
package org.mpisws.p2p.transport.rendezvous;

import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.ACCEPTOR_SOCKET;
import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.CONNECTION_RESPONSE_SUCCESS;
import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.CONNECTOR_SOCKET;

import java.io.IOException;
import java.net.BindException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.slf4j.Logger;
import rice.environment.random.RandomSource;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * The client side of the rendezvous handshakes: opens a socket to a rendezvous node (the middleman) and writes the
 * header of a connector (the local node wants a socket to a firewalled node) or of an acceptor (the local node is the
 * firewalled node and answers a request), then reads the answer of the middleman.
 *
 * protocol: see {@link RendezvousTransportLayerImpl}. Runs on the selector thread.
 */
class RendezvousSocketOpener<Identifier, HighIdentifier extends RendezvousContact> {
  private final RendezvousTransportLayerImpl<Identifier, HighIdentifier> layer;
  private final TransportLayer<Identifier, ByteBuffer> transportLayer;
  private final RandomSource random;
  private final String RENDEZVOUS_CONTACT_STRING;
  private final Logger logger;
  private final ContactDeserializer<Identifier, HighIdentifier> serializer;
  private final HighIdentifier localNodeHandle;

  RendezvousSocketOpener(RendezvousTransportLayerImpl<Identifier, HighIdentifier> layer,
      TransportLayer<Identifier, ByteBuffer> transportLayer, RandomSource random, String RENDEZVOUS_CONTACT_STRING) {
    this.layer = layer;
    this.transportLayer = transportLayer;
    this.random = random;
    this.RENDEZVOUS_CONTACT_STRING = RENDEZVOUS_CONTACT_STRING;
    this.logger = layer.logger;
    this.serializer = layer.serializer;
    this.localNodeHandle = layer.localNodeHandle;
  }

  void openSocketViaPilot(
      final HighIdentifier destination,
      final HighIdentifier middleMan,
      final SocketRequestHandle<Identifier> handle,
      final SocketCallback<Identifier> deliverSocketToMe,
      final @Nullable Map<String, Object> options) {

    if (middleMan.equals(localNodeHandle)) {
      throw new IllegalArgumentException("openSocketViaPilot("+destination+","+middleMan+","+handle+","+deliverSocketToMe+","+options+") can't use self as rendezvous.");
    }

    final int uid = random.nextInt();
    logger.atDebug().log(() -> "openSocketViaPilot<"+uid+">("+destination+","+middleMan+","+handle+","+deliverSocketToMe+","+options+")");

    // build header
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      sob.writeByte(CONNECTOR_SOCKET);
      serializer.serialize(destination, sob);
      serializer.serialize(localNodeHandle, sob);
      sob.writeInt(uid);
    } catch (IOException ioe) {
      deliverSocketToMe.receiveException(handle, ioe);
    }

    final ByteBuffer writeBuffer = sob.getByteBuffer(); // to write all the connection info
    final ByteBuffer readBuffer = ByteBuffer.allocate(1);  // to read success

    // open the socket
    transportLayer.openSocket(serializer.convert(middleMan), new SocketCallback<Identifier>() {
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> openedSocket) {

        try {
          new P2PSocketReceiver<Identifier>() {

            @Override
            public void receiveSelectResult(P2PSocket<Identifier> socket,
                boolean canRead, boolean canWrite) throws IOException {
              if (writeBuffer.hasRemaining()) {
                // write the header
                long bytesWritten = socket.write(writeBuffer);
                if (bytesWritten < 0) {
                  deliverSocketToMe.receiveException(handle, new ClosedChannelException("Channel closed detected to <"+uid+"> "+destination+" via "+middleMan+" in "+layer));
                  return;
                }
                if (writeBuffer.hasRemaining()) {
                  socket.register(false, true, this);
                  return;
                }
              }
              if (!writeBuffer.hasRemaining()) {
                // read for the response
                if (readBuffer.hasRemaining()) {
                  long bytesRead = socket.read(readBuffer);
                  if (bytesRead < 0) {
                    deliverSocketToMe.receiveException(handle, new ClosedChannelException("Channel closed detected to <"+uid+"> "+destination+" via "+middleMan+" in "+layer));
                    return;
                  }
                  if (readBuffer.hasRemaining()) {
                    socket.register(true, false, this);
                    return;
                  }
                }

                // interpret the response
                readBuffer.flip();
                byte response = readBuffer.get();
                switch(response) {
                case CONNECTION_RESPONSE_SUCCESS:
                  logger.atDebug().log(() -> "success in openSocketViaPilot<"+uid+">("+destination+","+middleMan+","+handle+","+deliverSocketToMe+","+options+")");
                  deliverSocketToMe.receiveResult(handle, socket);
                  return;
                default:
                  deliverSocketToMe.receiveException(handle, new ClosedChannelException("Failed to connect to <"+uid+"> "+destination+" via "+middleMan+" in "+layer+" response:"+response));
                  return;
                }
              }
            }

            @Override
            public void receiveException(P2PSocket<Identifier> socket,
                Exception ioe) {
              deliverSocketToMe.receiveException(handle, ioe);
            }
          }.receiveSelectResult(openedSocket, false, true);
        } catch (IOException ioe) {
          deliverSocketToMe.receiveException(handle, ioe);
        }
      }
      @Override
      public void receiveException(SocketRequestHandle<Identifier> request,
          Exception ex) {
        deliverSocketToMe.receiveException(handle, ex);
      }
    }, options);
  }

  /**
   * We are a firewalled node and got a connect request, now time to respond to it
   */
  void openAcceptSocket(final HighIdentifier requestor, final HighIdentifier middleMan, final int uid) {
    logger.trace("openAcceptSocket({},{},{})", requestor, middleMan, uid);
    // TODO: there is a case where the requestor can be contacted directly, in this case, just do that, but may have to change
    // some other parts of the code:
      // 1) Send message to middleman, rather than open socket
      // 2) Change Pilot Request to include the middleman and requestor, or, better yet, make PILOT_CONNECT_DIRECT
      // 3) Set accept the socket directly

    if (!middleMan.canContactDirect()) {
      throw new IllegalArgumentException("openAcceptSocket("+requestor+","+middleMan+","+uid+") middleMan is firewalled.");
    }


    // build header
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      sob.writeByte(ACCEPTOR_SOCKET);
      serializer.serialize(localNodeHandle, sob);
      serializer.serialize(requestor, sob);
      sob.writeInt(uid);
    } catch (IOException ioe) {
      logger.warn("Error serializing in openAcceptSocket({},{},{})", requestor, middleMan, uid, ioe);
      return;
    }

    final ByteBuffer writeBuffer = sob.getByteBuffer(); // to write all the connection info
    final ByteBuffer readBuffer = ByteBuffer.allocate(1);  // to read success


    transportLayer.openSocket(serializer.convert(middleMan), new SocketCallback<Identifier>() {
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> openedSocket) {
        try {
          new P2PSocketReceiver<Identifier>() {

            @Override
            public void receiveSelectResult(P2PSocket<Identifier> socket,
                boolean canRead, boolean canWrite) throws IOException {
              if (writeBuffer.hasRemaining()) {
                // write the header
                long bytesWritten = socket.write(writeBuffer);
                if (bytesWritten < 0) {
                  logger.warn("Channel closed in openAcceptSocket({},{},{})", requestor, middleMan, uid);
                  return;
                }
                if (writeBuffer.hasRemaining()) {
                  socket.register(false, true, this);
                  return;
                }
              }
              if (!writeBuffer.hasRemaining()) {
                // read for the response
                if (readBuffer.hasRemaining()) {
                  long bytesRead = socket.read(readBuffer);
                  if (bytesRead < 0) {
                    logger.warn("Channel closed in openAcceptSocket({},{},{})", requestor, middleMan, uid);
                    return;
                  }
                  if (readBuffer.hasRemaining()) {
                    socket.register(true, false, this);
                    return;
                  }
                }

                // interpret the response
                readBuffer.flip();
                byte response = readBuffer.get();
                switch(response) {
                case CONNECTION_RESPONSE_SUCCESS:
                  logger.trace("success in openAcceptSocket({},{},{})", requestor, middleMan, uid);
                  layer.callback.incomingSocket(socket);
                  return;
                default:
                  logger.warn("Failed to connect in openAcceptSocket({},{},{})", requestor, middleMan, uid);
                  return;
                }
              }
            }

            @Override
            public void receiveException(P2PSocket<Identifier> s,
                Exception ex) {
              logger.warn("Failure opening socket in openAcceptSocket({},{},{})", requestor, middleMan, uid, ex);
            }
          }.receiveSelectResult(openedSocket, false, true);
        } catch (IOException ioe) {
          logger.warn("Exception in openAcceptSocket({},{},{})", requestor, middleMan, uid, ioe);
        }
      }

      @Override
      public void receiveException(SocketRequestHandle<Identifier> request,
          Exception ex) {
        if (ex instanceof BindException) {
          // Address already in use: no further information
          openAcceptSocket(requestor, middleMan, uid);
          return;
        }
        logger.warn("Failure opening socket in openAcceptSocket({},{},{})", requestor, middleMan, uid, ex);
      }
    }, OptionsFactory.addOption(null, RENDEZVOUS_CONTACT_STRING, requestor));

  }
}
