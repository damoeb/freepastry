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
package org.mpisws.p2p.transport.priority;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.slf4j.Logger;
import rice.p2p.util.MathUtils;

/**
 * Sends the messages too big for the primary socket to one destination, in order, over a socket opened
 * for them alone; the socket is closed when the last one is out.
 */
final class BigMessageSender<Identifier> implements SocketCallback<Identifier>, P2PSocketReceiver<Identifier> {
  private final TransportLayer<Identifier, ByteBuffer> tl;
  private final Logger logger;

  /**
   * the owner's senders by destination; a sender removes itself when done
   */
  private final Map<Identifier, BigMessageSender<Identifier>> pendingBigMessages;

  Identifier i;

  /**
   * Put on back, get from front
   */
  @SuppressWarnings("JdkObsolete") // a short FIFO; LinkedList is fine here
  LinkedList<PendingMessage> msgs =
    new LinkedList<PendingMessage>();
  P2PSocket<Identifier> socket;
  ByteBuffer header;

  BigMessageSender(Identifier i, TransportLayer<Identifier, ByteBuffer> tl, Logger logger,
      Map<Identifier, BigMessageSender<Identifier>> pendingBigMessages) {
    this.tl = tl;
    this.logger = logger;
    this.pendingBigMessages = pendingBigMessages;
    this.i = i;
    header = ByteBuffer.wrap(PriorityTransportLayerImpl.BIG_MSG_SOCKET);
  }

  public void start(Map<String,Object> options) {
    logger.trace("Opening BIG message socket to:{}", i);
    tl.openSocket(i, this, options);
  }

  public MessageRequestHandle<Identifier, ByteBuffer> addMessage(
      ByteBuffer m, MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
      Map<String, Object> options) {
    logger.debug("Sending BIG message of size:{} to:{}", m.remaining(), i);
    PendingMessage ret = new PendingMessage(m,deliverAckToMe,options);
    msgs.addLast(ret);
    return ret;
  }

  @Override
  public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
    receiveException(ex);
  }
  @Override
  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
    receiveException(ioe);
  }
  public void receiveException(Exception ex) {
    pendingBigMessages.remove(this);
    for (PendingMessage foo : msgs) {
      foo.sendFailed(ex);
    }
    if (socket != null) {
      P2PSocket<Identifier> temp = socket;
      socket = null;
      temp.close();
    }
  }

  @Override
  public void receiveSelectResult(P2PSocket<Identifier> socket,
      boolean canRead, boolean canWrite) throws IOException {
    if (header.hasRemaining()) {
      if (socket.write(header) < 0) {
        logger.warn("Error writing BIG message header to:{}", socket);
        receiveException(new org.mpisws.p2p.transport.ClosedChannelException("Socket closed before writing BIG header."));
        return;
      }
      if (header.hasRemaining()) {
        // keep trying to write the header
        socket.register(false, true, this);
        return;
      } else {
        logger.trace("Wrote BIG message header to:{}", socket);
      }
    }

    try {
      sendNextMessage();
    } catch (IOException ioe) {
      receiveException(ioe);
    }
  }

  @Override
  public void receiveResult(SocketRequestHandle<Identifier> cancellable,
      P2PSocket<Identifier> sock) {
    socket = sock;
    try {
      logger.info("Opened BIG message socket to:{}", i);
      receiveSelectResult(sock, false, true);
    } catch (IOException ioe) {
      receiveException(ioe);
    }
  }

  void sendNextMessage() throws IOException {
    if (msgs.isEmpty()) {
      socket.close();
      pendingBigMessages.remove(i);
      return;
    }
    msgs.getFirst().send();

  }

  class PendingMessage implements MessageRequestHandle<Identifier, ByteBuffer>, P2PSocketReceiver<Identifier> {
    ByteBuffer msg;
    MessageCallback<Identifier, ByteBuffer> deliverAckToMe;
    Map<String, Object> options;
    ByteBuffer sizeBuffer;

    boolean started = false;

    public PendingMessage(ByteBuffer msg, MessageCallback<Identifier, ByteBuffer> deliverAckToMe,Map<String, Object> options) {
      this.msg = msg;
      this.deliverAckToMe = deliverAckToMe;
      this.options = options;
      this.sizeBuffer = ByteBuffer.wrap(MathUtils.intToByteArray(msg.remaining()));
    }

    public void sendFailed(Exception ex) {
      if (deliverAckToMe != null) {
        deliverAckToMe.sendFailed(this, ex);
      }
    }

    public void send() throws IOException {
      started = true;
      receiveSelectResult(socket, false, true);
    }

    @Override
    public Identifier getIdentifier() {
      return i;
    }

    @Override
    public ByteBuffer getMessage() {
      return msg;
    }

    @Override
    public Map<String, Object> getOptions() {
      return options;
    }

    @Override
    public boolean cancel() {
      if (!started) {
        msgs.remove(this);
        return true;
      }
      return false;
    }

    /**
     * The socket is blown, just fail them all.
     */
    @Override
    public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
      BigMessageSender.this.receiveException(ioe);
    }

    @Override
    @SuppressWarnings("ReferenceEquality") // the same instance is meant
    public void receiveSelectResult(P2PSocket<Identifier> socket,
        boolean canRead, boolean canWrite) throws IOException {
      if (sizeBuffer.hasRemaining()) {
        long ret = socket.write(sizeBuffer);
        if (ret == -1) {
          socket.close();
          receiveException(socket, new org.mpisws.p2p.transport.ClosedChannelException("Remote node closed channel: "+socket));
          return;
        }
        if (sizeBuffer.hasRemaining()) {
          socket.register(false, true, this);
          return;
        }
      }
      if (msg.hasRemaining()) {
        long ret = socket.write(msg);
        if (ret == -1) {
          socket.close();
          receiveException(socket, new org.mpisws.p2p.transport.ClosedChannelException("Remote node closed channel: "+socket));
          return;
        }
        logger.trace("BIG message wrote: {} of {}", ret, msg.capacity());
        if (msg.hasRemaining()) {
          socket.register(false, true, this);
        } else {
          // done
          if (msgs.removeFirst() != this) throw new RuntimeException("Error, removing first was not this!"+this);
          sendNextMessage();
        }
      }
    }
  }
}
