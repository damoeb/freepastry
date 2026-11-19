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
package org.mpisws.p2p.transport.wire;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import rice.selector.SelectionKeyHandler;

public class TCPLayer extends SelectionKeyHandler {
  // the size of the buffers for the socket
  public final int SOCKET_BUFFER_SIZE;
  private final boolean TCP_NO_DELAY;

  WireTransportLayer wire;

  // the key to accept from
  private @Nullable SelectionKey key;

  Logger logger;

  public TCPLayer(WireTransportLayer wire, boolean enableServer) throws IOException {
    this.wire = wire;
    this.logger = wire.getEnvironment().getLogManager().getLogger(TCPLayer.class, null);

    WireConfig config = wire.getConfig();
    SOCKET_BUFFER_SIZE = config.socketBufferSize();
    TCP_NO_DELAY = config.tcpNoDelay();
    // bind to port
    if (enableServer) {
      final ServerSocketChannel channel = ServerSocketChannel.open();
      channel.configureBlocking(false);
      channel.socket().setReuseAddress(true);
      channel.socket().bind(wire.getLocalIdentifier());
      logger.info("TCPLayer bound to {}", wire.getLocalIdentifier());

      this.key = wire.getEnvironment().getSelectorManager().register(channel, this, SelectionKey.OP_ACCEPT);
    }
  }

  public @Nullable SocketRequestHandle<InetSocketAddress> openSocket(
      InetSocketAddress destination,
      SocketCallback<InetSocketAddress> deliverSocketToMe,
      @Nullable Map<String, Object> options) {
    if (isDestroyed()) return null;
    if (logger.isTraceEnabled()) {
      logger.trace("openSocket("+destination+")", new Exception("Stack Trace"));
    } else {
      logger.debug("openSocket({})", destination);
    }
    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");
    try {
      wire.broadcastChannelOpened(destination, options, true);

      synchronized (sockets) {
        WireSocket wireSocket = new WireSocket(this, destination, deliverSocketToMe, options);
        sockets.add(wireSocket);
        return wireSocket;
      }
    } catch (IOException e) {
      logger.warn("GOT ERROR {} OPENING PATH - MARKING PATH {} AS DEAD!", e, destination, e);
      SocketRequestHandle<InetSocketAddress> can = new SocketRequestHandleImpl<InetSocketAddress>(destination, options, logger);
      deliverSocketToMe.receiveException(can, e);
      return can;
    }
  }

  final Collection<WireSocket> sockets = new HashSet<WireSocket>();

  protected void socketClosed(WireSocket wireSocket) {
    wire.broadcastChannelClosed(wireSocket.addr, wireSocket.options);
    sockets.remove(wireSocket);
  }

  public void destroy() {
    logger.info("destroy()");

    try {
      if (key != null) {
        key.channel().close();
        key.cancel();
        key.attach(null);
      }
    } catch (IOException ioe) {
      wire.getErrorHandler().receivedException(null, ioe);
    }

    // TODO: add a flag to disable this to simulate a silent fault
    for (WireSocket socket : new ArrayList<WireSocket>(sockets)) {
      socket.close();
    }
  }

  public void acceptSockets(final boolean enabled) {
    final SelectionKey serverKey = key;
    if (serverKey == null) return; // no TCP server, nothing to accept from
    Runnable task = () -> {
      if (enabled) {
        serverKey.interestOps(serverKey.interestOps() | SelectionKey.OP_ACCEPT);
      } else {
        serverKey.interestOps(serverKey.interestOps() & ~SelectionKey.OP_ACCEPT);
      }
    };

    // thread safety
    if (wire.getEnvironment().getSelectorManager().isSelectorThread()) {
      task.run();
    } else {
      wire.getEnvironment().getSelectorManager().invoke(task);
    }
  }

  /**
   * Specified by the SelectionKeyHandler interface. Is called whenever a key
   * has become acceptable, representing an incoming connection. This method
   * will accept the connection, and attach a SocketConnector in order to read
   * the greeting off of the channel. Once the greeting has been read, the
   * connector will hand the channel off to the appropriate node handle.
   *
   * @param key The key which is acceptable.
   */
  @Override
  public void accept(SelectionKey key) {
    assert wire.getEnvironment().getSelectorManager().isSelectorThread() : "selection keys are handled on the selector thread";
    try {
      SocketChannel channel = ((ServerSocketChannel) key.channel()).accept();
      if (channel == null) {
        // the connection went away (for instance the client reset it) between select() and accept()
        logger.debug("accept() found no pending connection");
        return;
      }
      WireSocket wireSocket;
      try {
        wireSocket = new WireSocket(this, channel);
      } catch (IOException e) {
        channel.close();
        throw e;
      }
      synchronized (sockets) {
        sockets.add(wireSocket);
      }
      wire.incomingSocket(wireSocket);
    } catch (IOException e) {
      logger.atWarn().log(() -> "ERROR (accepting connection): " + e);
      wire.getErrorHandler().receivedException(null, e);
    }
  }

  public boolean isDestroyed() {
    return wire.isDestroyed();
  }

  public boolean isTcpNoDelay() {
    return TCP_NO_DELAY;
  }
}
