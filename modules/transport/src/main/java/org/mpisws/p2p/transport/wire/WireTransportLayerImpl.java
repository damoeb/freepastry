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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketCountListener;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.TransportLayerListener;
import org.mpisws.p2p.transport.util.DefaultCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.slf4j.Logger;
import rice.environment.Environment;

public class WireTransportLayerImpl implements WireTransportLayer, SocketOpeningTransportLayer<InetSocketAddress> {
  // state
  private final InetSocketAddress bindAddress;

  // helpers
  private final UDPLayer udp;
  private final TCPLayer tcp;


  // abstract helpers
  private final Logger logger;
  private final Environment environment;

  private TransportLayerCallback<InetSocketAddress, ByteBuffer> callback;
  private ErrorHandler<InetSocketAddress> errorHandler;

  /**
   * true for modelnet, who needs to set the bind address even on outgoing sockets
   */
  private final boolean forceBindAddress;

  private final WireConfig config;


  /**
   *
   * @param bindAddress the address to bind to, if it fails, it will throw an exception
   * @param env will acquire the SelectorManager from the env
   */
  public WireTransportLayerImpl(
      InetSocketAddress bindAddress,
      Environment env,
      ErrorHandler<InetSocketAddress> errorHandler) throws IOException {
    this(bindAddress, env, errorHandler, true);
  }

  public WireTransportLayerImpl(
      InetSocketAddress bindAddress,
      Environment env,
      ErrorHandler<InetSocketAddress> errorHandler, boolean enableServer) throws IOException {
    this(bindAddress,env,errorHandler,enableServer,enableServer);
  }

  public WireTransportLayerImpl(
      InetSocketAddress bindAddress,
      Environment env,
      ErrorHandler<InetSocketAddress> errorHandler, boolean enableTCPServer, boolean enableUDPServer) throws IOException {
    this(bindAddress, env, errorHandler, enableTCPServer, enableUDPServer, WireConfig.fromParameters(env.getParameters()));
  }

  public WireTransportLayerImpl(
      InetSocketAddress bindAddress,
      Environment env,
      ErrorHandler<InetSocketAddress> errorHandler, boolean enableTCPServer, boolean enableUDPServer,
      WireConfig config) throws IOException {
    this.logger = env.getLogManager().getLogger(WireTransportLayer.class, null);
    this.bindAddress = bindAddress;
    this.environment = env;

    this.config = config;
    this.forceBindAddress = config.forceBindAddress();

    this.callback = new DefaultCallback<InetSocketAddress, ByteBuffer>(logger);
    this.errorHandler = errorHandler;

    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<InetSocketAddress>(logger);
    }

    if (enableUDPServer) {
      udp = new UDPLayerImpl(this);
    } else {
      udp = new BogusUDPLayerImpl();
    }
    try {
      tcp = new TCPLayer(this, enableTCPServer);
    } catch (IOException ioe) {
      udp.destroy();
      throw ioe;
    }
  }

  WireConfig getConfig() {
    return config;
  }

  @Override
  public void setCallback(TransportLayerCallback<InetSocketAddress, ByteBuffer> callback) {
    this.callback = callback;
  }

  @Override
  public @Nullable SocketRequestHandle<InetSocketAddress> openSocket(InetSocketAddress destination, SocketCallback<InetSocketAddress> deliverSocketToMe, @Nullable Map<String, Object> options) {
    return tcp.openSocket(destination, deliverSocketToMe, options);
  }

  @Override
  public MessageRequestHandle<InetSocketAddress, ByteBuffer> sendMessage(
      InetSocketAddress destination,
      ByteBuffer m,
      @Nullable MessageCallback<InetSocketAddress, ByteBuffer> deliverAckToMe,
      @Nullable Map<String, Object> options) {
    logger.debug("sendMessage({},{})", destination, m);
    return udp.sendMessage(destination, m, deliverAckToMe, options);
  }

  @Override
  public InetSocketAddress getLocalIdentifier() {
    return bindAddress;
  }

  boolean destroyed = false;
  @Override
  public void destroy() {
    logger.info("destroy()");
    destroyed = true;
    udp.destroy();
    tcp.destroy();
  }

  public boolean isDestroyed() {
    return destroyed;
  }

  @Override
  public void setErrorHandler(ErrorHandler<InetSocketAddress> handler) {
    if (handler == null) {
      this.errorHandler = new DefaultErrorHandler<InetSocketAddress>(logger);
      return;
    }
    this.errorHandler = handler;
  }

  @Override
  public void acceptMessages(boolean b) {
    udp.acceptMessages(b);
  }

  @Override
  public void acceptSockets(boolean b) {
    tcp.acceptSockets(b);
  }

  protected void messageReceived(InetSocketAddress address, ByteBuffer buffer, @Nullable Map<String, Object> options) throws IOException {
    callback.messageReceived(address, buffer, options);
  }

  protected void incomingSocket(P2PSocket<InetSocketAddress> sm) throws IOException {
    broadcastChannelOpened(sm.getIdentifier(), sm.getOptions(), false);
    callback.incomingSocket(sm);
  }

  final Collection<TransportLayerListener<InetSocketAddress>> listeners =
    new ArrayList<TransportLayerListener<InetSocketAddress>>();
  public void addTransportLayerListener(
      TransportLayerListener<InetSocketAddress> listener) {
    synchronized(listeners) {
      listeners.add(listener);
    }
  }

  public void removeTransportLayerListener(
      TransportLayerListener<InetSocketAddress> listener) {
    synchronized(listeners) {
      listeners.remove(listener);
    }
  }


  final Collection<SocketCountListener<InetSocketAddress>> slisteners =
    new ArrayList<SocketCountListener<InetSocketAddress>>();

  @Override
  public void addSocketCountListener(
      SocketCountListener<InetSocketAddress> listener) {
    synchronized(slisteners) {
      slisteners.add(listener);
    }
  }

  @Override
  public void removeSocketCountListener(
      SocketCountListener<InetSocketAddress> listener) {
    synchronized(slisteners) {
      slisteners.remove(listener);
    }
  }

  protected Iterable<SocketCountListener<InetSocketAddress>> getSlisteners() {
    synchronized(slisteners) {
      return new ArrayList<SocketCountListener<InetSocketAddress>>(slisteners);
    }
  }

  public void broadcastChannelOpened(InetSocketAddress addr, @Nullable Map<String, Object> options, boolean outgoing) {
    for (SocketCountListener<InetSocketAddress> listener : getSlisteners())
      listener.socketOpened(addr, options, outgoing);
  }

  public void broadcastChannelClosed(InetSocketAddress addr, @Nullable Map<String, Object> options) {
    for (SocketCountListener<InetSocketAddress> listener : getSlisteners())
      listener.socketClosed(addr, options);
  }

  public Environment getEnvironment() {
    return environment;
  }

  public ErrorHandler<InetSocketAddress> getErrorHandler() {
    return errorHandler;
  }

  public boolean isForceBindAddress() {
    return forceBindAddress;
  }
}
