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
package org.mpisws.p2p.transport.direct;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.LivenessTypes;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;


public class DirectTransportLayer<Identifier, MessageType> implements TransportLayer<Identifier, MessageType> {
  /** Simulated time until a socket to a dead node fails. */
  private static final int CONNECT_TIMEOUT_MILLIS = 5000;

  private boolean acceptSockets = true;
  private volatile boolean acceptMessages = true;
  private volatile @Nullable ErrorHandler<Identifier> errorHandler;

  private final Identifier localIdentifier;
  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  private TransportLayerCallback<Identifier, MessageType> callback;
  private final GenericNetworkSimulator<Identifier, MessageType> simulator;
  private final LivenessProvider<Identifier> livenessProvider;

  private final Environment environment;
  private final Logger logger;

  /**
   * The caller registers the new layer with its simulator, for example with
   * {@code rice.pastry.direct.NetworkSimulator.registerNode()}, before using it.
   */
  public DirectTransportLayer(Identifier local,
      GenericNetworkSimulator<Identifier, MessageType> simulator,
      LivenessProvider<Identifier> livenessProvider, Environment env) {
    this.localIdentifier = local;
    this.simulator = simulator;
    this.livenessProvider = livenessProvider;

    this.environment = env;
    this.logger = environment.getLogManager().getLogger(DirectTransportLayer.class, null);
  }

  @Override
  public void acceptMessages(boolean b) {
    acceptMessages = b;
  }

  @Override
  public void acceptSockets(boolean b) {
    acceptSockets = b;
  }

  @Override
  public Identifier getLocalIdentifier() {
    return localIdentifier;
  }

  static class CancelAndClose<Identifier, MessageType> implements Cancellable {
    DirectAppSocket<Identifier, MessageType> closeMe;
    Cancellable cancelMe;

    public CancelAndClose(DirectAppSocket<Identifier, MessageType> socket, CancellableTask task) {
      this.closeMe = socket;
      this.cancelMe = task;
    }

    @Override
    public boolean cancel() {
      closeMe.connectorEndpoint.close();
      return cancelMe.cancel();
    }

  }

  @Override
  public SocketRequestHandle<Identifier> openSocket(Identifier i, SocketCallback<Identifier> deliverSocketToMe, @Nullable Map<String, Object> options) {
    SocketRequestHandleImpl<Identifier> handle = new SocketRequestHandleImpl<Identifier>(i,options, logger);


    if (simulator.isAlive(i)) {
      int delay = (int)Math.round(simulator.networkDelay(localIdentifier, i));
      DirectAppSocket<Identifier, MessageType> socket = new DirectAppSocket<Identifier, MessageType>(i, localIdentifier, deliverSocketToMe, simulator, handle, options);
      CancelAndClose<Identifier, MessageType> cancelAndClose = new CancelAndClose<Identifier, MessageType>(socket, simulator.enqueueDelivery(socket.getAcceptorDelivery(),
          delay));
      handle.setSubCancellable(cancelAndClose);
    } else {
      int delay = CONNECT_TIMEOUT_MILLIS;  // TODO: Make this configurable
      handle.setSubCancellable(
          simulator.enqueueDelivery(
              new ConnectorExceptionDelivery<Identifier>(deliverSocketToMe, handle, new SocketTimeoutException()),delay));
    }

    return handle;
  }

  @Override
  public @Nullable MessageRequestHandle<Identifier, MessageType> sendMessage(
      Identifier i, MessageType m,
      @Nullable MessageCallback<Identifier, MessageType> deliverAckToMe,
      @Nullable Map<String, Object> options) {
    if (!simulator.isAlive(localIdentifier)) return null; // just make this stop, the local node is dead, he shouldn't be doing anything
    MessageRequestHandleImpl<Identifier, MessageType> handle = new MessageRequestHandleImpl<Identifier, MessageType>(i, m, options);

    if (livenessProvider.getLiveness(i, null) >= LivenessTypes.LIVENESS_DEAD) {
      logger.debug("Attempt to send message {} to a dead node {}!", m, i);

      if (deliverAckToMe != null) deliverAckToMe.sendFailed(handle, new NodeIsFaultyException(i));
    } else {
      if (simulator.isAlive(i)) {
        int delay = (int)Math.round(simulator.networkDelay(localIdentifier, i));
        handle.setSubCancellable(simulator.deliverMessage(m, i, localIdentifier, delay));
        if (deliverAckToMe != null) deliverAckToMe.ack(handle);
      } else {
        // drop message because the node is dead
      }
    }
    return handle;
  }

  @Override
  public void setCallback(TransportLayerCallback<Identifier, MessageType> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    errorHandler = handler;
  }

  @Override
  public void destroy() {
    simulator.remove(getLocalIdentifier());
  }

  public boolean canReceiveSocket() {
    return acceptSockets;
  }

  public void finishReceiveSocket(P2PSocket<Identifier> acceptorEndpoint) {
    try {
      callback.incomingSocket(acceptorEndpoint);
    } catch (IOException ioe) {
      logger.warn("Exception in {}", callback, ioe);
      ErrorHandler<Identifier> handler = errorHandler;
      if (handler != null) {
        handler.receivedException(acceptorEndpoint.getIdentifier(), ioe);
      }
    }
  }

  public Logger getLogger() {
    return logger;
  }

  int seq = Integer.MIN_VALUE;

  public synchronized int allocateNextSequenceNumber() {
    return seq++;
  }

  public void incomingMessage(Identifier i, MessageType m, @Nullable Map<String, Object> options) throws IOException {
    if (!acceptMessages) {
      logger.debug("Dropping message from {}: not accepting messages", i);
      return;
    }
    callback.messageReceived(i, m, options);
  }

  public Environment getEnvironment() {
    return environment;
  }
}
