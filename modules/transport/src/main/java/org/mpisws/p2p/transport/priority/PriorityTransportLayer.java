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
import java.util.ArrayList;
import java.util.HashMap;
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
import org.mpisws.p2p.transport.TransportLayerListener;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.LivenessTypes;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.selector.SelectorManager;


/**
 * Does 3 things:
 *  a) Sends messages on a Socket (depending on the options).
 *  b) Prioritizes messages into queues.
 *  c) calls sendFailed if there is a liveness change
 *
 * @author Jeff Hoye
 */
public class PriorityTransportLayer<Identifier>
    implements LivenessListener<Identifier>, TransportLayerCallback<Identifier, ByteBuffer>, TransportLayer<Identifier, ByteBuffer> {
  public static final String OPTION_PRIORITY = "OPTION_PRIORITY";
  public static final byte MEDIUM_PRIORITY = 0;
  public static final byte DEFAULT_PRIORITY = MEDIUM_PRIORITY;
  public static final int STATUS_NOT_CONNECTED = 0;
  public static final int STATUS_CONNECTING = 1;
  public static final int STATUS_CONNECTED = 2;


  TransportLayer<Identifier, ByteBuffer> tl;

  public static final byte PASSTHROUGH_SOCKET_B = 0;
  public static final byte PRIMARY_SOCKET_B = 1;
  public static final byte BIG_MSG_SOCKET_B = 2;
  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] PASSTHROUGH_SOCKET = {PASSTHROUGH_SOCKET_B};
  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] PRIMARY_SOCKET = {PRIMARY_SOCKET_B};
  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] BIG_MSG_SOCKET = {BIG_MSG_SOCKET_B};

  private int MAX_MSG_SIZE = 10000;
  private int MAX_QUEUE_SIZE = 30;

  /**
   * The biggest message size allowed when none is configured, see {@link PriorityConfig#maxBigMsgSize()}.
   */
  public static final int DEFAULT_MAX_BIG_MSG_SIZE = 4 * 1024 * 1024;

  /**
   * BIG messages open a socket especially for big messages.  This is the biggest message size allowed.
   */
  private final int MAX_BIG_MSG_SIZE;

  private final Logger logger;

  private final EntityManagerRegistry<Identifier> entityManagers;

  /**
   * what each EntityManager needs from this layer
   */
  private final EntityManagerContext<Identifier> entityManagerContext;

  /**
   * Never null: until the layer above calls {@link #setCallback}, (or after it passes null) this is
   * a {@link MissingCallback} that logs and drops what arrives instead of throwing a NullPointerException
   * on the selector thread.
   */
  private TransportLayerCallback<Identifier, ByteBuffer> callback = new MissingCallback();

  private ErrorHandler<Identifier> errorHandler;

  private final SelectorManager selectorManager;
  private final Environment environment;

  private final ArrayList<PrimarySocketListener<Identifier>> primarySocketListeners = new ArrayList<PrimarySocketListener<Identifier>>();

  /**
   * Builds the layer with the maximum message size and queue length from {@code config}.
   *
   * @param env the environment
   * @param config the priority parameters
   */
  public PriorityTransportLayer(TransportLayer<Identifier, ByteBuffer> tl,
      LivenessProvider<Identifier> livenessProvider,
      ProximityProvider<Identifier> proximityProvider,
      Environment env,
      PriorityConfig config,
      ErrorHandler<Identifier> handler) {
    this(tl, livenessProvider, proximityProvider, env, config.maxMsgSize(), config.maxQueueLength(), config.maxBigMsgSize(), handler);
  }

  public PriorityTransportLayer(TransportLayer<Identifier, ByteBuffer> tl,
      LivenessProvider<Identifier> livenessProvider,
      ProximityProvider<Identifier> proximityProvider,
      Environment env,
      int maxMsgSize,
      int maxQueueSize,
      ErrorHandler<Identifier> handler) {
    this(tl, livenessProvider, proximityProvider, env, maxMsgSize, maxQueueSize, DEFAULT_MAX_BIG_MSG_SIZE, handler);
  }

  public PriorityTransportLayer(TransportLayer<Identifier, ByteBuffer> tl,
      LivenessProvider<Identifier> livenessProvider,
      ProximityProvider<Identifier> proximityProvider,
      Environment env,
      int maxMsgSize,
      int maxQueueSize,
      int maxBigMsgSize,
      ErrorHandler<Identifier> handler) {
    this.MAX_BIG_MSG_SIZE = maxBigMsgSize;
    entityManagers = new EntityManagerRegistry<Identifier>(this::newEntityManager);
    this.logger = env.getLogManager().getLogger(PriorityTransportLayer.class, null);
    this.selectorManager = env.getSelectorManager();
    this.environment = env;
    this.MAX_MSG_SIZE = maxMsgSize;
    this.MAX_QUEUE_SIZE = maxQueueSize;
    this.tl = tl;
    logger.info("MAX_QUEUE_SIZE:{} MAX_MSG_SIZE:{}", MAX_QUEUE_SIZE, MAX_MSG_SIZE);
    this.entityManagerContext = new EntityManagerContext<Identifier>(tl, livenessProvider, proximityProvider,
        selectorManager, logger, MAX_MSG_SIZE, MAX_QUEUE_SIZE, MAX_BIG_MSG_SIZE, primarySocketListeners, listeners,
        () -> errorHandler, () -> callback, () -> destroyed, this::getEntityManager);
    tl.setCallback(this);
    livenessProvider.addLivenessListener(this);
    this.errorHandler = handler;
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    }
  }

  /**
   * The first byte tells whether this is a passthrough (the layer higher than us asked to open it), a
   * primary (our layer tried to open it) or a BIG message socket; see {@link IncomingSocketDispatcher}.
   */
  @Override
  public void incomingSocket(final P2PSocket<Identifier> s) throws IOException {
    s.register(true, false, new IncomingSocketDispatcher<Identifier>(s, entityManagerContext));
  }

  @Override
  public SocketRequestHandle<Identifier> openSocket(Identifier i, final SocketCallback<Identifier> deliverSocketToMe, @Nullable Map<String, Object> options) {
    if (deliverSocketToMe == null) throw new IllegalArgumentException("No handle to return socket to! (deliverSocketToMe must be non-null!)");

    final SocketRequestHandleImpl<Identifier> handle = new SocketRequestHandleImpl<Identifier>(i, options, logger);
    handle.setSubCancellable(tl.openSocket(i, new PassthroughSocketOpener<Identifier>(handle, deliverSocketToMe), options));

    return handle;
  }

  @Override
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  @Override
  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  @Override
  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  @Override
  public void messageReceived(Identifier i, ByteBuffer m, @Nullable Map<String, Object> options) throws IOException {
    assert selectorManager.isSelectorThread() : "transport callbacks run on the selector thread";
    callback.messageReceived(i, m, options);
    notifyListenersRead(m.remaining(), i, options);
  }

  @Override
  public @Nullable MessageRequestHandle<Identifier, ByteBuffer> sendMessage(final Identifier i, ByteBuffer m, final @Nullable MessageCallback<Identifier, ByteBuffer> deliverAckToMe, @Nullable Map<String, Object> opts) {
    logger.debug("sendMessage({},{},{},{})", i, m, deliverAckToMe, opts);
    // callers may pass null options, but queued messages need non-null options (e.g. EntityManager.startLivenessChecker())
    final @Nullable Map<String, Object> options = (opts == null) ? new HashMap<String, Object>() : opts;

    // if it is to be sent UDP, just pass it through
    if (options != null &&
        options.containsKey(WireTransportLayer.OPTION_TRANSPORT_TYPE)) {
        Integer val = (Integer)options.get(WireTransportLayer.OPTION_TRANSPORT_TYPE);
        if (val != null &&
            val.intValue() == WireTransportLayer.TRANSPORT_TYPE_DATAGRAM) {
          final int originalSize = m.remaining();
          return tl.sendMessage(i, m, new MessageCallback<Identifier, ByteBuffer>() {

            @Override
            public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
              notifyListenersWrote(originalSize, i, options);
              if (deliverAckToMe != null) deliverAckToMe.ack(msg);
            }

            @Override
            public void sendFailed(
                MessageRequestHandle<Identifier, ByteBuffer> msg,
                Exception reason) {
              notifyListenersDropped(originalSize, i, options);
              if (deliverAckToMe != null) deliverAckToMe.sendFailed(msg, reason);
            }

          }, options);
        }
    }

    return getEntityManager(i).send(i, m, deliverAckToMe, options);
  }

  @Override
  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = (callback == null) ? new MissingCallback() : callback;
  }

  /**
   * Stands in for the layer above while no callback is set: messages are logged and dropped,
   * passthrough sockets are logged and closed (so they don't leak).
   */
  class MissingCallback implements TransportLayerCallback<Identifier, ByteBuffer> {
    @Override
    public void messageReceived(Identifier i, ByteBuffer m, @Nullable Map<String, Object> options) throws IOException {
      logger.warn("No TransportLayerCallback set, dropping message of size {} from {}", m.remaining(), i);
    }

    @Override
    public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
      logger.warn("No TransportLayerCallback set, closing incoming socket {}", s);
      s.close();
    }
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  private boolean destroyed = false;
  @Override
  public void destroy() {
    if (destroyed) return;
    if (environment.getSelectorManager().isSelectorThread()) {
      destroyed = true;
      tl.destroy();
    } else {
      environment.getSelectorManager().invoke(() -> destroy());
    }
  }

  private EntityManager<Identifier> newEntityManager(Identifier i) {
    return new EntityManager<Identifier>(i, entityManagerContext);
  }

  protected EntityManager<Identifier> getEntityManager(Identifier i) {
    return entityManagers.get(i, selectorManager.isSelectorThread());
  }

  @Override
  public void livenessChanged(Identifier i, int val, @Nullable Map<String, Object> options) {
    if (val >= LivenessTypes.LIVENESS_DEAD) {
      getEntityManager(i).markDead();
      if (selectorManager.isSelectorThread()) {
        entityManagers.forgetIfIdle(i);
      }
    }
  }

  // *************************** Listeners *********************
  private final TransportListeners<Identifier> listeners = new TransportListeners<Identifier>();

  public void addTransportLayerListener(TransportLayerListener<Identifier> listener) {
    listeners.addTransportLayerListener(listener);
  }
  public void removeTransportLayerListener(TransportLayerListener<Identifier> listener) {
    listeners.removeTransportLayerListener(listener);
  }

  public void addPriorityTransportLayerListener(PriorityTransportLayerListener<Identifier> listener) {
    listeners.addPriorityTransportLayerListener(listener);
  }
  public void removePriorityTransportLayerListener(PriorityTransportLayerListener<Identifier> listener) {
    listeners.removePriorityTransportLayerListener(listener);
  }

  public void notifyListenersRead(int size, Identifier source,
      @Nullable Map<String, Object> options) {
    listeners.notifyRead(size, source, options);
  }

  public void notifyListenersWrote(int size, Identifier dest,
      @Nullable Map<String, Object> options) {
    listeners.notifyWrote(size, dest, options);
  }

  public void notifyListenersDropped(int size, Identifier dest,
      @Nullable Map<String, Object> options) {
    listeners.notifyDropped(size, dest, options);
  }


  /**
   * Returns if there is a primary connection to the identifier
   *
   * @return STATUS_NOT_CONNECTED, STATUS_CONNECTING, STATUS_CONNECTED
   */
  public int connectionStatus(Identifier i) {
    EntityManager<Identifier> manager = getEntityManager(i);
    // this may not be thread safe, but ... who cares, it at least won't throw an exception.
    if (manager.writingSocket != null) return STATUS_CONNECTED;
    if (manager.pendingSocket != null) return STATUS_CONNECTING;
    return STATUS_NOT_CONNECTED;
  }

  /**
   * open a primary connection; PrimarySocketListeners are notified when it is open
   */
  public void openPrimaryConnection(Identifier i, @Nullable Map<String, Object> options) {
    getEntityManager(i).openPrimarySocketHelper(i, options);
  }

  public void addPrimarySocketListener(PrimarySocketListener<Identifier> listener) {
    primarySocketListeners.add(listener);
  }

  public void removePrimarySocketListener(PrimarySocketListener<Identifier> listener) {
    primarySocketListeners.remove(listener);
  }

}
