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
package org.mpisws.p2p.transport.liveness;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

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
import org.mpisws.p2p.transport.util.ByteBuffers;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.environment.time.TimeSource;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.selector.Timer;
import rice.selector.TimerTask;

import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_ALIVE;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD_FOREVER;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_SUSPECTED;

public class LivenessTransportLayer<Identifier>
    implements TransportLayerCallback<Identifier, ByteBuffer>, OverrideLiveness<Identifier>, TransportLayer<Identifier, ByteBuffer>, LivenessProvider<Identifier>, Pinger<Identifier> {
  /** Initial size of the buffer a ping or pong is serialized into. */
  private static final int PING_BUFFER_SIZE = 1024;
  /** How long an idle identifier's liveness state is kept after its last use. */
  static final int MANAGER_IDLE_MILLIS = 300000;
  /** How many of the least recently used trackers are looked at for one that may be forgotten. */
  private static final int MAX_TRACKERS_SCANNED = 64;
  /** An idle socket's liveness is first checked after this many RTOs, then every IDLE_CHECK_PERIOD_MILLIS. */
  static final int IDLE_CHECK_RTO_MULTIPLIER = 4;
  static final int IDLE_CHECK_PERIOD_MILLIS = 30000;

  // how long to wait for a ping response to come back before declaring lost
  public final int PING_DELAY;

  // factor of jitter to adjust to the ping waits - we may wait up to this time before giving up
  public final float PING_JITTER;

  // how many tries to ping before giving up
  public final int NUM_PING_TRIES;

  // the minimum amount of time between check dead checks on dead routes
  final long CHECK_DEAD_THROTTLE;

  final int DEFAULT_RTO;

  /**
   * RTO helper see RFC 1122 for a detailed description of RTO calculation
   */
  int RTO_UBOUND;// = 10000; // 10 seconds
  /**
   * RTO helper see RFC 1122 for a detailed description of RTO calculation
   */
  int RTO_LBOUND;

  /**
   * RTO helper see RFC 1122 for a detailed description of RTO calculation
   */
  double gainH;

  /**
   * RTO helper see RFC 1122 for a detailed description of RTO calculation
   */
  double gainG;


  private final TransportLayer<Identifier, ByteBuffer> tl;
  final Logger logger;
  final Environment environment;
  final TimeSource time;
  protected Timer timer;
  private final RandomSource random;

  /**
   * Pass the msg to the callback if it is NORMAL
   */
  public static final byte HDR_NORMAL = 0;
  public static final byte HDR_PING = 1;
  public static final byte HDR_PONG = 2;

  /**
   * The liveness tracker of each route, least recently used first. A remote sender chooses the routes its
   * datagrams arrive on, so it holds at most {@link #maxTrackers}; guarded by itself.
   */
  final LinkedHashMap<Identifier, LivenessTracker<Identifier>> managers;

  /** the most trackers kept in {@link #managers} */
  final int maxTrackers;

  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  private TransportLayerCallback<Identifier, ByteBuffer> callback;

  ErrorHandler<Identifier> errorHandler;

  public LivenessTransportLayer(TransportLayer<Identifier, ByteBuffer> tl, Environment env, ErrorHandler<Identifier> errorHandler, int checkDeadThrottle) {
    this(tl, env, errorHandler, LivenessConfig.fromParameters(env.getParameters()).withCheckDeadThrottle(checkDeadThrottle));
  }

  public LivenessTransportLayer(TransportLayer<Identifier, ByteBuffer> tl, Environment env, ErrorHandler<Identifier> errorHandler, LivenessConfig config) {
    this.tl = tl;
    this.environment = env;
    this.logger = env.getLogManager().getLogger(LivenessTransportLayer.class, null);
    this.time = env.getTimeSource();
    this.timer = env.getSelectorManager().getTimer();
    random = env.getRandomSource();
    this.livenessListeners = new CopyOnWriteArrayList<LivenessListener<Identifier>>();
    this.pingListeners = new CopyOnWriteArrayList<PingListener<Identifier>>();
    this.managers = new LinkedHashMap<>(16, 0.75f, true);
    this.maxTrackers = config.maxTrackers();
    PING_DELAY = config.pingDelay();
    PING_JITTER = config.pingJitter();
    NUM_PING_TRIES = config.numPingTries();
    CHECK_DEAD_THROTTLE = config.checkDeadThrottle();
    DEFAULT_RTO = config.defaultRto();
    RTO_UBOUND = config.rtoUpperBound();
    RTO_LBOUND = config.rtoLowerBound();
    gainH = config.gainH();
    gainG = config.gainG();

    tl.setCallback(this);
    this.errorHandler = errorHandler;
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    }
  }

  @Override
  public void clearState(Identifier i) {
    logger.debug("clearState({})", i);
    removeTracker(i);
  }

  @Override
  public boolean checkLiveness(Identifier i, @Nullable Map<String, Object> options) {
    return getManager(i).checkLiveness(options);
  }

  public P2PSocket<Identifier> wrapSocketWithLivenessCheck(P2PSocket<Identifier> s, LivenessTracker<Identifier> manager) {
    LivenessCheckingSocket<Identifier> sock = new LivenessCheckingSocket<>(this, manager, s, manager.identifier.get());
    synchronized(manager.sockets) {
      manager.sockets.add(sock);
    }
    return sock;
  }

  public LivenessTracker<Identifier> getManager(Identifier i) {
    synchronized(managers) {
      long now = time.currentTimeMillis();
      LivenessTracker<Identifier> manager = managers.get(i);
      if (manager == null) {
        manager = new LivenessTracker<>(this, i);
        managers.put(i,manager);
        forgetIdleTrackers(now);
      }
      manager.lastUsed = now;
      return manager;
    }
  }

  /**
   * Forgets trackers, least recently used first: those idle for {@link #MANAGER_IDLE_MILLIS}, and as many as it
   * takes to bring the table down to {@link #maxTrackers}. A tracker in use (checking its route, carrying sockets,
   * waiting for a pong) is skipped; if none of the {@link #MAX_TRACKERS_SCANNED} least recently used ones is idle,
   * the least recently used one goes anyway, so the table stays bounded. Called with {@link #managers} locked.
   */
  private void forgetIdleTrackers(long now) {
    Iterator<LivenessTracker<Identifier>> it = managers.values().iterator();
    int scanned = 0;
    while (it.hasNext() && scanned < MAX_TRACKERS_SCANNED) {
      LivenessTracker<Identifier> manager = it.next();
      scanned++;
      boolean full = managers.size() > maxTrackers;
      if (!full && now - manager.lastUsed < MANAGER_IDLE_MILLIS) {
        return; // the rest were used more recently
      }
      if (!manager.inUse(now)) {
        it.remove();
      }
    }
    if (managers.size() > maxTrackers) {
      Identifier eldest = managers.keySet().iterator().next();
      logger.warn("{} liveness trackers in use; forgetting the least recently used one, {}", managers.size(), eldest);
      removeTracker(eldest);
    }
  }

  public @Nullable LivenessTracker<Identifier> removeTracker(Identifier i) {
    synchronized(managers) {
      LivenessTracker<Identifier> manager = managers.remove(i);
      if (manager != null) {
        if (manager.getPending() != null) manager.getPending().cancel();
      }
      return manager;
    }
  }

  @Override
  public int getLiveness(Identifier i, @Nullable Map<String, Object> options) {
    logger.trace("getLiveness({},{})", i, options);
    synchronized(managers) {
      LivenessTracker<Identifier> manager = managers.get(i);
      if (manager != null) {
        manager.lastUsed = time.currentTimeMillis();
        return manager.liveness;
      }
    }
    return LIVENESS_SUSPECTED;
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

  /**
   * TODO: make this a number of failures?  Say 3?  Use 1 if using SourceRoutes
   */
  boolean connectionExceptionMeansFaulty = true;

  @Override
  public @Nullable SocketRequestHandle<Identifier> openSocket(final Identifier i, final SocketCallback<Identifier> deliverSocketToMe, final @Nullable Map<String, Object> options) {
    // this code marks the Identifier faulty if there is an error connecting the socket.  It's possible that this
    // should be moved to the source route manager, but there needs to be a way to cancel the liveness
    // checks, or maybe the higher layer can ignore them.
    logger.info("openSocket({},{},{})", i, deliverSocketToMe, options);
    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");
    return tl.openSocket(i, new SocketCallback<Identifier>(){
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> sock) {
        deliverSocketToMe.receiveResult(cancellable, wrapSocketWithLivenessCheck(sock,getManager(i)));
      }

      @Override
      public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
        // the upper layer is probably going to retry, so mark this dead first
        if (ex instanceof java.nio.channels.ClosedChannelException) {
          // don't mark dead
        } else {
          logger.trace("Marking {} dead due to exception opening socket.", s, ex);
          getManager(i).markDead(options);
        }
        deliverSocketToMe.receiveException(s, ex);
      }
    }, options);
  }

  @Override
  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(
      final Identifier i,
      final ByteBuffer m,
      final @Nullable MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
      @Nullable Map<String, Object> options) {
    final MessageRequestHandleImpl<Identifier, ByteBuffer> handle =
      new MessageRequestHandleImpl<Identifier, ByteBuffer>(i, m, options);

    LivenessTracker<Identifier> mgr = getManager(i);
    if ((mgr != null) && (mgr.liveness >= LIVENESS_DEAD)) {
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(handle, new NodeIsFaultyException(i, m));
      return handle;
    }

    byte[] msgBytes = new byte[m.remaining()+1];
    msgBytes[0] = HDR_NORMAL;
    m.get(msgBytes, 1, m.remaining());
    final ByteBuffer myMsg = ByteBuffer.wrap(msgBytes);

    handle.setSubCancellable(tl.sendMessage(i, myMsg, new MessageCallback<Identifier, ByteBuffer>() {
      @Override
      @SuppressWarnings("ReferenceEquality") // the same instance is meant
      public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
        if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != handle.getSubCancelable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
        if (deliverAckToMe != null) deliverAckToMe.ack(handle);
      }
      @Override
      @SuppressWarnings("ReferenceEquality") // the same instance is meant
      public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception ex) {
        if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != handle.getSubCancelable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
        if (deliverAckToMe == null) {
          errorHandler.receivedException(i, ex);
        } else {
          deliverAckToMe.sendFailed(handle, ex);
        }
      }
    }, options));
    return handle;
  }

  @Override
  public void messageReceived(Identifier i, ByteBuffer m, @Nullable Map<String, Object> options) throws IOException {
    assert environment.getSelectorManager().isSelectorThread() : "transport callbacks run on the selector thread";
    if (!m.hasRemaining()) {
      throw new IOException("Empty liveness message from " + i);
    }
    byte hdr = m.get();
    switch(hdr) {
    case HDR_NORMAL:
      logger.trace("messageReceived({},{})", i, m);
      callback.messageReceived(i, m, options);
      return;
    case HDR_PING:
      logger.trace("messageReceived({}, PING)", i);
      pong(i, readTimeStamp(i, m), options);      // it's important to the rendezvous layer to reuse the options
      notifyPingListenersPing(i);
      return;
    case HDR_PONG:
      logger.trace("messageReceived({}, PONG)", i);
      long sendTime = readTimeStamp(i, m);
      // only the pong to a ping this node sent on this route counts: anyone can send a pong
      LivenessTracker<Identifier> manager;
      synchronized(managers) {
        manager = managers.get(i);
      }
      if (manager == null || !manager.pongReceived(sendTime)) {
        logger.debug("messageReceived({}, PONG {}): no such ping outstanding, ignored", i, sendTime);
        return;
      }
      int rtt = (int)(time.currentTimeMillis()-sendTime);
      if (rtt >= 0) {
        manager.updateRTO(rtt);
        synchronized(manager) {
          if (manager.getPending() != null) {
            manager.getPending().pingResponse(sendTime, options);
          }
        }
        manager.markAlive(options); // do this outside of the synchronized block
        notifyPingListenersPong(i,rtt, options);
      } else {
        logger.warn("I think the clock is fishy, rtt must be >= 0, was:{}", rtt);
        errorHandler.receivedUnexpectedData(i, ByteBuffers.arrayForRead(m), ByteBuffers.arrayStart(m), null);
      }
      return;
    default:
      errorHandler.receivedUnexpectedData(i, ByteBuffers.arrayForRead(m), ByteBuffers.arrayStart(m), null);
    }
  }

  /** Reads the time stamp of a ping or pong, which a peer may have cut short. */
  private static long readTimeStamp(Object from, ByteBuffer m) throws IOException {
    if (m.remaining() < Long.BYTES) {
      throw new IOException("Ping or pong from " + from + " has " + m.remaining() + " bytes, not a time stamp");
    }
    return m.getLong();
  }

  @Override
  public String toString() {
    return "LivenessTL{"+getLocalIdentifier()+"}";
  }

  /**
   * Send the ping.
   */
  @Override
  public boolean ping(final Identifier i, final @Nullable Map<String, Object> options) {
    logger.trace("ping({})", i);
    if (i.equals(tl.getLocalIdentifier())) return false;
    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer(PING_BUFFER_SIZE);
      sob.writeByte(HDR_PING);
      final long now = time.currentTimeMillis();
      new PingMessage(now).serialize(sob);
      getManager(i).pingSent(now);
      tl.sendMessage(i, ByteBuffer.wrap(sob.getBytes()), new MessageCallback<Identifier, ByteBuffer>(){
        @Override
        public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
        }
        @Override
        public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception reason) {
          if (logger.isTraceEnabled()) {
            logger.trace("ping("+i+","+now+","+options+") failed", reason);
          } else {
            logger.atDebug().log(() -> "ping("+i+","+now+","+options+") failed "+reason);
          }
        }
      }, options);
      return true;
    } catch (IOException ioe) {
      //Should not happen.  There must be a bug in our serialization code.
      errorHandler.receivedException(i, ioe);
    }
    return false;
  }

  /**
   * Send the pong();
   */
  public void pong(final Identifier i, final long senderTime, final @Nullable Map<String, Object> options) {
    logger.trace("pong({},{})", i, senderTime);
    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer(PING_BUFFER_SIZE);
      sob.writeByte(HDR_PONG);
      new PongMessage(senderTime).serialize(sob);
      tl.sendMessage(i, ByteBuffer.wrap(sob.getBytes()), new MessageCallback<Identifier, ByteBuffer>(){
        @Override
        public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
        }
        @Override
        public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception reason) {
          if (logger.isTraceEnabled()) {
            logger.trace("pong("+i+","+senderTime+","+options+") failed", reason);
          } else {
            logger.atDebug().log(() -> "pong("+i+","+senderTime+","+options+") failed "+reason);
          }
        }
      }, options);
    } catch (IOException ioe) {
      //Should not happen.  There must be a bug in our serialization code.
      errorHandler.receivedException(i, ioe);
    }
  }

  @Override
  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    }
  }

  boolean destroyed = false;
  @Override
  public void destroy() {
    if (destroyed) return;
    logger.info("destroy()");
    destroyed = true;
    tl.destroy();
    livenessListeners.clear();
    pingListeners.clear();
    List<LivenessTracker<Identifier>> all;
    synchronized(managers) {
      all = new ArrayList<>(managers.values());
      managers.clear();
    }
    for (LivenessTracker<Identifier> em : all) {
      em.destroy();
    }
  }

  @Override
  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    LivenessTracker<Identifier> m = getManager(s.getIdentifier());
    if (m.liveness > LIVENESS_SUSPECTED) {
      m.updated = 0L;
      m.checkLiveness(s.getOptions());
    }
    callback.incomingSocket(wrapSocketWithLivenessCheck(s, getManager(s.getIdentifier())));
  }

  final List<LivenessListener<Identifier>> livenessListeners;
  @Override
  public void addLivenessListener(LivenessListener<Identifier> name) {
    if (destroyed) return;
    livenessListeners.add(name);
  }

  @Override
  public boolean removeLivenessListener(LivenessListener<Identifier> name) {
    if (destroyed) return true;
    return livenessListeners.remove(name);
  }

  void notifyLivenessListeners(Identifier i, int liveness, @Nullable Map<String, Object> options) {
    if (destroyed) return;
    logger.trace("notifyLivenessListeners({},{})", i, liveness);
    for (LivenessListener<Identifier> listener : livenessListeners) {
      listener.livenessChanged(i, liveness, options);
    }
  }

  final List<PingListener<Identifier>> pingListeners;
  @Override
  public void addPingListener(PingListener<Identifier> name) {
    if (destroyed) return;
    pingListeners.add(name);
  }

  @Override
  public boolean removePingListener(PingListener<Identifier> name) {
    if (destroyed) return true;
    return pingListeners.remove(name);
  }

  private void notifyPingListenersPing(Identifier i) {
    // the lower layer may still deliver a message (e.g. a queued datagram) after we were destroyed
    if (destroyed) return;
    for (PingListener<Identifier> listener : pingListeners) {
      listener.pingReceived(i, null);
    }
  }

  private void notifyPingListenersPong(Identifier i, int rtt, @Nullable Map<String, Object> options) {
    if (destroyed) return;
    for (PingListener<Identifier> listener : pingListeners) {
      listener.pingResponse(i, rtt, options);
    }
  }

  /**
   * Pings a suspected entity with exponential backoff and marks it dead after numTries unanswered pings.
   *
   * @author jeffh
   */
  protected class DeadChecker extends TimerTask {

    // The number of tries that have occurred so far
    private int tries = 1;

    // the total number of tries before declaring death
    private final int numTries;

    // the path to check
    private final LivenessTracker<Identifier> manager;

    // for debugging
    long startTime; // the start time
    int initialDelay; // the initial expected delay

    @Nullable Map<String, Object> options;

    /**
     * Constructor for DeadChecker.
     *
     * @param manager the entity to check
     * @param numTries the number of pings before the entity is declared dead
     * @param initialDelay the expected round-trip time (the current RTO), for logging
     * @param options the options passed to ping() and to the liveness notifications
     */
    public DeadChecker(LivenessTracker<Identifier> manager, int numTries, int initialDelay, @Nullable Map<String, Object> options) {
      logger.atDebug().log(() -> "DeadChecker@"+System.identityHashCode(this)+" CHECKING DEATH OF PATH " + manager.identifier.get()+" rto:"+initialDelay+" options:"+options);

      this.manager = manager;
      this.numTries = numTries;
      this.options = options;

      this.initialDelay = initialDelay;
      this.startTime = time.currentTimeMillis();
    }

    /**
     * Called when a ping was answered: stops this checker.
     *
     * @param RTT the measured round-trip time
     * @param options the options of the ping
     */
    public void pingResponse(long RTT, @Nullable Map<String, Object> options) {
      if (!cancelled) {
        if (tries > 1) {
          long delay = time.currentTimeMillis()-startTime;
          logger.atDebug().log(() -> "DeadChecker.pingResponse("+manager.identifier.get()+") tries="+tries+
              " estimated="+initialDelay+" totalDelay="+delay);
        }
      }
      logger.atDebug().log(() -> "Terminated DeadChecker@"+System.identityHashCode(this)+"(" + manager.identifier.get() + ") due to ping.");
      cancel();
    }

    /**
     * Main processing method for the DeadChecker object
     *
     * value of tries before run() is called:the time since ping was called:the time since deadchecker was started
     *
     * 1:500:500
     * 2:1000:1500
     * 3:2000:3500
     * 4:4000:7500
     * 5:8000:15500 // ~15 seconds to find 1 path faulty, using source routes gives us 30 seconds to find a node faulty
     */
    @Override
    public void run() {
      if (destroyed) return;
      if (tries < numTries) {
        tries++;
        manager.markSuspected(options);

        Identifier temp = manager.identifier.get();
        if (temp != null) { // could happen during a garbage collection
          logger.atTrace().log(() -> "DeadChecker@"+System.identityHashCode(this)+"(" +temp+") pinging "+tries+" "+manager.getPending());
          ping(temp, options);
        }
        int absPD = (int)(PING_DELAY*Math.pow(2,tries-1));
        int jitterAmt = (int)(((float)absPD)*PING_JITTER);
        int scheduledTime = absPD-jitterAmt+random.nextInt(jitterAmt*2);
        timer.schedule(this,scheduledTime);
      } else {
        logger.atDebug().log(() -> "DeadChecker@"+System.identityHashCode(this)+"(" + manager.identifier.get() + ") expired - marking as dead.");
        manager.markDead(options);
      }
    }

    @Override
    public boolean cancel() {
      synchronized(manager) {
        manager.setPending(null);
      }
      return super.cancel();
    }

    @Override
    public String toString() {
      return "DeadChecker("+manager.identifier.get()+" #"+System.identityHashCode(this)+"):"+tries+"/"+numTries;
    }
  }


  @Override
  public void setLiveness(Identifier i, int liveness, @Nullable Map<String, Object> options) {
    LivenessTracker<Identifier> man = getManager(i);
    switch(liveness) {
    case LIVENESS_ALIVE:
      man.markAlive(options);
      return;
    case LIVENESS_SUSPECTED:
      man.markSuspected(options);
      return;
    case LIVENESS_DEAD:
      man.markDead(options);
      return;
    case LIVENESS_DEAD_FOREVER:
      man.markDeadForever(options);
      return;
    }
  }
}
