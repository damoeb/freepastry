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
package org.mpisws.p2p.transport.limitsockets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import rice.environment.Environment;
import rice.environment.time.TimeSource;
import rice.selector.Timer;
import rice.selector.TimerTask;

import static rice.environment.params.ConfigChecks.requireNonNegative;
import static rice.environment.params.ConfigChecks.requirePositive;

/**
 * Bounds the open sockets in two pools, so that sockets nobody has authenticated cannot push out established peers.
 *
 * <ul>
 *   <li>An incoming socket starts in the <b>unauthenticated pool</b> ({@code pastry_socket_scm_max_unauthenticated_sockets}).
 *       When the pool overflows, its oldest socket is closed: they are all equally untrusted, and a young one may be
 *       honest.
 *   <li>It moves to the <b>established pool</b> ({@code pastry_socket_scm_max_open_sockets}) when a layer above calls
 *       {@link P2PSocket#handshakeCompleted()} (the priority layer after reading its header, which over TLS is after
 *       the TLS handshake). A socket this node opens is established from the start.
 *   <li>When the established pool overflows, the least recently used socket that is neither pinned (see
 *       {@link SocketPinPolicy}: leaf set, routing table) nor waiting to write nor active within the last
 *       {@code pastry_socket_scm_min_idle_millis_before_eviction} is closed. If there is none, the newcomer is
 *       refused, unless it is pinned itself: a node can always reach its own neighbours.
 * </ul>
 *
 * Used on the selector thread.
 *
 * @author Jeff Hoye
 */
public class LimitSocketsTransportLayer<Identifier, MessageType> implements TransportLayer<Identifier, MessageType>, TransportLayerCallback<Identifier, MessageType> {
  /** the parameter that bounds the unauthenticated pool */
  public static final String MAX_UNAUTHENTICATED_SOCKETS_PARAM = "pastry_socket_scm_max_unauthenticated_sockets";
  /** the parameter that sets how long an established socket must be idle before it may be closed for a newcomer */
  public static final String MIN_IDLE_MILLIS_BEFORE_EVICTION_PARAM = "pastry_socket_scm_min_idle_millis_before_eviction";

  /** A socket whose output was shut down is closed after this delay. */
  private static final int CLOSE_AFTER_SHUTDOWN_MILLIS = 3000;

  private final int maxEstablishedSockets;
  private final int maxUnauthenticatedSockets;
  private final long minIdleMillisBeforeEviction;
  private final TransportLayer<Identifier, MessageType> transportLayer;
  /** incoming sockets before their handshake completed, oldest first */
  private final LinkedHashSet<LSSocket> unauthenticated = new LinkedHashSet<>();
  /** established sockets, least recently used first */
  private final LinkedHashMap<LSSocket, LSSocket> established;
  private final Logger logger;
  private final Environment environment;
  private final TimeSource timeSource;
  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  private TransportLayerCallback<Identifier, MessageType> callback;
  private final Timer timer;
  private ErrorHandler<Identifier> handler;
  private SocketPinPolicy<Identifier> pinPolicy = () -> identifier -> false;

  /**
   * Reads the size of the unauthenticated pool and the idle time before eviction from the parameters.
   *
   * @param maxSockets the size of the established pool
   */
  public LimitSocketsTransportLayer(int maxSockets, TransportLayer<Identifier, MessageType> transportLayer, @Nullable ErrorHandler<Identifier> handler, Environment env) {
    this(maxSockets,
        requirePositive(MAX_UNAUTHENTICATED_SOCKETS_PARAM, env.getParameters().getInt(MAX_UNAUTHENTICATED_SOCKETS_PARAM)),
        requireNonNegative(MIN_IDLE_MILLIS_BEFORE_EVICTION_PARAM, env.getParameters().getInt(MIN_IDLE_MILLIS_BEFORE_EVICTION_PARAM)),
        transportLayer, handler, env);
  }

  /**
   * @param maxEstablishedSockets the size of the established pool
   * @param maxUnauthenticatedSockets the size of the unauthenticated pool
   * @param minIdleMillisBeforeEviction an established socket active more recently than this is never closed for a
   *     newcomer
   */
  public LimitSocketsTransportLayer(int maxEstablishedSockets, int maxUnauthenticatedSockets, long minIdleMillisBeforeEviction,
      TransportLayer<Identifier, MessageType> transportLayer, @Nullable ErrorHandler<Identifier> handler, Environment env) {
    this.maxEstablishedSockets = requirePositive("maxEstablishedSockets", maxEstablishedSockets);
    this.maxUnauthenticatedSockets = requirePositive("maxUnauthenticatedSockets", maxUnauthenticatedSockets);
    this.minIdleMillisBeforeEviction = requireNonNegative("minIdleMillisBeforeEviction", minIdleMillisBeforeEviction);
    this.transportLayer = transportLayer;
    this.environment = env;
    this.logger = env.getLogManager().getLogger(LimitSocketsTransportLayer.class, null);
    this.timer = env.getSelectorManager().getTimer();
    this.timeSource = env.getTimeSource();
    this.established = new LinkedHashMap<LSSocket, LSSocket>(16, 0.75f, true);
    this.handler = handler == null ? new DefaultErrorHandler<Identifier>(logger) : handler;

    transportLayer.setCallback(this);
  }

  /** Decides which established sockets are never closed for a newcomer. */
  public void setPinPolicy(SocketPinPolicy<Identifier> pinPolicy) {
    this.pinPolicy = pinPolicy;
  }

  @Override
  public SocketRequestHandle<Identifier> openSocket(final Identifier identifier, final SocketCallback<Identifier> deliverSocketToMe, final @Nullable Map<String, Object> options) {
    if (logger.isTraceEnabled()) logger.trace("{}.openSocket({},{},{})", LimitSocketsTransportLayer.this, identifier, deliverSocketToMe, options, new Exception("Stack Trace"));

    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(identifier, options, logger) {
      @Override
      public boolean cancel() {
        logger.trace("{}.openSocket({},{}):{}.cancel()", this, identifier, deliverSocketToMe, this);
        return super.cancel();
      }

      @Override
      public String toString() {
        return LimitSocketsTransportLayer.this+"RequestHandle.openSocket("+identifier+","+deliverSocketToMe+","+options+")";
      }
    };

    ret.setSubCancellable(transportLayer.openSocket(identifier, new SocketCallback<Identifier>(){
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> socket) {
        logger.trace("{}.openSocket({},{}):{}.receiveResult()", this, identifier, deliverSocketToMe, ret);
        LSSocket wrapped = new LSSocket(socket, true);
        if (admitEstablished(wrapped)) {
          deliverSocketToMe.receiveResult(ret, wrapped);
        } else {
          socket.close();
          deliverSocketToMe.receiveException(ret, new ClosedChannelException(
              "Refused: all " + maxEstablishedSockets + " established sockets are in use. " + wrapped));
        }
      }
      @Override
      public void receiveException(SocketRequestHandle<Identifier> request, Exception ex) {
        logger.atTrace().log(() -> this+".openSocket("+identifier+","+deliverSocketToMe+"):"+ret+".receiveException("+ex+")");
        deliverSocketToMe.receiveException(ret, ex);
      }
      @Override
      public String toString() {
        return LimitSocketsTransportLayer.this+"SocketCallback.openSocket("+identifier+","+deliverSocketToMe+","+options+")";
      }

    }, options));

    return ret;
  }

  @Override
  public void incomingSocket(P2PSocket<Identifier> socket) throws IOException {
    logger.trace("{}.incomingSocket({})", this, socket);
    LSSocket wrapped = new LSSocket(socket, false);
    admitUnauthenticated(wrapped);
    callback.incomingSocket(wrapped);
  }

  /** Adds the socket to the unauthenticated pool, closing the oldest ones beyond its size. */
  private void admitUnauthenticated(LSSocket socket) {
    List<LSSocket> closeMe = new ArrayList<>();
    synchronized (this) {
      unauthenticated.add(socket);
      Iterator<LSSocket> oldestFirst = unauthenticated.iterator();
      while (unauthenticated.size() > maxUnauthenticatedSockets) {
        closeMe.add(oldestFirst.next());
        oldestFirst.remove();
      }
    }
    for (LSSocket oldest : closeMe) {
      environment.getLogRateLimiter().at(logger, Level.INFO, "limitsockets.unauthenticated-full")
          .log("Closing {}: {} sockets are in the handshake, the most there may be", oldest, maxUnauthenticatedSockets);
      oldest.close();
    }
  }

  /**
   * Moves the socket into the established pool. Beyond its size, closes the least recently used socket that may be
   * closed; if there is none, refuses the newcomer unless it is pinned.
   *
   * @return false if the socket was refused (it is in no pool then)
   */
  @SuppressWarnings("ReferenceEquality") // the newcomer itself is meant, not a socket equal to it
  private boolean admitEstablished(LSSocket socket) {
    List<LSSocket> closeMe = new ArrayList<>();
    boolean isAdmitted = true;
    synchronized (this) {
      unauthenticated.remove(socket);
      established.put(socket, socket);
      if (established.size() > maxEstablishedSockets) {
        Predicate<Identifier> isPinned = pinPolicy.pinnedIdentifiers();
        long idleSince = timeSource.currentTimeMillis() - minIdleMillisBeforeEviction;
        Iterator<LSSocket> leastRecentlyUsedFirst = established.keySet().iterator();
        while (established.size() > maxEstablishedSockets && leastRecentlyUsedFirst.hasNext()) {
          LSSocket candidate = leastRecentlyUsedFirst.next();
          if (candidate == socket || !candidate.mayBeClosedForNewcomer(idleSince, isPinned)) continue;
          closeMe.add(candidate);
          leastRecentlyUsedFirst.remove();
        }
        if (established.size() > maxEstablishedSockets && !isPinned.test(socket.getIdentifier())) {
          established.remove(socket);
          isAdmitted = false;
        }
      }
    }
    for (LSSocket victim : closeMe) {
      logger.debug("Closing {} for a newcomer: {} established sockets are open", victim, maxEstablishedSockets);
      victim.forceClose();
    }
    if (!isAdmitted) {
      environment.getLogRateLimiter().at(logger, Level.INFO, "limitsockets.established-full")
          .log("Refusing {}: all {} established sockets are pinned or active", socket, maxEstablishedSockets);
    }
    return isAdmitted;
  }

  class LSSocket extends SocketWrapperSocket<Identifier, Identifier> {
    boolean closed = false;
    boolean forcedClose = false;
    /** true until a layer above calls {@link #handshakeCompleted()}; false from the start for a socket this node opened */
    private boolean isUnauthenticated;
    private long lastActivityMillis;

    LSSocket(P2PSocket<Identifier> socket, boolean isOutgoing) {
      super(socket.getIdentifier(), socket, LimitSocketsTransportLayer.this.logger, handler, socket.getOptions());
      this.isUnauthenticated = !isOutgoing;
      this.lastActivityMillis = timeSource.currentTimeMillis();
    }

    /** Neither pinned, nor waiting to write (a message in flight), nor active since {@code idleSince}. */
    boolean mayBeClosedForNewcomer(long idleSince, Predicate<Identifier> isPinned) {
      return writer == null && lastActivityMillis <= idleSince && !isPinned.test(getIdentifier());
    }

    /**
     * Called when we force a socket closed.
     */
    public void forceClose() {
      logger.debug("{}.forceClose()", this);
      forcedClose = true;
      super.shutdownOutput();
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            close();
          } catch (Exception ignored) {
            // do nothing, it's probably already closed anyway
          }
        }
      }, CLOSE_AFTER_SHUTDOWN_MILLIS);
    }

    /**
     * Moves this socket from the unauthenticated to the established pool, then tells the layers below.
     */
    @Override
    public void handshakeCompleted() {
      super.handshakeCompleted();
      if (closed || !isUnauthenticated) return;
      isUnauthenticated = false;
      touch(this);
      if (!admitEstablished(this)) forceClose();
    }

    /**
     * Called by the higher layer
     */
    @Override
    public void close() {
      logger.trace("{}.close()", this);
      closed = true;
      synchronized (LimitSocketsTransportLayer.this) {
        unauthenticated.remove(this);
        established.remove(this);
      }
      super.close();
    }

    @Override
    public long read(ByteBuffer dsts) throws IOException {
      if (!closed) touch(this);
      try {
        return super.read(dsts);
      } catch (IOException ioe) {
        close();
        throw ioe;
      }
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<Identifier> receiver) {
      if (forcedClose) {
        if (wantToWrite) {
          receiver.receiveException(this, new ClosedChannelException("Limit Sockets forced close. "+this));
        }
        if (wantToRead) {
          super.register(true, false, receiver);
        }
        return;
      }
      if (!closed) touch(this);
      super.register(wantToRead, wantToWrite, receiver);
    }

    @Override
    public long write(ByteBuffer srcs) throws IOException {
      if (forcedClose) throw new ClosedChannelException("Limit Sockets forced close. "+this);
      if (!closed) touch(this);
      try {
        return super.write(srcs);
      } catch (IOException ioe) {
        close();
        throw ioe;
      }
    }

    @Override
    public String toString() {
      return LimitSocketsTransportLayer.this.toString()+"$LSSocket<"+identifier+">["+(closed?"closed":isUnauthenticated?"unauthenticated":"established")+"]@"+System.identityHashCode(this)+socket.toString();
    }
  }

  /** Records activity on the socket: it becomes the most recently used of the established pool. */
  public void touch(LSSocket socket) {
    socket.lastActivityMillis = timeSource.currentTimeMillis();
    synchronized (this) {
      // an access-ordered map moves the entry to the end on get()
      established.get(socket);
    }
  }

  /** The number of sockets in the established pool. */
  public synchronized int getEstablishedSocketCount() {
    return established.size();
  }

  /** The number of sockets in the unauthenticated pool. */
  public synchronized int getUnauthenticatedSocketCount() {
    return unauthenticated.size();
  }

  @Override
  public synchronized String toString() {
    return "LimitSocks<"+established.size()+"+"+unauthenticated.size()+">";
  }

  @Override
  public void acceptMessages(boolean enabled) {
    transportLayer.acceptMessages(enabled);
  }

  @Override
  public void acceptSockets(boolean enabled) {
    transportLayer.acceptSockets(enabled);
  }

  @Override
  public Identifier getLocalIdentifier() {
    return transportLayer.getLocalIdentifier();
  }

  @Override
  public @Nullable MessageRequestHandle<Identifier, MessageType> sendMessage(Identifier identifier, MessageType messageType, @Nullable MessageCallback<Identifier, MessageType> deliverAckToMe, @Nullable Map<String, Object> options) {
    return transportLayer.sendMessage(identifier, messageType, deliverAckToMe, options);
  }

  @Override
  public void messageReceived(Identifier identifier, MessageType messageType, @Nullable Map<String, Object> options) throws IOException {
    callback.messageReceived(identifier, messageType, options);
  }

  @Override
  public void setCallback(TransportLayerCallback<Identifier, MessageType> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.handler = handler;
    transportLayer.setErrorHandler(handler);
  }

  @Override
  public void destroy() {
    transportLayer.destroy();
  }


}
