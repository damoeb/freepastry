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
import java.lang.ref.WeakReference;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.identity.MemoryExpiredException;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.util.BoundedPriorityWriteQueue;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import rice.p2p.util.tuples.Tuple;
import rice.selector.SelectorLoop;
import rice.selector.TimerTask;

import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD;

/**
 * Responsible for writing messages to the socket.
 *
 * Synchronization: all state is changed on the selector thread, except the queue, which must be carefully
 * synchronized, and what {@link #send} touches, because sends may come from any thread: the sequence number
 * (guarded by the queue) and the pending big messages (a concurrent map; creating a sender is guarded by
 * the map itself). {@link #send} has to hand its caller a handle, so it cannot be marshalled onto the
 * selector as a whole; only the socket work it triggers is.
 *
 * If we have something to write that means !queue.isEmpty(), counting the message being written,
 *   we should have a writingSocket, or a pendingSocket
 *
 * We only touch writingSocket if there is an error, or on scheduleToWriteIfNeeded()
 *
 * We only set the message being written as a result of a call from receiveResult(socket, false, true);
 *
 * @author Jeff Hoye
 */
final class PeerMessageWriter<Identifier> implements P2PSocketReceiver<Identifier> {
  /** A stalled write is checked after this many proximities, kept between the bounds below. */
  private static final int LIVENESS_CHECK_PROXIMITY_MULTIPLIER = 4;
  private static final int MIN_LIVENESS_CHECK_DELAY_MILLIS = 5000;
  private static final int MAX_LIVENESS_CHECK_DELAY_MILLIS = 40000;

  private final PeerMessageWriterContext<Identifier> ctx;
  private final SelectorLoop selectorManager;
  private final Logger logger;
  private final TransportLayer<Identifier, ByteBuffer> tl;
  private final LivenessProvider<Identifier> livenessProvider;
  private final ProximityProvider<Identifier> proximityProvider;
  private final int MAX_MSG_SIZE;
  private final int MAX_BIG_MSG_SIZE;
  private final List<PrimarySocketListener<Identifier>> primarySocketListeners;

  // TODO: think about the behavior of this when it wraps around...
  private int seq = Integer.MIN_VALUE; // guarded by queue
  final BoundedPriorityWriteQueue<MessageWrapper<Identifier>> queue; // messages we want to send, and the one being written
  final Collection<P2PSocket<Identifier>> sockets;

  WeakReference<Identifier> identifier;

  @Nullable SocketRequestHandle<Identifier> pendingSocket; // the receipt that we are opening a socket
  @Nullable P2PSocket<Identifier> writingSocket; // don't try to write to multiple sockets, it will confuse things
  @Nullable P2PSocket<Identifier> closeWritingSocket; // could be a boolean, but we store the writingSocket here just for debugging, == writingSocket if should close it after the current write
  boolean registered = false;  // true if registered for writing
  // Invariant: if (queue.inFlight() != null) then (writingSocket != null)

  PeerMessageWriter(Identifier identifier, PeerMessageWriterContext<Identifier> ctx) {
    this.ctx = ctx;
    this.selectorManager = ctx.selectorManager;
    this.logger = ctx.logger;
    this.tl = ctx.tl;
    this.livenessProvider = ctx.livenessProvider;
    this.proximityProvider = ctx.proximityProvider;
    this.MAX_MSG_SIZE = ctx.maxMsgSize;
    this.MAX_BIG_MSG_SIZE = ctx.maxBigMsgSize;
    this.primarySocketListeners = ctx.primarySocketListeners;
    this.identifier = new WeakReference<Identifier>(identifier);
    queue = new BoundedPriorityWriteQueue<MessageWrapper<Identifier>>(ctx.maxQueueSize, MessageWrapper::drop, logger);
    sockets = new HashSet<P2PSocket<Identifier>>();
  }

  @Override
  public String toString() {
    return "EM{"+identifier.get()+"}";
  }

  TransportListeners<Identifier> listeners() {
    return ctx.listeners;
  }

  Logger logger() {
    return logger;
  }

  PeerMessageWriterContext<Identifier> context() {
    return ctx;
  }

  /**
   * Read an error, or socket was closed.
   *
   * The purpose of this method is to let the currently writing message to complete.
   *
   * @return true if we did it now
   */
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public boolean closeMe(P2PSocket<Identifier> socket) {
    if (logger.isTraceEnabled()) logger.atTrace().setCause(new Exception("Stack Trace")).log(() -> "closeMe("+socket+"):"+(socket == writingSocket)+","+queue.inFlight());
    if (socket == writingSocket) {
      if (queue.inFlight() == null) {
        sockets.remove(socket);
        socket.close();
        setWritingSocket(null);
        return true;
      }
      closeWritingSocket = writingSocket;
      return false;
    } else {
      sockets.remove(socket);
      socket.close();
      return true;
    }
  }

  /**
   * Get's the socket, both when we open it, and when a remote node opens it.
   *
   * @param receipt null if a remote node opened the socket
   */
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void primarySocketAvailable(P2PSocket<Identifier> s, @Nullable SocketRequestHandle<Identifier> receipt) {
    // make sure we're on the selector thread so synchronization of writingSocket is simple
    if (!selectorManager.isSelectorThread()) throw new IllegalStateException("Must be called on the selector");

    logger.debug("primarySocketAvailable({},{})", s, receipt);

    // set pendingSocket to null if possible
    synchronized(PeerMessageWriter.this) {
      if (receipt != null) {
        if (receipt == pendingSocket) {
          logger.info("{}.primarySocketAvailable setting pendingSocket to null {}", PeerMessageWriter.this, pendingSocket);
          stopLivenessChecker();
          logger.debug("got socket:{} clearing pendingSocket:{}", s, pendingSocket);
          pendingSocket = null;  // this is the one we requested
        } else {
          // why would this ever be equal? -Jeff 01.11.08
        }
      }
    }

    sockets.add(s);
    scheduleToWriteIfNeeded();

    // also, be able to read incoming messages on every socket
    new SizeReader<Identifier>(this, s);
  }

  public void setWritingSocket(@Nullable P2PSocket<Identifier> s/*, String loc*/) {
    logger.info("{}.setWritingSocket({})", this, s);
    writingSocket = s;
    if (primarySocketListeners.isEmpty()) return;
    if (s == null) {
      for (PrimarySocketListener<Identifier> l : primarySocketListeners) {
        l.notifyPrimarySocketClosed(identifier.get());
      }
    } else {
      for (PrimarySocketListener<Identifier> l : primarySocketListeners) {
        l.notifyPrimarySocketOpened(s.getIdentifier(), s.getOptions());
      }
    }
  }

  /**
   * Must be called on selectorManager.
   *
   * A) finds a writingSocket if possible
   *   opens one if needed
   */
  void scheduleToWriteIfNeeded() {
    if (!selectorManager.isSelectorThread()) throw new IllegalStateException("Must be called on the selector");

    Identifier temp = identifier.get();
    if (temp == null) {
      purge(new MemoryExpiredException("No record of identifier for "+this));
      return;
    }

    // make progress acquiring a writingSocket
    if (writingSocket == null) {
      registered = false;
      if (!sockets.isEmpty()) {
        setWritingSocket(sockets.iterator().next()/*, "scheduleToWriteIfNeeded"*/);
      } else {
        // we need to get a writingSocket
        if (pendingSocket == null) {
          MessageWrapper<Identifier> peek = peek();
          if (peek != null) {
            openPrimarySocketHelper(temp, peek.options);
          }
        }
      }
    }

    // register on the writingSocket if needed
    if (!registered && writingSocket != null) {
      if (haveMessageToSend()) {
        // maybe we should remember if we were registered, and don't reregister, but for now it doesn't hurt
        registered = true;  // may fail in this call and set registered back to false, so make sure to do this before calling register
        logger.trace("{}.scheduleToWriteIfNeeded() registering to write on {}", this, writingSocket);
        writingSocket.register(false, true, this);
      }
    }
  }

  public void openPrimarySocketHelper(final Identifier i, @Nullable Map<String, Object> options) {
    synchronized(this) {
      if (pendingSocket != null || writingSocket != null) return;
      logger.debug("Opening Primary Socket to {}", i);

      final SocketRequestHandleImpl<Identifier> handle = new SocketRequestHandleImpl<Identifier>(i, options, logger) {
        @Override
        public boolean cancel() {
          ctx.entityManagers.apply(i).receiveSocketException(this, new org.mpisws.p2p.transport.ClosedChannelException("Channel cancelled."));
          return super.cancel();
        }
      };

      logger.info("{}.openPrimarySocketHelper() setting pendingSocket to {}", PeerMessageWriter.this, handle);
      pendingSocket = handle;
      startLivenessChecker(i, options);

      handle.setSubCancellable(tl.openSocket(i, new PrimarySocketOpener<Identifier>(i, handle, ctx), options));

    }
  }


  @Nullable TimerTask livenessChecker = null;
  public void startLivenessChecker(final Identifier temp, final @Nullable Map<String, Object> options) {
    if (options == null) {
      throw new IllegalArgumentException("Options is null");
    }
    if (livenessChecker == null) {
      logger.atTrace().log(() -> "startLivenessChecker("+temp+","+options+") pend:"+pendingSocket+" writingS:"+writingSocket+" theQueue:"+queue.size());
      livenessChecker = new TimerTask() {
        @Override
        public void run() {
          synchronized(PeerMessageWriter.this) {
            if (cancelled) return;
            stopLivenessChecker(); // sets livenssChecker back to null
            if (ctx.destroyed.getAsBoolean()) return;
            livenessProvider.checkLiveness(temp, options);

            logger.info("{}.liveness checker setting pendingSocket to null {}", PeerMessageWriter.this, pendingSocket);
            // this checker is cancelled whenever pendingSocket is cleared
            Objects.requireNonNull(pendingSocket, "liveness checker runs without a pending socket").cancel();
            pendingSocket = null;
          }
          scheduleToWriteIfNeeded();  // will restart this livenessChecker, create a new pendingSocket
        }
      };

      int delay = proximityProvider.proximity(temp, options)*LIVENESS_CHECK_PROXIMITY_MULTIPLIER;
      if (delay < MIN_LIVENESS_CHECK_DELAY_MILLIS) delay = MIN_LIVENESS_CHECK_DELAY_MILLIS;
      if (delay > MAX_LIVENESS_CHECK_DELAY_MILLIS) delay = MAX_LIVENESS_CHECK_DELAY_MILLIS;

      selectorManager.schedule(livenessChecker, delay);
    }
  }

  public void stopLivenessChecker() {
    if (livenessChecker == null) return;
    logger.atTrace().log(() -> "stopLivenessChecker("+identifier.get()+") pend:"+pendingSocket+" writingS:"+writingSocket+" theQueue:"+queue.size());

    livenessChecker.cancel();
    livenessChecker = null;
  }

  /**
   * Returns the message being written, or the first in the queue, w/o making it the one being written
   */
  @Nullable MessageWrapper<Identifier> peek() {
    return queue.peek();
  }

  /**
   * True if this manager holds nothing a new one would not have: no queued or half-written message, no socket
   * and no socket being opened, no big message.
   */
  boolean isIdle() {
    if (!queue.isEmpty()) return false;
    synchronized (PeerMessageWriter.this) {
      if (pendingSocket != null) return false;
    }
    return writingSocket == null && sockets.isEmpty() && pendingBigMessages.isEmpty();
  }

  /**
   * True if we have a message to send
   */
  private boolean haveMessageToSend() {
    return !queue.isEmpty();
  }

  /**
   * This is called when the socket has an exception but was already opened.
   */
  @Override
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {

    if (logger.isTraceEnabled()) {
      logger.trace(this+".receiveException("+socket+","+ioe+"):"+queue.inFlight()+" wrS:"+writingSocket, ioe);
    } else logger.atInfo().log(() -> this+".receiveException("+socket+","+ioe+"):"+queue.inFlight()+" wrS:"+writingSocket+" "+ioe);
    registered = false;
    sockets.remove(socket);
    if (ioe instanceof ClosedChannelException) {
      // don't close, will get cleaned up by the reader
    } else {
      socket.close();
    }

    if (socket == writingSocket) {
      clearAndEnqueue(queue.inFlight());
    }
    scheduleToWriteIfNeeded();
  }

  @Override
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
    registered  = false;
    if (canRead || !canWrite) throw new IllegalStateException(this+" Expected only to write. canRead:"+canRead+" canWrite:"+canWrite+" socket:"+socket);
    if (socket != writingSocket) {
      // this is because the close() method calls receiveSelectResult
      logger.warn("receivedSelectResult({}, r:{} w:{}) ws:{}", socket, canRead, canWrite, writingSocket);
      return;
    }

    logger.trace("receivedSelectResult({},{},{}", socket, canRead, canWrite);
    MessageWrapper<Identifier> current = queue.poll();
    while (current != null && current.receiveSelectResult(writingSocket)) {
      current = queue.poll();
    }
    scheduleToWriteIfNeeded();
  }

  /**
   * TODO: The synchronization here may need work.
   *
   * This is called while we are waiting to open the new socket.
   */
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void receiveSocketException(SocketRequestHandleImpl<Identifier> handle, Exception ex) {
    synchronized(PeerMessageWriter.this) {
      if (handle == pendingSocket) {
        logger.info("{}.receiveSocketException({}) setting pendingSocket to null {}", PeerMessageWriter.this, ex, pendingSocket);

        stopLivenessChecker();
        pendingSocket = null;
      }
    }
    scheduleToWriteIfNeeded();
  }

  /**
   * This method is a keeper, but may need some additional functions, and/or error handling.
   */
  public void markDead() {
    purge(new NodeIsFaultyException(identifier.get()));
  }

  public void purge(IOException ioe) {
    logger.debug("{}purge({}):{}", this, ioe, queue.inFlight());
    ArrayList<Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>>> callSendFailed =
      new ArrayList<Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>>>();
    synchronized(queue) {
      // return NodeIsFaultyException to all of the message(s) deliverAckToMe(s)
      MessageWrapper<Identifier> inFlight = queue.inFlight();
      if (inFlight != null) {
        inFlight.reset();
        if (inFlight.deliverAckToMe != null) {
          callSendFailed.add(new Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>>(inFlight.deliverAckToMe, inFlight));
        }
        queue.clearInFlight();
      }
      for (MessageWrapper<Identifier> msg : queue.clear()) {
        if (msg.deliverAckToMe != null) {
          callSendFailed.add(new Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>>(msg.deliverAckToMe, msg));
        }
      }
    }

    for (Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>> t : callSendFailed) {
      t.first().sendFailed(t.second(), ioe);
    }

    synchronized(sockets) {
      for (P2PSocket<Identifier> sock : sockets) {
          sock.close();
      }
      sockets.clear();
    }
    setWritingSocket(null/*, "purge"*/);
    synchronized(PeerMessageWriter.this) {
      logger.info("{}.purge setting pendingSocket to null {}", PeerMessageWriter.this, pendingSocket);
      if (pendingSocket != null) {
        stopLivenessChecker();
        pendingSocket.cancel();
      }

      pendingSocket = null;
    }
  }

  // ******************************* Big messages **************************** //
  final Map<Identifier, BigMessageSender<Identifier>> pendingBigMessages = new ConcurrentHashMap<>();

  /**
   * Read a sizeBuf, then a msgBuff, repeat
   */
  void handleBigMsgSocket(P2PSocket<Identifier> socket) {
    logger.info("handling BIG message socket from:{}", socket);
    try {
      new BigMessageReader<Identifier>(ctx).receiveSelectResult(socket, true, false);
    } catch (IOException ioe) {
      logger.warn("Error reading big message from {}", socket, ioe);
      socket.close();
    }
  }

  // ************************* End handle big messages **********************

  /**
   * Note: We got to get rid of all the calls to poll().
   */
  public MessageRequestHandle<Identifier, ByteBuffer> send(
      Identifier temp,
      ByteBuffer message,
      @Nullable MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
      final @Nullable Map<String, Object> options) {
    logger.trace("{}send({})", this, message);

    // pick the priority
    int priority = PriorityTransportLayer.DEFAULT_PRIORITY;
    if (options != null) {
      if (options.containsKey(PriorityTransportLayer.OPTION_PRIORITY)) {
        priority = ((Integer)options.get(PriorityTransportLayer.OPTION_PRIORITY)).intValue();
      }
    }

    MessageWrapper<Identifier> ret;

    // throw an error if it's too large
    int remaining = message.remaining();
    if (remaining > MAX_MSG_SIZE) {
      // open a special socket for big messages (the new policy, Dec-2008)
      if (remaining > MAX_BIG_MSG_SIZE) {
        ret = new MessageWrapper<Identifier>(this, temp, message, deliverAckToMe, options, priority, 0);
        if (deliverAckToMe != null)
          deliverAckToMe.sendFailed(ret,
            new SocketException("Message too large. msg:"+message+" size:"+remaining+" max:"+Math.max(MAX_MSG_SIZE, MAX_BIG_MSG_SIZE)));

        return ret;
      }

      BigMessageSender<Identifier> pm;
      MessageRequestHandle<Identifier, ByteBuffer> ret2;
      boolean created = false;
      synchronized (pendingBigMessages) {
        pm = pendingBigMessages.get(temp);
        if (pm == null) {
          pm = new BigMessageSender<Identifier>(temp, tl, logger, pendingBigMessages);
          // note, this fixes a timing issue, because the socket is closed if there are no pending big messages
          ret2 = pm.addMessage(message, deliverAckToMe, options);
          pendingBigMessages.put(temp, pm);
          created = true;
        } else {
          ret2 = pm.addMessage(message, deliverAckToMe, options);
        }
      }
      if (created) pm.start(options);
      return ret2;
    }

    // make sure it's alive
    if (livenessProvider.getLiveness(temp, options) >= LIVENESS_DEAD) {
      ret = new MessageWrapper<Identifier>(this, temp, message, deliverAckToMe, options, priority, 0);
      if (deliverAckToMe != null)
        deliverAckToMe.sendFailed(ret, new NodeIsFaultyException(temp, message));
      return ret;
    }

    // enqueue the message
    synchronized (queue) {
      ret = new MessageWrapper<Identifier>(this, temp, message, deliverAckToMe, options, priority, seq++);
      queue.add(ret);
    }
    ctx.listeners.notifyEnqueued(ret.originalSize, temp, options);
    if (selectorManager.isSelectorThread()) {
      scheduleToWriteIfNeeded();
    } else {
      selectorManager.invoke(() -> scheduleToWriteIfNeeded());
    }

    return ret;
  }

  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  boolean complete(MessageWrapper<Identifier> wrapper) {
    logger.debug("{}.complete({})", this, wrapper);
    if (wrapper != queue.inFlight()) throw new IllegalArgumentException("Wrapper:"+wrapper+" messageThatIsBeingWritten:"+queue.inFlight());

    queue.clearInFlight();

    // notify deliverAckToMe
    wrapper.complete();

    // close the socket if we need to
    if (writingSocket != null && closeWritingSocket == writingSocket) {
      writingSocket.close();
      setWritingSocket(null/*, "complete("+wrapper+")"*/);
      closeWritingSocket = null;
      return false;
    }
    return true;
  }

  /**
   * Takes a cancelled message out of the queue, or stops it if it is being written but nothing of it has
   * gone out yet.
   *
   * @return true if the message won't be sent
   */
  boolean cancel(MessageWrapper<Identifier> wrapper) {
    synchronized(queue) {
      if (wrapper.equals(queue.inFlight())) {
        if (wrapper.message.position() == 0) {
          // TODO: can still cancel the message, but have to have special behavior when the socket calls us back
          queue.clearInFlight();
          return true;
        } else {
          return false;
        }
      }
      return queue.remove(wrapper);
    }
  }

  public void clearAndEnqueue(@Nullable MessageWrapper<Identifier> wrapper) {
    if (wrapper != queue.inFlight()) throw new IllegalArgumentException("Wrapper:"+wrapper+" messageThatIsBeingWritten:"+queue.inFlight());
    synchronized(queue) {
      MessageWrapper<Identifier> inFlight = queue.inFlight();
      if (inFlight != null) inFlight.reset();
      queue.clearInFlight();
      if (writingSocket != null) {
        sockets.remove(writingSocket);
        setWritingSocket(null/*, "CaE("+wrapper+")"*/);
      }
      if (wrapper != null) {
        wrapper.reset();
        queue.add(wrapper);
      }
    }
  }
}
