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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import rice.p2p.util.SortedLinkedList;
import rice.p2p.util.tuples.Tuple;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD;

/**
 * Responsible for writing messages to the socket.
 *
 * Synchronization: all state is changed on the selector thread, except the queue, which must be carefully
 * synchronized.
 *
 * If we have something to write that means !queue.isEmpty() || messageThatIsBeingWritten != null,
 *   we should have a writingSocket, or a pendingSocket
 *
 * We only touch writingSocket if there is an error, or on scheduleToWriteIfNeeded()
 *
 * We only change messageThatIsBeingWritten as a result of a call from receiveResult(socket, false, true);
 *
 * @author Jeff Hoye
 */
final class EntityManager<Identifier> implements P2PSocketReceiver<Identifier> {
  private final EntityManagerContext<Identifier> ctx;
  private final SelectorManager selectorManager;
  private final Logger logger;
  private final TransportLayer<Identifier, ByteBuffer> tl;
  private final LivenessProvider<Identifier> livenessProvider;
  private final ProximityProvider<Identifier> proximityProvider;
  private final int MAX_MSG_SIZE;
  private final int MAX_QUEUE_SIZE;
  private final int MAX_BIG_MSG_SIZE;
  private final List<PrimarySocketListener<Identifier>> primarySocketListeners;

  // TODO: think about the behavior of this when it wraps around...
  int seq = Integer.MIN_VALUE;
  SortedLinkedList<MessageWrapper<Identifier>> queue; // messages we want to send
  Collection<P2PSocket<Identifier>> sockets;

  WeakReference<Identifier> identifier;

  SocketRequestHandle<Identifier> pendingSocket; // the receipt that we are opening a socket
  P2PSocket<Identifier> writingSocket; // don't try to write to multiple sockets, it will confuse things
  P2PSocket<Identifier> closeWritingSocket; // could be a boolean, but we store the writingSocket here just for debugging, == writingSocket if should close it after the current write
  MessageWrapper<Identifier> messageThatIsBeingWritten; // the current message we are sending, if this is null, we aren't in the middle of sending a message
  boolean registered = false;  // true if registered for writing
  // Invariant: if (messageThatIsBeingWritten != null) then (writingSocket != null)

  EntityManager(Identifier identifier, EntityManagerContext<Identifier> ctx) {
    this.ctx = ctx;
    this.selectorManager = ctx.selectorManager;
    this.logger = ctx.logger;
    this.tl = ctx.tl;
    this.livenessProvider = ctx.livenessProvider;
    this.proximityProvider = ctx.proximityProvider;
    this.MAX_MSG_SIZE = ctx.maxMsgSize;
    this.MAX_QUEUE_SIZE = ctx.maxQueueSize;
    this.MAX_BIG_MSG_SIZE = ctx.maxBigMsgSize;
    this.primarySocketListeners = ctx.primarySocketListeners;
    this.identifier = new WeakReference<Identifier>(identifier);
    queue = new SortedLinkedList<MessageWrapper<Identifier>>();
    sockets = new HashSet<P2PSocket<Identifier>>();
  }

  public String toString() {
    return "EM{"+identifier.get()+"}";
  }

  TransportListeners<Identifier> listeners() {
    return ctx.listeners;
  }

  Logger logger() {
    return logger;
  }

  EntityManagerContext<Identifier> context() {
    return ctx;
  }

  /**
   * Read an error, or socket was closed.
   *
   * The purpose of this method is to let the currently writing message to complete.
   *
   * @param socket
   * @return true if we did it now
   */
  public boolean closeMe(P2PSocket<Identifier> socket) {
    if (logger.isTraceEnabled()) logger.atTrace().setCause(new Exception("Stack Trace")).log(() -> "closeMe("+socket+"):"+(socket == writingSocket)+","+messageThatIsBeingWritten);
    if (socket == writingSocket) {
      if (messageThatIsBeingWritten == null) {
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
   * @param s
   * @param receipt null if a remote node opened the socket
   */
  public void primarySocketAvailable(P2PSocket<Identifier> s, SocketRequestHandle<Identifier> receipt) {
    // make sure we're on the selector thread so synchronization of writingSocket is simple
    if (!selectorManager.isSelectorThread()) throw new IllegalStateException("Must be called on the selector");

    logger.debug("primarySocketAvailable({},{})", s, receipt);

    // set pendingSocket to null if possible
    synchronized(EntityManager.this) {
      if (receipt != null) {
        if (receipt == pendingSocket) {
          logger.info("{}.primarySocketAvailable setting pendingSocket to null {}", EntityManager.this, pendingSocket);
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

  public void setWritingSocket(P2PSocket<Identifier> s/*, String loc*/) {
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
  protected void scheduleToWriteIfNeeded() {
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

  public void openPrimarySocketHelper(final Identifier i, Map<String, Object> options) {
    synchronized(this) {
      if (pendingSocket != null || writingSocket != null) return;
      logger.debug("Opening Primary Socket to {}", i);

      final SocketRequestHandleImpl<Identifier> handle = new SocketRequestHandleImpl<Identifier>(i, options, logger) {
        public boolean cancel() {
          ctx.entityManagers.apply(i).receiveSocketException(this, new org.mpisws.p2p.transport.ClosedChannelException("Channel cancelled."));
          return super.cancel();
        }
      };

      logger.info("{}.openPrimarySocketHelper() setting pendingSocket to {}", EntityManager.this, handle);
      pendingSocket = handle;
      startLivenessChecker(i, options);

      handle.setSubCancellable(tl.openSocket(i, new PrimarySocketOpener<Identifier>(i, handle, ctx), options));

    }
  }


  TimerTask livenessChecker = null;
  public void startLivenessChecker(final Identifier temp, final Map<String, Object> options) {
    if (options == null) {
      throw new IllegalArgumentException("Options is null");
    }
    if (livenessChecker == null) {
      logger.atTrace().log(() -> "startLivenessChecker("+temp+","+options+") pend:"+pendingSocket+" writingS:"+writingSocket+" theQueue:"+queue.size());
      livenessChecker = new TimerTask() {
        @Override
        public void run() {
          synchronized(EntityManager.this) {
            if (cancelled) return;
            stopLivenessChecker(); // sets livenssChecker back to null
            if (ctx.destroyed.getAsBoolean()) return;
            livenessProvider.checkLiveness(temp, options);

          // if this throws a NPE, there is a bug, cause this should have been cancelled if pendingSocket == null
            logger.info("{}.liveness checker setting pendingSocket to null {}", EntityManager.this, pendingSocket);
            pendingSocket.cancel();
            pendingSocket = null;
          }
          scheduleToWriteIfNeeded();  // will restart this livenessChecker, create a new pendingSocket
        }
      };

      int delay = proximityProvider.proximity(temp, options)*4;
      if (delay < 5000) delay = 5000; // 1 second
      if (delay > 40000) delay = 40000; // 20 seconds

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
   * Returns the messageThatIsBeingWritten, or the first in the queue, w/o setting messageThatIsBeingWritten
   * @return
   */
  MessageWrapper<Identifier> peek() {
    synchronized(queue) {
      if (messageThatIsBeingWritten == null) {
        return queue.peek();
      }
      return messageThatIsBeingWritten;
    }
  }

  /**
   * Returns the messageThatIsBeingWritten, polls the queue if it is null
   * @return
   */
  private MessageWrapper<Identifier> poll() {
    synchronized(queue) {
      if (messageThatIsBeingWritten == null) {
        messageThatIsBeingWritten = queue.poll();
        logger.atTrace().log(() -> "poll("+identifier.get()+") set messageThatIsBeingWritten = "+messageThatIsBeingWritten);
      }
      if (queue.size() >= (MAX_QUEUE_SIZE-1) && logger.isInfoEnabled()) {
        logger.info(this+"polling from full queue (this is a good thing) "+messageThatIsBeingWritten);
      }
      return messageThatIsBeingWritten;
    }
  }

  /**
   * True if we have a message to send
   * @return
   */
  private boolean haveMessageToSend() {
    if (messageThatIsBeingWritten == null && queue.isEmpty()) return false;
    return true;
  }

  /**
   * This is called when the socket has an exception but was already opened.
   */
  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {

    if (logger.isTraceEnabled()) {
      logger.trace(this+".receiveException("+socket+","+ioe+"):"+messageThatIsBeingWritten+" wrS:"+writingSocket, ioe);
    } else logger.atInfo().log(() -> this+".receiveException("+socket+","+ioe+"):"+messageThatIsBeingWritten+" wrS:"+writingSocket+" "+ioe);
    registered = false;
    sockets.remove(socket);
    if (ioe instanceof ClosedChannelException) {
      // don't close, will get cleaned up by the reader
    } else {
      socket.close();
    }

    if (socket == writingSocket) {
      clearAndEnqueue(messageThatIsBeingWritten);
    }
    scheduleToWriteIfNeeded();
  }

  public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
    registered  = false;
    if (canRead || !canWrite) throw new IllegalStateException(this+" Expected only to write. canRead:"+canRead+" canWrite:"+canWrite+" socket:"+socket);
    if (socket != writingSocket) {
      // this is because the close() method calls receiveSelectResult
      logger.warn("receivedSelectResult({}, r:{} w:{}) ws:{}", socket, canRead, canWrite, writingSocket);
      return;
    }

    logger.trace("receivedSelectResult({},{},{}", socket, canRead, canWrite);
    MessageWrapper<Identifier> current = poll();
    while (current != null && current.receiveSelectResult(writingSocket)) {
      current = poll();
    }
    scheduleToWriteIfNeeded();
  }

  /**
   * TODO: The synchronization here may need work.
   *
   * This is called while we are waiting to open the new socket.
   *
   * @param handle
   * @param ex
   */
  public void receiveSocketException(SocketRequestHandleImpl<Identifier> handle, Exception ex) {
    synchronized(EntityManager.this) {
      if (handle == pendingSocket) {
        logger.info("{}.receiveSocketException({}) setting pendingSocket to null {}", EntityManager.this, ex, pendingSocket);

        stopLivenessChecker();
        pendingSocket = null;
      }
    }
    scheduleToWriteIfNeeded();
  }

  /**
   * Enqueue the message.
   * @param ret
   */
  private void enqueue(MessageWrapper<Identifier> ret) {
    synchronized(queue) {
      queue.add(ret);

      // drop the lowest priority message if the queue is overflowing
      while (queue.size() > MAX_QUEUE_SIZE) {
        MessageWrapper<Identifier> w = queue.removeLast();
        logger.info("Dropping {} because queue is full. MAX_QUEUE_SIZE:{}", w, MAX_QUEUE_SIZE);
        w.drop();
      }
    }
  }

  /**
   * This method is a keeper, but may need some additional functions, and/or error handling.
   *
   */
  public void markDead() {
    purge(new NodeIsFaultyException(identifier.get()));
  }

  public void purge(IOException ioe) {
    logger.debug("{}purge({}):{}", this, ioe, messageThatIsBeingWritten);
    ArrayList<Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>>> callSendFailed =
      new ArrayList<Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>>>();
    synchronized(queue) {
      // return NodeIsFaultyException to all of the message(s) deliverAckToMe(s)
      if (messageThatIsBeingWritten != null) {
        messageThatIsBeingWritten.reset();
        if (messageThatIsBeingWritten.deliverAckToMe != null) {
          callSendFailed.add(new Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>>(messageThatIsBeingWritten.deliverAckToMe, messageThatIsBeingWritten));
        }
        messageThatIsBeingWritten = null;
      }
      for (MessageWrapper<Identifier> msg : queue) {
        if (msg.deliverAckToMe != null) {
          callSendFailed.add(new Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>>(msg.deliverAckToMe, msg));
        }
      }
      queue.clear();
    }

    for (Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper<Identifier>> t : callSendFailed) {
      t.a().sendFailed(t.b(), ioe);
    }

    synchronized(sockets) {
      for (P2PSocket<Identifier> sock : sockets) {
          sock.close();
      }
      sockets.clear();
    }
    setWritingSocket(null/*, "purge"*/);
    synchronized(EntityManager.this) {
      logger.info("{}.purge setting pendingSocket to null {}", EntityManager.this, pendingSocket);
      if (pendingSocket != null) {
        stopLivenessChecker();
        pendingSocket.cancel();
      }

      pendingSocket = null;
    }
  }

  // ******************************* Big messages **************************** //
  Map<Identifier, BigMessageSender<Identifier>> pendingBigMessages = new HashMap<Identifier, BigMessageSender<Identifier>>();

  /**
   * Read a sizeBuf, then a msgBuff, repeat
   *
   * @param socket
   */
  protected void handleBigMsgSocket(P2PSocket<Identifier> socket) {
    logger.info("handling BIG message socket from:{}", socket);
    try {
      new BigMessageReader<Identifier>(ctx).receiveSelectResult(socket, true, false);
    } catch (IOException ioe) {
      logger.warn("Error reading big message from {}", socket, ioe);
    }
  }

  // ************************* End handle big messages **********************

  /**
   * Note: We got to get rid of all the calls to poll().
   *
   * @param message
   * @param deliverAckToMe
   * @param options
   * @return
   */
  public MessageRequestHandle<Identifier, ByteBuffer> send(
      Identifier temp,
      ByteBuffer message,
      MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
      final Map<String, Object> options) {
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

      BigMessageSender<Identifier> pm = pendingBigMessages.get(temp);
      if (pm == null) {
        pm = new BigMessageSender<Identifier>(temp, tl, logger, pendingBigMessages);
        // note, this fixes a timing issue, because the socket is closed if there are no pending big messages
        MessageRequestHandle<Identifier, ByteBuffer> ret2 = pm.addMessage(message,deliverAckToMe,options);
        pendingBigMessages.put(temp, pm);
        pm.start(options);
        return ret2;
      } else {
        return pm.addMessage(message,deliverAckToMe,options);
      }


    }

    // make sure it's alive
    if (livenessProvider.getLiveness(temp, options) >= LIVENESS_DEAD) {
      ret = new MessageWrapper<Identifier>(this, temp, message, deliverAckToMe, options, priority, 0);
      if (deliverAckToMe != null)
        deliverAckToMe.sendFailed(ret, new NodeIsFaultyException(temp, message));
      return ret;
    }

    // enqueue the message
    ret = new MessageWrapper<Identifier>(this, temp, message, deliverAckToMe, options, priority, seq++);
    ctx.listeners.notifyEnqueued(ret.originalSize, temp, options);
    enqueue(ret);
    if (selectorManager.isSelectorThread()) {
      scheduleToWriteIfNeeded();
    } else {
      selectorManager.invoke(() -> scheduleToWriteIfNeeded());
    }

    return ret;
  }

  protected boolean complete(MessageWrapper<Identifier> wrapper) {
    logger.debug("{}.complete({})", this, wrapper);
    if (wrapper != messageThatIsBeingWritten) throw new IllegalArgumentException("Wrapper:"+wrapper+" messageThatIsBeingWritten:"+messageThatIsBeingWritten);

    synchronized(queue) {
      messageThatIsBeingWritten = null;
    }

    // notify deliverAckToMe
    wrapper.complete();

    // close the socket if we need to
    if (closeWritingSocket == writingSocket) {
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
      if (wrapper.equals(messageThatIsBeingWritten)) {
        if (wrapper.message.position() == 0) {
          // TODO: can still cancel the message, but have to have special behavior when the socket calls us back
          messageThatIsBeingWritten = null;
          return true;
        } else {
          return false;
        }
      }
      return queue.remove(wrapper);
    }
  }

  public void clearAndEnqueue(MessageWrapper<Identifier> wrapper) {
    if (wrapper != messageThatIsBeingWritten) throw new IllegalArgumentException("Wrapper:"+wrapper+" messageThatIsBeingWritten:"+messageThatIsBeingWritten);
    synchronized(queue) {
      if (messageThatIsBeingWritten != null) messageThatIsBeingWritten.reset();
      messageThatIsBeingWritten = null;
      if (writingSocket != null) {
        sockets.remove(writingSocket);
        setWritingSocket(null/*, "CaE("+wrapper+")"*/);
      }
      if (wrapper != null) {
        wrapper.reset();
        enqueue(wrapper);
      }
    }
  }
}
