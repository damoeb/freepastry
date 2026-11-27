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

import org.slf4j.event.Level;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.util.MathUtils;

/**
 * Reads the messages that arrive on a BIG message socket: a 4-byte size, then the message, repeated
 * until the remote node closes the socket.
 *
 * <p>The buffer grows with the bytes that actually arrive (doubling from {@link #INITIAL_CHUNK}) instead
 * of being allocated at the announced size, and every byte it holds is reserved from the node's
 * {@link ReceiveBudget}, within the peer's share (design 3e). When the budget has no room, the reader stops reading the
 * socket and tries again after {@link #BUDGET_RETRY_MILLIS}: TCP slows the sender. Each message has a deadline from
 * its size and the minimum receive rate; a socket that misses it is closed and its reservation released, so a peer
 * that trickles a message cannot hold memory forever. The reservation ends when the message is handed to the callback
 * or the socket is closed. A complete message waits for the peer's message budget like any other (design 3d).
 */
final class BigMessageReader<Identifier> implements P2PSocketReceiver<Identifier> {
  /** the buffer size reserved when a size header arrives, before any of the message has */
  static final int INITIAL_CHUNK = 8 * 1024;
  /** how long a reader waits for room in the receive budget before it tries again */
  static final long BUDGET_RETRY_MILLIS = 100;

  private final PeerMessageWriterContext<Identifier> ctx;
  byte[] sizeBytes = new byte[Integer.BYTES];
  ByteBuffer sizeBuf = ByteBuffer.wrap(sizeBytes);
  @Nullable ByteBuffer msgBuf = null;
  /** the announced size of the message being read */
  private int announced;
  /** the bytes of {@link #msgBuf} reserved from the budget */
  private long reserved;
  /** closes the socket if the message being read is not complete in time */
  private @Nullable Cancellable deadline;
  /** set once the socket is closed: a pending retry or deadline then does nothing */
  private boolean isClosed;

  BigMessageReader(PeerMessageWriterContext<Identifier> ctx) {
    this.ctx = ctx;
  }

  private void closeAndRelease(P2PSocket<Identifier> socket) {
    isClosed = true;
    cancelDeadline();
    ctx.budgets.memory.release(socket.getIdentifier(), reserved);
    reserved = 0;
    msgBuf = null;
    socket.close();
  }

  private void cancelDeadline() {
    Cancellable deadline = this.deadline;
    if (deadline != null) {
      deadline.cancel();
      this.deadline = null;
    }
  }

  @Override
  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
    ctx.errorHandler.get().receivedException(socket.getIdentifier(), ioe);
    closeAndRelease(socket);
  }

  @Override
  public void receiveSelectResult(P2PSocket<Identifier> socket,
      boolean canRead, boolean canWrite) throws IOException {
    if (isClosed) return;
    if (msgBuf == null) {
      if (sizeBuf.hasRemaining()) {
        long ret = socket.read(sizeBuf);
        if (ret == -1) {
          closeAndRelease(socket);
          return;
        }
        if (sizeBuf.hasRemaining()) {
          socket.register(true, false, this);
          return;
        }
        int size = MathUtils.byteArrayToInt(sizeBytes);
        if (size <= 0 || size > ctx.maxBigMsgSize) {
          ctx.logRateLimiter.at(ctx.logger, Level.WARN, "priority.big-message-too-big").log("Closing socket, BIG message of size:{} is invalid or too big! (max:{}) from:{}", size, ctx.maxBigMsgSize, socket);
          closeAndRelease(socket);
          return;
        }
        announced = size;
        startDeadline(socket, size);
        ctx.logger.trace("Receiving BIG message of size:{} from:{}", size, socket);
      }
      // the size is known, the first chunk is not reserved yet
      int initial = Math.min(announced, INITIAL_CHUNK);
      if (!ctx.budgets.memory.tryReserve(socket.getIdentifier(), initial)) {
        waitForBudget(socket);
        return;
      }
      reserved = initial;
      msgBuf = ByteBuffer.allocate(initial);
    }

    ByteBuffer msgBuf = Objects.requireNonNull(this.msgBuf, "the first chunk is reserved first");
    while (true) {
      long ret = socket.read(msgBuf);
      if (ret == -1) {
        closeAndRelease(socket);
        return;
      }
      if (msgBuf.hasRemaining()) {
        socket.register(true, false, this);
        return;
      }
      if (msgBuf.capacity() < announced) {
        // the buffer is full and more is coming: grow it, within the budget
        int grown = (int) Math.min((long) announced, 2L * msgBuf.capacity());
        if (!ctx.budgets.memory.tryReserve(socket.getIdentifier(), grown - msgBuf.capacity())) {
          waitForBudget(socket);
          return;
        }
        reserved += grown - msgBuf.capacity();
        ByteBuffer bigger = ByteBuffer.allocate(grown);
        msgBuf.flip();
        bigger.put(msgBuf);
        msgBuf = bigger;
        this.msgBuf = bigger;
        continue;
      }
      // done with this msg
      cancelDeadline();
      ctx.logger.debug("Received BIG message of size:{} from:{}", msgBuf.capacity(), socket);
      msgBuf.flip();
      deliverWhenAdmitted(socket, msgBuf);
      return;
    }
  }

  private void startDeadline(P2PSocket<Identifier> socket, int size) {
    long deadlineMillis = ctx.budgets.receiveDeadlineMillis(size);
    if (deadlineMillis == PeerBudgets.NO_DEADLINE) return;
    deadline = ctx.budgets.scheduler.after(deadlineMillis, () -> {
      if (isClosed || deadline == null) return;
      deadline = null;
      ctx.logRateLimiter.at(ctx.logger, Level.WARN, "priority.big-message-deadline").log("Closing socket, BIG message of size:{} did not arrive within {} ms from:{}", size, deadlineMillis, socket);
      closeAndRelease(socket);
    });
  }

  /** Stops reading until the receive budget may have room: what is reserved stays, the deadline keeps running. */
  private void waitForBudget(P2PSocket<Identifier> socket) {
    if (!ctx.budgets.canWait()) {
      // no scheduler to resume the reader: close, as without per-peer budgets
      ctx.logRateLimiter.at(ctx.logger, Level.WARN, "priority.big-message-budget").log("Closing socket, the budget for BIG messages in flight is used up, BIG message of size:{} from:{}", announced, socket);
      closeAndRelease(socket);
      return;
    }
    ctx.logRateLimiter.at(ctx.logger, Level.INFO, "priority.big-message-budget").log("Pausing a BIG message of size:{} from:{}, the receive budget has no room for it now", announced, socket);
    ctx.budgets.scheduler.after(BUDGET_RETRY_MILLIS, () -> resume(socket));
  }

  private void resume(P2PSocket<Identifier> socket) {
    try {
      receiveSelectResult(socket, true, false);
    } catch (IOException ioe) {
      receiveException(socket, ioe);
    }
  }

  /** Hands the complete message to the callback once the peer's message budget admits it. */
  private void deliverWhenAdmitted(P2PSocket<Identifier> socket, ByteBuffer message) {
    if (isClosed || ctx.destroyed.getAsBoolean()) return;
    long wait = ctx.budgets.messages.admit(socket.getIdentifier());
    if (wait != PeerReadBudget.DELIVER_NOW) {
      ctx.budgets.messages.resumeLater(wait, () -> deliverWhenAdmitted(socket, message));
      return;
    }
    sizeBuf.clear();
    // the callback owns the message from here on
    ctx.budgets.memory.release(socket.getIdentifier(), reserved);
    reserved = 0;
    this.msgBuf = null;
    try {
      ctx.listeners.notifyRead(message.remaining(), socket.getIdentifier(), socket.getOptions());
      ctx.callback.get().messageReceived(socket.getIdentifier(), message, socket.getOptions());
    } catch (IOException ioe) {
      receiveException(socket, ioe);
      return;
    }
    socket.register(true, false, this);
  }
}
