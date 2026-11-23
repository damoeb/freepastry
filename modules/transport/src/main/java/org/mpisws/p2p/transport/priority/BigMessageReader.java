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
import rice.p2p.util.MathUtils;

/**
 * Reads the messages that arrive on a BIG message socket: a 4-byte size, then the message, repeated
 * until the remote node closes the socket.
 *
 * <p>The buffer grows with the bytes that actually arrive (doubling from {@link #INITIAL_CHUNK}) instead
 * of being allocated at the announced size, and every byte it holds is reserved from the node's
 * {@link BigMessageBudget}; a socket that would exceed the budget is closed. The reservation ends when
 * the message is handed to the callback or the socket is closed.
 */
final class BigMessageReader<Identifier> implements P2PSocketReceiver<Identifier> {
  /** the buffer size reserved when a size header arrives, before any of the message has */
  static final int INITIAL_CHUNK = 8 * 1024;

  private final PeerMessageWriterContext<Identifier> ctx;
  byte[] sizeBytes = new byte[Integer.BYTES];
  ByteBuffer sizeBuf = ByteBuffer.wrap(sizeBytes);
  @Nullable ByteBuffer msgBuf = null;
  /** the announced size of the message being read */
  private int announced;
  /** the bytes of {@link #msgBuf} reserved from the budget */
  private long reserved;

  BigMessageReader(PeerMessageWriterContext<Identifier> ctx) {
    this.ctx = ctx;
  }

  private void closeAndRelease(P2PSocket<Identifier> socket) {
    ctx.bigMsgBudget.release(reserved);
    reserved = 0;
    msgBuf = null;
    socket.close();
  }

  @Override
  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
    ctx.errorHandler.get().receivedException(socket.getIdentifier(), ioe);
    closeAndRelease(socket);
  }

  @Override
  public void receiveSelectResult(P2PSocket<Identifier> socket,
      boolean canRead, boolean canWrite) throws IOException {
    if (sizeBuf.hasRemaining()) {
      long ret = socket.read(sizeBuf);
      if (ret == -1) {
        closeAndRelease(socket);
        return;
      }
      if (sizeBuf.hasRemaining()) {
        socket.register(true, false, this);
        return;
      } else {
        int size = MathUtils.byteArrayToInt(sizeBytes);
        if (size <= 0 || size > ctx.maxBigMsgSize) {
          ctx.logRateLimiter.at(ctx.logger, Level.WARN, "priority.big-message-too-big").log("Closing socket, BIG message of size:{} is invalid or too big! (max:{}) from:{}", size, ctx.maxBigMsgSize, socket);
          closeAndRelease(socket);
          return;
        }
        int initial = Math.min(size, INITIAL_CHUNK);
        if (!ctx.bigMsgBudget.tryReserve(initial)) {
          ctx.logRateLimiter.at(ctx.logger, Level.WARN, "priority.big-message-budget").log("Closing socket, the budget for BIG messages in flight is used up, BIG message of size:{} from:{}", size, socket);
          closeAndRelease(socket);
          return;
        }
        reserved = initial;
        announced = size;
        msgBuf = ByteBuffer.allocate(initial);
        ctx.logger.trace("Receiving BIG message of size:{} from:{}", size, socket);
      }
    }

    ByteBuffer msgBuf = Objects.requireNonNull(this.msgBuf, "the size header is read first");
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
        if (!ctx.bigMsgBudget.tryReserve(grown - msgBuf.capacity())) {
          ctx.logRateLimiter.at(ctx.logger, Level.WARN, "priority.big-message-budget").log("Closing socket, the budget for BIG messages in flight is used up, BIG message of size:{} from:{}", announced, socket);
          closeAndRelease(socket);
          return;
        }
        reserved = grown;
        ByteBuffer bigger = ByteBuffer.allocate(grown);
        msgBuf.flip();
        bigger.put(msgBuf);
        msgBuf = bigger;
        this.msgBuf = bigger;
        continue;
      }
      // done with this msg
      ctx.logger.debug("Received BIG message of size:{} from:{}", msgBuf.capacity(), socket);
      msgBuf.flip();
      sizeBuf.clear();
      // the callback owns the message from here on
      ctx.bigMsgBudget.release(reserved);
      reserved = 0;
      this.msgBuf = null;
      ctx.listeners.notifyRead(msgBuf.remaining(), socket.getIdentifier(), socket.getOptions());
      ctx.callback.get().messageReceived(socket.getIdentifier(), msgBuf, socket.getOptions());
      socket.register(true, false, this);
      return;
    }
  }
}
