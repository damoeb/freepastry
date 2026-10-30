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
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.slf4j.Logger;

/**
 * A message queued in an {@link EntityManager}: the payload headed with its size, its priority and
 * sequence number (which order the queue), and the receipt handed to the sender.
 */
final class MessageWrapper<Identifier> implements
    Comparable<MessageWrapper<Identifier>>,
    MessageRequestHandle<Identifier, ByteBuffer> {
  private final EntityManager<Identifier> owner;
  private final TransportListeners<Identifier> listeners;
  private final Logger logger;

  int priority;
  int seq;
  Identifier myIdentifier;

  P2PSocket<Identifier> socket; // null if we aren't registered, aka, we aren't pending/writing

  ByteBuffer originalMessage;
  ByteBuffer message;
  MessageCallback<Identifier, ByteBuffer> deliverAckToMe;
  Map<String, Object> options;
  int originalSize;
  boolean cancelled = false; // true when cancel is called
  boolean completed = false; // true when completed is called

  MessageWrapper(
      EntityManager<Identifier> owner,
      Identifier temp,
      ByteBuffer message,
      MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
      Map<String, Object> options, int priority, int seq) {
    this.owner = owner;
    this.listeners = owner.listeners();
    this.logger = owner.logger();
    this.originalSize = message.remaining();

    this.myIdentifier = temp;
    this.originalMessage = message;

    // head the message with the size
    int size = message.remaining();
    this.message = ByteBuffer.allocate(message.remaining()+Integer.BYTES);
    this.message.put((byte)((size >>> 24) & 0xFF));
    this.message.put((byte)((size >>> 16) & 0xFF));
    this.message.put((byte)((size >>>  8) & 0xFF));
    this.message.put((byte)((size >>>  0) & 0xFF));
    this.message.put(message);
    this.message.clear();

    this.deliverAckToMe = deliverAckToMe;
    this.options = options;
    this.priority = priority;
    this.seq = seq;
  }

  public void complete() {
    completed = true;
    if (deliverAckToMe != null) deliverAckToMe.ack(this);
    listeners.notifyWrote(originalSize, myIdentifier, options);
  }

  /**
   * When is this registered?  May be registered too often.
   *
   * @return true if should keep writing
   */
  public boolean receiveSelectResult(P2PSocket<Identifier> socket) throws IOException {
    logger.trace("{}.receiveSelectResult({})", this, socket);
    try {
    if (this.socket != null && this.socket != socket) {
      // this shouldn't happen
      logger.atWarn().log(() -> this+" Socket changed!!! can:"+cancelled+" comp:"+completed+" socket:"+socket+" writingSocket:"+owner.writingSocket+" this.socket:"+this.socket);
      socket.shutdownOutput();

      // do we need to reset?
      return false;
    }

    // in case we don't complete the write, remember where we are writing
    this.socket = socket;

    if (cancelled && message.position() == 0) {
      logger.trace("{}.rsr({}) cancelled", this, socket);
      // cancel
      return true;
    } else {
      long bytesWritten;
      if ((bytesWritten = socket.write(message)) == -1) {
        // socket was closed, need to register new socket
        logger.trace("{}.rsr({}) socket was closed", this, socket);
        owner.clearAndEnqueue(this);
        return false;
      }
      logger.trace("{} wrote {} bytes of {} remaining:{}", this, bytesWritten, message.capacity(), message.remaining());

      if (message.hasRemaining()) {
        logger.trace("{}.rsr({}) has remaining", this, socket);
        return false;
      }
    }

    return owner.complete(this);
    } catch (IOException ioe) {
      // note, clearAndEnqueue() gets called later by the writer when the stack unravels again
      logger.trace("{}.rsr({})", this, socket, ioe);
      throw ioe;
    }
  }

  public void drop() {
    // TODO: make sure we've done everything necessary here to clean this up
    if (deliverAckToMe != null) deliverAckToMe.sendFailed(this, new QueueOverflowException(owner.identifier.get(), originalMessage));
    listeners.notifyDropped(originalSize, myIdentifier, options);
  }

  /**
   * Compares first on priority, second on seq.
   */
  public int compareTo(MessageWrapper<Identifier> that) {
    if (this.priority == that.priority) {
      return this.seq-that.seq;
    }
    return this.priority-that.priority;
  }

  public Identifier getIdentifier() {
    return myIdentifier;
  }

  public ByteBuffer getMessage() {
    return originalMessage;
  }

  public Map<String, Object> getOptions() {
    return options;
  }

  public void reset() {
    message.clear();
    socket = null;
  }

  public boolean cancel() {
    cancelled = true;
    return owner.cancel(this);
  }
  public String toString() {
    return "MessagWrapper{"+message+"}@"+System.identityHashCode(this)+"->"+owner.identifier.get()+" pri:"+priority+" seq:"+seq+" s:"+this.socket;
  }

}
