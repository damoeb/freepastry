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
package org.mpisws.p2p.transport.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import rice.p2p.util.SortedLinkedList;

/**
 * Messages waiting to be written to one socket, in their natural order, and the one being written.
 *
 * The queue holds at most maxSize messages besides the one being written. Adding to a full queue drops the
 * last message in order (the lowest priority, the newest among equals) and hands it to the drop callback.
 *
 * Synchronization: every method locks this queue. A caller that needs several steps to be atomic locks the
 * queue around them; the drop callback runs under the lock.
 */
public final class BoundedPriorityWriteQueue<E extends Comparable<E>> {
  private final SortedLinkedList<E> queue = new SortedLinkedList<E>();
  private final int maxSize;
  private final Consumer<? super E> onDrop;
  private final Logger logger;
  private @Nullable E inFlight;

  public BoundedPriorityWriteQueue(int maxSize, Consumer<? super E> onDrop, Logger logger) {
    this.maxSize = maxSize;
    this.onDrop = onDrop;
    this.logger = logger;
  }

  /**
   * Adds the message, then drops the last messages while the queue is over its size.
   */
  public synchronized void add(E e) {
    queue.add(e);
    while (queue.size() > maxSize) {
      E w = queue.removeLast();
      logger.info("Dropping {} because queue is full. MAX_QUEUE_SIZE:{}", w, maxSize);
      onDrop.accept(w);
    }
  }

  /**
   * The message being written, or else the first one waiting; the queue does not change.
   */
  public synchronized @Nullable E peek() {
    if (inFlight == null) return queue.peek();
    return inFlight;
  }

  /**
   * The message being written; if there is none, the first one waiting becomes it.
   */
  public synchronized @Nullable E poll() {
    if (inFlight == null) {
      inFlight = queue.poll();
      logger.trace("poll() set the message being written = {}", inFlight);
    }
    if (queue.size() >= (maxSize-1) && logger.isInfoEnabled()) {
      logger.info("polling from full queue ({}) (this is a good thing) {}", queue.size(), inFlight);
    }
    return inFlight;
  }

  public synchronized @Nullable E inFlight() {
    return inFlight;
  }

  /**
   * Nothing is being written any more.
   */
  public synchronized void clearInFlight() {
    inFlight = null;
  }

  /**
   * Takes a waiting message out of the queue.
   *
   * @return true if it was waiting
   */
  public synchronized boolean remove(E e) {
    return queue.remove(e);
  }

  /**
   * True if nothing is waiting and nothing is being written.
   */
  public synchronized boolean isEmpty() {
    return inFlight == null && queue.isEmpty();
  }

  /**
   * The number of waiting messages.
   */
  public synchronized int size() {
    return queue.size();
  }

  /**
   * The waiting messages, in order.
   */
  public synchronized List<E> waiting() {
    return new ArrayList<E>(queue);
  }

  /**
   * Empties the queue, leaving the message being written alone.
   *
   * @return the messages that were waiting, in order
   */
  public synchronized List<E> clear() {
    List<E> ret = waiting();
    queue.clear();
    return ret;
  }
}
