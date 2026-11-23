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


package rice.pastry;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;


/**
 * What a {@link PastryNode} tells about its start: the listeners that are told when the node becomes ready (or not)
 * or fails to join, and the threads that block until it is ready or has failed to join.
 */
final class ReadyAndJoinSignals {
  private volatile boolean joinFailed = false;

  private JoinFailedException joinFailedReason;

  /**
   * Signalled when the node becomes ready or fails to join, see {@link #await}.
   */
  private final ReentrantLock readyLock = new ReentrantLock();
  private final Condition readyOrJoinFailed = readyLock.newCondition();

  /**
   * Registered listeners.  Notified in reverse order of registration.
   */
  private final CopyOnWriteArrayList<PastryNodeListener> pastryNodeListeners = new CopyOnWriteArrayList<PastryNodeListener>();

  void addListener(PastryNodeListener listener) {
    if (listener == null) throw new NullPointerException();
    pastryNodeListeners.addIfAbsent(listener);
  }

  void removeListener(PastryNodeListener listener) {
    pastryNodeListeners.remove(listener);
  }

  /**
   * @param node the node that the listeners are told about
   * @param event Boolean (ready) or JoinFailedException
   */
  void notifyListeners(PastryNode node, Object event) {
    PastryNodeListener[] listeners = pastryNodeListeners.toArray(new PastryNodeListener[0]);
    for (int i = listeners.length-1; i >= 0; i--) {
      if (event instanceof Boolean b) {
        listeners[i].pastryNodeReady(node, b.booleanValue());
      } else if (event instanceof JoinFailedException joinFailedException) {
        listeners[i].pastryNodeJoinFailed(node, joinFailedException);
      }
    }
  }

  void setJoinFailed(JoinFailedException reason) {
    joinFailedReason = reason;
    joinFailed = true;
  }

  boolean joinFailed() {
    return joinFailed;
  }

  JoinFailedException joinFailedReason() {
    return joinFailedReason;
  }

  void signal() {
    readyLock.lock();
    try {
      readyOrJoinFailed.signalAll();
    } finally {
      readyLock.unlock();
    }
  }

  /**
   * Blocks until the node is ready or failed to join, or until the timeout elapses.
   *
   * @param isReady tells whether the node is ready
   * @return true if the node is ready or failed to join, false if the timeout elapsed first
   */
  boolean await(long timeout, TimeUnit unit, BooleanSupplier isReady) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    readyLock.lock();
    try {
      while (!isReady.getAsBoolean() && !joinFailed) {
        if (nanos <= 0) return false;
        nanos = readyOrJoinFailed.awaitNanos(nanos);
      }
      return true;
    } finally {
      readyLock.unlock();
    }
  }
}
