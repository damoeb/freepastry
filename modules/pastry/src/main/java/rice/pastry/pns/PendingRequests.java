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
package rice.pastry.pns;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import rice.Continuation;
import rice.p2p.commonapi.Cancellable;

/**
 * The continuations waiting for the answer to a request, by request. Each waits with a timeout, which is cancelled
 * when the continuation is answered or removed. Thread safe; answers are delivered while holding the lock.
 *
 * @param <K> identifies a request, such as the node asked
 * @param <V> the answer
 */
final class PendingRequests<K, V> {
  private record Waiter<V>(Continuation<V, Exception> continuation, Cancellable timeout) {}

  private final Map<K, List<Waiter<V>>> waiting = new HashMap<>();

  /** Adds {@code c} to the continuations waiting for {@code key}; {@code timeout} is cancelled when it is answered. */
  synchronized void add(K key, Continuation<V, Exception> c, Cancellable timeout) {
    waiting.computeIfAbsent(key, k -> new ArrayList<>()).add(new Waiter<>(c, timeout));
  }

  /**
   * Stops {@code c} waiting for {@code key}, and cancels its timeout.
   *
   * @return true if {@code c} was waiting
   */
  synchronized boolean remove(K key, Continuation<V, Exception> c) {
    List<Waiter<V>> waiters = waiting.get(key);
    if (waiters == null) return false;
    boolean ret = false;
    Iterator<Waiter<V>> i = waiters.iterator();
    while (i.hasNext()) {
      Waiter<V> w = i.next();
      if (w.continuation().equals(c)) {
        ret = true;
        w.timeout().cancel();
        i.remove();
      }
    }
    if (waiters.isEmpty()) waiting.remove(key);
    return ret;
  }

  /** Answers every continuation waiting for {@code key} with {@code result}, after cancelling its timeout. */
  synchronized void complete(K key, V result) {
    List<Waiter<V>> waiters = waiting.remove(key);
    if (waiters == null) return;
    for (Waiter<V> w : waiters) {
      w.timeout().cancel();
      w.continuation().receiveResult(result);
    }
  }
}
