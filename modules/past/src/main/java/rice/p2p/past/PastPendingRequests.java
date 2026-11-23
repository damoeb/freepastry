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


package rice.p2p.past;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import rice.p2p.commonapi.CancellableTask;

/**
 * The requests of a {@link PastImpl} that wait for their response: the future each response completes, the timer that reports
 * the request lost, and the counter that numbers the requests. Requests may be sent from application threads while
 * the selector thread delivers responses and timeouts.
 */
final class PastPendingRequests {
  private final Logger logger;

  // the unique ids used by the messages sent across the wire
  private int id = Integer.MIN_VALUE;

  // the hashtable of outstanding messages
  // concurrent, and like the Hashtables they replace they reject null keys and values
  private final Map<Integer, CompletableFuture<Object>> outstanding = new ConcurrentHashMap<Integer, CompletableFuture<Object>>();

  // the hashtable of outstanding timer tasks
  private final Map<Integer, CancellableTask> timers = new ConcurrentHashMap<Integer, CancellableTask>();

  PastPendingRequests(Logger logger) {
    this.logger = logger;
  }

  /**
   * Returns a new uid for a message
   *
   * @return A new id
   */
  synchronized int nextRequestId() {
    return id++;
  }

  /**
   * Loads the future of a request into the pending table
   *
   * @param uid The id of the message
   * @param timer reports the request lost
   * @param response completed by the response
   */
  void insert(int uid, CancellableTask timer, CompletableFuture<Object> response) {
    logger.trace("Loading request {} into pending table", uid);
    timers.put(Integer.valueOf(uid), timer);
    outstanding.put(Integer.valueOf(uid), response);
  }

  /**
   * Removes and returns the future of a request from the pending table
   *
   * @param uid The id of the message
   * @return The future to complete, or null if no request waits under this id
   */
  CompletableFuture<Object> remove(int uid) {
    logger.trace("Removing and returning request {} from pending table", uid);
    CancellableTask timer = timers.remove(Integer.valueOf(uid));

    if (timer != null)
      timer.cancel();

    // the response arrives untyped from the wire; the requester's dependent stages expect the type of its request
    return outstanding.remove(Integer.valueOf(uid));
  }
}
