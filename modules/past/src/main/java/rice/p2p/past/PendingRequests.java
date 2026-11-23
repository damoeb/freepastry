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
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import rice.Continuation;
import rice.p2p.commonapi.CancellableTask;

/**
 * The requests of a {@link PastImpl} that wait for their response: the continuation of each, the timer that reports
 * the request lost, and the counter that numbers the requests. Requests may be sent from application threads while
 * the selector thread delivers responses and timeouts.
 */
final class PendingRequests {
  private final Logger logger;

  // the unique ids used by the messages sent across the wire
  private int id = Integer.MIN_VALUE;

  // the hashtable of outstanding messages
  // concurrent, and like the Hashtables they replace they reject null keys and values
  private final Map<Integer, Continuation<?, Exception>> outstanding = new ConcurrentHashMap<Integer, Continuation<?, Exception>>();

  // the hashtable of outstanding timer tasks
  private final Map<Integer, CancellableTask> timers = new ConcurrentHashMap<Integer, CancellableTask>();

  PendingRequests(Logger logger) {
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
   * Loads the provided continuation into the pending table
   *
   * @param uid The id of the message
   * @param command The continuation to run
   */
  void insert(int uid, CancellableTask timer, Continuation<?, Exception> command) {
    logger.trace("Loading continuation {} into pending table", uid);
    timers.put(Integer.valueOf(uid), timer);
    outstanding.put(Integer.valueOf(uid), command);
  }

  /**
   * Removes and returns the provided continuation from the pending table
   *
   * @param uid The id of the message
   * @return The continuation to run
   */
  Continuation<Object, Exception> remove(int uid) {
    logger.trace("Removing and returning continuation {} from pending table", uid);
    CancellableTask timer = timers.remove(Integer.valueOf(uid));

    if (timer != null)
      timer.cancel();

    // the response arrives untyped from the wire; each pending continuation expects the response type of its request
    @SuppressWarnings("unchecked")
    Continuation<Object, Exception> command = (Continuation<Object, Exception>) outstanding.remove(Integer.valueOf(uid));
    return command;
  }
}
