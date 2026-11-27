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

import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.past.messaging.PastMessage;

/**
 * The requests of a {@link PastImpl} that wait for their response: per request the future its response completes,
 * the timer that reports it lost, its type, and the node that may answer it. Requests may be sent from application
 * threads while the selector thread delivers responses and timeouts.
 *
 * <p>A response counts only for the request it answers:
 * <ul>
 * <li>Request ids are random 64-bit numbers, so only a node that saw the request knows its id: the node it was sent
 *     to, the nodes on the route of a routed request, and in plain mode whoever reads the wire.</li>
 * <li>The response must be of the type of the request (a Past response is the request message itself).</li>
 * <li>A request sent to a node (lookupHandle, fetch, insert, refresh) may be answered only by that node. A routed
 *     request (lookup, lookupHandles, the replica-set request of an insert or refresh) may be answered by any node
 *     on its route: a node on the way answers a lookup from its cache and the lookupHandles request when it knows
 *     the whole replica set, so neither the root nor a replica can be required. For routed requests the random id
 *     is the only binding; what such a response says is checked elsewhere (content-hash objects) or is trusted
 *     (mutable content, replica sets: C6, D1).</li>
 * </ul>
 * The responder is the node the endpoint says the response arrived from. In certified mode the transport
 * authenticated it; in plain mode it is what the peer claims, so there the check stops only an honest mix-up and
 * the random id is what stops an off-route node. A response whose sender the endpoint cannot tell (it was created on
 * this node, or a wrapper such as multiring delivered it without its previous hop) is checked by id and type only.
 */
final class PastPendingRequests {
  private final Logger logger;

  // the source of the request ids, which must not be guessable
  private final SecureRandom random = new SecureRandom();

  // the requests that wait for a response, by request id
  // concurrent, and like the Hashtables they replace they reject null keys and values
  private final Map<Long, PendingRequest> outstanding = new ConcurrentHashMap<Long, PendingRequest>();

  /**
   * A request that waits for its response.
   *
   * @param response completed by the response
   * @param timer reports the request lost
   * @param type the message type of the request, which the response must have
   * @param responder the id of the node the request was sent to, which alone may answer it, or null for a routed
   *     request
   */
  private record PendingRequest(CompletableFuture<Object> response, CancellableTask timer, short type, @Nullable Id responder) {
  }

  PastPendingRequests(Logger logger) {
    this.logger = logger;
  }

  /**
   * Returns a new id for a request: random, so that a node that did not see the request cannot guess it.
   *
   * @return A new id
   */
  long nextRequestId() {
    return random.nextLong();
  }

  /**
   * Loads the future of a request into the pending table
   *
   * @param request the request, under whose id and type the response is expected
   * @param responder the id of the node the request was sent to, or null for a routed request
   * @param timer reports the request lost
   * @param response completed by the response
   */
  void insert(PastMessage request, @Nullable Id responder, CancellableTask timer, CompletableFuture<Object> response) {
    logger.trace("Loading request {} into pending table", request.getUID());
    outstanding.put(Long.valueOf(request.getUID()), new PendingRequest(response, timer, request.getType(), responder));
  }

  /**
   * Removes and returns the future of the request that a response answers, if the response may answer it.
   *
   * @param response the response
   * @param sender the node the response arrived from, or null if the endpoint cannot tell
   * @return The future to complete, or null if no request waits under this id, or the response is of another type
   *     or from a node that may not answer it (the request then keeps waiting)
   */
  @Nullable CompletableFuture<Object> removeAnswered(PastMessage response, @Nullable NodeHandle sender) {
    Long uid = Long.valueOf(response.getUID());
    PendingRequest request = outstanding.get(uid);
    if (request == null) {
      logger.debug("Dropping response {}: no request waits for it", response);
      return null;
    }
    if (request.type() != response.getType()) {
      logger.info("Dropping response {} from {}: the request has type {}", response, sender, request.type());
      return null;
    }
    if (request.responder() != null && sender != null && !request.responder().equals(sender.getId())) {
      logger.info("Dropping response {} from {}: the request was sent to {}", response, sender, request.responder());
      return null;
    }
    return remove(uid, request);
  }

  /**
   * Removes and returns the future of a request from the pending table, whatever answers it: for the local report
   * that the request was lost.
   *
   * @param uid The id of the request
   * @return The future to complete, or null if no request waits under this id
   */
  @Nullable CompletableFuture<Object> remove(long uid) {
    Long key = Long.valueOf(uid);
    PendingRequest request = outstanding.get(key);
    return request == null ? null : remove(key, request);
  }

  private @Nullable CompletableFuture<Object> remove(Long uid, PendingRequest request) {
    logger.trace("Removing and returning request {} from pending table", uid);
    if (!outstanding.remove(uid, request)) {
      return null;
    }
    request.timer().cancel();
    // the response arrives untyped from the wire; the requester's dependent stages expect the type of its request
    return request.response();
  }
}
