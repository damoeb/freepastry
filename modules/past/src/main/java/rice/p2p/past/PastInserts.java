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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import rice.Futures;
import rice.Outcome;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.past.PastImpl.MessageBuilder;
import rice.p2p.past.messaging.InsertMessage;
import rice.p2p.past.messaging.LookupHandlesMessage;
import rice.p2p.past.messaging.PastMessage;
import rice.p2p.past.rawserialization.SocketStrategy;

/**
 * The insert of an object on behalf of a {@link PastImpl}: it finds the replica nodes of the id and sends the insert
 * to each of them, and is done when enough of them stored the object. It calls back into the Past for requests,
 * handles and caching, so a subclass that overrides those still takes part.
 */
final class PastInserts {
  private final PastImpl past;
  private final Endpoint endpoint;
  private final Logger logger;

  PastInserts(PastImpl past, Endpoint endpoint, Logger logger) {
    this.past = past;
    this.endpoint = endpoint;
    this.logger = logger;
  }

  CompletableFuture<NodeHandleSet> getHandles(Id id, final int max) {
    NodeHandleSet nodeHandleSet = endpoint.replicaSet(id, max);

    if (nodeHandleSet.size() == max) {
      return CompletableFuture.completedFuture(nodeHandleSet);
    }
    return past.<NodeHandleSet>sendRequest(id, new LookupHandlesMessage(past.nextRequestId(), id, max, past.getLocalNodeHandle(), id))
        .thenCompose(replicas -> {
          // check to make sure we've fetched the correct number of replicas
          // the deal with this is for the small ring.  If you are requesting
          // 4 nodes, but the ring is only 3, you are only going to get 3
          // Note: this is still kind of funky, because the past.getReplicationFactor()+1
          // argument is kind of weird, but I don't know how to get it right
          // -Jeff 1/24/07
          if (Math.min(max,endpoint.replicaSet(endpoint.getLocalNodeHandle().getId(), past.getReplicationFactor()+1).size()) > replicas.size())
            return CompletableFuture.failedFuture(new PastException("Only received " + replicas.size() + " replicas - cannot insert as we know about more nodes."));
          return CompletableFuture.completedFuture(replicas);
        });
  }

  CompletableFuture<Boolean[]> doInsert(final Id id, final MessageBuilder builder, final boolean useSocket) {
    // first, we get all of the replicas for this id
    return past.getHandles(id, past.getReplicationFactor()+1).thenCompose(replicas -> {
      logger.trace("Received replicas {} for id {}", replicas, id);

      // then we send inserts to each replica and wait for at least
      // threshold * num to return successfully
      List<CompletableFuture<Object>> inserts = new ArrayList<CompletableFuture<Object>>(replicas.size());
      for (int i=0; i<replicas.size(); i++) {
        NodeHandle handle = replicas.getHandle(i);
        PastMessage msg = builder.buildMessage();
        inserts.add(useSocket ? past.sendViaSocket(handle, msg) : past.sendRequest(handle, msg));
      }
      return Futures.<Object, Boolean[]>quorum(inserts, this::isInsertDone, PastInserts::replicaSuccesses);
    });
  }

  /**
   * Done once enough replicas stored the object; once all answered without that, the insert fails.
   */
  private boolean isInsertDone(List<@Nullable Outcome<Object>> outcomes) throws PastException {
    int numSuccess = 0;
    boolean haveAllAnswered = true;
    for (Outcome<Object> outcome : outcomes) {
      if (outcome == null)
        haveAllAnswered = false;
      else if (outcome instanceof Outcome.Success<Object> success && Boolean.TRUE.equals(success.value()))
        numSuccess++;
    }

    if (numSuccess >= (past.SUCCESSFUL_INSERT_THRESHOLD * outcomes.size()))
      return true;

    if (haveAllAnswered) {
      for (int i=0; i<outcomes.size(); i++)
        if (outcomes.get(i) instanceof Outcome.Failure<Object> failure)
          logger.warn("result[{}]:", i, failure.cause());

      throw new PastException("Had only " +  numSuccess + " successful inserts out of " + outcomes.size() + " - aborting.");
    }
    return false;
  }

  /** Per replica: true if it stored the object, answered nothing, or has not answered yet. */
  private static Boolean[] replicaSuccesses(List<@Nullable Outcome<Object>> outcomes) {
    Boolean[] successes = new Boolean[outcomes.size()];
    for (int i=0; i<successes.length; i++) {
      Outcome<Object> outcome = outcomes.get(i);
      successes[i] = Boolean.valueOf(outcome == null
          || (outcome instanceof Outcome.Success<Object> success && (success.value() == null || Boolean.TRUE.equals(success.value()))));
    }
    return successes;
  }

  CompletableFuture<Boolean[]> insert(final PastContent obj) {
    logger.trace("Inserting the object {} with the id {}", obj, obj.getId());

    logger.atTrace().log(() -> " Inserting data of class " + obj.getClass().getName() + " under " + obj.getId().toStringFull());

    return past.doInsert(obj.getId(), () -> new InsertMessage(past.nextRequestId(), obj, past.getLocalNodeHandle(), obj.getId()),
        past.socketStrategy.sendAlongSocket(SocketStrategy.TYPE_INSERT, obj))
        .thenCompose(array -> past.cache(obj).handle((_, cacheFailure) -> {
          if (cacheFailure != null) {
            logger.warn("Caching after insert failed, continuing", Futures.unwrap(cacheFailure));
          }
          return array;
        }));
  }
}
