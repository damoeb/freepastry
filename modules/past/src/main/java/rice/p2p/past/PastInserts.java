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

import org.slf4j.Logger;
import rice.Continuation;
import rice.Continuation.MultiContinuation;
import rice.Continuation.NamedContinuation;
import rice.Continuation.SimpleContinuation;
import rice.Continuation.StandardContinuation;
import rice.Futures;
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

  void getHandles(Id id, final int max, Continuation<? super NodeHandleSet, Exception> command) {
    NodeHandleSet nodeHandleSet = endpoint.replicaSet(id, max);

    if (nodeHandleSet.size() == max) {
      command.receiveResult(nodeHandleSet);
    } else {
      Futures.forward(past.<NodeHandleSet>sendRequest(id, new LookupHandlesMessage(past.nextRequestId(), id, max, past.getLocalNodeHandle(), id)), new StandardContinuation<NodeHandleSet, NodeHandleSet, Exception>(command) {
        @Override
        public void receiveResult(NodeHandleSet replicas) {

          // check to make sure we've fetched the correct number of replicas
          // the deal with this is for the small ring.  If you are requesting
          // 4 nodes, but the ring is only 3, you are only going to get 3
          // Note: this is still kind of funky, because the past.getReplicationFactor()+1
          // argument is kind of weird, but I don't know how to get it right
          // -Jeff 1/24/07
          if (Math.min(max,endpoint.replicaSet(endpoint.getLocalNodeHandle().getId(), past.getReplicationFactor()+1).size()) > replicas.size())
            parent.receiveException(new PastException("Only received " + replicas.size() + " replicas - cannot insert as we know about more nodes."));
          else
            parent.receiveResult(replicas);
        }
      });
    }
  }

  void doInsert(final Id id, final MessageBuilder builder, Continuation<? super Boolean[], Exception> command, final boolean useSocket) {
    // first, we get all of the replicas for this id
    past.getHandles(id, past.getReplicationFactor()+1, new StandardContinuation<NodeHandleSet, Boolean[], Exception>(command) {
      @Override
      public void receiveResult(NodeHandleSet replicas) {
        logger.trace("Received replicas {} for id {}", replicas, id);

        // then we send inserts to each replica and wait for at least
        // threshold * num to return successfully
        MultiContinuation<Boolean[]> multiContinuation = new MultiContinuation<Boolean[]>(parent, replicas.size()) {
          @Override
          public boolean isDone() throws Exception {
            int numSuccess = 0;
            for (int i=0; i<haveResult.length; i++)
              if (haveResult[i] && Boolean.TRUE.equals(result[i]))
                numSuccess++;

            if (numSuccess >= (past.SUCCESSFUL_INSERT_THRESHOLD * haveResult.length))
              return true;

            if (super.isDone()) {
              for (int i=0; i<result.length; i++)
                if (result[i] instanceof Exception)
                  logger.warn("result[{}]:", i, (Exception)result[i]);

              throw new PastException("Had only " +  numSuccess + " successful inserts out of " + result.length + " - aborting.");
            }
            return false;
          }

          @Override
          public Boolean[] getResult() {
            Boolean[] successes = new Boolean[result.length];
            for (int i=0; i<successes.length; i++)
              successes[i] = Boolean.valueOf((result[i] == null) || Boolean.TRUE.equals(result[i]));

            return successes;
          }
        };

        for (int i=0; i<replicas.size(); i++) {
          NodeHandle handle = replicas.getHandle(i);
          PastMessage msg = builder.buildMessage();
          Continuation<Object, Exception> continuation = new NamedContinuation<Object, Exception>("InsertMessage to " + replicas.getHandle(i) + " for " + id, multiContinuation.getSubContinuation(i));
          Futures.forward(useSocket ? past.sendViaSocket(handle, msg) : past.sendRequest(handle, msg), continuation);
        }
      }
    });
  }

  void insert(final PastContent obj, final Continuation<? super Boolean[], Exception> command) {
    logger.trace("Inserting the object {} with the id {}", obj, obj.getId());

    logger.atTrace().log(() -> " Inserting data of class " + obj.getClass().getName() + " under " + obj.getId().toStringFull());

    past.doInsert(obj.getId(), () -> new InsertMessage(past.nextRequestId(), obj, past.getLocalNodeHandle(), obj.getId()), new StandardContinuation<Boolean[], Boolean[], Exception>(command) {
      @Override
      public void receiveResult(final Boolean[] array) {
        past.cache(obj, new SimpleContinuation()  {
          @Override
          public void receiveResult(Object result) {
            parent.receiveResult(array);
          }
          @Override
          public void receiveException(Exception e) {
            logger.warn("Caching after insert failed, continuing", e);
            parent.receiveResult(array);
          }
        });
      }
    },
    past.socketStrategy.sendAlongSocket(SocketStrategy.TYPE_INSERT, obj));
  }
}
