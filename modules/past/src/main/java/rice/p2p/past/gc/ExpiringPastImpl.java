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

package rice.p2p.past.gc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.Continuation;
import rice.Continuation.ListenerContinuation;
import rice.Continuation.StandardContinuation;
import rice.Futures;
import rice.Outcome;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.past.ObjectNotFoundException;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastImpl;
import rice.p2p.past.PastPolicy;
import rice.environment.params.ConfigChecks;
import rice.p2p.past.RefreshFailedException;
import rice.p2p.past.messaging.GCCollectMessage;
import rice.p2p.past.messaging.GCInsertMessage;
import rice.p2p.past.messaging.GCLookupHandlesMessage;
import rice.p2p.past.messaging.GCRefreshMessage;
import rice.p2p.past.messaging.InsertMessage;
import rice.p2p.past.messaging.PastMessageType;
import rice.p2p.past.messaging.FetchHandleMessage;
import rice.p2p.past.messaging.PastMessage;
import rice.p2p.past.rawserialization.SocketStrategy;
import rice.persistence.Cache;
import rice.persistence.CachedStorage;

/**
 * This class is an implementation of the ExpiringPast interface, which provides
 * Past services with garbage collection.
 *
 * @version $Id: ExpiringPastImpl.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Andreas Haeberlen
 */
@SuppressWarnings("unchecked")
public class ExpiringPastImpl extends PastImpl implements ExpiringPast {

  /**
   * The default expiration, or when objects inserted with no timeout will expire
   */
  public static final long DEFAULT_EXPIRATION = INFINITY_EXPIRATION;

  /**
   * The real factory, which is not wrapped with a GCIdFactory
   */
  private final IdFactory realFactory;

  /** The longest time from now a refresh may extend the lease of an object to, p2p_past_gc_max_lease, in milliseconds */
  private final long maxLease;

  // internal tracing stats
  private int refreshed = 0;

  protected class GCPastDeserializer extends PastDeserializer {

    @Override
    protected Message deserializeGC(InputBuffer buf, PastMessageType type) throws IOException {
      try {
        return switch (type) {
          case GC_INSERT -> GCInsertMessage.readFrom(buf, getEndpoint(), contentDeserializer);
          case GC_LOOKUP_HANDLES -> GCLookupHandlesMessage.buildGC(buf, getEndpoint());
          case GC_REFRESH -> GCRefreshMessage.build(buf, getEndpoint());
          default -> throw unknownType(type.id());
        };
      } catch (IOException e) {
        logger.atError().log(() -> "Exception in deserializer in "+ExpiringPastImpl.this.getEndpoint().toString()+":"+getInstance()+" "+e);
        throw e;
      }
    }
  }

  @Override
  public String toString() {
    if (getEndpoint() == null) return super.toString();
    return "ExpiringPastImpl["+getEndpoint().getApplicationInstanceName()+"]";
  }


  /**
   * Constructor for ExpiringPast
   *
   * @param node The node below this Past implementation
   * @param manager The storage manager to be used by Past
   * @param backup The cache used for previously-responsible objects (can be null)
   * @param replicas The number of object replicas
   * @param instance The unique instance name of this Past
   * @param trash The storage manager to place the deleted objects into (if null, they are removed)
   * @param policy The policy this past instance should use
   * @param collectionInterval The frequency with which ExpiringPast should collection local expired objects
   */
  public ExpiringPastImpl(Node node, CachedStorage manager, Cache backup, int replicas, String instance, PastPolicy policy, long collectionInterval, CachedStorage trash) {
    super(new ExpiringIdNode(node), manager, backup, replicas, instance, policy, trash);
    this.realFactory = node.getIdFactory();
    this.maxLease = ConfigChecks.requirePositive("p2p_past_gc_max_lease", getEnvironment().getParameters().getLong("p2p_past_gc_max_lease"));

    getEndpoint().scheduleMessage(new GCCollectMessage(0, getLocalNodeHandle(), node.getId()), collectionInterval, collectionInterval);
    getEndpoint().setDeserializer(new GCPastDeserializer());
  }

  /**
   * Inserts an object that never expires: {@code insert(obj, INFINITY_EXPIRATION)}, for simplicity and for the
   * applications of plain Past.
   *
   * @param obj the object to be inserted
   * @return one Boolean per replica, or failed with a PastException if too few replicas stored the object
   */
  @Override
  public CompletableFuture<Boolean[]> insert(PastContent obj) {
    return insert(obj, INFINITY_EXPIRATION);
  }

  /**
   * Inserts an object with the given ID into this instance of Past, to be stored until the given expiration; the
   * application refreshes it with {@link #refresh(Id[], long[])} while it is interested in it.
   *
   * @param obj the object to be inserted
   * @param expiration the time until which the object must be stored
   * @return one Boolean per replica, or failed with a PastException if too few replicas stored the object
   */
  @Override
  public CompletableFuture<Boolean[]> insert(final PastContent obj, final long expiration) {
    logger.atDebug().log(() -> "Inserting data of class " + obj.getClass().getName() + " under " + obj.getId().toStringFull());

    return doInsert(obj.getId(), () -> new GCInsertMessage(nextRequestId(), obj, expiration, getLocalNodeHandle(), obj.getId()),
        socketStrategy.sendAlongSocket(SocketStrategy.TYPE_INSERT, obj));
  }

  /**
   * Updates the objects stored under the provided keys to expire no earlier than the provided expiration times
   * (each capped at the maximum lease from now).
   *
   * @param array The keys which to refresh
   * @param expirations The times to extend the lifetimes to, one per key
   * @return per key, in order: Success(true) if at least one replica refreshed it (or already had a later
   *     expiration), a Failure with an ObjectNotFoundException if replicas answered but none had it, or with a
   *     RefreshFailedException if no replica answered
   */
  @Override
  public CompletableFuture<List<Outcome<Boolean>>> refresh(final Id[] array, long[] expirations) {
    logger.debug("Refreshing {} data elements", array.length);

    GCIdSet gcIdSet = new GCIdSet(realFactory);
    for (int i=0; i<array.length; i++)
      gcIdSet.addId(new ExpiringId(array[i], expirations[i]));

    final Map<Id, Boolean> outcome = new HashMap<>();
    return refresh(gcIdSet, outcome).thenApply(_ -> collectRefreshResults(array, outcome));
  }

  /**
   * Turns the replicas' answers into the per-id result of {@link #refresh(Id[], long[])}.
   *
   * @param ids The ids that were to be refreshed
   * @param outcome Per id: TRUE if at least one replica refreshed it (or already had a later expiration), FALSE if
   *   replicas answered but none had the object, absent if no replica answered at all
   * @return Per id Success(true), or a Failure with an ObjectNotFoundException or a RefreshFailedException
   */
  static List<Outcome<Boolean>> collectRefreshResults(Id[] ids, Map<Id, Boolean> outcome) {
    List<Outcome<Boolean>> result = new ArrayList<Outcome<Boolean>>(ids.length);
    for (Id id : ids) {
      Boolean refreshed = outcome.get(id);
      if (refreshed == null) {
        result.add(new Outcome.Failure<Boolean>(new RefreshFailedException("No replica answered the refresh of " + id)));
      } else if (refreshed.booleanValue()) {
        result.add(new Outcome.Success<Boolean>(Boolean.TRUE));
      } else {
        result.add(new Outcome.Failure<Boolean>(new ObjectNotFoundException("No object found under " + id)));
      }
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Internal method which actually does the refreshing.  Should not be called
   * by external applications.
   *
   * @param ids The ids to refresh, each with the time to extend its lifetime until
   * @return completed once every id was sent to its replicas
   */
  protected CompletableFuture<Void> refresh(final GCIdSet ids) {
    return refresh(ids, new HashMap<>());
  }

  /**
   * Does the refreshing, recording in outcome the replicas' answers per id: asks the neighbors of the first id for
   * their handles, groups the ids whose replicas are all among them by replica, sends each replica one refresh
   * after the other (a failed one is skipped), and starts over with the ids that are left.
   *
   * @param ids The ids to refresh, each with the time to extend its lifetime until
   * @param outcome Collects per id whether a replica refreshed it, see {@link #collectRefreshResults(Id[], Map)}
   * @return completed once every id was sent to its replicas, or failed if the neighbors could not be asked
   */
  private CompletableFuture<Void> refresh(final GCIdSet ids, final Map<Id, Boolean> outcome) {
    final Logger logger = getEnvironment().getLogManager().getLogger(ExpiringPastImpl.class, getInstance());
    logger.atDebug().log(() -> "REFRESH: CALLED WITH "+ ids.numElements() + " ELEMENTS");

    if (ids.numElements() == 0) {
      return CompletableFuture.completedFuture(null);
    }

    final Id[] array = ids.asArray();
    ExpiringId start = (ExpiringId) array[0];
    logger.debug("REFRESH: GETTINGS ALL HANDLES OF {}", start);

    return this.<NodeHandleSet>sendRequest(start.id(), new GCLookupHandlesMessage(nextRequestId(), start.id(), getLocalNodeHandle(), start.id()))
        .thenCompose(nodeHandleSet -> {
          final ReplicaMap map = new ReplicaMap();

          logger.debug("REFRESH: GOT {} SET OF HANDLES!", nodeHandleSet);

          return getEndpoint().process(() -> {
            logger.debug("REFRESH: ON PROCESSING THREAD!");

            for (int i=0; i<array.length; i++) {
              ExpiringId id = (ExpiringId) array[i];

              NodeHandleSet replicas = getEndpoint().replicaSet(id.id(), getReplicationFactor()+1, nodeHandleSet.getHandle(nodeHandleSet.size()-1), nodeHandleSet);

              // if we have all of the replicas, go ahead and refresh this item
              if ((replicas != null) && ((replicas.size() == nodeHandleSet.size()) || (replicas.size() == getReplicationFactor()+1))) {
                for (int j=0; j<replicas.size(); j++)
                  map.addReplica(replicas.getHandle(j), id);

                refreshed++;
                ids.removeId(id);
              }
            }

            logger.debug("REFRESH: DONE WITH PROCESSING THREAD - MOVING TO NORMAL THREAD!");

            return map;
          });
        })
        .thenCompose(map -> {
          logger.debug("REFRESH: BACK ON NORMAL THREAD!");

          CompletableFuture<Void> sent = CompletableFuture.completedFuture(null);
          Iterator<NodeHandle> iterator = map.getReplicas();
          while (iterator.hasNext()) {
            NodeHandle next = iterator.next();
            sent = sent.thenCompose(_ -> sendRefresh(next, map.getIds(next), outcome, logger));
          }
          return sent;
        })
        .thenCompose(_ -> {
          logger.debug("REFRESH: DONE SENDING REQUESTS, RECURSING");

          return refresh(ids, outcome);
        });
  }

  /** Sends one replica the refresh of its ids and records its answers; a failed refresh is logged and skipped. */
  private CompletableFuture<Void> sendRefresh(NodeHandle replica, GCIdSet sent, Map<Id, Boolean> outcome, Logger logger) {
    // the replica answers one Boolean per key, in the order of the message's keys
    final Id[] sentIds = sent.asArray();
    logger.debug("REFRESH: SENDING REQUEST TO {} FOR IDSET {}", replica, sent);

    return this.<Object>sendRequest(replica, new GCRefreshMessage(nextRequestId(), sent, getLocalNodeHandle(), replica.getId()))
        .handle((answers, failure) -> {
          if (failure != null) {
            logger.atDebug().log(() -> "GOT EXCEPTION " + Futures.unwrap(failure) + " REFRESHING ITEMS - CONTINUING");
          } else if (answers instanceof Boolean[] booleans && booleans.length == sentIds.length) {
            for (int k = 0; k < sentIds.length; k++)
              outcome.merge(((ExpiringId) sentIds[k]).id(), booleans[k], Boolean::logicalOr);
          }
          return null;
        });
  }

  /**
   * This method is invoked on applications when the underlying node
   * is about to forward the given message with the provided target to
   * the specified next hop.  Applications can change the contents of
   * the message, specify a different nextHop (through re-routing), or
   * completely terminate the message.
   *
   * @param message The message being sent, containing an internal message
   * along with a destination key and nodeHandle next hop.
   *
   * @return Whether or not to forward the message further
   */
  @Override
  public boolean forward(final RouteMessage message) {
    try {
      if (message.getMessage(getEndpoint().getDeserializer()) instanceof GCLookupHandlesMessage)
        return true;
      else
        return super.forward(message);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** The expiration as far as the maximum lease allows, counted from now: whoever asks for a lease, it ends no later. */
  private long cappedExpiration(long requestedExpiration) {
    return Math.min(requestedExpiration, getEnvironment().getTimeSource().currentTimeMillis() + maxLease);
  }

  /** Stores an authentic, allowed insert, without the lock of PastImpl, with the expiration of the insert as metadata. */
  @Override
  protected void storeInsert(InsertMessage insertMessage) {
    Id theId = insertMessage.getContent().getId();
    if (theId == null) {
      logger.error("Error: null Id from {} from {} in {}", insertMessage.getContent(), insertMessage, this);
    }
    long expiration = cappedExpiration(((GCInsertMessage) insertMessage).getExpiration());
    checkAndStore(insertMessage, content -> ((ExpiringPastContent) content).getMetadata(expiration), getResponseContinuation(insertMessage));
  }

  /**
   * This method is called on the application at the destination node
   * for the given id.
   *
   * @param id The destination id of the message
   * @param message The message being sent
   */
  @Override
  public void deliver(Id id, Message message) {
    final PastMessage msg = (PastMessage) message;

    if (msg.isResponse()) {
      super.deliver(id, message);
    } else {
      switch (msg) {
        case GCInsertMessage imsg -> {
          inserts++;
          deliverInsert(imsg);
        }
        case GCRefreshMessage rmsg -> {
          final Iterator<ExpiringId> expiringIdIterator = Arrays.asList(rmsg.getKeys()).iterator();
          // filled by one step of the (sequential) continuation chain after the other
          final List<Object> result = new ArrayList<Object>();
          other += rmsg.getKeys().length;

          StandardContinuation<Object, Object, Exception> process = new StandardContinuation<Object, Object, Exception>(getResponseContinuation(msg)) {
            private boolean started;

            @Override
            public void receiveResult(Object answer) {
              // one answer per key, in order, so the caller can match them up: the first call only starts the chain
              if (started)
                result.add(Boolean.TRUE.equals(answer));
              started = true;

              if (expiringIdIterator.hasNext()) {
                final ExpiringId id = expiringIdIterator.next();
                /* nobody can lease an object for longer than the maximum lease from now */
                final long expiration = cappedExpiration(id.expiration());

                /* skip the object if we don't have it yet */
                if (storage.exists(id.id())) {
                  ExpiringPastMetadata metadata = (ExpiringPastMetadata) storage.getMetadata(id.id());

                  if (metadata != null) {
                    /* only allow the lifetime to be extended, otherwise skip */
                    if (metadata.expiration() < expiration) {
                      Futures.forward(storage.setMetadata(id.id(), metadata.setExpiration(expiration)), this);
                    } else {
                      /* already refreshed to a later expiration, which counts as success */
                      receiveResult(true);
                    }
                  } else {
                    setMissingMetadata(id.id(), expiration, this);
                  }
                } else {
                  /* a refresh carries no proof of ownership, so it must not bring back what the collection removed */
                  receiveResult(false);
                }
              } else {
                parent.receiveResult(result.toArray(new Boolean[0]));
              }
            }
          };

          process.receiveResult(null);
        }
        case GCLookupHandlesMessage lmsg -> {
          NodeHandleSet nodeHandleSet = getEndpoint().neighborSet(lmsg.getMax());
          nodeHandleSet.removeHandle(getLocalNodeHandle().getId());
          nodeHandleSet.putHandle(getLocalNodeHandle());

          logger.trace("Returning neighbor set {} for lookup handles of id {} max {} at {}", nodeHandleSet, lmsg.getId(), lmsg.getMax(), getEndpoint().getId());
          getResponseContinuation(msg).receiveResult(nodeHandleSet);
        }
        case GCCollectMessage cmsg -> {
          // get all ids which expiration before now
          // ids stored without metadata have DEFAULT_EXPIRATION, which is never reached, so only the expired ones are collected
          collect(storage.scanMetadataValuesHead(new ExpiringPastMetadata(getEnvironment().getTimeSource().currentTimeMillis())), new ListenerContinuation<Object, Exception>("Removal of expired ids", getEnvironment()));
        }
        case FetchHandleMessage fmsg -> {
          fetchHandles++;

          Futures.forward(storage.getObject(fmsg.getId()), new StandardContinuation<Object, Object, Exception>(getResponseContinuation(msg)) {
            @Override
            public void receiveResult(Object result) {
              ExpiringPastContent content = (ExpiringPastContent) result;

              if (content != null) {
                logger.debug("Retrieved data for fetch handles of id {}", fmsg.getId());
                ExpiringPastMetadata metadata = (ExpiringPastMetadata) storage.getMetadata(fmsg.getId());

                if (metadata != null)
                  parent.receiveResult(content.getHandle(ExpiringPastImpl.this, metadata.expiration()));
                else
                  parent.receiveResult(content.getHandle(ExpiringPastImpl.this, DEFAULT_EXPIRATION));
              } else {
                parent.receiveResult(null);
              }
            }
          });
        }
        default -> super.deliver(id, message);
      }
    }
  }

  /**
   * Internal method which collects all of the objects in the given set
   *
   * @param map The ids to collect (only the keys are used)
   * @param command The command to call once done
   */
  protected void collect(SortedMap<Id, ?> map, Continuation<? super Boolean, Exception> command) {
    final Iterator<Id> idIterator = map.keySet().iterator();

    Continuation<Object, Exception> remove = new StandardContinuation<Object, Boolean, Exception>(command) {
      @Override
      public void receiveResult(Object previousResult) {
        if (idIterator.hasNext()) {
          final Id gid = idIterator.next();

          if (trash != null) {
            Futures.forward(storage.getObject(gid), new StandardContinuation<Object, Object, Exception>(this) {
              @Override
              public void receiveResult(Object storedObject) {
                if (storedObject != null) {
                  Futures.forward(trash.store(gid, storage.getMetadata(gid), storedObject), new StandardContinuation<Boolean, Object, Exception>(parent) {
                    @Override
                    public void receiveResult(Boolean result) {
                      Futures.forward(storage.unstore(gid), parent);
                    }
                  });
                } else {
                  // continue with the remove loop (parent), not this getObject continuation
                  Futures.forward(storage.unstore(gid), parent);
                }
              }
            });
          } else {
            Futures.forward(storage.unstore(gid), this);
          }
        } else {
          parent.receiveResult(Boolean.TRUE);
        }
      }
    };

    remove.receiveResult(null);
  }

  // ---- REPLICATION MANAGER METHODS -----

  /**
   * This upcall is invoked to tell the client to fetch the given id,
   * and to call the given command with the boolean result once the fetch
   * is completed.  The client *MUST* call the command at some point in the
   * future, as the manager waits for the command to return before continuing.
   *
   * @param id The id to fetch
   */
  @Override
  public void fetch(final Id id, NodeHandle hint, Continuation<? super Boolean, Exception> command) {
    logger.trace("Sending out replication fetch request for the id {}", id);
    final ExpiringId expiringId = (ExpiringId) id;
    // the peer names the expiration: it counts only up to the maximum lease, like that of an insert
    final long expiration = cappedExpiration(expiringId.expiration());

    if (expiration < getEnvironment().getTimeSource().currentTimeMillis()) {
      command.receiveResult(Boolean.TRUE);
    } else if (storage.exists(expiringId.id())) {
      ExpiringPastMetadata metadata = (ExpiringPastMetadata) storage.getMetadata(expiringId.id());

      if (metadata == null) {
        setMissingMetadata(expiringId.id(), expiration, command);
      } else if (metadata.expiration() < expiration) {
        Futures.forward(storage.setMetadata(expiringId.id(), metadata.setExpiration(expiration)), command);
      } else {
        command.receiveResult(Boolean.TRUE);
      }
    } else {
      Futures.forward(policy.fetch(expiringId.id(), hint, backup, this), new StandardContinuation<Object, Boolean, Exception>(command) {
        @Override
        public void receiveResult(Object result) {
          if (result == null) {
            logger.warn("Could not fetch id {} - policy returned null in namespace {}", id, getInstance());
            parent.receiveResult(Boolean.valueOf(false));
          } else {
            // the peer that answered is not trusted: store only what was asked for
            if (!(result instanceof ExpiringPastContent content) || !expiringId.id().equals(content.getId())) {
              logger.warn("Dropping fetched replica of id {}: not ExpiringPastContent for that id: {}", id, result);
              parent.receiveResult(Boolean.valueOf(false));
              return;
            }
            logger.trace("inserting replica of id {}", id);

            Futures.forward(storage.getStorage().store(expiringId.id(), content.getMetadata(expiration), content), parent);
          }
        }
      });
    }
  }

  /**
   * This upcall is to notify the client that the given id can be safely removed
   * from the storage.  The client may choose to perform advanced behavior, such
   * as caching the object, or may simply delete it.
   *
   * @param id The id to remove
   */
  @Override
  public void remove(Id id, Continuation<? super Boolean, Exception> command) {
    super.remove(((ExpiringId) id).id(), command);
  }

  /**
   * This upcall should return the set of keys that the application
   * currently stores in this range. Should return a empty IdSet (not null),
   * in the case that no keys belong to this range.
   *
   * @param range the requested range
   */
  @Override
  public IdSet scan(IdRange range) {
    GCIdRange gcRange = (GCIdRange) range;
    return new GCIdSet(storage.getStorage().scan(gcRange.range()), storage.getStorage().scanMetadata(gcRange.range()));
  }

  /**
   * This upcall should return whether or not the given id is currently stored
   * by the client.
   *
   * @param id The id in question
   * @return Whether or not the id exists
   */
  @Override
  public boolean exists(Id id) {
    if (id instanceof ExpiringId gcId)
      return storage.getStorage().exists(gcId.id());
    else
      return storage.getStorage().exists(id);
  }

  @Override
  public void existsInOverlay(Id id, Continuation<? super Boolean, Exception> command) {
    if (id instanceof ExpiringId gcId) {
      super.existsInOverlay(gcId.id(), command);
    } else {
      super.existsInOverlay(id, command);
    }
  }

  /** Gives an object that was stored without metadata the metadata of the given expiration. */
  private void setMissingMetadata(Id id, long expiration, Continuation<? super Boolean, Exception> command) {
    Futures.forward(storage.getObject(id), new StandardContinuation<Object, Boolean, Exception>(command) {
      @Override
      public void receiveResult(Object result) {
        Futures.forward(storage.setMetadata(id, ((ExpiringPastContent) result).getMetadata(expiration)), parent);
      }
    });
  }

  @Override
  public void reInsert(final Id id, Continuation<? super Boolean, Exception> command) {
    if (id instanceof ExpiringId gcId) {
      // what if the ExpiringId's expiration is different than the metadata's?
      Futures.forward(storage.getObject(gcId.id()), new StandardContinuation<Object, Boolean, Exception>(command) {
        @Override
        public void receiveResult(final Object storedObject) {
          Futures.forward(insert((PastContent)storedObject, gcId.expiration()).thenApply(PastImpl::isAnyStored), parent);
        }
      });
    } else {
      ExpiringPastMetadata metadata = (ExpiringPastMetadata) storage.getMetadata(id);
      if (metadata == null) {
        // no metadata, just reinsert with no expiration
        super.reInsert(id, command);
      } else {
        reInsert(new ExpiringId(id, metadata.expiration()), command);
      }
    }
  }

  protected class ReplicaMap {
    private final HashMap<NodeHandle, IdSet> map = new HashMap<NodeHandle, IdSet>();
    public void addReplica(NodeHandle handle, ExpiringId id) {
      IdSet idSet = (IdSet) map.get(handle);

      if (idSet == null) {
        idSet = new GCIdSet(realFactory);
        map.put(handle, idSet);
      }

      idSet.addId(id);
    }

    public Iterator<NodeHandle> getReplicas() {
      return map.keySet().iterator();
    }

    public GCIdSet getIds(NodeHandle replica) {
      return (GCIdSet) map.get(replica);
    }
  }
}
