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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.slf4j.Logger;
import rice.Continuation;
import rice.Continuation.ListenerContinuation;
import rice.Continuation.NamedContinuation;
import rice.Continuation.StandardContinuation;
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
import rice.p2p.past.PastException;
import rice.p2p.past.PastImpl;
import rice.p2p.past.PastPolicy;
import rice.p2p.past.RefreshFailedException;
import rice.p2p.past.messaging.GCCollectMessage;
import rice.p2p.past.messaging.GCInsertMessage;
import rice.p2p.past.messaging.GCLookupHandlesMessage;
import rice.p2p.past.messaging.GCRefreshMessage;
import rice.p2p.past.messaging.PastMessageType;
import rice.p2p.past.messaging.FetchHandleMessage;
import rice.p2p.past.messaging.PastMessage;
import rice.p2p.past.rawserialization.SocketStrategy;
import rice.persistence.Cache;
import rice.persistence.StorageManager;

/**
 * This class is an implementation of the GCPast interface, which provides
 * Past services with garbage collection.
 *
 * @version $Id: GCPastImpl.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Andreas Haeberlen
 */
@SuppressWarnings("unchecked")
public class GCPastImpl extends PastImpl implements GCPast {

  /**
   * The default expiration, or when objects inserted with no timeout will expire
   */
  public static final long DEFAULT_EXPIRATION = INFINITY_EXPIRATION;

  /**
   * The real factory, which is not wrapped with a GCIdFactory
   */
  private final IdFactory realFactory;

  // internal tracing stats
  private int refreshed = 0;

  protected class GCPastDeserializer extends PastDeserializer {

    @Override
    protected Message deserializeGC(InputBuffer buf, PastMessageType type) throws IOException {
      try {
        return switch (type) {
          case GC_INSERT -> GCInsertMessage.buildGC(buf, getEndpoint(), contentDeserializer);
          case GC_LOOKUP_HANDLES -> GCLookupHandlesMessage.buildGC(buf, getEndpoint());
          case GC_REFRESH -> GCRefreshMessage.build(buf, getEndpoint());
          default -> throw unknownType(type.id());
        };
      } catch (IOException e) {
        logger.atError().log(() -> "Exception in deserializer in "+GCPastImpl.this.getEndpoint().toString()+":"+getInstance()+" "+e);
        throw e;
      }
    }
  }

  @Override
  public String toString() {
    if (getEndpoint() == null) return super.toString();
    return "GCPastImpl["+getEndpoint().getInstance()+"]";
  }


  /**
   * Constructor for GCPast
   *
   * @param node The node below this Past implementation
   * @param manager The storage manager to be used by Past
   * @param backup The cache used for previously-responsible objects (can be null)
   * @param replicas The number of object replicas
   * @param instance The unique instance name of this Past
   * @param trash The storage manager to place the deleted objects into (if null, they are removed)
   * @param policy The policy this past instance should use
   * @param collectionInterval The frequency with which GCPast should collection local expired objects
   */
  public GCPastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, long collectionInterval, StorageManager trash) {
    super(new GCNode(node), manager, backup, replicas, instance, policy, trash);
    this.realFactory = node.getIdFactory();

    getEndpoint().scheduleMessage(new GCCollectMessage(0, getLocalNodeHandle(), node.getId()), collectionInterval, collectionInterval);
    getEndpoint().setDeserializer(new GCPastDeserializer());
  }

  /**
   * Inserts an object with the given ID into this instance of Past.
   * Asynchronously returns a PastException to command, if the
   * operation was unsuccessful.  If the operation was successful, a
   * Boolean[] is returned representing the responses from each of
   * the replicas which inserted the object.
   *
   * This method is equivalent to
   *
   * insert(obj, INFINITY_EXPIRATION, command)
   *
   * as it inserts the object with a timeout value of infinity.  This
   * is done for simplicity, as well as backwards-compatibility for
   * applications.
   *
   * @param obj the object to be inserted
   * @param command Command to be performed when the result is received
   */
  @Override
  public void insert(PastContent obj, Continuation<? super Boolean[], Exception> command) {
    insert(obj, INFINITY_EXPIRATION, command);
  }

  /**
   * Inserts an object with the given ID into this instance of Past.
   * Asynchronously returns a PastException to command, if the
   * operation was unsuccessful.  If the operation was successful, a
   * Boolean[] is returned representing the responses from each of
   * the replicas which inserted the object.
   *
   * The contract for this method is that the provided object will be
   * stored until the provided expiration time.  Thus, if the application
   * determines that it is still interested in this object, it must refresh
   * the object via the refresh() method.
   *
   * @param obj the object to be inserted
   * @param expiration the time until which the object must be stored
   * @param command Command to be performed when the result is received
   */
  @Override
  public void insert(final PastContent obj, final long expiration, Continuation<? super Boolean[], Exception> command) {
    logger.atDebug().log(() -> "Inserting data of class " + obj.getClass().getName() + " under " + obj.getId().toStringFull());

    doInsert(obj.getId(), () -> new GCInsertMessage(getUID(), obj, expiration, getLocalNodeHandle(), obj.getId()), command,
    socketStrategy.sendAlongSocket(SocketStrategy.TYPE_INSERT, obj));
  }

  /**
    * Updates the objects stored under the provided keys id to expire no
   * earlier than the provided expiration time.  Asyncroniously returns
   * the result to the caller via the provided continuation.
   *
   * The result of this operation is an Object[], which is the same length
   * as the input array of Ids.  Each element in the array is either
   * Boolean(true), representing that the refresh succeeded for the
   * cooresponding Id, or an Exception describing why the refresh failed.
   * Specifically, the possible exceptions which can be returned are:
   *
   * ObjectNotFoundException - if no object was found under the given key
   * RefreshFailedException - if the refresh operation failed for any other
   *   reason (the getMessage() will describe the failure)
   *
   * @param array The keys which to refresh
   * @param expirations The times to extend the lifetimes to, one per key
   * @param command Command to be performed when the result is received
   */
  @Override
  public void refresh(final Id[] array, long[] expirations, Continuation<? super Object[], Exception> command) {
    logger.debug("Refreshing {} data elements", array.length);

    GCIdSet set = new GCIdSet(realFactory);
    for (int i=0; i<array.length; i++)
      set.addId(new GCId(array[i], expirations[i]));

    final Map<Id, Boolean> outcome = new HashMap<>();
    refresh(set, outcome, new StandardContinuation<Object, Object[], Exception>(command) {
      @Override
      public void receiveResult(Object o) {
        parent.receiveResult(collectRefreshResults(array, outcome));
      }
    });
  }

  /**
   * Turns the replicas' answers into the per-id result of {@link #refresh(Id[], long[], Continuation)}.
   *
   * @param ids The ids that were to be refreshed
   * @param outcome Per id: TRUE if at least one replica refreshed it (or already had a later expiration), FALSE if
   *   replicas answered but none had the object, absent if no replica answered at all
   * @return Per id Boolean.TRUE, an ObjectNotFoundException or a RefreshFailedException
   */
  static Object[] collectRefreshResults(Id[] ids, Map<Id, Boolean> outcome) {
    Object[] result = new Object[ids.length];
    for (int i = 0; i < ids.length; i++) {
      Boolean refreshed = outcome.get(ids[i]);
      if (refreshed == null) {
        result[i] = new RefreshFailedException("No replica answered the refresh of " + ids[i]);
      } else if (refreshed.booleanValue()) {
        result[i] = true;
      } else {
        result[i] = new ObjectNotFoundException("No object found under " + ids[i]);
      }
    }
    return result;
  }

  /**
   * Internal method which actually does the refreshing.  Should not be called
   * by external applications.
   *
   * @param ids The ids to refresh, each with the time to extend its lifetime until
   * @param command The command to return the result to
   */
  protected void refresh(final GCIdSet ids, Continuation<? super Object, Exception> command) {
    refresh(ids, new HashMap<>(), command);
  }

  /**
   * Does the refreshing, recording in outcome the replicas' answers per id.
   *
   * @param ids The ids to refresh, each with the time to extend its lifetime until
   * @param outcome Collects per id whether a replica refreshed it, see {@link #collectRefreshResults(Id[], Map)}
   * @param command The command to return the result to
   */
  private void refresh(final GCIdSet ids, final Map<Id, Boolean> outcome, Continuation<? super Object, Exception> command) {
    final Logger logger = getEnvironment().getLogManager().getLogger(GCPastImpl.class, getInstance());
    logger.atDebug().log(() -> "REFRESH: CALLED WITH "+ ids.numElements() + " ELEMENTS");

    if (ids.numElements() == 0) {
      command.receiveResult(new Object[0]);
      return;
    }

    final Id[] array = ids.asArray();
    GCId start = (GCId) array[0];
    logger.debug("REFRESH: GETTINGS ALL HANDLES OF {}", start);


    sendRequest(start.id(), new GCLookupHandlesMessage(getUID(), start.id(), getLocalNodeHandle(), start.id()),
                new NamedContinuation<Object, Exception>("GCLookupHandles for " + start.id(), command) {
      @Override
      public void receiveResult(Object o) {
        final NodeHandleSet set = (NodeHandleSet) o;
        final ReplicaMap map = new ReplicaMap();

        logger.debug("REFRESH: GOT {} SET OF HANDLES!", set);

        getEndpoint().process(() -> {
          logger.debug("REFRESH: ON PROCESSING THREAD!");

          for (int i=0; i<array.length; i++) {
            GCId id = (GCId) array[i];

            NodeHandleSet replicas = getEndpoint().replicaSet(id.id(), getReplicationFactor()+1, set.getHandle(set.size()-1), set);

            // if we have all of the replicas, go ahead and refresh this item
            if ((replicas != null) && ((replicas.size() == set.size()) || (replicas.size() == getReplicationFactor()+1))) {
              for (int j=0; j<replicas.size(); j++)
                map.addReplica(replicas.getHandle(j), id);

              refreshed++;
              ids.removeId(id);
            }
          }

          logger.debug("REFRESH: DONE WITH PROCESSING THREAD - MOVING TO NORMAL THREAD!");

          return null;
        }, new StandardContinuation<Object, Object, Exception>(parent) {
          @Override
          public void receiveResult(Object o) {
            logger.debug("REFRESH: BACK ON NORMAL THREAD!");

            final Iterator<NodeHandle> iterator = map.getReplicas();

            Continuation<Object, Exception> send = new StandardContinuation<Object, Object, Exception>(parent) {
              @Override
              public void receiveResult(Object o) {
                if (iterator.hasNext()) {
                  NodeHandle next = iterator.next();
                  GCIdSet sent = map.getIds(next);
                  // the replica answers one Boolean per key, in the order of the message's keys
                  final Id[] sentIds = sent.asArray();
                  logger.debug("REFRESH: SENDING REQUEST TO {} FOR IDSET {}", next, sent);


                  sendRequest(next, new GCRefreshMessage(getUID(), sent, getLocalNodeHandle(), next.getId()),
                              new NamedContinuation<Object, Exception>("GCRefresh to " + next, new StandardContinuation<Object, Object, Exception>(this) {
                    @Override
                    public void receiveResult(Object answers) {
                      if (answers instanceof Boolean[] booleans && booleans.length == sentIds.length) {
                        for (int k = 0; k < sentIds.length; k++)
                          outcome.merge(((GCId) sentIds[k]).id(), booleans[k], Boolean::logicalOr);
                      }
                      parent.receiveResult(null);
                    }
                  }));
                } else {
                  logger.debug("REFRESH: DONE SENDING REQUESTS, RECURSING");

                  refresh(ids, outcome, parent);
                }
              }

              @Override
              public void receiveException(Exception e) {
                logger.atDebug().log(() -> "GOT EXCEPTION " + e + " REFRESHING ITEMS - CONTINUING");
                receiveResult(null);
              }
            };

            send.receiveResult(null);
          }
        });
      }
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
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
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

          // make sure the policy allows the insert
          if (policy.allowInsert(imsg.getContent())) {
            Id theId = imsg.getContent().getId();
            if (theId == null) {
              logger.error("Error: null Id from {} from {} in {}", imsg.getContent(), imsg, this);
            }
            storage.getObject(theId, new StandardContinuation<Object, Object, Exception>(getResponseContinuation(msg)) {
              @Override
              public void receiveResult(Object o) {
                try {
                  // allow the object to check the insert, and then insert the data
                  GCPastContent content = (GCPastContent) imsg.getContent().checkInsert(imsg.getContent().getId(), (PastContent) o);
                  storage.store(content.getId(), content.getMetadata(imsg.getExpiration()), content, parent);
                } catch (PastException e) {
                  parent.receiveException(e);
                }
              }
            });
          } else {
            getResponseContinuation(msg).receiveResult(Boolean.valueOf(false));
          }
        }
        case GCRefreshMessage rmsg -> {
          final Iterator<GCId> i = Arrays.asList(rmsg.getKeys()).iterator();
          // filled by one step of the (sequential) continuation chain after the other
          final List<Object> result = new ArrayList<Object>();
          other += rmsg.getKeys().length;

          StandardContinuation<Object, Object, Exception> process = new StandardContinuation<Object, Object, Exception>(getResponseContinuation(msg)) {
            private boolean started;

            @Override
            public void receiveResult(Object o) {
              // one answer per key, in order, so the caller can match them up: the first call only starts the chain
              if (started)
                result.add(Boolean.TRUE.equals(o));
              started = true;

              if (i.hasNext()) {
                final GCId id = i.next();

                /* skip the object if we don't have it yet */
                if (storage.exists(id.id())) {
                  GCPastMetadata metadata = (GCPastMetadata) storage.getMetadata(id.id());

                  if (metadata != null) {
                    /* only allow the lifetime to be extended, otherwise skip */
                    if (metadata.expiration() < id.expiration()) {
                      storage.setMetadata(id.id(), metadata.setExpiration(id.expiration()), this);
                    } else {
                      /* already refreshed to a later expiration, which counts as success */
                      receiveResult(true);
                    }
                  } else {
                    storage.getObject(id.id(), new StandardContinuation<Object, Object, Exception>(this) {
                      @Override
                      public void receiveResult(Object o) {
                        storage.setMetadata(id.id(), ((GCPastContent) o).getMetadata(id.expiration()), parent);
                      }
                    });
                  }
                } else {
                  /* but first check and see if it's in the trash, so we can uncollect it */
                  if (trash != null) {
                    trash.getObject(id.id(), new StandardContinuation<Object, Object, Exception>(this) {
                      @Override
                      public void receiveResult(Object o) {
                        if ((o != null) && (o instanceof GCPastContent content)) {
                          logger.debug("GCREFRESH: Restoring object {} from trash!", id);

                          storage.store(id.id(), content.getMetadata(id.expiration()), content, new StandardContinuation<Boolean, Object, Exception>(parent) {
                            @Override
                            public void receiveResult(Boolean o) {
                              trash.unstore(id.id(), parent);
                            }
                          });
                        } else {
                          parent.receiveResult(false);
                        }
                      }
                    });
                  } else {
                    receiveResult(false);
                  }
                }
              } else {
                parent.receiveResult(result.toArray(new Boolean[0]));
              }
            }
          };

          process.receiveResult(null);
        }
        case GCLookupHandlesMessage lmsg -> {
          NodeHandleSet set = getEndpoint().neighborSet(lmsg.getMax());
          set.removeHandle(getLocalNodeHandle().getId());
          set.putHandle(getLocalNodeHandle());

          logger.trace("Returning neighbor set {} for lookup handles of id {} max {} at {}", set, lmsg.getId(), lmsg.getMax(), getEndpoint().getId());
          getResponseContinuation(msg).receiveResult(set);
        }
        case GCCollectMessage cmsg -> {
          // get all ids which expiration before now
          // ids stored without metadata have DEFAULT_EXPIRATION, which is never reached, so only the expired ones are collected
          collect(storage.scanMetadataValuesHead(new GCPastMetadata(getEnvironment().getTimeSource().currentTimeMillis())), new ListenerContinuation<Object, Exception>("Removal of expired ids", getEnvironment()));
        }
        case FetchHandleMessage fmsg -> {
          fetchHandles++;

          storage.getObject(fmsg.getId(), new StandardContinuation<Object, Object, Exception>(getResponseContinuation(msg)) {
            @Override
            public void receiveResult(Object o) {
              GCPastContent content = (GCPastContent) o;

              if (content != null) {
                logger.debug("Retrieved data for fetch handles of id {}", fmsg.getId());
                GCPastMetadata metadata = (GCPastMetadata) storage.getMetadata(fmsg.getId());

                if (metadata != null)
                  parent.receiveResult(content.getHandle(GCPastImpl.this, metadata.expiration()));
                else
                  parent.receiveResult(content.getHandle(GCPastImpl.this, DEFAULT_EXPIRATION));
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
    final Iterator<Id> i = map.keySet().iterator();

    Continuation<Object, Exception> remove = new StandardContinuation<Object, Boolean, Exception>(command) {
      @Override
      public void receiveResult(Object o) {
        if (i.hasNext()) {
          final Id gid = i.next();

          if (trash != null) {
            storage.getObject(gid, new StandardContinuation<Object, Object, Exception>(this) {
              @Override
              public void receiveResult(Object o) {
                if (o != null) {
                  trash.store(gid, storage.getMetadata(gid), o, new StandardContinuation<Boolean, Object, Exception>(parent) {
                    @Override
                    public void receiveResult(Boolean o) {
                      storage.unstore(gid, parent);
                    }
                  });
                } else {
                  // continue with the remove loop (parent), not this getObject continuation
                  storage.unstore(gid, parent);
                }
              }
            });
          } else {
            storage.unstore(gid, this);
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
    final GCId gcid = (GCId) id;

    if (gcid.expiration() < getEnvironment().getTimeSource().currentTimeMillis()) {
      command.receiveResult(Boolean.TRUE);
    } else if (storage.exists(gcid.id())) {
      GCPastMetadata metadata = (GCPastMetadata) storage.getMetadata(gcid.id());

      if (metadata == null) {
        storage.getObject(gcid.id(), new StandardContinuation<Object, Boolean, Exception>(command) {
          @Override
          public void receiveResult(Object o) {
            GCPastContent content = (GCPastContent) o;
            storage.setMetadata(content.getId(), content.getMetadata(gcid.expiration()), parent);
          }
        });
      } else if (metadata.expiration() < gcid.expiration()) {
        storage.setMetadata(gcid.id(), metadata.setExpiration(gcid.expiration()), command);
      } else {
        command.receiveResult(Boolean.TRUE);
      }
    } else {
      policy.fetch(gcid.id(), hint, backup, this, new StandardContinuation<Object, Boolean, Exception>(command) {
        @Override
        public void receiveResult(Object o) {
          if (o == null) {
            logger.warn("Could not fetch id {} - policy returned null in namespace {}", id, getInstance());
            parent.receiveResult(Boolean.valueOf(false));
          } else {
            // the peer that answered is not trusted: store only what was asked for
            if (!(o instanceof GCPastContent content) || !gcid.id().equals(content.getId())) {
              logger.warn("Dropping fetched replica of id {}: not GCPastContent for that id: {}", id, o);
              parent.receiveResult(Boolean.valueOf(false));
              return;
            }
            logger.trace("inserting replica of id {}", id);

            storage.getStorage().store(gcid.id(), content.getMetadata(gcid.expiration()), content, parent);
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
    super.remove(((GCId) id).id(), command);
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
    if (id instanceof GCId gcId)
      return storage.getStorage().exists(gcId.id());
    else
      return storage.getStorage().exists(id);
  }

  @Override
  public void existsInOverlay(Id id, Continuation<? super Boolean, Exception> command) {
    if (id instanceof GCId gcId) {
      super.existsInOverlay(gcId.id(), command);
    } else {
      super.existsInOverlay(id, command);
    }
  }

  @Override
  public void reInsert(final Id id, Continuation<? super Boolean, Exception> command) {
    if (id instanceof GCId gcId) {
      // what if the GCId's expiration is different than the metadata's?
      storage.getObject(gcId.id(), new StandardContinuation<Object, Boolean, Exception>(command) {
        @Override
        public void receiveResult(final Object o) {
          insert((PastContent)o, gcId.expiration(), new StandardContinuation<Boolean[], Boolean, Exception>(parent) {
            @Override
            public void receiveResult(Boolean[] result) {
              Boolean results[] = result;
              for (int i = 0; i < results.length; i++) {
                if (results[i].booleanValue()) {
                  parent.receiveResult(Boolean.TRUE);
                  return;
                }
              }
              parent.receiveResult(Boolean.FALSE);
            }
          });
        }
      });
    } else {
      GCPastMetadata metadata = (GCPastMetadata) storage.getMetadata(id);
      if (metadata == null) {
        // no metadata, just reinsert with no expiration
        super.reInsert(id, command);
      } else {
        reInsert(new GCId(id, metadata.expiration()), command);
      }
    }
  }

  protected class ReplicaMap {
    private final HashMap<NodeHandle, IdSet> map = new HashMap<NodeHandle, IdSet>();
    public void addReplica(NodeHandle handle, GCId id) {
      IdSet set = (IdSet) map.get(handle);

      if (set == null) {
        set = new GCIdSet(realFactory);
        map.put(handle, set);
      }

      set.addId(id);
    }

    public Iterator<NodeHandle> getReplicas() {
      return map.keySet().iterator();
    }

    public GCIdSet getIds(NodeHandle replica) {
      return (GCIdSet) map.get(replica);
    }
  }
}
