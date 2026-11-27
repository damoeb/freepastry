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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import rice.Continuation;
import rice.Futures;
import rice.Outcome;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
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
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.past.PastPolicy.DefaultPastPolicy;
import rice.p2p.past.gc.ExpiringId;
import rice.p2p.past.messaging.ContinuationMessage;
import rice.p2p.past.messaging.FetchHandleMessage;
import rice.p2p.past.messaging.FetchMessage;
import rice.p2p.past.messaging.GCCollectMessage;
import rice.p2p.past.messaging.GCRefreshMessage;
import rice.p2p.past.messaging.InsertMessage;
import rice.p2p.past.messaging.LookupHandlesMessage;
import rice.p2p.past.messaging.LookupMessage;
import rice.p2p.past.messaging.MessageLostMessage;
import rice.p2p.past.messaging.PastMessage;
import rice.p2p.past.messaging.PastMessageType;
import rice.p2p.past.rawserialization.DefaultSocketStrategy;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.rawserialization.PastContentHandleDeserializer;
import rice.p2p.past.rawserialization.SocketStrategy;
import rice.p2p.replication.Replication;
import rice.p2p.replication.manager.ReplicaFetchCoordinator;
import rice.p2p.replication.manager.ReplicaFetchCoordinatorImpl;
import rice.p2p.replication.manager.ReplicationManagerClient;
import rice.persistence.Cache;
import rice.persistence.CachedStorage;
import rice.persistence.LockManager;

/**
 * This is an implementation of the Past interface.
 *
 * @version $Id: PastImpl.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
@SuppressWarnings("unchecked")
public class PastImpl implements Past, Application, ReplicationManagerClient {
  /** Timeout for opening, reading and writing the app sockets that carry large messages. */



  // ----- STATIC FIELDS -----
  // the number of milliseconds to wait before declaring a message lost
  public final int MESSAGE_TIMEOUT;// = 30000;

  // the percentage of successful replica inserts in order to declare success
  public final double SUCCESSFUL_INSERT_THRESHOLD;// = 0.5;


  // ----- VARIABLE FIELDS -----

  // this application's endpoint
  private final Endpoint endpoint;

  // the storage manager used by this Past
  protected CachedStorage storage;

  /** Builds the ids that content-hash objects are checked against. */
  private final IdFactory idFactory;

  // The trash can, or where objects should go once removed.  If null, they are deleted
  protected CachedStorage trash;

  // The backup store, or location of over-replicated objects, helping PAST to better deal with churn
  protected Cache backup;

  // the replication factor for Past
  private final int replicationFactor;

  // the replica manager used by Past
  private final ReplicaFetchCoordinator replicaManager;

  private final LockManager lockManager;

  // the policy used for application-specific behavior
  protected PastPolicy policy;

  // sends and receives the messages that go over a socket of their own
  private final PastSocketTransport socketTransport;

  // the lookup of an object by its id
  private final PastLookups lookups;

  // the fetch of a replica that the replication asks for
  private final ReplicaFetcher replicaFetcher;

  // the insert of an object
  private final PastInserts insertOperations;

  // the requests that wait for a response
  private final PastPendingRequests pending;

  // the instance name we are running with
  private final String instance;

  // debug variables
  protected int inserts = 0;
  protected int fetchHandles = 0;
  protected int other = 0;

  private final Environment environment;
  protected Logger logger;

  protected PastContentDeserializer contentDeserializer;
  private PastContentHandleDeserializer contentHandleDeserializer;

  protected SocketStrategy socketStrategy;

  /**
   * Constructor for Past, using the default policy
   *
   * @param node The node below this Past implementation
   * @param manager The storage manager to be used by Past
   * @param replicas The number of object replicas
   * @param instance The unique instance name of this Past
   */
  public PastImpl(Node node, CachedStorage manager, int replicas, String instance) {
    this(node, manager, replicas, instance, new DefaultPastPolicy());
  }

  /**
   * Constructor for Past
   *
   * @param node The node below this Past implementation
   * @param manager The storage manager to be used by Past
   * @param replicas The number of object replicas
   * @param instance The unique instance name of this Past
   */
  public PastImpl(Node node, CachedStorage manager, int replicas, String instance, PastPolicy policy) {
    this(node, manager, null, replicas, instance, policy, null);
  }


  protected class PastDeserializer implements MessageDeserializer {
    @Override
    public Message deserialize(InputBuffer buf, short type, int priority,
        NodeHandle sender) throws IOException {
      PastMessageType messageType = PastMessageType.find(type);
      if (messageType == null) {
        throw unknownType(type);
      }
      try {
        return switch (messageType) {
          case FETCH_HANDLE -> FetchHandleMessage.build(buf, endpoint, contentHandleDeserializer);
          case FETCH -> FetchMessage.build(buf, endpoint, contentDeserializer, contentHandleDeserializer);
          case INSERT -> InsertMessage.build(buf, endpoint, contentDeserializer);
          case LOOKUP_HANDLES -> LookupHandlesMessage.build(buf, endpoint);
          case LOOKUP -> LookupMessage.build(buf, endpoint, contentDeserializer);
          case GC_INSERT, GC_LOOKUP_HANDLES, GC_REFRESH -> deserializeGC(buf, messageType);
          // local messages, never sent
          case MESSAGE_LOST, GC_COLLECT -> throw unknownType(type);
        };
      } catch (IOException e) {
        logger.atError().log(() -> "Exception in deserializer in "+PastImpl.this.endpoint.toString()+":"+instance+" "+e);
        throw e;
      }
    }

    /**
     * Reads a ExpiringPast message; plain Past does not know them.
     */
    protected Message deserializeGC(InputBuffer buf, PastMessageType type) throws IOException {
      throw unknownType(type.id());
    }

    protected IOException unknownType(short type) {
      return new IOException("Unknown type:"+type+" in "+PastImpl.this.toString());
    }
  }
  public PastImpl(Node node, CachedStorage manager, Cache backup, int replicas, String instance, PastPolicy policy, CachedStorage trash) {
    this(node, manager, backup, replicas, instance, policy, trash, false);
  }

  /**
   *
   * @param useOwnSocket send all inserts/fetches over a socket (default is false)
   */
  public PastImpl(Node node, CachedStorage manager, Cache backup, int replicas, String instance, PastPolicy policy, CachedStorage trash, boolean useOwnSocket) {
    this(node, manager, backup, replicas, instance, policy, trash, new DefaultSocketStrategy(useOwnSocket));
  }

  /**
   * Constructor for Past
   *
   * @param node The node below this Past implementation
   * @param manager The storage manager to be used by Past
   * @param replicas The number of object replicas
   * @param instance The unique instance name of this Past
   */
  public PastImpl(Node node, CachedStorage manager, Cache backup, int replicas, String instance, PastPolicy policy, CachedStorage trash, SocketStrategy strategy) {
    this.environment = node.getEnvironment();
    this.idFactory = node.getIdFactory();
    logger = environment.getLogManager().getLogger(getClass(), instance);
    PastConfig config = PastConfig.fromParameters(environment.getParameters());
    MESSAGE_TIMEOUT = config.messageTimeout();
    SUCCESSFUL_INSERT_THRESHOLD = config.successfulInsertThreshold();
    this.socketStrategy = strategy;
    this.storage = manager;
    this.backup = backup;
    this.contentDeserializer = PastContentDeserializer.NONE;
    this.contentHandleDeserializer = PastContentHandleDeserializer.CONTENT_HASH;
    this.endpoint = node.buildEndpoint(this, instance);
    this.endpoint.setDeserializer(new PastDeserializer());
    this.policy = policy;
    this.instance = instance;
    this.trash = trash;

    this.pending = new PastPendingRequests(logger);
    this.replicaFetcher = new ReplicaFetcher(this, logger);
    this.lookups = new PastLookups(this, logger);
    this.insertOperations = new PastInserts(this, endpoint, logger);
    this.replicationFactor = replicas;

    this.replicaManager = buildReplicationManager(node, instance);

    this.lockManager = new LockManager(environment);

    this.socketTransport = new PastSocketTransport(this, endpoint, pending, logger, config.maxSocketMessageBytes());
    socketTransport.acceptSockets();
    endpoint.register();
  }

  @Override
  public String toString() {
    if (endpoint == null) return super.toString();
    return "PastImpl["+endpoint.getApplicationInstanceName()+"]";
  }

  public Environment getEnvironment() {
    return environment;
  }

  // ----- INTERNAL METHODS -----

  /**
   * Internal method which builds the replication manager.  Can be overridden by subclasses.
   *
   * @param node The node to base the RM off of
   * @param instance The instance name to use
   * @return The replication manager, ready for use
   */
  protected ReplicaFetchCoordinator buildReplicationManager(Node node, String instance) {
    return new ReplicaFetchCoordinatorImpl(node, this, replicationFactor, instance);
  }

  /**
   * Returns the endpoint associated with the Past - ONLY FOR TESTING - DO NOT USE
   *
   * @return The endpoint
   */
  public Endpoint getEndpoint() {
    return endpoint;
  }

  /**
   * Returns a new id for a request: a random 64-bit number, so that a node that did not see the request cannot
   * guess it and answer it.
   *
   * @return A new id
   */
  protected long nextRequestId() {
    return pending.nextRequestId();
  }

  /**
   * Returns a continuation which will respond to the given message.
   *
   * @return A new id
   */
  protected Continuation<Object, Exception> getResponseContinuation(final PastMessage msg) {
    logger.trace("Getting the Continuation to respond to the message {}", msg);
    final ContinuationMessage continuationMessage = (ContinuationMessage) msg;

    return new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        continuationMessage.receiveResult(result);
        endpoint.route(null, continuationMessage, msg.getSource());
      }

      @Override
      public void receiveException(Exception e) {
        continuationMessage.receiveException(e);
        endpoint.route(null, continuationMessage, msg.getSource());
      }
    };
  }

  /**
   * Answers the given request with the outcome of a future, once it completes: its value as the response, or its
   * failure (unwrapped) as the exception. The response is the request message itself, a {@link ContinuationMessage}.
   *
   * @param msg the request
   * @param response its outcome
   */
  protected void respond(PastMessage msg, CompletionStage<?> response) {
    Futures.forward(response, getResponseContinuation(msg), getEnvironment());
  }

  /**
   * Do like above, but use a socket
   */
  protected Continuation<Object, Exception> getFetchResponseContinuation(final PastMessage msg) {
    final ContinuationMessage continuationMessage = (ContinuationMessage) msg;

    return new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        continuationMessage.receiveResult(result);
        PastContent content = (PastContent)result;
        if (socketStrategy.sendAlongSocket(SocketStrategy.TYPE_FETCH, content)) {
          socketTransport.send(msg.getSource(), continuationMessage, null);
        } else {
          endpoint.route(null, continuationMessage, msg.getSource());
        }
      }

      @Override
      public void receiveException(Exception e) {
        continuationMessage.receiveException(e);
        endpoint.route(null, continuationMessage, msg.getSource());
      }
    };
  }

  /**
   * Sends a request over a socket of its own; see {@link #sendRequest(Id, PastMessage, NodeHandle)} for the future.
   */
  <T> CompletableFuture<T> sendViaSocket(final NodeHandle handle, final PastMessage msg) {
    CompletableFuture<Object> response = newRequest();
    socketTransport.send(handle, msg, response);
    return castResponse(response);
  }

  /**
   * Sends a request message across the wire; see {@link #sendRequest(Id, PastMessage, NodeHandle)}.
   *
   * @param id The destination id
   * @param message The message to send.
   * @return completed by the response
   */
  protected <T> CompletableFuture<T> sendRequest(Id id, PastMessage message) {
    return sendRequest(id, message, null);
  }

  /**
   * Sends a request message across the wire; see {@link #sendRequest(Id, PastMessage, NodeHandle)}.
   *
   * @param handle The node handle to send directly too
   * @param message The message to send.
   * @return completed by the response
   */
  protected <T> CompletableFuture<T> sendRequest(NodeHandle handle, PastMessage message) {
    return sendRequest(null, message, handle);
  }

  /**
   * Sends a request message across the wire, using the provided handle as a hint, and waits for its response.
   *
   * @param id The destination id
   * @param message The message to send.
   * @param hint The first hop hint
   * @return completed on the selector thread with the response, or failed with the exception the responder sent
   *     (a {@link PastRemoteException}), with a {@link PastException} if no response came within the message
   *     timeout, or with an {@code EnvironmentDestroyedException}; its type is the response type of the message
   */
  protected <T> CompletableFuture<T> sendRequest(Id id, PastMessage message, NodeHandle hint) {
    logger.atTrace().log(() -> "Sending request message " + message + " {"+message.getUID()+"} to id " + id + " via " + hint);
    CompletableFuture<Object> response = newRequest();
    CancellableTask timer = endpoint.scheduleMessage(new MessageLostMessage(message.getUID(), getLocalNodeHandle(), id, message, hint), MESSAGE_TIMEOUT);
    // a request sent to a node may be answered by that node only; a routed one by any node on its route
    pending.insert(message, id == null ? hint.getId() : null, timer, response);
    endpoint.route(id, message, hint);
    return castResponse(response);
  }

  /** A future for the response of a request, failed when the environment is destroyed. */
  private CompletableFuture<Object> newRequest() {
    return environment.track(new CompletableFuture<Object>());
  }

  /** The response arrives untyped from the wire; the requester knows its type from the message it sent. */
  @SuppressWarnings("unchecked")
  private static <T> CompletableFuture<T> castResponse(CompletableFuture<Object> response) {
    return (CompletableFuture<T>) (CompletableFuture<?>) response;
  }

  /**
   * Handles the response message from a request: it completes the request only if it may answer it, see
   * {@link PastPendingRequests}. The local report that a request was lost may answer any request.
   *
   * @param message The message that arrived
   * @param sender the node it arrived from, or null if it was created here or the endpoint cannot tell
   */
  private void handleResponse(PastMessage message, @Nullable NodeHandle sender) {
    logger.debug("handling reponse message {} from {}", message, sender);
    CompletableFuture<Object> request = message instanceof MessageLostMessage && sender == null
        ? pending.remove(message.getUID())
        : pending.removeAnswered(message, sender);

    if (request != null) {
      message.returnResponse(request, environment, instance);
    }
  }

  /**
   * Internal method which returns the handles to an object.  It first checks to see if
   * the handles can be determined locally, and if so, returns.  Otherwise, it sends a
   * LookupHandles messsage out to find out the nodes.
   *
   * @param id The id to fetch the handles for
   * @param max The maximum number of handles to return
   * @return completed with the replica set
   */
  protected CompletableFuture<NodeHandleSet> getHandles(Id id, final int max) {
    return insertOperations.getHandles(id, max);
  }

  /**
   * Tells whether an object that came from a node we do not trust may be believed. Only content-hash objects can
   * be told: their payload has to hash to their id, and that id has to be the requested one.
   *
   * @param requested the id the object was asked for, or null if not known
   * @param content the object, or null
   * @return false if the object is a content-hash object that is not what it claims to be
   */
  protected boolean isAuthentic(Id requested, PastContent content) {
    if (content instanceof ContentHashPastContent hashed) {
      if (!hashed.verifyContentHash(idFactory)) {
        logger.warn("Rejecting content-hash object {}: its payload does not hash to its id", hashed.getId());
        return false;
      }
      Id wanted = requested instanceof ExpiringId gcid ? gcid.id() : requested;
      if (wanted != null && !wanted.equals(hashed.getId())) {
        logger.warn("Rejecting content-hash object {}: {} was asked for", hashed.getId(), wanted);
        return false;
      }
    }
    return true;
  }

  /**
   * Method which inserts the given object into the cache
   *
   * @param content The content to cache
   * @return completed once done, with true if nothing was to be cached
   */
  public CompletableFuture<Boolean> cache(final PastContent content) {
    logger.trace("Inserting PastContent object {} into cache", content);

    if ((content != null) && ! content.isMutable() && isAuthentic(null, content))
      return storage.cache(content.getId(), null, content);
    return CompletableFuture.completedFuture(Boolean.valueOf(true));
  }

  /**
   * Internal method which actually performs an insert for a given object.
   * Here so that subclasses can override the types of insert messages
   * which are sent across the wire.
   *
   * @param id The id of the object to insert
   * @param builder The object which builds the messages
   * @param useSocket Whether to send the insert messages over a socket
   * @return completed once enough replicas stored the object, see {@link #insert(PastContent)}
   */
  protected CompletableFuture<Boolean[]> doInsert(final Id id, final MessageBuilder builder, final boolean useSocket) {
    return insertOperations.doInsert(id, builder, useSocket);
  }

  // ----- PAST METHODS -----

  /**
   * Inserts an object with the given ID into this instance of Past: the insert is sent to the replicas of the id,
   * and is done once at least SUCCESSFUL_INSERT_THRESHOLD of them stored it.
   *
   * @param obj the object to be inserted
   * @return one Boolean per replica, or failed with a PastException if too few replicas stored the object
   */
  @Override
  public CompletableFuture<Boolean[]> insert(final PastContent obj) {
    return insertOperations.insert(obj);
  }

  /**
   * Retrieves the object stored in this instance of Past with the given ID, and caches it locally.
   *
   * This method is not safe if the object is immutable and storage
   * nodes are not trusted. In this case, clients should used the
   * lookUpHandles method to obtains the handles of all primary
   * replicas and determine which replica is fresh in an
   * application-specific manner.
   *
   * @param id the key to be queried
   * @return the object, or null if no node has it
   */
  @Override
  public CompletableFuture<PastContent> lookup(final Id id) {
    return lookup(id, true);
  }

  /**
   * Method which performs the same as lookup(), but allows the
   * callee to specify if the data should be cached.
   *
   * @param id the key to be queried
   * @param cache Whether or not the data should be cached
   * @return the object, or null if no node has it
   */
  @Override
  public CompletableFuture<PastContent> lookup(final Id id, final boolean cache) {
    return lookups.lookup(id, cache);
  }

  /**
   * Retrieves the handles of up to max replicas of the object stored
   * in this instance of Past with the given ID.
   *
   * Each replica handle is obtained from a different primary storage
   * root for the the given key. If max exceeds the replication factor
   * r of this Past instance, only r replicas are returned.
   *
   * @param id the key to be queried
   * @param max the maximal number of replicas requested
   * @return one entry per replica asked, null where it has no handle or its request failed
   */
  @Override
  public CompletableFuture<PastContentHandle[]> lookupHandles(final Id id, int max) {
    logger.debug("Retrieving handles of up to {} replicas of the object stored in Past with id {}", max, id);

    logger.atTrace().log(() -> "Fetching up to " + max + " handles of " + id.toStringFull());

    return getHandles(id, max).thenCompose(replicas -> {
      logger.trace("Receiving replicas {} for lookup Id {}", replicas, id);

      List<CompletableFuture<PastContentHandle>> handles = new ArrayList<CompletableFuture<PastContentHandle>>(replicas.size());
      for (int i=0; i<replicas.size(); i++)
        handles.add(lookupHandle(id, replicas.getHandle(i)));
      return Futures.collect(handles);
    }).thenApply(PastImpl::handlesOf);
  }

  /** The handles the replicas answered with, null for a replica without one or whose request failed. */
  private static PastContentHandle[] handlesOf(List<Outcome<PastContentHandle>> outcomes) {
    PastContentHandle[] handles = new PastContentHandle[outcomes.size()];

    for (int i=0; i<handles.length; i++)
      if (outcomes.get(i) instanceof Outcome.Success<PastContentHandle> success)
        handles[i] = success.value();

    return handles;
  }

  /**
   * Retrieves the handle for the given object stored on the requested node.
   *
   * @param id the key to be queried
   * @param handle The node on which the handle is requested
   * @return the handle, or null if the node has no such object
   */
  @Override
  public CompletableFuture<PastContentHandle> lookupHandle(Id id, NodeHandle handle) {
    logger.debug("Retrieving handle for id {} from node {}", id, handle);

    return sendRequest(handle, new FetchHandleMessage(nextRequestId(), id, getLocalNodeHandle(), handle.getId()));
  }

  /**
   * Retrieves the object associated with a given content handle.
   *
   * The client must authenticate the object. In case of failure, an
   * alternate replica can be obtained using a different handle obtained via
   * lookupHandles().
   *
   * @param handle the handle of the object to fetch
   * @return the object, or failed with a PastException if it is a content-hash object that is not the one requested
   */
  @Override
  public CompletableFuture<PastContent> fetch(PastContentHandle handle) {
    logger.debug("Retrieving object associated with content handle {}", handle);

    logger.atTrace().log(() -> "Fetching object under id " + handle.getId().toStringFull() +  " on " + handle.getNodeHandle());

    NodeHandle nodeHandle = handle.getNodeHandle();
    return this.<PastContent>sendRequest(nodeHandle, new FetchMessage(nextRequestId(), handle, getLocalNodeHandle(), nodeHandle.getId()))
        .thenCompose(content -> {
          // the node that holds the object may be forging it
          if (isAuthentic(handle.getId(), content)) {
            return CompletableFuture.completedFuture(content);
          }
          return CompletableFuture.failedFuture(new PastException("Fetched object is not the one requested: " + handle.getId()));
        });
  }

  /**
   * get the nodeHandle of the local Past node
   *
   * @return the nodehandle
   */
  @Override
  public NodeHandle getLocalNodeHandle() {
    return endpoint.getLocalNodeHandle();
  }

  /**
   * Returns the number of replicas used in this Past
   *
   * @return the number of replicas for each object
   */
  @Override
  public int getReplicationFactor() {
    return replicationFactor;
  }


  // ----- COMMON API METHODS -----

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
    Message internal;
    try {
      internal = message.getMessage(endpoint.getDeserializer());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (internal instanceof LookupMessage lmsg) {
      Id id = lmsg.getId();

      // if it is a request, look in the cache
      if (! lmsg.isResponse()) {
        logger.trace("Lookup message {} is a request; look in the cache", lmsg);
        if (storage.exists(id)) {
          // deliver the message, which will do what we want
          logger.debug("Request for {} satisfied locally - responding", id);
          deliver(endpoint.getId(), lmsg);
          return false;
        }
      }
    } else if (internal instanceof LookupHandlesMessage lmsg) {

      if (! lmsg.isResponse()) {
        if (endpoint.replicaSet(lmsg.getId(), lmsg.getMax()).size() == lmsg.getMax()) {
          logger.debug("Hijacking lookup handles request for {}", lmsg.getId());

          deliver(endpoint.getId(), lmsg);
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Delivers a message and tells which node it arrived from: a response then completes only the request it may
   * answer, see {@link PastPendingRequests}. A request is handled by {@link #deliver(Id, Message)}.
   *
   * @param id The destination id of the message
   * @param message The message being sent
   * @param previousHop the node it arrived from, null if it was created here or the endpoint cannot tell
   */
  @Override
  public void deliver(Id id, Message message, @Nullable NodeHandle previousHop) {
    if (message instanceof PastMessage msg && msg.isResponse()) {
      handleResponse(msg, previousHop);
    } else {
      deliver(id, message);
    }
  }

  /**
   * This method is called on the application at the destination node
   * for the given id. A response that arrives here has no known sender and is checked by its id and type only.
   *
   * @param id The destination id of the message
   * @param message The message being sent
   */
  @Override
  public void deliver(Id id, Message message) {
    final PastMessage msg = (PastMessage) message;

    if (msg.isResponse()) {
      handleResponse(msg, null);
    } else {
      logger.info("Received message {} with destination {}", message, id);

      switch (msg) {
        case InsertMessage imsg -> deliverInsert(imsg);
        case LookupMessage lmsg -> {
          // if the data is here, we send the reply; the requester caches it (see lookup())
          respond(lmsg, storage.getObject(lmsg.getId()).thenApply(result -> {
            logger.debug("Received object {} for id {}", result, lmsg.getId());
            return result;
          }));
        }
        case LookupHandlesMessage lmsg -> {
          NodeHandleSet nodeHandleSet = endpoint.replicaSet(lmsg.getId(), lmsg.getMax());
          logger.trace("Returning replica set {} for lookup handles of id {} max {} at {}", nodeHandleSet, lmsg.getId(), lmsg.getMax(), endpoint.getId());
          getResponseContinuation(msg).receiveResult(nodeHandleSet);
        }
        case FetchMessage fmsg ->
          // has to be special to determine how to send the message
          Futures.forward(storage.getObject(fmsg.getHandle().getId()), getFetchResponseContinuation(msg), getEnvironment());
        case FetchHandleMessage fmsg -> {
          fetchHandles++;

          respond(msg, storage.getObject(fmsg.getId()).thenApply(result -> {
            PastContent content = (PastContent) result;

            if (content == null) {
              return null;
            }
            logger.debug("Retrieved data for fetch handles of id {}", fmsg.getId());
            return content.getHandle(PastImpl.this);
          }));
        }
        case MessageLostMessage _, GCRefreshMessage _, GCCollectMessage _ ->
          logger.error("ERROR - Received message {}of unknown type.", msg);
      }
    }
  }

  /**
   * Handles an insert at the destination node: the payload must be authentic, this node must be a replica of the id
   * and the policy must allow the insert, before {@link #storeInsert(InsertMessage)} stores it.
   */
  protected void deliverInsert(InsertMessage insertMessage) {
    if (!isAuthentic(null, insertMessage.getContent())) {
      getResponseContinuation(insertMessage).receiveException(new PastException("Insert refused: the payload does not hash to the id"));
    } else if (!isReplicaOf(insertMessage.getContent().getId())) {
      logger.info("Refusing insert of {} from {}: this node is not one of its replicas", insertMessage.getContent().getId(), insertMessage.getSource());
      getResponseContinuation(insertMessage).receiveResult(false);
    } else if (policy.isInsertAllowed(insertMessage.getContent())) {
      storeInsert(insertMessage);
    } else {
      getResponseContinuation(insertMessage).receiveResult(Boolean.valueOf(false));
    }
  }

  /**
   * Whether this node is one of the replicas of the id as its own leaf set sees them: the replication factor plus one
   * nodes closest to the id, the ones an insert is sent to. A node on the route of the replica-set request could
   * otherwise answer it with colluders, and the insert would land on nodes that are no replicas.
   */
  private boolean isReplicaOf(Id id) {
    if (id == null) {
      return false;
    }
    NodeHandleSet replicas = endpoint.replicaSet(id, replicationFactor + 1);
    return replicas != null && replicas.containsHandleFor(endpoint.getId());
  }

  /** Stores an authentic, allowed insert under the lock of its id. Subclasses override this for another store step. */
  protected void storeInsert(final InsertMessage insertMessage) {
    inserts++;
    final Id msgid = insertMessage.getContent().getId();

    // the lock is released however checkAndStore ends; a failed lock was never taken
    respond(insertMessage, lockManager.lock(msgid).thenCompose(_ ->
        checkAndStore(insertMessage, content -> null).whenComplete((_, _) -> lockManager.unlock(msgid))));
  }

  /**
   * Lets the inserted object check the insert against what is stored, and then stores what it returns, with the
   * metadata that the function derives from it.
   *
   * @return whether it was stored, or failed with the PastException of a refusal by the object
   */
  protected CompletableFuture<Boolean> checkAndStore(final InsertMessage insertMessage, final Function<PastContent, Object> metadata) {
    final Id id = insertMessage.getContent().getId();

    return storage.getObject(id).thenCompose(result -> {
      PastContent content;
      try {
        content = insertMessage.getContent().checkInsert(id, (PastContent) result);
      } catch (PastException e) {
        return CompletableFuture.failedFuture(e);
      }
      return storage.store(id, metadata.apply(content), content);
    });
  }

  /**
   * This method is invoked to inform the application that the given node
   * has either joined or left the neighbor set of the local node, as the set
   * would be returned by the neighborSet call.
   *
   * @param handle The handle that has joined/left
   * @param joined Whether the node has joined or left
   */
  @Override
  public void update(NodeHandle handle, boolean joined) {
  }


  // ----- REPLICATION MANAGER METHODS -----

  /**
   * This upcall is invoked to tell the client to fetch the given id; the manager waits for the returned future
   * before continuing.
   *
   * @param id The id to fetch
   * @return whether the replica was fetched and stored
   */
  @Override
  public CompletableFuture<Boolean> fetch(final Id id, NodeHandle hint) {
    return replicaFetcher.fetch(id, hint);
  }

  /**
   * This upcall is to notify the client that the given id can be safely removed
   * from the storage.  The client may choose to perform advanced behavior, such
   * as caching the object, or may simply delete it.
   *
   * @param id The id to remove
   * @return whether the id was unstored
   */
  @Override
  public CompletableFuture<Boolean> remove(final Id id) {
    if (backup != null) {
      return storage.getObject(id)
          .thenCompose(storedObject -> backup.cache(id, storage.getMetadata(id), storedObject))
          .thenCompose(_ -> storage.unstore(id));
    }
    return storage.unstore(id);
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
    return storage.getStorage().scan(range);
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
    return storage.getStorage().exists(id);
  }

  @Override
  public CompletableFuture<Boolean> existsInOverlay(Id id) {
    return lookupHandles(id, replicationFactor+1).thenApply(PastImpl::isAnyHandle);
  }

  @Override
  public CompletableFuture<Boolean> reInsert(Id id) {
    return storage.getObject(id).thenCompose(storedObject -> insert((PastContent)storedObject)).thenApply(PastImpl::isAnyStored);
  }

  /** Whether a replica answered with a handle. */
  private static Boolean isAnyHandle(PastContentHandle[] handles) {
    for (PastContentHandle handle : handles) {
      if (handle != null) {
        return Boolean.TRUE;
      }
    }
    return Boolean.FALSE;
  }

  /**
   * Whether a replica stored a reinserted object.
   *
   * @param replicaSuccesses the result of an insert
   * @return true if any replica stored it
   */
  protected static Boolean isAnyStored(Boolean[] replicaSuccesses) {
    for (Boolean isStored : replicaSuccesses) {
      if (isStored.booleanValue()) {
        return Boolean.TRUE;
      }
    }
    return Boolean.FALSE;
  }

  // ----- UTILITY METHODS -----

  /**
   * Returns the replica manager for this Past instance.  Should *ONLY* be used
   * for testing.  Messing with this will cause unknown behavior.
   *
   * @return This Past's replica manager
   */
  public Replication getReplication() {
    return replicaManager.getReplication();
  }

  /**
   * Returns this Past's storage manager. Should *ONLY* be used
   * for testing.  Messing with this will cause unknown behavior.
   *
   * @return This Past's storage manager.
   */
  public CachedStorage getStorageManager() {
    return storage;
  }

  /**
   * Class which builds a message
   */
  public interface MessageBuilder {
    public PastMessage buildMessage();
  }

  public String getInstance() {
    return instance;
  }

  @Override
  public void setContentDeserializer(PastContentDeserializer deserializer) {
    contentDeserializer = deserializer;
  }

  public PastContentDeserializer getContentDeserializer() {
    return contentDeserializer;
  }

  @Override
  public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer) {
    contentHandleDeserializer = deserializer;
  }

}
