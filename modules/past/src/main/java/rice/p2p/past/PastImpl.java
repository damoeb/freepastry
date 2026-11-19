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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.slf4j.Logger;
import rice.Continuation;
import rice.Continuation.MultiContinuation;
import rice.Continuation.NamedContinuation;
import rice.Continuation.SimpleContinuation;
import rice.Continuation.StandardContinuation;
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
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
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
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.persistence.Cache;
import rice.persistence.LockManager;
import rice.persistence.LockManager;
import rice.persistence.StorageManager;

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
  private static final int SOCKET_TIMEOUT_MILLIS = 10000;



  // ----- STATIC FIELDS -----
  // the number of milliseconds to wait before declaring a message lost
  public final int MESSAGE_TIMEOUT;// = 30000;

  // the percentage of successful replica inserts in order to declare success
  public final double SUCCESSFUL_INSERT_THRESHOLD;// = 0.5;


  // ----- VARIABLE FIELDS -----

  // this application's endpoint
  private final Endpoint endpoint;

  // the storage manager used by this Past
  protected StorageManager storage;

  /** Builds the ids that content-hash objects are checked against. */
  private final IdFactory idFactory;

  // The trash can, or where objects should go once removed.  If null, they are deleted
  protected StorageManager trash;

  // The backup store, or location of over-replicated objects, helping PAST to better deal with churn
  protected Cache backup;

  // the replication factor for Past
  private final int replicationFactor;

  // the replica manager used by Past
  private final ReplicaFetchCoordinator replicaManager;

  private final LockManager lockManager;

  // the policy used for application-specific behavior
  protected PastPolicy policy;

  // the unique ids used by the messages sent across the wire
  private int id;

  // the hashtable of outstanding messages
  // (requests may be sent from application threads while the selector thread delivers responses and timeouts)
  private final Map<Integer, Continuation<?, Exception>> outstanding;

  // the hashtable of outstanding timer tasks
  private final Map<Integer, CancellableTask> timers;

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
  public PastImpl(Node node, StorageManager manager, int replicas, String instance) {
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
  public PastImpl(Node node, StorageManager manager, int replicas, String instance, PastPolicy policy) {
    this(node, manager, null, replicas, instance, policy, null);
  }


  protected class PastDeserializer implements MessageDeserializer {
    @Override
    public Message deserialize(InputBuffer buf, short type, int priority,
        NodeHandle sender) throws IOException {
      PastMessageType t = PastMessageType.find(type);
      if (t == null) {
        throw unknownType(type);
      }
      try {
        return switch (t) {
          case FETCH_HANDLE -> FetchHandleMessage.build(buf, endpoint, contentHandleDeserializer);
          case FETCH -> FetchMessage.build(buf, endpoint, contentDeserializer, contentHandleDeserializer);
          case INSERT -> InsertMessage.build(buf, endpoint, contentDeserializer);
          case LOOKUP_HANDLES -> LookupHandlesMessage.build(buf, endpoint);
          case LOOKUP -> LookupMessage.build(buf, endpoint, contentDeserializer);
          case GC_INSERT, GC_LOOKUP_HANDLES, GC_REFRESH -> deserializeGC(buf, t);
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
  public PastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, StorageManager trash) {
    this(node, manager, backup, replicas, instance, policy, trash, false);
  }

  /**
   *
   * @param useOwnSocket send all inserts/fetches over a socket (default is false)
   */
  public PastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, StorageManager trash, boolean useOwnSocket) {
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
  public PastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, StorageManager trash, SocketStrategy strategy) {
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

    this.id = Integer.MIN_VALUE;
    // concurrent, and like the Hashtables they replace they reject null keys and values
    this.outstanding = new ConcurrentHashMap<Integer, Continuation<?, Exception>>();
    this.timers = new ConcurrentHashMap<Integer, CancellableTask>();
    this.replicationFactor = replicas;

    this.replicaManager = buildReplicationManager(node, instance);

    this.lockManager = new LockManager(environment);

    this.endpoint.accept(new AppSocketReceiver() {

      @Override
      public void receiveSocket(AppSocket socket) {
        logger.debug("Received Socket from {}", socket);
        socket.register(true, false, SOCKET_TIMEOUT_MILLIS, this);
        endpoint.accept(this);
      }

      @Override
      public void receiveSelectResult(AppSocket socket, boolean canRead,
          boolean canWrite) {
        logger.trace("Reading from {}", socket);
        try {
          ByteBuffer[] bb = pendingSocketTransactions.get(socket);
          if (bb == null) {
            // this is a new message

            // read the size
            bb = new ByteBuffer[1];
            bb[0] = ByteBuffer.allocate(Integer.BYTES);
            if (socket.read(bb[0]) == -1) {
              close(socket);
              return;
            }

            // TODO: handle a partial read of the 4-byte size header
            byte[] sizeArr = bb[0].array();
            int size = MathUtils.byteArrayToInt(sizeArr);

            logger.trace("Found object of size {} from {}", size, socket);

            // allocate a buffer to store the object, save it in the pst
            bb[0] = ByteBuffer.allocate(size);
            pendingSocketTransactions.put(socket,bb);
          }

          // now we have a bb

          // read some bytes
          if (socket.read(bb[0]) == -1) {
            close(socket);
          }

          // deserialize or reregister
          if (bb[0].remaining() == 0) {
            // make sure to clear things up so we can keep receiving
            pendingSocketTransactions.remove(socket);

            logger.trace("bb[0].limit() {} bb[0].remaining() {} from {}", bb[0].limit(), bb[0].remaining(), socket);

            // deserialize the object
            SimpleInputBuffer sib = new SimpleInputBuffer(bb[0].array());

            short type = sib.readShort();

            PastMessage result = (PastMessage)endpoint.getDeserializer().deserialize(sib,type,(byte)0,null);
            deliver(null,result);

          }

          // there will be more data on the socket if we haven't received everything yet
          // need to register either way to be able to read from the sockets when they are closed remotely, could alternatively close early
          // cause we are currently only sending 1 message per socket, but it's better to just keep reading in case we one day reuse sockets
            socket.register(true, false, SOCKET_TIMEOUT_MILLIS, this);

            // no recursive call to read the next object: calling read when not ready throws an exception
        } catch (IOException ioe) {
          receiveException(socket, ioe);
        }
      }

      @Override
      public void receiveException(AppSocket socket, Exception e) {
        logger.warn("Error receiving message", e);
        close(socket);
      }

      void close(AppSocket socket) {
        if (socket == null) return;
        pendingSocketTransactions.remove(socket);
        socket.close();
      }

    });
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
   * Returns a new uid for a message
   *
   * @return A new id
   */
  protected synchronized int nextRequestId() {
    return id++;
  }

  /**
   * Returns a continuation which will respond to the given message.
   *
   * @return A new id
   */
  protected Continuation<Object, Exception> getResponseContinuation(final PastMessage msg) {
    logger.trace("Getting the Continuation to respond to the message {}", msg);
    final ContinuationMessage cmsg = (ContinuationMessage) msg;

    return new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object o) {
        cmsg.receiveResult(o);
        endpoint.route(null, cmsg, msg.getSource());
      }

      @Override
      public void receiveException(Exception e) {
        cmsg.receiveException(e);
        endpoint.route(null, cmsg, msg.getSource());
      }
    };
  }

  /**
   * Do like above, but use a socket
   */
  protected Continuation<Object, Exception> getFetchResponseContinuation(final PastMessage msg) {
    final ContinuationMessage cmsg = (ContinuationMessage) msg;

    return new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object o) {
        cmsg.receiveResult(o);
        PastContent content = (PastContent)o;
        if (socketStrategy.sendAlongSocket(SocketStrategy.TYPE_FETCH, content)) {
          sendViaSocket(msg.getSource(), cmsg, null);
        } else {
          endpoint.route(null, cmsg, msg.getSource());
        }
      }

      @Override
      public void receiveException(Exception e) {
        cmsg.receiveException(e);
        endpoint.route(null, cmsg, msg.getSource());
      }
    };
  }

  /**
   * AppSocket -> ByteBuffer[]
   *
   * Used for receiving the objects.
   */
  WeakHashMap<AppSocket, ByteBuffer[]> pendingSocketTransactions = new WeakHashMap<AppSocket, ByteBuffer[]>();

  private void sendViaSocket(final NodeHandle handle, final PastMessage m, final Continuation<?, Exception> c) {
    if (c != null) {
      CancellableTask timer = endpoint.scheduleMessage(new MessageLostMessage(m.getUID(), getLocalNodeHandle(), null, m, handle), MESSAGE_TIMEOUT);
      insertPending(m.getUID(), timer, c);
    }

    // create a bb[] to be written
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      sob.writeInt(0); // place holder for size...
      sob.writeShort(m.getType());
      m.serialize(sob);
    } catch (IOException ioe) {
      if (c != null) c.receiveException(ioe);
    }

    // add the size back to the beginning...
    int size = sob.getWritten()-Integer.BYTES; // remove the size of the size :)
    logger.trace("Sending size of {} to {} to send {}", size, handle, m);
    byte[] bytes = sob.getBytes();
    MathUtils.intToByteArray(size,bytes,0);

    // prepare the bytes for writing
    final ByteBuffer[] bb = new ByteBuffer[1];
    bb[0] = ByteBuffer.wrap(bytes, 0, sob.getWritten()); // the whole thing


    logger.debug("Opening socket to {} to send {}", handle, m);
    endpoint.connect(handle, new AppSocketReceiver() {

      @Override
      public void receiveSocket(AppSocket socket) {
        logger.trace("Opened socket to {}:{} to send {}", handle, socket, m);
        socket.register(false, true, SOCKET_TIMEOUT_MILLIS, this);
      }

      @Override
      public void receiveSelectResult(AppSocket socket, boolean canRead,
          boolean canWrite) {
        logger.trace("Writing to {}:{} to send {}", handle, socket, m);


        try {
          socket.write(bb[0]);
        } catch (IOException ioe) {
          if (c != null)
            c.receiveException(ioe);
          else
            logger.warn("Error sending {}", m, ioe);
          return; // don't continue to try to send
        }
        if (bb[0].remaining() > 0) {
          socket.register(false, true, SOCKET_TIMEOUT_MILLIS, this);
        } else {
          socket.close();
        }
      }

      @Override
      public void receiveException(AppSocket socket, Exception e) {
        if (c != null) c.receiveException(e);
      }
    },
    SOCKET_TIMEOUT_MILLIS);
  }


  /**
   * Sends a request message across the wire, and stores the appropriate
   * continuation.
   *
   * @param id The destination id
   * @param message The message to send.
   * @param command The command to run once a result is received
   */
  protected void sendRequest(Id id, PastMessage message, Continuation<?, Exception> command) {
    sendRequest(id, message, null, command);
  }

  /**
   * Sends a request message across the wire, and stores the appropriate
   * continuation.
   *
   * @param handle The node handle to send directly too
   * @param message The message to send.
   * @param command The command to run once a result is received
   */
  protected void sendRequest(NodeHandle handle, PastMessage message, Continuation<?, Exception> command) {
    sendRequest(null, message, handle, command);
  }

  /**
   * Sends a request message across the wire, and stores the appropriate
   * continuation.  Sends the message using the provided handle as a hint.
   *
   * @param id The destination id
   * @param message The message to send.
   * @param hint The first hop hint
   * @param command The command to run once a result is received
   */
  protected void sendRequest(Id id, PastMessage message, NodeHandle hint, Continuation<?, Exception> command) {
    logger.atTrace().log(() -> "Sending request message " + message + " {"+message.getUID()+"} to id " + id + " via " + hint);
    CancellableTask timer = endpoint.scheduleMessage(new MessageLostMessage(message.getUID(), getLocalNodeHandle(), id, message, hint), MESSAGE_TIMEOUT);
    insertPending(message.getUID(), timer, command);
    endpoint.route(id, message, hint);
  }

  /**
   * Loads the provided continuation into the pending table
   *
   * @param uid The id of the message
   * @param command The continuation to run
   */
  private void insertPending(int uid, CancellableTask timer, Continuation<?, Exception> command) {
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
  private Continuation<Object, Exception> removePending(int uid) {
    logger.trace("Removing and returning continuation {} from pending table", uid);
    CancellableTask timer = timers.remove(Integer.valueOf(uid));

    if (timer != null)
      timer.cancel();

    // the response arrives untyped from the wire; each pending continuation expects the response type of its request
    @SuppressWarnings("unchecked")
    Continuation<Object, Exception> command = (Continuation<Object, Exception>) outstanding.remove(Integer.valueOf(uid));
    return command;
  }

  /**
   * Handles the response message from a request.
   *
   * @param message The message that arrived
   */
  private void handleResponse(PastMessage message) {
    logger.debug("handling reponse message {} from the request", message);
    Continuation<Object, Exception> command = removePending(message.getUID());

    if (command != null) {
      message.returnResponse(command, environment, instance);
    }
  }

  /**
   * Internal method which returns the handles to an object.  It first checks to see if
   * the handles can be determined locally, and if so, returns.  Otherwise, it sends a
   * LookupHandles messsage out to find out the nodes.
   *
   * @param id The id to fetch the handles for
   * @param max The maximum number of handles to return
   * @param command The command to call with the result (NodeHandle[])
   */
  protected void getHandles(Id id, final int max, Continuation<? super NodeHandleSet, Exception> command) {
    NodeHandleSet set = endpoint.replicaSet(id, max);

    if (set.size() == max) {
      command.receiveResult(set);
    } else {
      sendRequest(id, new LookupHandlesMessage(nextRequestId(), id, max, getLocalNodeHandle(), id), new StandardContinuation<NodeHandleSet, NodeHandleSet, Exception>(command) {
        @Override
        public void receiveResult(NodeHandleSet replicas) {

          // check to make sure we've fetched the correct number of replicas
          // the deal with this is for the small ring.  If you are requesting
          // 4 nodes, but the ring is only 3, you are only going to get 3
          // Note: this is still kind of funky, because the replicationFactor+1
          // argument is kind of weird, but I don't know how to get it right
          // -Jeff 1/24/07
          if (Math.min(max,endpoint.replicaSet(endpoint.getLocalNodeHandle().getId(), replicationFactor+1).size()) > replicas.size())
            parent.receiveException(new PastException("Only received " + replicas.size() + " replicas - cannot insert as we know about more nodes."));
          else
            parent.receiveResult(replicas);
        }
      });
    }
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
   * @param command The command to run once done
   */
  public void cache(final PastContent content, final Continuation<? super Boolean, Exception> command) {
    logger.trace("Inserting PastContent object {} into cache", content);

    if ((content != null) && ! content.isMutable() && isAuthentic(null, content))
      storage.cache(content.getId(), null, content, command);
    else
      command.receiveResult(Boolean.valueOf(true));
  }

  /**
   * Internal method which actually performs an insert for a given object.
   * Here so that subclasses can override the types of insert messages
   * which are sent across the wire.
   *
   * @param id The id of the object to insert
   * @param builder The object which builds the messages
   * @param command The command to call once done
   * @param useSocket Whether to send the insert messages over a socket
   */
  protected void doInsert(final Id id, final MessageBuilder builder, Continuation<? super Boolean[], Exception> command, final boolean useSocket) {
    // first, we get all of the replicas for this id
    getHandles(id, replicationFactor+1, new StandardContinuation<NodeHandleSet, Boolean[], Exception>(command) {
      @Override
      public void receiveResult(NodeHandleSet replicas) {
        logger.trace("Received replicas {} for id {}", replicas, id);

        // then we send inserts to each replica and wait for at least
        // threshold * num to return successfully
        MultiContinuation<Boolean[]> multi = new MultiContinuation<Boolean[]>(parent, replicas.size()) {
          @Override
          public boolean isDone() throws Exception {
            int numSuccess = 0;
            for (int i=0; i<haveResult.length; i++)
              if (haveResult[i] && Boolean.TRUE.equals(result[i]))
                numSuccess++;

            if (numSuccess >= (SUCCESSFUL_INSERT_THRESHOLD * haveResult.length))
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
            Boolean[] b = new Boolean[result.length];
            for (int i=0; i<b.length; i++)
              b[i] = Boolean.valueOf((result[i] == null) || Boolean.TRUE.equals(result[i]));

            return b;
          }
        };

        for (int i=0; i<replicas.size(); i++) {
          NodeHandle handle = replicas.getHandle(i);
          PastMessage m = builder.buildMessage();
          Continuation<Object, Exception> c = new NamedContinuation<Object, Exception>("InsertMessage to " + replicas.getHandle(i) + " for " + id, multi.getSubContinuation(i));
          if (useSocket) {
            sendViaSocket(handle, m, c);
          } else {
            sendRequest(handle, m, c);
          }
        }
      }
    });
  }


  // ----- PAST METHODS -----

  /**
   * Inserts an object with the given ID into this instance of Past.
   * Asynchronously returns a PastException to command, if the
   * operation was unsuccessful.  If the operation was successful, a
   * Boolean[] is returned representing the responses from each of
   * the replicas which inserted the object.
   *
   * @param obj the object to be inserted
   * @param command Command to be performed when the result is received
   */
  @Override
  public void insert(final PastContent obj, final Continuation<? super Boolean[], Exception> command) {
    logger.trace("Inserting the object {} with the id {}", obj, obj.getId());

    logger.atTrace().log(() -> " Inserting data of class " + obj.getClass().getName() + " under " + obj.getId().toStringFull());

    doInsert(obj.getId(), () -> new InsertMessage(nextRequestId(), obj, getLocalNodeHandle(), obj.getId()), new StandardContinuation<Boolean[], Boolean[], Exception>(command) {
      @Override
      public void receiveResult(final Boolean[] array) {
        cache(obj, new SimpleContinuation()  {
          @Override
          public void receiveResult(Object o) {
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
    socketStrategy.sendAlongSocket(SocketStrategy.TYPE_INSERT, obj));
  }

  /**
   * Retrieves the object stored in this instance of Past with the
   * given ID.  Asynchronously returns a PastContent object as the
   * result to the provided Continuation, or a PastException. This
   * method is provided for convenience; its effect is identical to a
   * lookupHandles() and a subsequent fetch() to the handle that is
   * nearest in the network.
   *
   * The client must authenticate the object. In case of failure, an
   * alternate replica of the object can be obtained via
   * lookupHandles() and fetch().
   *
   * This method is not safe if the object is immutable and storage
   * nodes are not trusted. In this case, clients should used the
   * lookUpHandles method to obtains the handles of all primary
   * replicas and determine which replica is fresh in an
   * application-specific manner.
   *
   * @param id the key to be queried
   * @param command Command to be performed when the result is received
   */
  @Override
  public void lookup(final Id id, final Continuation<? super PastContent, Exception> command) {
    lookup(id, true, command);
  }

  /**
   * Method which performs the same as lookup(), but allows the
   * callee to specify if the data should be cached.
   *
   *
   * @param id the key to be queried
   * @param cache Whether or not the data should be cached
   * @param command Command to be performed when the result is received
   */
  @Override
  public void lookup(final Id id, final boolean cache, final Continuation<? super PastContent, Exception> command) {
    logger.atTrace().log(() -> " Performing lookup on " + id.toStringFull());

    storage.getObject(id, new StandardContinuation<Object, PastContent, Exception>(command) {
      @Override
      public void receiveResult(Object o) {
        if (o != null && isAuthentic(id, (PastContent) o)) {
          command.receiveResult((PastContent) o);
        } else {
          lookupViaMessage(id, cache, command);
        }
      }
    });
  }

  /**
   * Second step of a lookup: asks the node the key routes to. If that node has no authentic object, or the message
   * failed, all replicas are tried, just in case; this may fail too, but at least we tried.
   */
  private void lookupViaMessage(final Id id, final boolean cache, final Continuation<? super PastContent, Exception> command) {
    sendRequest(id, new LookupMessage(nextRequestId(), id, getLocalNodeHandle(), id), new NamedContinuation<PastContent, Exception>("LookupMessage for " + id, command) {
      @Override
      public void receiveResult(final PastContent o) {
        if (o != null && isAuthentic(id, o)) {
          cacheAndReply(o, cache, command);
        } else {
          lookupViaReplicas(id, cache, command);
        }
      }

      @Override
      public void receiveException(Exception e) {
        receiveResult(null);
      }
    });
  }

  /**
   * Last step of a lookup: the first replica that has a handle is fetched, and the lookup answers null if there is
   * none, which means the object doesn't exist anywhere.
   */
  private void lookupViaReplicas(final Id id, final boolean cache, final Continuation<? super PastContent, Exception> command) {
    lookupHandles(id, replicationFactor + 1, new StandardContinuation<PastContentHandle[], PastContent, Exception>(command) {
      @Override
      public void receiveResult(PastContentHandle[] handles) {
        for (PastContentHandle handle : handles) {
          if (handle != null) {
            fetch(handle, new StandardContinuation<PastContent, PastContent, Exception>(command) {
              @Override
              public void receiveResult(final PastContent o) {
                cacheAndReply(o, cache, command);
              }
            });

            return;
          }
        }

        // there were no replicas of the object
        command.receiveResult(null);
      }
    });
  }

  /** Tries to cache a looked-up object locally for future use, and answers with it whether or not caching worked. */
  private void cacheAndReply(final PastContent o, final boolean cache, final Continuation<? super PastContent, Exception> command) {
    if (!cache) {
      command.receiveResult(o);
      return;
    }

    cache(o, new SimpleContinuation() {
      @Override
      public void receiveResult(Object object) {
        command.receiveResult(o);
      }

      @Override
      public void receiveException(Exception e) {
        logger.warn("Caching after lookup failed, continuing", e);
        command.receiveResult(o);
      }
    });
  }

  /**
   * Retrieves the handles of up to max replicas of the object stored
   * in this instance of Past with the given ID.  Asynchronously
   * returns an array of PastContentHandles as the result to the
   * provided Continuation, or a PastException.
   *
   * Each replica handle is obtained from a different primary storage
   * root for the the given key. If max exceeds the replication factor
   * r of this Past instance, only r replicas are returned.
   *
   * This method will return a PastContentHandle[] array containing all
   * of the handles.
   *
   * @param id the key to be queried
   * @param max the maximal number of replicas requested
   * @param command Command to be performed when the result is received
   */
  @Override
  public void lookupHandles(final Id id, int max, final Continuation<? super PastContentHandle[], Exception> command) {
    logger.debug("Retrieving handles of up to {} replicas of the object stored in Past with id {}", max, id);

    logger.atTrace().log(() -> "Fetching up to " + max + " handles of " + id.toStringFull());

    getHandles(id, max, new StandardContinuation<NodeHandleSet, PastContentHandle[], Exception>(command) {
      @Override
      public void receiveResult(NodeHandleSet replicas) {
        logger.trace("Receiving replicas {} for lookup Id {}", replicas, id);

        MultiContinuation<PastContentHandle[]> multi = new MultiContinuation<PastContentHandle[]>(parent, replicas.size()) {
          @Override
          public PastContentHandle[] getResult() {
            PastContentHandle[] p = new PastContentHandle[result.length];

            for (int i=0; i<result.length; i++)
              if (result[i] instanceof PastContentHandle)
                p[i] = (PastContentHandle) result[i];

            return p;
          }
        };

        for (int i=0; i<replicas.size(); i++)
          lookupHandle(id, replicas.getHandle(i), multi.getSubContinuation(i));
      }
    });
  }

  /**
   * Retrieves the handle for the given object stored on the requested
   * node.  Asynchronously returns a PostContentHandle (or null) to
   * the provided continuation.
   *
   * @param id the key to be queried
   * @param handle The node on which the handle is requested
   * @param command Command to be performed when the result is received
   */
  @Override
  public void lookupHandle(Id id, NodeHandle handle, Continuation<? super PastContentHandle, Exception> command) {
    logger.debug("Retrieving handle for id {} from node {}", id, handle);

    sendRequest(handle, new FetchHandleMessage(nextRequestId(), id, getLocalNodeHandle(), handle.getId()),
                new NamedContinuation<PastContentHandle, Exception>("FetchHandleMessage to " + handle + " for " + id, command));
  }

  /**
   * Retrieves the object associated with a given content handle.
   * Asynchronously returns a PastContent object as the result to the
   * provided Continuation, or a PastException.
   *
   * The client must authenticate the object. In case of failure, an
   * alternate replica can be obtained using a different handle obtained via
   * lookupHandles().
   *
   * @param handle the handle of the object to fetch
   * @param command Command to be performed when the result is received
   */
  @Override
  public void fetch(PastContentHandle handle, Continuation<? super PastContent, Exception> command) {
    logger.debug("Retrieving object associated with content handle {}", handle);

    logger.atTrace().log(() -> "Fetching object under id " + handle.getId().toStringFull() +  " on " + handle.getNodeHandle());

    NodeHandle han = handle.getNodeHandle();
    // the node that holds the object may be forging it
    Continuation<PastContent, Exception> checked = new StandardContinuation<PastContent, PastContent, Exception>(command) {
      @Override
      public void receiveResult(PastContent content) {
        if (isAuthentic(handle.getId(), content)) {
          parent.receiveResult(content);
        } else {
          parent.receiveException(new PastException("Fetched object is not the one requested: " + handle.getId()));
        }
      }
    };
    sendRequest(han, new FetchMessage(nextRequestId(), handle, getLocalNodeHandle(), han.getId()),
                new NamedContinuation<PastContent, Exception>("FetchMessage to " + handle.getNodeHandle() + " for " + handle.getId(), checked));
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
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
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
      handleResponse((PastMessage) message);
    } else {
      logger.info("Received message {} with destination {}", message, id);

      switch (msg) {
        case InsertMessage imsg -> deliverInsert(imsg);
        case LookupMessage lmsg -> {
          // if the data is here, we send the reply; the requester caches it (see lookup())
          storage.getObject(lmsg.getId(), new StandardContinuation<Object, Object, Exception>(getResponseContinuation(lmsg)) {
            @Override
            public void receiveResult(Object o) {
              logger.debug("Received object {} for id {}", o, lmsg.getId());

              // send result back
              parent.receiveResult(o);
            }
          });
        }
        case LookupHandlesMessage lmsg -> {
          NodeHandleSet set = endpoint.replicaSet(lmsg.getId(), lmsg.getMax());
          logger.trace("Returning replica set {} for lookup handles of id {} max {} at {}", set, lmsg.getId(), lmsg.getMax(), endpoint.getId());
          getResponseContinuation(msg).receiveResult(set);
        }
        case FetchMessage fmsg -> {
          Continuation<Object, Exception> c;
          c = getFetchResponseContinuation(msg); // has to be special to determine how to send the message

          storage.getObject(fmsg.getHandle().getId(), c);
        }
        case FetchHandleMessage fmsg -> {
          fetchHandles++;

          storage.getObject(fmsg.getId(), new StandardContinuation<Object, Object, Exception>(getResponseContinuation(msg)) {
            @Override
            public void receiveResult(Object o) {
              PastContent content = (PastContent) o;

              if (content != null) {
                logger.debug("Retrieved data for fetch handles of id {}", fmsg.getId());
                parent.receiveResult(content.getHandle(PastImpl.this));
              } else {
                parent.receiveResult(null);
              }
            }
          });
        }
        case MessageLostMessage _, GCRefreshMessage _, GCCollectMessage _ ->
          logger.error("ERROR - Received message {}of unknown type.", msg);
      }
    }
  }

  /**
   * Handles an insert at the destination node: the payload must be authentic and the policy must allow the insert,
   * before {@link #storeInsert(InsertMessage)} stores it.
   */
  protected void deliverInsert(InsertMessage imsg) {
    if (!isAuthentic(null, imsg.getContent())) {
      getResponseContinuation(imsg).receiveException(new PastException("Insert refused: the payload does not hash to the id"));
    } else if (policy.isInsertAllowed(imsg.getContent())) {
      storeInsert(imsg);
    } else {
      getResponseContinuation(imsg).receiveResult(Boolean.valueOf(false));
    }
  }

  /** Stores an authentic, allowed insert under the lock of its id. Subclasses override this for another store step. */
  protected void storeInsert(final InsertMessage imsg) {
    inserts++;
    final Id msgid = imsg.getContent().getId();

    lockManager.lock(msgid, new StandardContinuation<Object, Object, Exception>(getResponseContinuation(imsg)) {
      @Override
      public void receiveResult(Object result) {
        checkAndStore(imsg, content -> null, new StandardContinuation<Boolean, Object, Exception>(parent) {
          @Override
          public void receiveResult(Boolean result) {
            getResponseContinuation(imsg).receiveResult(result);
            lockManager.unlock(msgid);
          }
        });
      }
    });
  }

  /**
   * Lets the inserted object check the insert against what is stored, and then stores what it returns, with the
   * metadata that the function derives from it; a refusal by the object goes to the continuation as an exception.
   */
  protected void checkAndStore(final InsertMessage imsg, final Function<PastContent, Object> metadata, Continuation<? super Boolean, Exception> command) {
    final Id id = imsg.getContent().getId();

    storage.getObject(id, new StandardContinuation<Object, Boolean, Exception>(command) {
      @Override
      public void receiveResult(Object o) {
        try {
          PastContent content = imsg.getContent().checkInsert(id, (PastContent) o);
          storage.store(id, metadata.apply(content), content, parent);
        } catch (PastException e) {
          parent.receiveException(e);
        }
      }
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

    policy.fetch(id, hint, backup, this, new StandardContinuation<Object, Boolean, Exception>(command) {
      @Override
      public void receiveResult(Object o) {
        if (o == null) {
          logger.warn("Could not fetch id {} - policy returned null in namespace {}", id, instance);
          parent.receiveResult(Boolean.valueOf(false));
        } else {
          logger.trace("inserting replica of id {}", id);

          // the peer that answered is not trusted: store only what was asked for, never another id's object
          if (!(o instanceof PastContent content)) {
            logger.warn("Dropping fetched replica of id {}: not PastContent but {}", id, o.getClass().getName());
            parent.receiveResult(Boolean.valueOf(false));
          } else if (!id.equals(content.getId())) {
            logger.warn("Dropping fetched replica of id {}: it carries the id {}", id, content.getId());
            parent.receiveResult(Boolean.valueOf(false));
          } else {
            storeReplica(id, content, parent);
          }
        }
      }
    });
  }

  /**
   * Stores a replica fetched for the id, letting the content check it against what is stored locally, as an
   * insert does. A refusal drops the replica.
   */
  private void storeReplica(final Id id, final PastContent content, final Continuation<? super Boolean, Exception> command) {
    storage.getObject(id, new StandardContinuation<Object, Boolean, Exception>(command) {
      @Override
      public void receiveResult(Object existing) {
        try {
          PastContent checked = content.checkInsert(id, (PastContent) existing);
          storage.getStorage().store(id, null, checked, parent);
        } catch (PastException e) {
          logger.warn("Dropping fetched replica of id {}: {}", id, e.getMessage());
          parent.receiveResult(Boolean.valueOf(false));
        }
      }
    });
  }

  /**
   * This upcall is to notify the client that the given id can be safely removed
   * from the storage.  The client may choose to perform advanced behavior, such
   * as caching the object, or may simply delete it.
   *
   * @param id The id to remove
   */
  @Override
  public void remove(final Id id, Continuation<? super Boolean, Exception> command) {
    if (backup != null) {
      storage.getObject(id, new StandardContinuation<Object, Boolean, Exception>(command) {
        @Override
        public void receiveResult(Object o) {
          backup.cache(id, storage.getMetadata(id), o, new StandardContinuation<Boolean, Boolean, Exception>(parent) {
            @Override
            public void receiveResult(Boolean o) {
              storage.unstore(id, parent);
            }
          });
        }
      });
    } else {
      storage.unstore(id, command);
    }
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
  public void existsInOverlay(Id id, Continuation<? super Boolean, Exception> command) {
    lookupHandles(id, replicationFactor+1, new StandardContinuation<Object[], Boolean, Exception>(command) {
      @Override
      public void receiveResult(Object[] result) {
        Object results[] = result;
        for (int i = 0; i< results.length; i++) {
          if (results[i] instanceof PastContentHandle) {
            parent.receiveResult(Boolean.TRUE);
            return;
          }
        }
        parent.receiveResult(Boolean.FALSE);
      }
    });
  }

  @Override
  public void reInsert(Id id, Continuation<? super Boolean, Exception> command) {
    storage.getObject(id, new StandardContinuation<Object, Boolean, Exception>(command) {
      @Override
      public void receiveResult(final Object o) {
        insert((PastContent)o, new StandardContinuation<Boolean[], Boolean, Exception>(parent) {
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
  public StorageManager getStorageManager() {
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
