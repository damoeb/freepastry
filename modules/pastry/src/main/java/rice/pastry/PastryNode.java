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
package rice.pastry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.proximity.ProximityListener;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import rice.Continuation;
import rice.Destructable;
import rice.Executable;
import rice.environment.Environment;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.commonapi.exception.AppNotRegisteredException;
import rice.p2p.commonapi.exception.AppSocketException;
import rice.p2p.commonapi.exception.NoReceiverAvailableException;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.client.PastryAppl;
import rice.pastry.join.JoinProtocol;
import rice.pastry.leafset.InitiateLeafSetMaintenance;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.MessageDispatch;
import rice.pastry.messaging.PRawMessage;
import rice.pastry.routing.InitiateRouteSetMaintenance;
import rice.pastry.routing.RouteSetProtocol;
import rice.pastry.routing.Router;
import rice.pastry.routing.RoutingTable;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;
import rice.pastry.transport.PMessageReceiptImpl;
import rice.pastry.transport.SocketAdapter;

import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_ALIVE;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD;

/**
 * A Pastry node is single entity in the pastry network.
 *
 * @version $Id: PastryNode.java 4664 2009-02-23 18:06:07Z jeffh $
 *
 * @author Andrew Ladd
 */

public class PastryNode implements
    rice.p2p.commonapi.Node, Destructable, NodeHandleFactory<NodeHandle>,
    LivenessProvider<NodeHandle>, ProximityProvider<NodeHandle>,
    ProximityListener<NodeHandle>,
    TransportLayerCallback<NodeHandle, RawMessage>,
    LivenessListener<NodeHandle> {

  public static final byte CONNECTION_OK = 0;
  public static final byte CONNECTION_NO_APP = 1;
  public static final byte CONNECTION_NO_ACCEPTOR = 2;


  private final Id myNodeId;

  private Environment myEnvironment;

  private MessageDispatch myMessageDispatch;

  private LeafSet leafSet;

  private RoutingTable routeSet;

  protected NodeHandle localhandle;

  @SuppressWarnings("JdkObsolete") // apps register from any thread; Vector's synchronization guards the list
  private final Vector<PastryAppl> apps = new Vector<PastryAppl>();

  private final Logger logger;

  ReadyStrategy readyStrategy;

  private volatile boolean joinFailed = false;

  /**
   * Signalled when the node becomes ready or fails to join, see {@link #awaitReadyOrJoinFailed}.
   */
  private final ReentrantLock readyLock = new ReentrantLock();
  private final Condition readyOrJoinFailed = readyLock.newCondition();

  private boolean isDestroyed = false;

  private Router router;

  /**
   * Used to deserialize NodeHandles
   */
  private NodeHandleFactory<? extends NodeHandle> handleFactory;

  /**
   * Call initiateJoin on this class.
   */
  private JoinProtocol joiner;

  /**
   * Call boot on this class.
   */
  private Bootstrapper<?> bootstrapper;

  /**
   * The top level transport layer.
   */
  private TransportLayer<NodeHandle, RawMessage> tl;

  private ProximityProvider<NodeHandle> proxProvider;

  /** certified mode: decides which node ids count as authenticated; null accepts every id */
  private @Nullable Predicate<NodeHandle> identityPolicy;

  /**
   * Constructor, with NodeId. Need to set the node's ID before this node is
   * inserted as localHandle.localNode.
   */
  private final PastryConfig config;

  public PastryNode(Id id, Environment e) {
    myEnvironment = e;
    myNodeId = id;
    config = PastryConfig.fromParameters(e.getParameters());

    readyStrategy = getDefaultReadyStrategy();

    logger = e.getLogManager().getLogger(getClass(), null);
    e.addDestructable(this);
  }

  public void boot(Object o) {
    if (o == null) {
      getBootstrapper().boot(Collections.emptyList());
    } else {
      if (o instanceof Collection) {
        boot((Collection<?>)o);
      } else {
        getBootstrapper().boot(Collections.singleton(o));
      }
    }
  }

  public void boot(Collection<?> o2) {
    ArrayList<Object> o = new ArrayList<Object>(o2);
    while (o.remove(null)); // remove all null
    getBootstrapper().boot(o);
  }

  ReadyStrategy defaultReadyStrategy = null;
  /**
   * Simple Ready Strategy
   */
  public ReadyStrategy getDefaultReadyStrategy() {
    if (defaultReadyStrategy != null) return defaultReadyStrategy;
    defaultReadyStrategy = new ReadyStrategy() {
      private boolean ready = false;

      @Override
      public void setReady(boolean r) {
        if (r != ready) {
          synchronized(PastryNode.this) {
            ready = r;
          }
          notifyReadyObservers();
        }
      }

      @Override
      public boolean isReady() {
        return ready;
      }

      @Override
      public void start() {
        setReady(true);
      }

      @Override
      public void stop() {
        // don't need to do any initialization
      }
    };
    return defaultReadyStrategy;
  }

  public void setReadyStrategy(ReadyStrategy rs) {
    readyStrategy = rs;
  }

  /**
   * Combined accessor method for various members of PastryNode. These are
   * generated by node factories, and assigned here.
   *
   * The transport elements are assigned via {@link #setSocketElements}, called by
   * {@link rice.pastry.transport.TransportPastryNodeFactory}.
   *
   * @param lh
   *          Node handle corresponding to this node.
   * @param md
   *          Message dispatcher.
   * @param ls
   *          Leaf set.
   * @param rt
   *          Routing table.
   * @param router
   *          Router.
   */
  public void setElements(NodeHandle lh, MessageDispatch md, LeafSet ls, RoutingTable rt, Router router) {
    localhandle = lh;
    setMessageDispatch(md);
    leafSet = ls;
    routeSet = rt;
    this.router = router;
  }

  public void setJoinProtocols(Bootstrapper<?> boot, JoinProtocol joinP, LeafSetProtocol leafsetP, RouteSetProtocol routeP) {
    this.bootstrapper = boot;
    this.joiner = joinP;
  }


  @Override
  public rice.p2p.commonapi.NodeHandle getLocalNodeHandle() {
    return localhandle;
  }

  @Override
  public Environment getEnvironment() {
    return myEnvironment;
  }

  /**
   * @return the overlay geometry and protocol version, read from the parameters when the node was
   *         built
   */
  public PastryConfig getConfig() {
    return config;
  }

  public NodeHandle getLocalHandle() {
    return localhandle;
  }

  public Id getNodeId() {
    return myNodeId;
  }

  public boolean isReady() {
    return readyStrategy.isReady();
  }

  /**
   * FOR TESTING ONLY - DO NOT USE!
   */
  public MessageDispatch getMessageDispatch() {
    return myMessageDispatch;
  }

  public void setMessageDispatch(MessageDispatch md) {
    myMessageDispatch = md;
    addDestructable(myMessageDispatch);
  }

  public Destructable addDestructable(Destructable d) {
    destructables.add(d);
    return d;
  }

  /**
   * Overridden by derived classes, and invoked when the node has joined
   * successfully. This should probably be abstract, but maybe in a later
   * version.
   *
   * @param state
   *          true when the node is ready, false when not
   */
  public void nodeIsReady(boolean state) {

  }

  /**
   * Marks this node ready; same as {@code setReady(true)}.
   */
  public void setReady() {
    setReady(true);
  }

  public void setReady(boolean ready) {
    readyStrategy.setReady(ready);
  }

  @Override
  public NodeHandle coalesce(NodeHandle newHandle) {
    logger.trace("coalesce({})", newHandle);
    return coalesce(handleFactory, newHandle);
  }

  /**
   * The factory only accepts its own handle type; this cast is what the raw call used to do.
   */
  @SuppressWarnings("unchecked")
  private static <NH extends NodeHandle> NH coalesce(NodeHandleFactory<NH> factory, NodeHandle newHandle) {
    return factory.coalesce((NH)newHandle);
  }

  /**
   * This variable makes it so notifyReady() is only called on the apps once.
   * Deprecating
   */
  private boolean neverBeenReady = true;

  public void notifyReadyObservers() {
    assert getEnvironment().getSelectorManager().isSelectorThread() : "a node is only set ready on the selector thread";

    // It is possible to have the setReady() invoked more than once if the
    // message denoting the termination of join protocol is duplicated.
    boolean ready = readyStrategy.isReady();
    logger.info("PastryNode.notifyReadyObservers({})", ready);

    if (ready) {
      nodeIsReady(true);

      notifyPastryNodeListeners(Boolean.valueOf(true));

      if (neverBeenReady) {
        // notify applications
        // we iterate over private copy to allow addition of new apps in the
        // context of notifyReady()
        List<PastryAppl> tmpApps = new ArrayList<PastryAppl>(apps);
        for (PastryAppl pastryAppl : tmpApps)
          pastryAppl.notifyReady();
        neverBeenReady = false;
      }

      // signal any apps that might be waiting for the node to get ready
      signalReadyOrJoinFailed();
    } else {
      nodeIsReady(false);
      notifyPastryNodeListeners(Boolean.valueOf(false));

    }
  }

  public LeafSet getLeafSet() {
    return leafSet;
  }

  public RoutingTable getRoutingTable() {
    return routeSet;
  }

  public void addLeafSetListener(NodeSetListener listener) {
    leafSet.addNodeSetListener(listener);
  }
  public void deleteLeafSetListener(NodeSetListener listener) {
    leafSet.deleteNodeSetListener(listener);
  }

  public void addRouteSetListener(NodeSetListener listener) {
    routeSet.addNodeSetListener(listener);
  }

  public void removeRouteSetListener(NodeSetListener listener) {
    routeSet.removeNodeSetListener(listener);
  }

  /****************** PastryNodeListener **********/

  /**
   * Registered listeners.  Notified in reverse order of registration.
   */
  private final ArrayList<PastryNodeListener> pastryNodeListeners = new ArrayList<PastryNodeListener>();

  /**
   * Register to be notified when the node becomes ready (or not) or fails to join.
   * Has no effect if the listener is already registered.
   *
   * @param listener the listener
   */
  public void addPastryNodeListener(PastryNodeListener listener) {
    if (listener == null) throw new NullPointerException();
    synchronized (pastryNodeListeners) {
      if (!pastryNodeListeners.contains(listener)) pastryNodeListeners.add(listener);
    }
  }

  /**
   * It is safe to call this from within a listener callback.
   *
   * @param listener the listener
   */
  public void removePastryNodeListener(PastryNodeListener listener) {
    synchronized (pastryNodeListeners) {
      pastryNodeListeners.remove(listener);
    }
  }

  /**
   * @param event Boolean (ready) or JoinFailedException
   */
  private void notifyPastryNodeListeners(Object event) {
    PastryNodeListener[] l;
    synchronized (pastryNodeListeners) {
      l = pastryNodeListeners.toArray(new PastryNodeListener[pastryNodeListeners.size()]);
    }
    for (int i = l.length-1; i >= 0; i--) {
      if (event instanceof Boolean b) {
        l[i].pastryNodeReady(this, b.booleanValue());
      } else if (event instanceof JoinFailedException joinFailedException) {
        l[i].pastryNodeJoinFailed(this, joinFailedException);
      }
    }
  }

  /**
   * Certified mode: sets the test that tells whether a node handle's id is authenticated. The routing table and the
   * leaf set admit only such handles, and application messages sent directly to this node are delivered only if
   * their sender passes. The test is called on the selector thread. Null (the default) accepts every id.
   */
  public void setIdentityPolicy(@Nullable Predicate<NodeHandle> policy) {
    identityPolicy = policy;
    getLeafSet().setAdmission(policy);
    getRoutingTable().setAdmission(policy);
  }

  /** @return false in certified mode if the handle's id is not authenticated (yet) */
  public boolean acceptsIdentity(@Nullable NodeHandle handle) {
    return handle == null || identityPolicy == null || identityPolicy.test(handle);
  }

  /**
   * Opens a connection to the node and closes it as soon as it is established. With a TLS layer and a bind
   * strategy that checks the peer's certificate, this is what authenticates the node's id.
   *
   * @param onDone called on the selector thread with true if the connection was established (the peer's certificate
   *     was accepted) and with false if it was not
   */
  public void probe(NodeHandle handle, Consumer<Boolean> onDone) {
    tl.openSocket(handle, new SocketCallback<NodeHandle>() {
      @Override
      public void receiveResult(SocketRequestHandle<NodeHandle> cancellable, P2PSocket<NodeHandle> sock) {
        sock.close();
        onDone.accept(Boolean.TRUE);
      }

      @Override
      public void receiveException(SocketRequestHandle<NodeHandle> s, Exception ex) {
        logger.debug("probe of {} failed", handle, ex);
        onDone.accept(Boolean.FALSE);
      }
    }, null);
  }

  /**
   * message receiver interface. Messages and maintenance timers all run on the
   * selector thread; the lock only matters for callers on other threads.
   */
  public synchronized void receiveMessage(Message msg) {
    if (isDestroyed) return;
    logger.debug("receiveMessage({})", msg);
    myMessageDispatch.dispatchMessage(msg);
  }

  /**
   * Registers a message receiver with this Pastry node.
   *
   * @param address
   *          the address that the receiver will be at.
   * @param receiver
   *          the message receiver.
   */
  public void registerReceiver(int address,
      PastryAppl receiver) {
    logger.debug("registerReceiver({},{}):{}", address, receiver, receiver.getDeserializer());
    myMessageDispatch.registerReceiver(address, receiver);
  }

  /**
   * Registers an application with this pastry node.
   *
   * @param app
   *          the application
   */

  public void registerApp(PastryAppl app) {
    if (isReady())
      app.notifyReady();
    apps.add(app);
  }

  @Override
  public String toString() {
    return "PastryNode"+localhandle;
  }

  // Common API Support

  @Override
  public rice.p2p.commonapi.Endpoint buildEndpoint(
      rice.p2p.commonapi.Application application, String instance) {
    return new rice.pastry.commonapi.PastryEndpoint(this, application, instance);
  }

  /**
   * Returns the Id of this node
   *
   * @return This node's Id
   */
  @Override
  public rice.p2p.commonapi.Id getId() {
    return getNodeId();
  }

  /**
   * Returns a factory for Ids specific to this node's protocol.
   *
   * @return A factory for creating Ids.
   */
  @Override
  public rice.p2p.commonapi.IdFactory getIdFactory() {
    return new rice.pastry.commonapi.PastryIdFactory(getEnvironment());
  }

  /**
   * Schedules a job for processing on the dedicated processing thread, should
   * one exist. CPU intensive jobs, such as encryption, erasure encoding, or
   * bloom filter creation should never be done in the context of the underlying
   * node's thread, and should only be done via this method.
   *
   * @param task
   *          The task to run on the processing thread
   * @param command
   *          The command to return the result to once it's done
   */
  public <R, E extends Exception> void process(Executable<R, E> task, Continuation<R, E> command) {
    try {
      myEnvironment.getProcessor().process(task,
          command,
          myEnvironment.getSelectorManager(),
          myEnvironment.getTimeSource(),
          myEnvironment.getLogManager());

    } catch (final Exception e) {
      // scheduling failures are handed to the continuation as-is, whatever E is
      @SuppressWarnings("unchecked")
      E error = (E)e;
      command.receiveException(error);
    }
  }


  HashSet<Destructable> destructables = new HashSet<Destructable>();

  /**
   * Method which kills a PastryNode.  Note, this doesn't implicitly kill the environment.
   *
   * Make sure to call super.destroy() !!!
   */
  @Override
  public void destroy() {
    if (isDestroyed) return;
    logger.info("Destroying {}", this);
    isDestroyed = true;
    for (Destructable d : destructables) {
      logger.info("Destroying {}", d);
      d.destroy();
    }
    getEnvironment().removeDestructable(this);
    if (getEnvironment().getSelectorManager().isSelectorThread()) {
      if (tl != null) tl.destroy();
    } else {
      getEnvironment().getSelectorManager().invoke(() -> {
        if (tl != null) tl.destroy();
      });
    }
  }


  /**
   * Called by PastryAppl to ask the transport layer to open a Socket to its counterpart on another node.
   *
   * @param i the node to connect to
   * @param deliverSocketToMe receives the socket, or the exception
   * @param appl the application whose counterpart to connect to
   * @param timeout currently ignored
   * @return a handle to cancel the request
   */
  public SocketRequestHandle<NodeHandle> connect(final NodeHandle i, final AppSocketReceiver deliverSocketToMe,
      final PastryAppl appl, int timeout) {

    final SocketRequestHandleImpl<NodeHandle> handle = new SocketRequestHandleImpl<NodeHandle>(i, null, logger);

    Runnable r = new Runnable() {
      @Override
      public void run() {

        // use the proper application address
        final ByteBuffer b = ByteBuffer.allocate(4);
        b.asIntBuffer().put(appl.getAddress());
        b.clear();


        handle.setSubCancellable(tl.openSocket(i,
          new SocketCallback<NodeHandle>(){
            @Override
            @SuppressWarnings("ReferenceEquality") // the same instance is meant
            public void receiveResult(SocketRequestHandle<NodeHandle> c,
                P2PSocket<NodeHandle> result) {

              if (c != handle.getSubCancellable()) throw new RuntimeException("c != handle.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+handle.getSubCancellable());

              logger.trace("openSocket({}):receiveResult({})", i, result);
              result.register(false, true, new P2PSocketReceiver<NodeHandle>() {
                @Override
                public void receiveSelectResult(P2PSocket<NodeHandle> socket,
                    boolean canRead, boolean canWrite) throws IOException {
                  if (canRead || !canWrite) throw new IOException("Expected to write! "+canRead+","+canWrite);

                  // write the appId
                  if (socket.write(b) == -1) {
                    deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, getEnvironment()), new ClosedChannelException("Remote node closed socket while opening.  Try again."));
                    return;
                  }

                  // keep working or pass up the new socket
                  if (b.hasRemaining()) {
                    // keep writing
                    socket.register(false, true, this);
                  } else {
                    // read the response
                    final ByteBuffer answer = ByteBuffer.allocate(1);
                    socket.register(true, false, new P2PSocketReceiver<NodeHandle>(){

                      @Override
                      public void receiveSelectResult(P2PSocket<NodeHandle> socket, boolean canRead, boolean canWrite) throws IOException {

                        if (socket.read(answer) == -1) {
                          deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, getEnvironment()), new ClosedChannelException("Remote node closed socket while opening.  Try again."));
                          return;
                        };

                        if (answer.hasRemaining()) {
                          socket.register(true, false, this);
                        } else {
                          answer.clear();

                          byte connectResult = answer.get();
                          switch(connectResult) {
                            case CONNECTION_OK:
                              // on connector side
                              deliverSocketToMe.receiveSocket(new SocketAdapter<NodeHandle>(socket, getEnvironment()));
                              return;
                            case CONNECTION_NO_APP:
                              deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, getEnvironment()), new AppNotRegisteredException(appl.getAddress()));
                              return;
                            case CONNECTION_NO_ACCEPTOR:
                              deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, getEnvironment()), new NoReceiverAvailableException());
                              return;
                            default:
                              deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, getEnvironment()), new AppSocketException("Unknown error "+connectResult));
                              return;
                          }
                        }
                      }

                      @Override
                      public void receiveException(P2PSocket<NodeHandle> socket, Exception ioe) {
                        deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, getEnvironment()), ioe);
                      }
                    });
                  }
                }

                @Override
                public void receiveException(P2PSocket<NodeHandle> socket,
                    Exception e) {
                  deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, getEnvironment()), e);
                }
              });
            }

            @Override
            public void receiveException(SocketRequestHandle<NodeHandle> s, Exception ex) {
              // TODO: return something with a proper toString()
              deliverSocketToMe.receiveException(null, ex);
            }
          },
        null));
      }
    };
    if (myEnvironment.getSelectorManager().isSelectorThread()) {
      r.run();
    } else {
      myEnvironment.getSelectorManager().invoke(r);
    }


    return handle;
  }



  private JoinFailedException joinFailedReason;
  public void joinFailed(JoinFailedException cje) {
    logger.atWarn().log(() -> "joinFailed("+cje+")");
    joinFailedReason = cje;
    joinFailed = true;
    signalReadyOrJoinFailed();
    notifyPastryNodeListeners(cje);
  }

  private void signalReadyOrJoinFailed() {
    readyLock.lock();
    try {
      readyOrJoinFailed.signalAll();
    } finally {
      readyLock.unlock();
    }
  }

  /**
   * Blocks until this node is ready or failed to join, or until the timeout elapses. Don't call this on the
   * selector thread: the node can only become ready there.
   *
   * @param timeout the maximum time to wait
   * @param unit the unit of the timeout
   * @return true if the node is ready or failed to join, false if the timeout elapsed first
   * @throws InterruptedException if the thread was interrupted while waiting
   */
  public boolean awaitReadyOrJoinFailed(long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    readyLock.lock();
    try {
      while (!isReady() && !joinFailed) {
        if (nanos <= 0) return false;
        nanos = readyOrJoinFailed.awaitNanos(nanos);
      }
      return true;
    } finally {
      readyLock.unlock();
    }
  }

  /**
   * Returns true if there was a fatal error Joining
   */
  public boolean joinFailed() {
    return joinFailed;
  }

  public JoinFailedException joinFailedReason() {
    return joinFailedReason;
  }

  public Router getRouter() {
    return router;
  }

  @Override
  public String printRouteState() {
    String ret = leafSet.toString()+"\n";
    ret+=routeSet.toString();
    return ret;
  }

  // from TLPastryNode

  // Period (in seconds) at which the leafset and routeset maintenance tasks, respectively, are invoked.
  // 0 means never.
  private int leafSetMaintFreq, routeSetMaintFreq;


  private LivenessProvider<NodeHandle> livenessProvider;

  public void setSocketElements(int lsmf, int rsmf,
      TransportLayer<NodeHandle, RawMessage> tl,
      LivenessProvider<NodeHandle> livenessProvider,
      ProximityProvider<NodeHandle> proxProvider,
      NodeHandleFactory<? extends NodeHandle> handleFactory) {
    this.leafSetMaintFreq = lsmf;
    this.routeSetMaintFreq = rsmf;
    this.handleFactory = handleFactory;
    this.proxProvider = proxProvider;
    proxProvider.addProximityListener(this);

    this.tl = tl;
    this.livenessProvider = livenessProvider;
    tl.setCallback(this);
    livenessProvider.addLivenessListener(this);
  }

  Map<String, Object> vars = new HashMap<String, Object>();
  public Map<String, Object> getVars() {
    return vars;
  }

  @Override
  public void incomingSocket(P2PSocket<NodeHandle> s) throws IOException {

    // read the appId
    final ByteBuffer appIdBuffer = ByteBuffer.allocate(4);

    s.register(true, false, new P2PSocketReceiver<NodeHandle>() {

      @Override
      public void receiveSelectResult(
          P2PSocket<NodeHandle> socket,
          boolean canRead, boolean canWrite) throws IOException {
        // read the appId
        if (socket.read(appIdBuffer) == -1) {
          logger.warn("AppId Socket from {} closed unexpectedly.", socket);
          return;
        }

        if (appIdBuffer.hasRemaining()) {
          // read the rest;
          socket.register(true, false, this);
        } else {
          appIdBuffer.clear();
          final int appId = appIdBuffer.asIntBuffer().get();

          // we need to write the result, and there is a timing issue on the appl, so we need to first request to write, then do everything
          // the alternative approach is to return a dummy socket (or a wrapper) and cache any registration request until we write the response
          socket.register(false, true, new P2PSocketReceiver<NodeHandle>(){

            @Override
            public void receiveSelectResult(P2PSocket<NodeHandle> socket,
                boolean canRead, boolean canWrite) throws IOException {

              PastryAppl acceptorAppl = getMessageDispatch().getDestinationByAddress(appId);

              ByteBuffer toWrite = ByteBuffer.allocate(1);
              boolean success = false;

              if (acceptorAppl == null) {
                logger.atWarn().log(() -> "Sending error to connecter "+socket+" "+new AppNotRegisteredException(appId));
                toWrite.put(CONNECTION_NO_APP);
                toWrite.clear();
                socket.write(toWrite);
                socket.close();
              } else {
                synchronized(acceptorAppl) {
                // try to register with the application
                  if (acceptorAppl.canReceiveSocket()) {
                    toWrite.put(CONNECTION_OK);
                    toWrite.clear();
                    success = true;
                  } else {
                    logger.atWarn().log(() -> "Sending error to connecter "+socket+" "+new NoReceiverAvailableException());
                    toWrite.put(CONNECTION_NO_ACCEPTOR);
                    toWrite.clear();
                  }

                  socket.write(toWrite);
                  if (toWrite.hasRemaining()) {
                    // this sucks, because the synchronization with the app-receiver becomes all wrong, this shouldn't normally happen
                    logger.warn("couldn't write 1 bite!!! {}", toWrite);
                    socket.close();
                    return;
                  }

                  if (success) {
                    acceptorAppl.finishReceiveSocket(new SocketAdapter<NodeHandle>(socket, getEnvironment()));
                  }
                } // sync
              } // if (acceptorAppl!=null)
            } // rSR()

            @Override
            public void receiveException(P2PSocket<NodeHandle> socket, Exception ioe) {
              logger.warn("incomingSocket({})", socket, ioe);
            }
          });
        }
      }

      @Override
      public void receiveException(
          P2PSocket<NodeHandle> socket,
          Exception ioe) {
        logger.warn("incomingSocket({})", socket, ioe);
      }

    });
  }




  /**
   * The proximity of the node handle.
   *
   */
  public int proximity(NodeHandle nh) {
    return proximity(nh, null);
  }

  @Override
  public int proximity(NodeHandle nh, Map<String, Object> options) {
    return proxProvider.proximity(nh, options);
  }

  /**
   * Schedule the specified message to be sent to the local node after a
   * specified delay. Useful to provide timeouts.
   *
   * @param msg a message that will be delivered to the local node after the
   *      specified delay
   * @param delay time in milliseconds before message is to be delivered
   * @return the scheduled event object; can be used to cancel the message
   */
  public ScheduledMessage scheduleMsg(Message msg, long delay) {
    ScheduledMessage sm = new ScheduledMessage(this, msg);
    getEnvironment().getSelectorManager().getTimer().schedule(sm, delay);
    return sm;
  }


  /**
   * Schedule the specified message for repeated fixed-delay delivery to the
   * local node, beginning after the specified delay. Subsequent executions take
   * place at approximately regular intervals separated by the specified period.
   * Useful to initiate periodic tasks.
   *
   * @param msg a message that will be delivered to the local node after the
   *      specified delay
   * @param delay time in milliseconds before message is to be delivered
   * @param period time in milliseconds between successive message deliveries
   * @return the scheduled event object; can be used to cancel the message
   */
  public ScheduledMessage scheduleMsg(Message msg, long delay, long period) {
    ScheduledMessage sm = new ScheduledMessage(this, msg);
    getEnvironment().getSelectorManager().getTimer().schedule(sm, delay, period);
    return sm;
  }

  /**
   * Schedule the specified message for repeated fixed-rate delivery to the
   * local node, beginning after the specified delay. Subsequent executions take
   * place at approximately regular intervals, separated by the specified
   * period.
   *
   * @param msg a message that will be delivered to the local node after the
   *      specified delay
   * @param delay time in milliseconds before message is to be delivered
   * @param period time in milliseconds between successive message deliveries
   * @return the scheduled event object; can be used to cancel the message
   */
  public ScheduledMessage scheduleMsgAtFixedRate(Message msg, long delay, long period) {
    ScheduledMessage sm = new ScheduledMessage(this, msg);
    getEnvironment().getSelectorManager().getTimer().scheduleAtFixedRate(sm, delay, period);
    return sm;
  }

  /**
   * Deliver message to the NodeHandle.
   *
   * @param handle the destination
   * @param msg the message
   * @param deliverAckToMe notified when the message is sent or fails, may be null
   * @param tempOptions transport options for this message, may be null
   * @return a receipt for the message
   */
  public PMessageReceipt send(final NodeHandle handle,
      final PRawMessage msg,
      final PMessageNotification deliverAckToMe,
      Map<String, Object> tempOptions) {

    // set up the priority field in the options
    if (tempOptions != null && tempOptions.containsKey(PriorityTransportLayer.OPTION_PRIORITY)) {
      // already has the priority;
    } else {
      if (tempOptions == null) {
        tempOptions = new HashMap<String, Object>();
      } else {
        tempOptions = new HashMap<String, Object>(tempOptions);
      }
      tempOptions.put(PriorityTransportLayer.OPTION_PRIORITY, msg.getPriority());
    }

    final Map<String, Object> options = tempOptions;

    if (handle.equals(localhandle)) {
      receiveMessage(msg);
      PMessageReceipt ret = new PMessageReceipt() {

        @Override
        public boolean cancel() {
          return false;
        }

        @Override
        public NodeHandle getIdentifier() {
          return localhandle;
        }

        @Override
        public Map<String, Object> getOptions() {
          return options;
        }

        @Override
        public Message getMessage() {
          return msg;
        }
        @Override
        public String toString() {
          return "TLPN$PMsgRecpt{"+msg+","+localhandle+"}";
        }
      };
      if (deliverAckToMe != null) deliverAckToMe.sent(ret);
      return ret;
    }

    final PRawMessage rm = msg;

    final PMessageReceiptImpl ret = new PMessageReceiptImpl(msg, options);
    final MessageCallback<NodeHandle, RawMessage> callback;
    if (deliverAckToMe == null) {
      callback = null;
    } else {
      callback = new MessageCallback<NodeHandle, RawMessage>(){
        @Override
        public void ack(MessageRequestHandle<NodeHandle, RawMessage> msg) {
          if (ret.getInternal() == null) ret.setInternal(msg);
          deliverAckToMe.sent(ret);
        }
        @Override
        public void sendFailed(MessageRequestHandle<NodeHandle, RawMessage> msg, Exception reason) {
          if (ret.getInternal() == null) ret.setInternal(msg);
          deliverAckToMe.sendFailed(ret, reason);
        }
      };
    }
    if (getEnvironment().getSelectorManager().isSelectorThread()) {
      ret.setInternal(tl.sendMessage(handle, rm, callback, options));
    } else {
      getEnvironment().getSelectorManager().invoke(() -> ret.setInternal(tl.sendMessage(handle, rm, callback, options)));
    }
    return ret;
  }

  @Override
  public void messageReceived(NodeHandle i, RawMessage m, Map<String, Object> options) throws IOException {
    receiveMessage((Message)m);
  }


  @Override
  public NodeHandle readNodeHandle(InputBuffer buf) throws IOException {
    return handleFactory.readNodeHandle(buf);
  }

  /**
   * The identifier type depends on the transport the node was built with
   * (e.g. InetSocketAddress for sockets, NodeHandle for the direct simulator);
   * the caller states which one it expects.
   */
  @SuppressWarnings("unchecked")
  public <Identifier> Bootstrapper<Identifier> getBootstrapper() {
    return (Bootstrapper<Identifier>)bootstrapper;
  }




  private @Nullable ScheduledMessage routeSetMaintTask;
  private @Nullable ScheduledMessage leafSetMaintTask;

  private static boolean isScheduled(@Nullable ScheduledMessage task) {
    return task != null && !task.isCancelled();
  }

  /** The periodic routeset maintenance task, or null if it is not scheduled. */
  @Nullable ScheduledMessage routeSetMaintenanceTask() {
    return isScheduled(routeSetMaintTask) ? routeSetMaintTask : null;
  }

  /** The periodic leafset maintenance task, or null if it is not scheduled. */
  @Nullable ScheduledMessage leafSetMaintenanceTask() {
    return isScheduled(leafSetMaintTask) ? leafSetMaintTask : null;
  }

  /**
   * Called after the node is initialized, on every boot. The periodic maintenance tasks are scheduled by the
   * first call; later calls (a rejoin boots the node again) keep them.
   *
   * @param bootstrap The node which this node should boot off of.
   */
  public void doneNode(Collection<NodeHandle> bootstrap) {
    logger.info("doneNode:{}", bootstrap);
    logger.info("doneNode:{}", bootstrap);
    // doneNode runs on every boot, also on a rejoin, but the maintenance tasks are scheduled only once
    if (routeSetMaintFreq > 0 && !isScheduled(routeSetMaintTask)) {
      // schedule the routeset maintenance event
      routeSetMaintTask = scheduleMsgAtFixedRate(new InitiateRouteSetMaintenance(),
        routeSetMaintFreq * 1000L, routeSetMaintFreq * 1000L);
      logger.atInfo().log(() -> "Scheduling routeSetMaint for "+routeSetMaintFreq * 1000+","+routeSetMaintFreq * 1000);
    }
    if (leafSetMaintFreq > 0 && !isScheduled(leafSetMaintTask)) {
      // schedule the leafset maintenance event
      leafSetMaintTask = scheduleMsgAtFixedRate(new InitiateLeafSetMaintenance(),
        leafSetMaintFreq * 1000L, leafSetMaintFreq * 1000L);
      logger.atInfo().log(() -> "Scheduling leafSetMaint for "+leafSetMaintFreq * 1000+","+leafSetMaintFreq * 1000);
    }

    joiner.initiateJoin(bootstrap);
  }

  @Override
  public void livenessChanged(NodeHandle i, int val, Map<String, Object> options) {
    if (val == LIVENESS_ALIVE) {
      i.update(NodeHandle.DECLARED_LIVE);
    } else {
      if (val >= LIVENESS_DEAD) {
        i.update(NodeHandle.DECLARED_DEAD);
      }
    }

    notifyLivenessListeners((NodeHandle)i, val, options);
  }

  final Collection<LivenessListener<NodeHandle>> livenessListeners = new ArrayList<LivenessListener<NodeHandle>>();
  @Override
  public void addLivenessListener(LivenessListener<NodeHandle> name) {
    synchronized(livenessListeners) {
      livenessListeners.add(name);
    }
  }

  @Override
  public boolean removeLivenessListener(LivenessListener<NodeHandle> name) {
    synchronized(livenessListeners) {
      return livenessListeners.remove(name);
    }
  }

  protected void notifyLivenessListeners(NodeHandle i, int val, Map<String, Object> options) {
    logger.debug("notifyLivenessListeners({},{})", i, val);
    ArrayList<LivenessListener<NodeHandle>> temp;
    synchronized(livenessListeners) {
      temp = new ArrayList<LivenessListener<NodeHandle>>(livenessListeners);
    }
    for (LivenessListener<NodeHandle> ll : temp) {
      ll.livenessChanged(i, val, options);
    }
  }

  @Override
  public boolean checkLiveness(NodeHandle i, Map<String, Object> options) {
    return livenessProvider.checkLiveness(i, options);
  }

  @Override
  public int getLiveness(NodeHandle i, Map<String, Object> options) {
    return livenessProvider.getLiveness(i, options);
  }

  public boolean isAlive(NodeHandle i) {
    return (livenessProvider.getLiveness(i, null) < LIVENESS_DEAD);
  }


  @Override
  public void proximityChanged(NodeHandle handle, int val, Map<String, Object> options) {
    handle.update(NodeHandle.PROXIMITY_CHANGED);
  }

  public LivenessProvider<NodeHandle> getLivenessProvider() {
    return livenessProvider;
  }

  @Override
  public void clearState(NodeHandle i) {
    livenessProvider.clearState(i);
  }

  @Override
  public void addProximityListener(ProximityListener<NodeHandle> listener) {
    proxProvider.addProximityListener(listener);
  }

  @Override
  public boolean removeProximityListener(ProximityListener<NodeHandle> listener) {
    return proxProvider.removeProximityListener(listener);
  }


  @Override
  public void addNodeHandleFactoryListener(NodeHandleFactoryListener<? super NodeHandle> listener) {
    handleFactory.addNodeHandleFactoryListener(listener);
  }

  @Override
  public void removeNodeHandleFactoryListener(NodeHandleFactoryListener<? super NodeHandle> listener) {

    handleFactory.removeNodeHandleFactoryListener(listener);
  }

  NodeHandleFetcher nodeHandleFetcher;
  public void setNodeHandleFetcher(NodeHandleFetcher nodeHandleFetcher) {
    this.nodeHandleFetcher = nodeHandleFetcher;
  }

  public void getNodeHandle(Object o, Continuation<NodeHandle, Exception> c) {
    nodeHandleFetcher.getNodeHandle(o, c);
  }
}
