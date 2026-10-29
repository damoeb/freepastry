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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Observer;
import java.util.Vector;

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
import rice.p2p.commonapi.LegacyObservable;
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
import rice.pastry.messaging.PJavaSerializedMessage;
import rice.pastry.messaging.PRawMessage;
import rice.pastry.routing.InitiateRouteSetMaintenance;
import rice.pastry.routing.RouteSetProtocol;
import rice.pastry.routing.Router;
import rice.pastry.routing.RoutingTable;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;
import rice.pastry.transport.PMessageReceiptImpl;
import rice.pastry.transport.SocketAdapter;

/**
 * A Pastry node is single entity in the pastry network.
 * 
 * @version $Id: PastryNode.java 4664 2009-02-23 18:06:07Z jeffh $
 * 
 * @author Andrew Ladd
 */

public class PastryNode extends LegacyObservable implements 
    rice.p2p.commonapi.Node, Destructable, NodeHandleFactory<NodeHandle>, 
    LivenessProvider<NodeHandle>, ProximityProvider<NodeHandle>, 
    ProximityListener<NodeHandle>,
    TransportLayerCallback<NodeHandle, RawMessage>, 
    LivenessListener<NodeHandle> {

  /**
   * Used by AppSockets
   */
  public static final byte CONNECTION_UNKNOWN_ERROR = -1;
  public static final byte CONNECTION_UNKNOWN = -100;
  public static final byte CONNECTION_OK = 0;
  public static final byte CONNECTION_NO_APP = 1;
  public static final byte CONNECTION_NO_ACCEPTOR = 2;

  
  protected Id myNodeId;

  private Environment myEnvironment;

  private MessageDispatch myMessageDispatch;

  protected LeafSet leafSet;

  protected RoutingTable routeSet;

  protected NodeHandle localhandle;

  protected Vector<PastryAppl> apps;

  protected Logger logger;
  
  ReadyStrategy readyStrategy;
  
  protected boolean joinFailed = false;
  
  protected boolean isDestroyed = false;
  
  protected Router router;
  
  /**
   * Used to deserialize NodeHandles
   */
  protected NodeHandleFactory<? extends NodeHandle> handleFactory;

  /**
   * Call initiateJoin on this class.
   */
  protected JoinProtocol joiner;

  /**
   * Call boot on this class.
   */
  protected Bootstrapper<?> bootstrapper;
  
  /**
   * The top level transport layer.
   */
  protected TransportLayer<NodeHandle, RawMessage> tl;

  protected ProximityProvider<NodeHandle> proxProvider;
  
  /**
   * Constructor, with NodeId. Need to set the node's ID before this node is
   * inserted as localHandle.localNode.
   */
  public PastryNode(Id id, Environment e) {
    myEnvironment = e;
    myNodeId = id;
    
    readyStrategy = getDefaultReadyStrategy();
    
    apps = new Vector<PastryAppl>();
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
      
      public void setReady(boolean r) {
        if (r != ready) {
          synchronized(PastryNode.this) {
            ready = r;
          }
          notifyReadyObservers();
        }
      }
      
      public boolean isReady() {
        return ready;  
      }
      
      public void start() {
        setReady(true);
      }

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
  

  public rice.p2p.commonapi.NodeHandle getLocalNodeHandle() {
    return localhandle;
  }

  public Environment getEnvironment() {
    return myEnvironment; 
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

  public boolean removeDestructable(Destructable d) {
    return destructables.remove(d);    
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
  
    // It is possible to have the setReady() invoked more than once if the
    // message denoting the termination of join protocol is duplicated.
    boolean ready = readyStrategy.isReady();
    logger.info("PastryNode.notifyReadyObservers({})", ready);

    if (ready) {
      nodeIsReady(); // deprecate this
      nodeIsReady(true);

      notifyPastryNodeListeners(Boolean.valueOf(true));

      if (neverBeenReady) {
        // notify applications
        // we iterate over private copy to allow addition of new apps in the
        // context of notifyReady()
        Vector<PastryAppl> tmpApps = new Vector<PastryAppl>(apps);
        Iterator<PastryAppl> it = tmpApps.iterator();
        while (it.hasNext())
          ((PastryAppl) (it.next())).notifyReady();
        neverBeenReady = false;
      }

      // signal any apps that might be waiting for the node to get ready
      synchronized (this) {
        // NN: not a problem, because we already changed the state in the calling method
        notifyAll();
      }
    } else {
      nodeIsReady(false);
      notifyPastryNodeListeners(Boolean.valueOf(false));

    }
  }

  /**
   * Called by the layered Pastry application to check if the local pastry node
   * is the one that is currently closest to the object key id.
   * 
   * @param key
   *          the object key id
   * 
   * @return true if the local node is currently the closest to the key.
   */
  public boolean isClosest(Id key) {

    if (leafSet.mostSimilar(key) == 0)
      return true;
    else
      return false;
  }

  public LeafSet getLeafSet() {
    return leafSet;
  }

  public RoutingTable getRoutingTable() {
    return routeSet;
  }

  /**
   * Add a leaf set observer to the Pastry node.
   * 
   * @deprecated use addLeafSetListener
   * @param o the observer.
   */
  @Deprecated
  public void addLeafSetObserver(Observer o) {
    leafSet.addObserver(o);
  }

  /**
   * Delete a leaf set observer from the Pastry node.
   * 
   * @deprecated use deleteLeafSetListener
   * @param o the observer.
   */
  @Deprecated
  public void deleteLeafSetObserver(Observer o) {
    leafSet.deleteObserver(o);
  }

  public void addLeafSetListener(NodeSetListener listener) {
    leafSet.addNodeSetListener(listener);
  }
  public void deleteLeafSetListener(NodeSetListener listener) {
    leafSet.deleteNodeSetListener(listener);
  }
  /**
   * Add a route set observer to the Pastry node.
   * 
   * @deprecated use addRouteSetListener
   * @param o the observer.
   */
  @Deprecated
  public void addRouteSetObserver(Observer o) {
    routeSet.addObserver(o);
  }

  /**
   * Delete a route set observer from the Pastry node.
   * 
   * @deprecated use deleteRouteSetListener
   * @param o the observer.
   */
  @Deprecated
  public void deleteRouteSetObserver(Observer o) {
    routeSet.deleteObserver(o);
  }

  public void addRouteSetListener(NodeSetListener listener) {
    routeSet.addNodeSetListener(listener);
  }

  public void removeRouteSetListener(NodeSetListener listener) {
    routeSet.removeNodeSetListener(listener);
  }

  /****************** PastryNodeListener (replacement for java.util.Observer) **********/

  /**
   * Registered listeners, including adapted Observers.  Notified in reverse order 
   * of registration, like java.util.Observable.
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
   * @param event Boolean (ready), JoinFailedException, or (for Observers only) anything
   *   passed to notifyObservers() by a subclass
   */
  private void notifyPastryNodeListeners(Object event) {
    PastryNodeListener[] l;
    synchronized (pastryNodeListeners) {
      l = pastryNodeListeners.toArray(new PastryNodeListener[pastryNodeListeners.size()]);
    }
    for (int i = l.length-1; i >= 0; i--) {
      if (l[i] instanceof ObserverListener) {
        ((ObserverListener)l[i]).observer.update(this, event);
      } else if (event instanceof Boolean) {
        l[i].pastryNodeReady(this, ((Boolean)event).booleanValue());
      } else if (event instanceof JoinFailedException) {
        l[i].pastryNodeJoinFailed(this, (JoinFailedException)event);
      }
    }
  }

  /**
   * Adapts a java.util.Observer.  Equal iff the observers are equal.
   */
  @Deprecated
  private static final class ObserverListener implements PastryNodeListener {
    final Observer observer;

    ObserverListener(Observer observer) {
      this.observer = observer;
    }

    public boolean equals(Object o) {
      return (o instanceof ObserverListener) && observer.equals(((ObserverListener)o).observer);
    }

    public int hashCode() {
      return observer.hashCode();
    }

    public String toString() {
      return observer.toString();
    }
  }

  /**
   * @deprecated use addPastryNodeListener
   */
  @Deprecated
  @Override
  public void addObserver(Observer o) {
    if (o == null) throw new NullPointerException();
    addPastryNodeListener(new ObserverListener(o));
  }

  /**
   * @deprecated use removePastryNodeListener
   */
  @Deprecated
  @Override
  public void deleteObserver(Observer o) {
    removePastryNodeListener(new ObserverListener(o));
  }

  /**
   * Same semantics as Observable: only notifies if setChanged() was called.
   */
  @Override
  public void notifyObservers(Object arg) {
    synchronized (this) {
      if (!hasChanged()) return;
      clearChanged();
    }
    notifyPastryNodeListeners(arg);
  }

  @Override
  public void notifyObservers() {
    notifyObservers(null);
  }

  /**
   * @return the number of registered PastryNodeListeners (including Observers)
   */
  @Override
  public int countObservers() {
    synchronized (pastryNodeListeners) {
      return pastryNodeListeners.size();
    }
  }

  /**
   * Removes all PastryNodeListeners (including Observers).
   */
  @Override
  public void deleteObservers() {
    synchronized (pastryNodeListeners) {
      pastryNodeListeners.clear();
    }
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

  public String toString() {
    return "PastryNode"+localhandle;
  }

  // Common API Support

  /**
   * This returns a VirtualizedNode specific to the given application and
   * instance name to the application, which the application can then use in
   * order to send an receive messages.
   * 
   * @deprecated use buildEndpoint() endpoint.register()
   * 
   * @param application
   *          The Application
   * @param instance
   *          An identifier for a given instance
   * @return The endpoint specific to this application, which can be used for
   *         message sending/receiving.
   */
  @Deprecated
  public rice.p2p.commonapi.Endpoint registerApplication(
      rice.p2p.commonapi.Application application, String instance) {
    return new rice.pastry.commonapi.PastryEndpoint(this, application, instance, true);
  }

  public rice.p2p.commonapi.Endpoint buildEndpoint(
      rice.p2p.commonapi.Application application, String instance) {
    return new rice.pastry.commonapi.PastryEndpoint(this, application, instance, false);
  }

  /**
   * Returns the Id of this node
   * 
   * @return This node's Id
   */
  public rice.p2p.commonapi.Id getId() {
    return getNodeId();
  }

  /**
   * Returns a factory for Ids specific to this node's protocol.
   * 
   * @return A factory for creating Ids.
   */
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
  public void destroy() {
    if (isDestroyed) return;
    logger.info("Destroying {}", this);
    isDestroyed = true;
    Iterator<Destructable> i = destructables.iterator();
    while(i.hasNext()) {
      Destructable d = i.next();
      logger.info("Destroying {}", d);
      d.destroy(); 
    }
    getEnvironment().removeDestructable(this);
    if (getEnvironment().getSelectorManager().isSelectorThread()) {
      if (tl != null) tl.destroy();
    } else {
      getEnvironment().getSelectorManager().invoke(new Runnable() {
        public void run() {
          if (tl != null) tl.destroy();
        }
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
      public void run() {
        
        // use the proper application address
        final ByteBuffer b = ByteBuffer.allocate(4);
        b.asIntBuffer().put(appl.getAddress());
        b.clear();
        
        
        handle.setSubCancellable(tl.openSocket(i, 
          new SocketCallback<NodeHandle>(){    
            public void receiveResult(SocketRequestHandle<NodeHandle> c, 
                P2PSocket<NodeHandle> result) {
              
              if (c != handle.getSubCancellable()) throw new RuntimeException("c != handle.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+handle.getSubCancellable());
              
              logger.trace("openSocket({}):receiveResult({})", i, result);
              result.register(false, true, new P2PSocketReceiver<NodeHandle>() {        
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
                    
                      public void receiveException(P2PSocket<NodeHandle> socket, Exception ioe) {
                        deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, getEnvironment()), ioe);
                      }                
                    });
                  }
                }
              
                public void receiveException(P2PSocket<NodeHandle> socket,
                    Exception e) {
                  deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, getEnvironment()), e);
                }        
              }); 
            }    
        
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


  
  protected JoinFailedException joinFailedReason;
  public void joinFailed(JoinFailedException cje) {
    logger.atWarn().log(() -> "joinFailed("+cje+")");
    joinFailedReason = cje;
    synchronized(this) {
      joinFailed = true;
      this.notifyAll();
    }
    notifyPastryNodeListeners(cje); 
  }

  /**
   * Returns true if there was a fatal error Joining
   * @return
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
  
  public String printRouteState() {
    String ret = leafSet.toString()+"\n";
    ret+=routeSet.toString();
    return ret;
  }
  
  // from TLPastryNode
  
  // Period (in seconds) at which the leafset and routeset maintenance tasks, respectively, are invoked.
  // 0 means never.
  protected int leafSetMaintFreq, routeSetMaintFreq;

  protected ScheduledMessage leafSetRoutineMaintenance = null;
  protected ScheduledMessage routeSetRoutineMaintenance = null;
  
  protected LivenessProvider<NodeHandle> livenessProvider;

  public void setSocketElements(int lsmf, int rsmf, 
      TransportLayer<NodeHandle, RawMessage> tl,
      LivenessProvider<NodeHandle> livenessProvider,
      ProximityProvider<NodeHandle> proxProvider,
      NodeHandleFactory<? extends NodeHandle> handleFactory) {
    this.localhandle = localhandle;
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
  
  public void incomingSocket(P2PSocket<NodeHandle> s) throws IOException {
    
    // read the appId
    final ByteBuffer appIdBuffer = ByteBuffer.allocate(4);
    
    s.register(true, false, new P2PSocketReceiver<NodeHandle>() {
    
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
          
            public void receiveException(P2PSocket<NodeHandle> socket, Exception ioe) {
              logger.warn("incomingSocket({})", socket, ioe);
              return;
            }          
          });
        }
      }
    
      public void receiveException(
          P2PSocket<NodeHandle> socket,
          Exception ioe) {
        logger.warn("incomingSocket({})", socket, ioe);
      }
    
    });
  }

  protected void acceptAppSocket(int appId) throws AppSocketException {
    PastryAppl acceptorAppl = getMessageDispatch().getDestinationByAddress(appId);
    if (acceptorAppl == null) throw new AppNotRegisteredException(appId);
    if (!acceptorAppl.canReceiveSocket()) throw new NoReceiverAvailableException();
  }




  /**
   * The proximity of the node handle.
   * 
   * @param nh
   * @return
   */
  public int proximity(NodeHandle nh) {
    return proximity(nh, null);
  }
  
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
      final Message msg, 
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

        public boolean cancel() {
          return false;
        }

        public NodeHandle getIdentifier() {
          return localhandle;
        }

        public Map<String, Object> getOptions() {
          return options;
        }

        public Message getMessage() {
          return msg;
        }
        public String toString() {
          return "TLPN$PMsgRecpt{"+msg+","+localhandle+"}";
        }
      }; 
      if (deliverAckToMe != null) deliverAckToMe.sent(ret);
      return ret;
    }
    
    final PRawMessage rm;
    if (msg instanceof PRawMessage) {
      rm = (PRawMessage)msg; 
    } else {
      rm = new PJavaSerializedMessage(msg); 
    }
      
    final PMessageReceiptImpl ret = new PMessageReceiptImpl(msg, options);
    final MessageCallback<NodeHandle, RawMessage> callback;
    if (deliverAckToMe == null) {
      callback = null;
    } else {
      callback = new MessageCallback<NodeHandle, RawMessage>(){        
        public void ack(MessageRequestHandle<NodeHandle, RawMessage> msg) {
          if (ret.getInternal() == null) ret.setInternal(msg);
          deliverAckToMe.sent(ret);
        }      
        public void sendFailed(MessageRequestHandle<NodeHandle, RawMessage> msg, Exception reason) {        
          if (ret.getInternal() == null) ret.setInternal(msg);
          deliverAckToMe.sendFailed(ret, reason);
        }     
      };      
    }
    if (getEnvironment().getSelectorManager().isSelectorThread()) {              
      ret.setInternal(tl.sendMessage(handle, rm, callback, options));
    } else {
      getEnvironment().getSelectorManager().invoke(new Runnable() {      
        public void run() {
          ret.setInternal(tl.sendMessage(handle, rm, callback, options));
        }      
      });
    }
    return ret;
  }
  
  public void messageReceived(NodeHandle i, RawMessage m, Map<String, Object> options) throws IOException {
    if (m.getType() == 0 && (m instanceof PJavaSerializedMessage)) {
      receiveMessage(((PJavaSerializedMessage)m).getMessage());
    } else {
      receiveMessage((Message)m);
    }
  }


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




  /**
   * Called after the node is initialized.
   * 
   * @param bootstrap The node which this node should boot off of.
   */
  public void doneNode(Collection<NodeHandle> bootstrap) { 
    logger.info("doneNode:{}", bootstrap);
    logger.info("doneNode:{}", bootstrap);
    if (routeSetMaintFreq > 0) {
      // schedule the routeset maintenance event
      routeSetRoutineMaintenance = scheduleMsgAtFixedRate(new InitiateRouteSetMaintenance(),
        routeSetMaintFreq * 1000, routeSetMaintFreq * 1000);
      logger.atInfo().log(() -> "Scheduling routeSetMaint for "+routeSetMaintFreq * 1000+","+routeSetMaintFreq * 1000);
    }
    if (leafSetMaintFreq > 0) {
      // schedule the leafset maintenance event
      leafSetRoutineMaintenance = scheduleMsgAtFixedRate(new InitiateLeafSetMaintenance(),
        leafSetMaintFreq * 1000, leafSetMaintFreq * 1000);
      logger.atInfo().log(() -> "Scheduling leafSetMaint for "+leafSetMaintFreq * 1000+","+leafSetMaintFreq * 1000);
    }
    
    joiner.initiateJoin(bootstrap);
  }
    
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
  
  Collection<LivenessListener<NodeHandle>> livenessListeners = new ArrayList<LivenessListener<NodeHandle>>();
  public void addLivenessListener(LivenessListener<NodeHandle> name) {
    synchronized(livenessListeners) {
      livenessListeners.add(name);
    }    
  }
  
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

  public boolean checkLiveness(NodeHandle i, Map<String, Object> options) {    
    return livenessProvider.checkLiveness(i, options);
  }

  public int getLiveness(NodeHandle i, Map<String, Object> options) {
    return livenessProvider.getLiveness(i, options);
  }

  public int getLiveness(NodeHandle i) {
    return livenessProvider.getLiveness(i, null);
  }

  public boolean isAlive(NodeHandle i) {
    return (livenessProvider.getLiveness(i, null) < LIVENESS_DEAD);
  }


  public void proximityChanged(NodeHandle handle, int val, Map<String, Object> options) {
    handle.update(NodeHandle.PROXIMITY_CHANGED);     
  }

  public LivenessProvider<NodeHandle> getLivenessProvider() {
    return livenessProvider;
  }

  public ProximityProvider<NodeHandle> getProxProvider() {
    return proxProvider;
  }
  
  public TransportLayer<NodeHandle, RawMessage> getTL() {
    return tl;
  }

  public void clearState(NodeHandle i) {
    livenessProvider.clearState(i);
  }

  public void addProximityListener(ProximityListener<NodeHandle> listener) {
    proxProvider.addProximityListener(listener);
  }

  public boolean removeProximityListener(ProximityListener<NodeHandle> listener) {
    return proxProvider.removeProximityListener(listener);
  }

  /**
   * Overridden by derived classes, and invoked when the node has joined
   * successfully.
   * 
   * This one is for backwards compatibility. It will soon be deprecated.
   * 
   * @deprecated use nodeIsReady(boolean)
   * 
   */
  @Deprecated
  public void nodeIsReady() {
    
    // nothing, used to cancel the joinEvent
  }
  
  public NodeHandleFactory<? extends NodeHandle> getHandleFactory() {
    return handleFactory;
  }

  /**
   * @deprecated misspelled; use {@link #getHandleFactory()}
   */
  @Deprecated
  public NodeHandleFactory<? extends NodeHandle> getHandleFactroy() {
    return getHandleFactory();
  }

  
  /******************* network listeners *********************/
  // the list of network listeners
  private ArrayList<NetworkListener> networkListeners = new ArrayList<NetworkListener>();

  public void addNetworkListener(NetworkListener listener) {
    synchronized(networkListeners) {
      networkListeners.add(listener);
    }
  }
  
  public void removeNetworkListener(NetworkListener listener) {
    synchronized(networkListeners) {
      networkListeners.remove(listener);
    }
  }
  
  protected Iterable<NetworkListener> getNetworkListeners() {
    synchronized(networkListeners) {
      return new ArrayList<NetworkListener>(networkListeners);
    }
  }
  
  public void broadcastChannelClosed(InetSocketAddress addr) {
    for (NetworkListener listener : getNetworkListeners())
      listener.channelClosed(addr);
  }
  
  public void broadcastChannelOpened(InetSocketAddress addr, int reason) {
    for (NetworkListener listener : getNetworkListeners())
      listener.channelOpened(addr, reason);
  }
  
  public void broadcastSentListeners(int address, short msgType, InetSocketAddress dest, int size, int wireType) {
    for (NetworkListener listener : getNetworkListeners())
      listener.dataSent(address, msgType, dest, size, wireType);
  }
  
  public void broadcastReceivedListeners(int address, short msgType, InetSocketAddress from, int size, int wireType) {
    for (NetworkListener listener : getNetworkListeners())
      listener.dataReceived(address, msgType, from, size, wireType);
  }

  public void addNodeHandleFactoryListener(NodeHandleFactoryListener<? super NodeHandle> listener) {
    handleFactory.addNodeHandleFactoryListener(listener);
  }

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

