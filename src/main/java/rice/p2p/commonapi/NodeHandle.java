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

package rice.p2p.commonapi;

import java.io.*;
import java.util.*;

import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * @(#) NodeHandle.java
 *
 * This class is an abstraction of a node handle from the CommonAPI paper. A
 * node handle is a handle to a known node, which conceptually includes the
 * node's Id, as well as the node's underlying network address (such as IP/port).
 *
 * This class is (unfortunately) an abstact class due to the need to be observable.
 * Observe it with {@link #addNodeHandleListener(NodeHandleListener)}; the inherited
 * {@code java.util.Observable} methods are kept only for compatibility and are
 * implemented on top of the listener list.
 *
 * @version $Id: NodeHandle.java 4231 2008-06-24 12:22:18Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public abstract class NodeHandle extends LegacyObservable implements Serializable  {

  // constants defining types of observable events
  public static final Integer PROXIMITY_CHANGED = Integer.valueOf(1);
  public static final Integer DECLARED_DEAD = Integer.valueOf(2);
  public static final Integer DECLARED_LIVE = Integer.valueOf(3);
  
  // serialver
  private static final long serialVersionUID = 4761193998848368227L;
  
  /**
   * Returns this node's id.
   *
   * @return The corresponding node's id.
   */
  public abstract Id getId();

  /**
   * Returns whether or not this node is currently alive
   *
   * @deprecated use Endpoint.isAlive(NodeHandle) 
   * @return Whether or not this node is currently alive
   */
  @Deprecated
  public abstract boolean isAlive();

  /**
   * Returns the current proximity value of this node
   *
   * @deprecated use Endpoint.proximity(NodeHandle)
   * @return The current proximity value of this node
   */
  @Deprecated
  public abstract int proximity();
  
  /**
   * Requests that the underlying transport layer check to ensure
   * that the remote node is live.  If the node is found to be live, nothing
   * happens, but if the node does not respond, the transport layer
   * make take steps to verify that the node is dead.  Such steps
   * could include finding an alternate route to the node.
   *
   * @return Whether or not the node is currently alive
   */
  public abstract boolean checkLiveness();

  public abstract void serialize(OutputBuffer buf) throws IOException;

  /****************** Typed listeners (replacement for java.util.Observer) **********/

  /**
   * A registered listener and its priority.
   */
  private static final class ListenerEntry {
    final NodeHandleListener listener;
    int priority;

    ListenerEntry(NodeHandleListener listener, int priority) {
      this.listener = listener;
      this.priority = priority;
    }

    public String toString() {
      return listener+":"+priority;
    }
  }

  /**
   * Higher priority first; List.sort is stable, so equal priorities keep insertion order.
   */
  private static final Comparator<ListenerEntry> BY_PRIORITY = 
    (a, b) -> Integer.compare(b.priority, a.priority);

  private transient List<ListenerEntry> listeners = new ArrayList<ListenerEntry>();

  /**
   * Need to construct listeners in deserialization.
   */
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    listeners = new ArrayList<ListenerEntry>();
  }

  /**
   * Registers a listener with priority 0.
   * 
   * @param listener the listener to be notified of DECLARED_DEAD/DECLARED_LIVE/PROXIMITY_CHANGED events
   */
  public void addNodeHandleListener(NodeHandleListener listener) {
    addNodeHandleListener(listener, 0);
  }

  /**
   * Registers a listener.  If the listener is already registered with a different
   * priority, only its priority is changed.
   * 
   * @param listener the listener
   * @param priority higher priority listeners will be called first
   */
  public void addNodeHandleListener(NodeHandleListener listener, int priority) {
    synchronized (listeners) {
      for (ListenerEntry e : listeners) {
        if (e.listener.equals(listener) && e.priority != priority) {
          int oldPriority = e.priority;
          e.priority = priority;
          listeners.sort(BY_PRIORITY);
          nodeHandleListenerPriorityChanged(listener, oldPriority, priority);
          return;
        }
      }
      listeners.add(new ListenerEntry(listener, priority));
      listeners.sort(BY_PRIORITY);
    }
  }

  /**
   * Hook called (while holding the listener lock) when addNodeHandleListener() changed
   * the priority of an already registered listener.  Does nothing by default.
   */
  protected void nodeHandleListenerPriorityChanged(NodeHandleListener listener, int oldPriority, int newPriority) {
  }

  /**
   * Unregisters a listener.
   * 
   * @param listener the listener
   * @return true if the listener was registered
   */
  public boolean removeNodeHandleListener(NodeHandleListener listener) {
    synchronized (listeners) {
      for (int i = 0; i < listeners.size(); i++) {
        if (listeners.get(i).listener.equals(listener)) {
          listeners.remove(i);
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Notifies all registered listeners (highest priority first).  Listeners may 
   * add or remove listeners during the notification.
   * 
   * @param event the event
   */
  protected void notifyNodeHandleListeners(Object event) {
    List<ListenerEntry> l;
    synchronized (listeners) {
      l = new ArrayList<ListenerEntry>(listeners);
    }
    for (ListenerEntry e : l) {
      e.listener.nodeHandleUpdate(this, event);
    }
  }

  /****************** Deprecated java.util.Observer compatibility **********/

  /**
   * Adapts a java.util.Observer to a NodeHandleListener.  Equal iff the observers are equal.
   */
  @Deprecated
  private static final class ObserverListener implements NodeHandleListener {
    final Observer observer;

    ObserverListener(Observer observer) {
      this.observer = observer;
    }

    public void nodeHandleUpdate(NodeHandle handle, Object event) {
      observer.update(handle, event);
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
   * @deprecated for implementing the deprecated Observer methods in subclasses only
   * @return a listener that forwards to the observer, equal to any other listener wrapping an equal observer
   */
  @Deprecated
  protected static NodeHandleListener observerListener(Observer o) {
    return new ObserverListener(o);
  }

  /**
   * @deprecated use addNodeHandleListener
   */
  @Deprecated
  @Override
  public void addObserver(Observer o) {
    addNodeHandleListener(observerListener(o));
  }

  /**
   * @deprecated use removeNodeHandleListener
   */
  @Deprecated
  @Override
  public void deleteObserver(Observer o) {
    removeNodeHandleListener(observerListener(o));
  }

  /**
   * Same semantics as Observable: only notifies if setChanged() was called.
   * Subclasses should call notifyNodeHandleListeners() instead.
   */
  @Override
  public void notifyObservers(Object arg) {
    synchronized (this) {
      if (!hasChanged()) return;
      clearChanged();
    }
    notifyNodeHandleListeners(arg);
  }

  @Override
  public void notifyObservers() {
    notifyObservers(null);
  }

  /**
   * @return the number of registered listeners (including Observers)
   */
  @Override
  public int countObservers() {
    synchronized (listeners) {
      return listeners.size();
    }
  }

  /**
   * Removes all listeners (including Observers).
   */
  @Override
  public void deleteObservers() {
    synchronized (listeners) {
      listeners.clear();
    }
  }
}
