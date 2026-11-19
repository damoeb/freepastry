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

package rice.p2p.scribe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.NodeHandleSet;

/**
 * The local node's view of every Scribe tree it takes part in: one {@link TopicTreeState} per topic, plus
 * the reverse indexes from each parent and child to its topics.
 *
 * The indexes decide when the Scribe registers itself as a liveness listener on a node handle: as long as
 * a handle is a parent or child of some topic.
 *
 * The map of topic managers doubles as the lock for the tree state ({@link #lock()}).
 */
final class TopicRegistry {

  /**
   * the hashtable of topic -> TopicTreeState
   */
  // Hashtable on purpose: the registry locks this very object (synchronized (topicManagers)) and hands out its synchronized views
  @SuppressWarnings("JdkObsolete")
  private final Hashtable<Topic, TopicTreeState> topicManagers = new Hashtable<Topic, TopicTreeState>();

  /**
   * This contains a mapping of child - > all topics for which the local node
   * has this node(hashtable key) as a child
   */
  private final HashMap<NodeHandle, Collection<Topic>> allChildren = new HashMap<NodeHandle, Collection<Topic>>();

  /**
   * This contains a mapping of parent - > all topics for which the local node
   * has this node(hashtable key) as a parent
   */
  private final HashMap<NodeHandle, Collection<Topic>> allParents = new HashMap<NodeHandle, Collection<Topic>>();

  /**
   * Topics that we are the root.
   */
  private final Set<Topic> roots = new HashSet<Topic>();

  /**
   * Topics that (should) have an outsanding subscription.
   * Topics that are in this set have no parent, and we are not the root.
   */
  private final Set<Topic> pending = new HashSet<Topic>();

  private final Endpoint endpoint;

  private final NodeHandle localHandle;

  /**
   * Registered on every handle that is a parent or child of one of our topics.
   */
  private final NodeHandleListener livenessListener;

  private final Logger logger;

  TopicRegistry(Endpoint endpoint, NodeHandle localHandle, NodeHandleListener livenessListener, Logger logger) {
    this.endpoint = endpoint;
    this.localHandle = localHandle;
    this.livenessListener = livenessListener;
    this.logger = logger;
  }

  /**
   * The lock that guards the tree state.
   */
  Object lock() {
    return topicManagers;
  }

  TopicTreeState get(Topic topic) {
    return topicManagers.get(topic);
  }

  /**
   * Creates and registers a manager for the topic. Need to be holding lock: {@link #lock()}
   */
  TopicTreeState create(Topic topic) {
    TopicTreeState manager = new TopicTreeState(topic, this, endpoint, localHandle, logger);
    topicManagers.put(topic, manager);
    return manager;
  }

  /**
   * Lazy constructor.
   * @return never null
   */
  TopicTreeState getOrCreate(Topic topic) {
    synchronized(topicManagers) {
      TopicTreeState manager = topicManagers.get(topic);

      // if we don't know about the topic, we subscribe, otherwise,
      // we simply add the child to the list
      if (manager == null) {
        manager = create(topic);
      }
      return manager;
    }
  }

  void remove(Topic topic) {
    topicManagers.remove(topic);
  }

  Collection<Topic> topics() {
    // it would be safer to return a copy, but faster to do it this way
    return topicManagers.keySet();
  }

  Collection<TopicTreeState> managers() {
    return topicManagers.values();
  }

  /**
   * Remembers that we are subscribing to these topics.
   */
  void markPending(Collection<Topic> topics) {
    pending.addAll(topics);
  }

  /**
   * Returns whether or not this Scribe is the root for the given topic
   *
   * @param topic The topic in question
   * @return Whether or not we are currently the root
   */
  boolean isRoot(Topic topic) {
    NodeHandleSet set = endpoint.replicaSet(topic.id(), 1);

    if (set.size() == 0)
      return false;
    else
      return set.getHandle(0).getId().equals(endpoint.getId());
  }

  // ----- per-topic queries -----

  Collection<ScribeMultiClient> getClientsByTopic(Topic topic) {
    TopicTreeState manager = topicManagers.get(topic);
    if (manager != null) {
      return manager.getClients();
    }
    return new ArrayList<ScribeMultiClient>();
  }

  Collection<NodeHandle> getChildrenOfTopic(Topic topic) {
    TopicTreeState manager = topicManagers.get(topic);
    if (manager != null) {
      return manager.getChildren();
    }

    return new ArrayList<NodeHandle>(0);
  }

  NodeHandle getParent(Topic topic) {
    if (topicManagers.get(topic) != null) {
      return topicManagers.get(topic).getParent();
    }

    return null;
  }

  // ----- parent/child indexes -----

  /**
   * This method should be invoked after the state change in the Topic Manager
   * has been made. This helps us to know the current state of the system and
   * thus generate WARNING messages only in cases of redundancy
   *
   * Need to be holding lock: topicManagers
   *
   */
  void addToAllChildren(Topic topic, NodeHandle child) {
    logger.info("addToAllChildren({},{})", topic, child);

    // Child added
    Collection<Topic> topics = allChildren.get(child);

    if (topics == null) {
      if (endpoint.isAlive(child)) {
        if (!allParents.containsKey(child)) child.addNodeHandleListener(livenessListener);
      } else {
        logger.warn("addToAllChildren({},{}) child.isAlive() == false", topic, child, new Exception("Stack Trace"));
      }
      topics = new ArrayList<Topic>();
      allChildren.put(child, topics);
    }
    if (!topics.contains(topic)) {
      topics.add(topic);
    }
  }

  /**
   * Need to be holding lock: topicManagers
   *
   */
  void removeFromAllChildren(Topic topic, NodeHandle child) {
    logger.info("removeFromAllChildren({},{})", topic, child);

    // Child added
    Collection<Topic> topics = allChildren.get(child);

    if (topics == null) {
      return;
    }
    topics.remove(topic);

    if (topics.isEmpty()) {
      allChildren.remove(child);
      if (!allParents.containsKey(child)) child.removeNodeHandleListener(livenessListener);

    }
  }

  // This method should be invoked after the state change in the Topic Manager
  // has been made. This helps us to know the current state of the system and
  // thus generate WARNING messages only in cases of redundancy
  void addToAllParents(Topic topic, NodeHandle parent) {
    logger.info("addToAllParents({},{})", topic, parent);

    if (parent == null || parent.equals(localHandle)) {
      if (isRoot(topic)) {
        roots.add(topic);
      }
      // This could very well be the case, but in such instances we need not do
      // anything
      return;
    }

    // Parent added
    Collection<Topic> topics = allParents.get(parent);
    if (topics == null) {
      if (endpoint.isAlive(parent)) {
        if (!allChildren.containsKey(parent)) parent.addNodeHandleListener(livenessListener);
      } else {
        logger.warn("addToAllParents({},{}) parent.isAlive() == false", topic, parent, new Exception("Stack Trace"));
      }
      topics = new ArrayList<Topic>();
      allParents.put(parent, topics);
    }

    if (!topics.contains(topic)) {
      topics.add(topic);
    }
  }

  void removeFromAllParents(Topic topic, NodeHandle parent) {
    logger.info("removeFromAllParents({},{})", topic, parent);

    if (parent == null || parent.equals(localHandle)) {
      // This could very well be the case, but in such instances we need not do
      // anything
      roots.remove(topic);
      pending.remove(topic);

      return;
    }

    // Parent removed
    Collection<Topic> topics = allParents.get(parent);

    if (topics == null) {
      return;
    }
    topics.remove(topic);

    if (topics.isEmpty()) {
      allParents.remove(parent);
      if (!allChildren.containsKey(parent)) parent.removeNodeHandleListener(livenessListener);
    }
  }

  boolean allParentsContains(Topic topic, NodeHandle parent) {
    if (parent == null) {
      return false;
    }
    if (allParents.containsKey(parent)) {
      Collection<Topic> topics = allParents.get(parent);
      if (topics.contains(topic)) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  boolean allParentsContainsParent(NodeHandle parent) {
    if (parent == null) {
      return false;
    }
    if (allParents.containsKey(parent)) {
      return true;
    } else {
      return false;
    }
  }

  // This returns the topics for which the parameter 'parent' is a Scribe tree parent of the local node
  Collection<Topic> getTopicsByParent(NodeHandle parent) {
    // handle local node as null or the localHandle
    if (parent == null) parent = localHandle;
    if (parent.equals(localHandle)) return List.copyOf(roots);

    Collection<Topic> topic = allParents.get(parent);
    if (topic == null) {
      return Collections.emptyList();
    }

    return List.copyOf(topic);
  }

  // This returns the topics for which the parameter 'child' is a Scribe tree child of the local node
  Collection<Topic> getTopicsByChild(NodeHandle child) {
    if (child.equals(localHandle)) {
      logger.warn("ScribeImpl.getTopicsByChild() called with localHandle! Why would you do that?");
    }

    Collection<Topic> topic = allChildren.get(child);
    if (topic == null) {
      return Collections.emptyList();
    }
    return List.copyOf(topic);
  }

  @Override
  public String toString() {
    return topicManagers.toString();
  }

  /**
   * Forgets every topic and stops listening to the liveness of parents and children.
   */
  void destroy() {
    topicManagers.clear();
    roots.clear();

    for (NodeHandle handle : allChildren.keySet()) {
      handle.removeNodeHandleListener(livenessListener);
    }
    for (NodeHandle handle : allParents.keySet()) {
      handle.removeNodeHandleListener(livenessListener);
    }
    allChildren.clear();
    allParents.clear();
  }
}
