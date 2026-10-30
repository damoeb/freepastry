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
import java.util.List;

import org.slf4j.Logger;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.maintenance.MaintainableScribe;
import rice.p2p.scribe.messaging.DropMessage;
import rice.p2p.scribe.messaging.SubscribeAckMessage;

/**
 * Keeps track of the local node's part of one topic's tree: its parent, children, path to the root and
 * the local clients subscribed to the topic.
 *
 * @version $Id: ScribeImpl.java 4495 2008-09-26 14:48:02Z jeffh $
 * @author amislove
 */
public class TopicManager {

  /**
   * The topic this manager tracks
   */
  private final Topic topic;

  /**
   * The registry that owns this manager; its lock guards the tree state, and it keeps the global
   * parent/child indexes in step with this manager.
   */
  private final TopicRegistry registry;

  private final Endpoint endpoint;

  private final NodeHandle localHandle;

  private final Logger logger;

  /**
   * The current path to the root for this node
   */
  private List<Id> pathToRoot;

  /**
   * The local clients subscribed to the topic
   */
  private final ArrayList<ScribeMultiClient> clients;

  /**
   * The local node's children in the topic's tree
   */
  private final ArrayList<NodeHandle> children;

  /**
   * The local node's parent in the topic's tree, or null if it has none
   */
  private NodeHandle parent;

  /**
   * Constructor for TopicManager.
   *
   * @param topic the topic to manage
   */
  TopicManager(Topic topic, TopicRegistry registry, Endpoint endpoint, NodeHandle localHandle, Logger logger) {
    this.topic = topic;
    this.registry = registry;
    this.endpoint = endpoint;
    this.localHandle = localHandle;
    this.logger = logger;
    this.clients = new ArrayList<ScribeMultiClient>();
    this.children = new ArrayList<NodeHandle>();

    // this is a bit ugly: normally we can't be holding the topicManagers lock when calling setPathToRoot()
    // because it sends messages, but it is ok in this case because we have no children
    setPathToRoot(null);
  }

  /**
   * Gets the topic of the TopicManager object
   *
   * @return The Parent value
   */
  public Topic getTopic() {
    return topic;
  }

  /**
   * Gets the Parent attribute of the TopicManager object
   *
   * @return The Parent value
   */
  public NodeHandle getParent() {
    return parent;
  }

  /**
   * Gets the Clients attribute of the TopicManager object
   *
   * @return The Clients value
   */
  public Collection<ScribeMultiClient> getClients() {
    return Collections.unmodifiableCollection(clients);
  }

  /**
   * Gets the Children attribute of the TopicManager object
   *
   * @return The Children value
   */
  public Collection<NodeHandle> getChildren() {
    return Collections.unmodifiableCollection(children);
  }

  /**
   * Gets the PathToRoot attribute of the TopicManager object
   *
   * @return The PathToRoot value
   */
  public List<Id> getPathToRoot() {
    return pathToRoot;
  }

  /**
   * Sets the PathToRoot attribute of the TopicManager object
   *
   * Don't hold the topicManagers lock, this method sends messages
   *
   * @param pathToRoot The new PathToRoot value
   */
  public void setPathToRoot(List<Id> pathToRoot) {
    // make sure this returns if it didn't change

    // build the path to the root for the new node
    if (pathToRoot == null) {
      this.pathToRoot = new ArrayList<Id>();
    } else {
      this.pathToRoot = new ArrayList<Id>(pathToRoot);
    }
    this.pathToRoot.add(endpoint.getId());

    if (!children.isEmpty()) {
      List<NodeHandle> sendDrop = new ArrayList<NodeHandle>();
      List<NodeHandle> sendUpdate = new ArrayList<NodeHandle>();

      // now send the information out to our children, prevent routing loops
      synchronized(registry.lock()) {
        Collection<NodeHandle> children = getChildren();
        for (NodeHandle child : children) {
          if (this.pathToRoot.contains(child.getId())) {
            sendDrop.add(child);

            // Can't call removeChild() here because you will get a ConcurrentModificationException
          } else {
            sendUpdate.add(child);
          }
        }

        for (NodeHandle child : sendDrop) {
          removeChild(child);
        }
      }

      for (NodeHandle child : sendDrop) {
        endpoint.route(null, new DropMessage(localHandle, topic), child);
      }
      for (NodeHandle child : sendUpdate) {
        // TODO: send a dedicated update message instead of a maintenance SubscribeAckMessage
        endpoint.route(null, new SubscribeAckMessage(localHandle, Collections.singletonList(topic), Collections.singletonList(getPathToRoot()), MaintainableScribe.MAINTENANCE_ID), child);
      }
    }
  }

  /**
   * Sets the Parent attribute of the TopicManager object
   *
   * @param handle The new Parent value
   */
  public void setParent(NodeHandle handle, List<Id> pathToRoot) {
    logger.info("{}setParent({},{}) prev:{}", this, handle, pathToRoot, parent);

    if ((handle != null) && !endpoint.isAlive(handle)) {
      logger.warn("Setting dead parent {} for {}", handle, topic);
    }

    if ((handle != null) && (parent != null)) {
      if (handle.equals(parent)) {
        // not a real change
        setPathToRoot(pathToRoot);
        return;
      }
      logger.debug("Unexpectedly changing parents for topic {}:{}=>{}", topic, parent, handle);
    }

    NodeHandle prevParent = parent;

    parent = handle;
    setPathToRoot(pathToRoot);

    synchronized(registry.lock()) {
      // We remove the stale parent from global data structure of parent ->
      // topics
      registry.removeFromAllParents(topic, prevParent);

      // We add the new parent from the global data structure of parent ->
      // topics
      registry.addToAllParents(topic, parent);
    }
  }

  public String toString() {
    return topic.toString();
  }

  /**
   * Adds a feature to the Client attribute of the TopicManager object
   *
   * @param client The feature to be added to the Client attribute
   */
  public void addClient(ScribeMultiClient client) {
    if (client == null) return;

    if (!clients.contains(client)) {
      clients.add(client);
    }
  }

  /**
   * @param client the client to remove
   * @return true if there are no children/clients remaining (you can unsubscribe from the parent)
   */
  public boolean removeClient(ScribeMultiClient client) {
    clients.remove(client);

    boolean unsub = ((clients.size() == 0) && (children.size() == 0));

    return unsub;
  }

  /**
   * Adds a feature to the Child attribute of the TopicManager object
   *
   * Need to be holding lock: topicManagers
   *
   * @param child The feature to be added to the Child attribute
   */
  public void addChild(NodeHandle child) {
    logger.info("addChild( {}, {})", topic, child);

    if (!children.contains(child)) {
      if (endpoint.isAlive(child)) {
        children.add(child);
        // We update this information to the global data structure of children
        // -> topics
        registry.addToAllChildren(topic, child);
      } else {
        logger.warn("WARNING: addChild({}, {}) did not add child since the child.isAlive() failed", topic, child, new Exception("Stack Trace"));
      }
    }
  }

  /**
   * Removes the child from the topic.
   *
   * Need to be holding lock: topicManagers
   *
   * @param child the child to be removed
   * @return true if we can unsubscribe (IE, no clients nor children)
   */
  public boolean removeChild(NodeHandle child) {
    logger.info("removeChild( {}, {})", topic, child);

    children.remove(child);

    boolean unsub = ((clients.size() == 0) && (children.size() == 0));

    // We update this information to the global data structure of children ->
    // topics
    registry.removeFromAllChildren(topic,child);

    return unsub;
  }
}
