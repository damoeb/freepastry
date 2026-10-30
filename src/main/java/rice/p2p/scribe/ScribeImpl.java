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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.environment.params.Parameters;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.scribe.javaserialized.JavaScribeContentDeserializer;
import rice.p2p.scribe.maintenance.MaintainableScribe;
import rice.p2p.scribe.maintenance.ScribeMaintenancePolicy;
import rice.p2p.scribe.messaging.AnycastFailureMessage;
import rice.p2p.scribe.messaging.AnycastMessage;
import rice.p2p.scribe.messaging.DropMessage;
import rice.p2p.scribe.messaging.MaintenanceMessage;
import rice.p2p.scribe.messaging.PublishMessage;
import rice.p2p.scribe.messaging.PublishRequestMessage;
import rice.p2p.scribe.messaging.ScribeMessage;
import rice.p2p.scribe.messaging.SubscribeAckMessage;
import rice.p2p.scribe.messaging.SubscribeFailedMessage;
import rice.p2p.scribe.messaging.SubscribeLostMessage;
import rice.p2p.scribe.messaging.SubscribeMessage;
import rice.p2p.scribe.messaging.UnsubscribeMessage;
import rice.p2p.scribe.rawserialization.JavaSerializedScribeContent;
import rice.p2p.scribe.rawserialization.RawScribeContent;
import rice.p2p.scribe.rawserialization.ScribeContentDeserializer;

/**
 * The provided implementation of Scribe.
 *
 * @version $Id: ScribeImpl.java 4495 2008-09-26 14:48:02Z jeffh $
 * @author Alan Mislove
 */
public class ScribeImpl implements Scribe, MaintainableScribe, Application, NodeHandleListener {
  /**
   * The interval with which to perform maintenance
   */
  public final int MAINTENANCE_INTERVAL;

  /**
   * the timeout for a subscribe message
   */
  public final int MESSAGE_TIMEOUT;

  /**
   * the per-topic tree state and the parent/child indexes
   */
  private final TopicRegistry registry;

  /**
   * this scribe's policy
   */
  private ScribePolicy policy;

  /**
   * this scribe's maintenancePolicy
   */
  private ScribeMaintenancePolicy maintenancePolicy;

  /**
   * this application's endpoint
   */
  private final Endpoint endpoint;

  /**
   * the local node handle
   */
  private final NodeHandle localHandle;

  /**
   * the subscriber side: our outstanding subscriptions and our parents
   */
  private final SubscriptionManager subscriptions;

  Environment environment;

  Logger logger;

  private String instance;

  private final Node node;

  ScribeContentDeserializer contentDeserializer;

  /**
   * Constructor for Scribe, using the default policy.
   *
   * @param node The node below this Scribe implementation
   * @param instance The unique instance name of this Scribe
   */
  public ScribeImpl(Node node, String instance) {
    this(node, new ScribePolicy.DefaultScribePolicy(node.getEnvironment()), instance);
  }

  /**
   * Constructor for Scribe
   *
   * @param node The node below this Scribe implementation
   * @param policy The policy for this Scribe
   * @param instance The unique instance name of this Scribe
   */
  public ScribeImpl(Node node, ScribePolicy policy, String instance) {
    this(node, policy, instance, new ScribeMaintenancePolicy.DefaultScribeMaintenancePolicy(node.getEnvironment()));
  }

  /**
   * Constructor for Scribe
   *
   * @param node The node below this Scribe implementation
   * @param policy The policy for this Scribe
   * @param instance The unique instance name of this Scribe
   */
  public ScribeImpl(Node node, ScribePolicy policy, String instance, ScribeMaintenancePolicy maintenancePolicy) {
    this.environment = node.getEnvironment();
    this.node = node;
    logger = environment.getLogManager().getLogger(ScribeImpl.class, instance);

    Parameters p = environment.getParameters();
    MAINTENANCE_INTERVAL = p.getInt("p2p_scribe_maintenance_interval");
    MESSAGE_TIMEOUT = p.getInt("p2p_scribe_message_timeout");
    this.instance = instance;
    this.endpoint = node.buildEndpoint(this, instance);
    this.contentDeserializer = new JavaScribeContentDeserializer();
    this.endpoint.setDeserializer(new ScribeMessageDeserializer(endpoint, () -> contentDeserializer, instance, logger));
    this.policy = policy;
    this.maintenancePolicy = maintenancePolicy;
    this.localHandle = endpoint.getLocalNodeHandle();
    this.registry = new TopicRegistry(endpoint, localHandle, this, logger);
    this.subscriptions = new SubscriptionManager(endpoint, localHandle, node, registry, this::getPolicy, maintenancePolicy, this, MESSAGE_TIMEOUT, logger);

    endpoint.register();

    // schedule the period liveness checks of the parent
    endpoint.scheduleMessage(new MaintenanceMessage(), environment.getRandomSource().nextInt(MAINTENANCE_INTERVAL), MAINTENANCE_INTERVAL);

    logger.trace("Starting up Scribe");
  }

  public Environment getEnvironment() {
    return environment;
  }

  /**
   * Returns the current policy for this scribe object
   *
   * @return The current policy for this scribe
   */
  public ScribePolicy getPolicy() {
    return policy;
  }

  /**
   * Sets the current policy for this scribe object
   *
   * @param policy The current policy for this scribe
   */
  public void setPolicy(ScribePolicy policy) {
    this.policy = policy;
  }

  /**
   * Returns the Id of the local node
   *
   * @return The Id of the local node
   */
  public Id getId() {
    return endpoint.getId();
  }

  public int numChildren(Topic topic) {
    return registry.numChildren(topic);
  }

  /**
   * Returns true if there is a TopicManager associated with this topic (any
   * parent/children/client exists)
   */
  public boolean containsTopic(Topic topic) {
    return registry.containsTopic(topic);
  }

  public Collection<ScribeMultiClient> getClientsByTopic(Topic topic) {
    return registry.getClientsByTopic(topic);
  }

  public Collection<NodeHandle> getChildrenOfTopic(Topic topic) {
    return registry.getChildrenOfTopic(topic);
  }


  /**
   * Returns the parent for a given topic
   *
   * @param topic The topic to return the parent of
   * @return The parent of the topic
   */
  public NodeHandle getParent(Topic topic) {
    return registry.getParent(topic);
  }

  /**
   * Returns whether or not this Scribe is the root for the given topic
   *
   * @param topic The topic in question
   * @return Whether or not we are currently the root
   */
  public boolean isRoot(Topic topic) {
    return registry.isRoot(topic);
  }

  public NodeHandle getRoot(Topic topic) {
    NodeHandleSet set = endpoint.replicaSet(topic.getId(), 1);

    if (set.size() == 0)
      return null;
    else
      return set.getHandle(0);
  }

  // ----- SCRIBE METHODS -----
  public boolean containsChild(Topic topic, NodeHandle child) {
    return registry.containsChild(topic, child);
  }

  public void subscribe(Collection<Topic> topics) {
    doSubscribe(topics, null, null, null);
  }

  /**
   * Subscribes the given client to the provided topic. Any message published to the topic will be
   * delivered to the Client via the deliver() method.
   *
   * @param topic The topic to subscribe to
   * @param client The client to give messages to
   */
  public void subscribe(Topic topic, ScribeMultiClient client) {
    doSubscribe(Collections.singletonList(topic), client, null, null);
  }

  /**
   * Subscribes the given client to the provided topic. Any message published to the topic will be
   * delivered to the Client via the deliver() method.
   *
   * @param topic The topic to subscribe to
   * @param client The client to give messages to
   * @param content The content to include in the subscribe
   * @param hint The first hop of the message, or null
   */
  public void subscribe(Topic topic, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    doSubscribe(Collections.singletonList(topic), client, toRawScribeContent(content), hint);
  }
  public void subscribe(Topic topic, ScribeMultiClient client, RawScribeContent content, NodeHandle hint) {
    doSubscribe(Collections.singletonList(topic), client, content, hint);
  }
  public void subscribe(Collection<Topic> theTopics, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    doSubscribe(theTopics, client, toRawScribeContent(content), hint);
  }
  public void subscribe(Collection<Topic> theTopics, ScribeMultiClient client, RawScribeContent content, NodeHandle hint) {
    doSubscribe(theTopics, client, content, hint);
  }

  protected RawScribeContent toRawScribeContent(ScribeContent content) {
    return content instanceof RawScribeContent ? (RawScribeContent)content : new JavaSerializedScribeContent(content);
  }

  /**
   * This method prevents re-subscription to topics you are already a part of.
   * However it allows subscription if you were the root, but there is now a root.
   */
  protected void doSubscribe(Collection<Topic> theTopics, ScribeMultiClient client, RawScribeContent content, NodeHandle hint) {
    subscriptions.doSubscribe(theTopics, client, content, hint);
  }


  public void unsubscribe(Topic topic, ScribeMultiClient client) {
    unsubscribe(Collections.singletonList(topic), client);
  }
  public void unsubscribe(Collection<Topic> topicsToUnsubscribe, ScribeMultiClient client) {
    subscriptions.unsubscribe(topicsToUnsubscribe, client);
  }

  /**
   * Publishes the given message to the topic.
   *
   * @param topic The topic to publish to
   * @param content The content to publish
   */
  public void publish(Topic topic, ScribeContent content) {
    publish(topic, content instanceof RawScribeContent ? (RawScribeContent)content : new JavaSerializedScribeContent(content));
  }

  public void publish(Topic topic, RawScribeContent content) {
    logger.trace("Publishing content {} to topic {}", content, topic);

    endpoint.route(topic.getId(), new PublishRequestMessage(localHandle, topic, content), null);
  }

  /**
   * Anycasts the given content to a member of the given topic
   *
   * @param topic The topic to anycast to
   * @param content The content to anycast
   */
  public void anycast(Topic topic, ScribeContent content) {
    anycast(topic, content, null);
  }

  public void anycast(Topic topic, ScribeContent content, NodeHandle hint) {
    if (content instanceof RawScribeContent) {
      anycast(topic, (RawScribeContent)content, hint);
    } else {
      anycast(topic, new JavaSerializedScribeContent(content), hint);
    }
  }

  public void anycast(Topic topic, RawScribeContent content) {
    anycast(topic, content, null);
  }

  public void anycast(Topic topic, RawScribeContent content, NodeHandle hint) {
    logger.trace("Anycasting content {} to topic {} with hint {}", content, topic, hint);

    AnycastMessage aMsg = new AnycastMessage(localHandle, topic, content);

    policy.directAnycast(aMsg, getParent(topic), getChildrenOfTopic(topic));

    if (hint == null || localHandle.equals(hint)) {
      // There is a bug in Freepastry where if a message is routed with a hint
      // equal to itself then even if the node is not the destimation for the id
      // field, the deliver() method will be called

      endpoint.route(topic.getId(), aMsg, null);
    } else {

      endpoint.route(topic.getId(), aMsg, hint);
    }
  }

  /**
   * Adds a child to the given topic
   *
   * @param topic The topic to add the child to
   * @param child The child to add
   */
  public void addChild(Topic topic, NodeHandle child) {
    if (addChildHelper(topic, child)) {
      // a new TopicManager was created
      subscribe(Collections.singletonList(topic), null, maintenancePolicy.implicitSubscribe(Collections.singletonList(topic)), null);
    }

    // note, there is a bit of a synchronization issue between this call and the addChildHelper call, but I don't want
    // to be holding a lock when calling addChildHelper, because it calls in to user code
    TopicManager manager = getTopicManager(topic);

    // we send a confirmation back to the child
    endpoint.route(null, new SubscribeAckMessage(localHandle,
        Collections.singletonList(topic),
        Collections.singletonList(manager.getPathToRoot()), MAINTENANCE_ID), child);
  }

  public void setParent(Topic topic, NodeHandle parent, List<Id> pathToRoot) {
    TopicManager manager = getTopicManager(topic);
    manager.setParent(parent, pathToRoot);
  }

  /**
   * Lazy constructor.
   * @param topic
   * @return never null
   */
  public TopicManager getTopicManager(Topic topic) {
    return registry.getOrCreate(topic);
  }

  /**
   * Adds a child to the given topic and notifies the policy and the topic's clients.
   *
   * @param topic The topic
   * @param child The child to add
   * @return true if we need to subscribe to this topic because implicitly subscribing
   */
  protected boolean addChildHelper(Topic topic, NodeHandle child) {
    logger.trace("addChild({},{})", topic, child);

    boolean ret = false;

    TopicManager manager;

    List<ScribeMultiClient> clientList;
    synchronized(registry.lock()) {
      manager = registry.get(topic);

      // if we don't know about the topic, we subscribe, otherwise,
      // we simply add the child to the list
      if (manager == null) {
        manager = registry.create(topic);

        logger.trace("Implicitly subscribing to topic {}", topic);
        ret = true;
      }

      // need to be holding topicManagers for the call to addToAllChildren
      manager.addChild(child);
      clientList = new ArrayList<ScribeMultiClient>(manager.getClients());
    }


    // and lastly notify the policy and all of the clients
    policy.childAdded(topic, child);

    for (ScribeMultiClient client : clientList) {
      client.childAdded(topic, child);
    }

    return ret;
  }

  /**
   * Removes a child from the given topic
   *
   * @param topic The topic to remove the child from
   * @param child The child to remove
   */
  public void removeChild(Topic topic, NodeHandle child) {
    removeChild(topic, child, true);
  }

  /**
   * Removes a child from the given topic
   *
   * @param topic The topic to remove the child from
   * @param child The child to remove
   * @param sendDrop Whether or not to send a drop message to the chil
   */
  protected void removeChild(Topic topic, NodeHandle child, boolean sendDrop) {
    logger.debug("Removing child {} from topic {}", child, topic);

    boolean sendUnsubscribe = false;

    TopicManager manager;

    synchronized(registry.lock()) {
      manager = registry.get(topic);
      if (manager != null) {
        // if this is the last child and there are no clients, then
        // we unsubscribe, if we are not the root
        NodeHandle parent = manager.getParent();

        sendUnsubscribe = manager.removeChild(child);
        if (sendUnsubscribe) {
          logger.info("Removing TopicManager for topic: {}", topic);

          registry.remove(topic);
          // Since we will be removing the topicManager which will also get rid of
          // the parent, we need to remove the stale parent from the global data
          // structure of parent -> topics
          removeFromAllParents(topic, parent);
        }
      }
    }

    if (manager != null) {
      NodeHandle parent = manager.getParent();
      if (sendUnsubscribe) {
        logger.debug("We no longer need topic {} - unsubscribing from parent {}", topic, parent);

        if (parent != null) {
          endpoint.route(null, new UnsubscribeMessage(localHandle, Collections.singletonList(topic)), parent);
        }
      }

      if ((sendDrop) && (endpoint.isAlive(child))) {
        logger.debug("Informing child {} that he has been dropped from topic {}", child, topic);

        // now, we tell the child that he has been dropped
        endpoint.route(null, new DropMessage(localHandle, topic), child);
      }

      // and lastly notify the policy and all of the clients
      policy.childRemoved(topic, child);

      List<ScribeMultiClient> clientList;
      synchronized(registry.lock()) {
        clientList = new ArrayList<ScribeMultiClient>(manager.getClients());
      }
      for (ScribeMultiClient client : clientList) {
        client.childRemoved(topic, child);
      }
    } else {
      logger.warn("Unexpected attempt to remove child {} from unknown topic {}", child, topic);
    }
  }

  public Collection<Topic> getTopicsByClient(ScribeMultiClient client) {
    return registry.getTopicsByClient(client);
  }

  protected void recvAnycastFail(Topic topic, NodeHandle failedAtNode,
      ScribeContent content) {
    logger.debug("received anycast failure message from {} for topic {}", failedAtNode, topic);
    policy.recvAnycastFail(topic, failedAtNode, content);
  }

  /**
   * Need to be holding lock: topicManagers
   */
  protected void addToAllChildren(Topic t, NodeHandle child) {
    registry.addToAllChildren(t, child);
  }

  /**
   * Need to be holding lock: topicManagers
   */
  protected void removeFromAllChildren(Topic t, NodeHandle child) {
    registry.removeFromAllChildren(t, child);
  }

  protected void addToAllParents(Topic t, NodeHandle parent) {
    registry.addToAllParents(t, parent);
  }

  protected void removeFromAllParents(Topic t, NodeHandle parent) {
    registry.removeFromAllParents(t, parent);
  }

  public boolean allParentsContains(Topic t, NodeHandle parent) {
    return registry.allParentsContains(t, parent);
  }

  public boolean allParentsContainsParent(NodeHandle parent) {
    return registry.allParentsContainsParent(parent);
  }

  public void printAllParentsDataStructure() {
    registry.printAllParentsDataStructure();
  }

  public void printAllChildrenDataStructure() {
    registry.printAllChildrenDataStructure();
  }

  // This returns the topics for which the parameter 'parent' is a Scribe tree parent of the local node
  public Collection<Topic> getTopicsByParent(NodeHandle parent) {
    return registry.getTopicsByParent(parent);
  }

  // This returns the topics for which the parameter 'child' is a Scribe tree child of the local node
  public Collection<Topic> getTopicsByChild(NodeHandle child) {
    return registry.getTopicsByChild(child);
  }



  // ----- COMMON API METHODS -----

  /**
   * This method is invoked on applications when the underlying node is about to forward the given
   * message with the provided target to the specified next hop. Applications can change the
   * contents of the message, specify a different nextHop (through re-routing), or completely
   * terminate the message.
   *
   * @param message The message being sent, containing an internal message along with a destination
   *      key and nodeHandle next hop.
   * @return Whether or not to forward the message further
   */
  public boolean forward(final RouteMessage message) {

    Message internalMessage;
    try {
      internalMessage = message.getMessage(endpoint.getDeserializer());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    logger.trace("Forward called with {} {}", internalMessage, internalMessage.getClass().getName());
    if(internalMessage instanceof ScribeMessage) {
      policy.intermediateNode((ScribeMessage)internalMessage);
    }

    if (internalMessage instanceof AnycastMessage) {
      AnycastMessage aMessage = (AnycastMessage) internalMessage;

      // get the topic manager associated with this topic
      if (aMessage.getTopic() == null) {
        throw new RuntimeException("topic is null!");
      }
      TopicManager manager = registry.get(aMessage.getTopic());

      // if it's a subscribe message, we must handle it differently
      if (internalMessage instanceof SubscribeMessage) {
        SubscribeMessage sMessage = (SubscribeMessage) internalMessage;
        return handleForwardSubscribeMessage(sMessage);
      } else {
        // Note that since forward() is called also on the outgoing path, it
        // could be that the last visited node of the anycast message is itself,
        // then in that case we return true
        logger.trace("DEBUG: Anycast message.forward(1)");
        // There is a special case in the modified exhaustive anycast traversal
        // algorithm where the traversal ends at the node which happens to be
        // the best choice in the bag of prospectiveresponders. In this scenario
        // the local node's anycast() method will be visited again

        if (endpoint.getLocalNodeHandle().equals(aMessage.getLastVisited())
            && !endpoint.getLocalNodeHandle().equals(
                aMessage.getInitialRequestor())) {
          if (logger.isTraceEnabled()) {
            logger.trace("Bypassing forward logic of anycast message becuase local node is the last visited node "
                    + aMessage.getLastVisited() + " of in the anycast message ");
            if (isRoot(aMessage.getTopic())) {
                logger.trace("Local node is the root of anycast group "
                    + aMessage.getTopic());
            }
          }
          return true;
        }

        // if we are not associated with this topic at all, let the
        // anycast continue
        if (manager == null) {
          logger.trace("Manager of anycast group is null");
          return true;
        }

        Collection<ScribeMultiClient> clients = manager.getClients();

        // see if one of our clients will accept the anycast
        for (ScribeMultiClient client : clients) {
          if (client.anycast(aMessage.getTopic(), aMessage.getContent())) {
            logger.trace("Accepting anycast message from {} for topic {}", aMessage.getSource(), aMessage.getTopic());

            return false;
          }
        }

        // if we are the orginator for this anycast and it already has a destination,
        // we let it go ahead
        if (aMessage.getSource().getId().equals(endpoint.getId()) &&
            (message.getNextHopHandle() != null) &&
            (!localHandle.equals(message.getNextHopHandle()))) {
          logger.trace("DEBUG: Anycast message.forward(2), before returning true");
          return true;
        }

        logger.trace("Rejecting anycast message from {} for topic {}", aMessage.getSource(), aMessage.getTopic());
      }

      // add the local node to the visited list
      aMessage.addVisited(endpoint.getLocalNodeHandle());

      // allow the policy to select the order in which the nodes are visited
      policy.directAnycast(aMessage, manager.getParent(), manager.getChildren());

      // reset the source of the message to be us
      aMessage.setSource(endpoint.getLocalNodeHandle());

      // get the next hop
      NodeHandle handle = aMessage.getNext();

      // make sure that the next node is alive
      while ((handle != null) && (!endpoint.isAlive(handle))) {
        handle = aMessage.getNext();
      }

      logger.trace("Forwarding anycast message for topic {}on to {}", aMessage.getTopic(), handle);

      if (handle == null) {
        logger.debug("Anycast {} failed.", aMessage);

          logger.info("Anycast failed at this intermediate node:{}\nAnycastMessage ANYCASTFAILEDHOPS {} {}", aMessage, aMessage.getVisitedSize(), aMessage.getContent());
          // We will send an anycast failure message
          // TODO: use a faster constructor when the content is raw-serialized
          AnycastFailureMessage aFailMsg = new AnycastFailureMessage(endpoint.getLocalNodeHandle(), aMessage.getTopic(), aMessage.getContent());
          endpoint.route(null, aFailMsg, aMessage.getInitialRequestor());
      } else {

        logger.trace("forward() routing {} to {}", aMessage, handle);
        endpoint.route(null, aMessage, handle);
      }

      return false;
    }

    return true;
  }

  /**
   * This is complicated because the SubscribeMessage may have many topics to subscribe to at once.
   * Also, for every topic we don't accept, we must branch the SubscribeMessage based on the routing table.
   *
   *
   * @param sMessage
   * @return true if it needs to be forward, false if we handled it
   */
  protected boolean handleForwardSubscribeMessage(SubscribeMessage sMessage) {
    logger.trace("handleForwardScribeMessage({})", sMessage);

    // if this is our own subscribe message, ignore it
    if (sMessage.getSource().getId().equals(endpoint.getId())) {
      logger.info("Bypassing forward logic of subscribemessage {} becuase local node is the subscriber source.", sMessage);
      return true;
    }

    // Note: deliver only gets called on the root
    // deliver() should only be called under the following circumstances
    // a) we are the root and the subscriber
    // b) nobody would take the node, so we return him a SubscribeFailedMessage
    // only return true if there are topics left in the SubscribeMessage that we aren't responsible for
    // c) we filled the request

    // first, we have to make sure that we don't create a loop, which would occur
    // if the subscribing node's previous parent is on our path to the root
    // also check to make sure that we have not already accepted the node
    // first, we'll distill the list into 3 categories
    // 1) topics that will cause a loop
    // 2) topics that are already subscribed
    // 3) topics that we need to ask the policy
    logger.trace("handleForwardSubscribeMessage() here 1 {}", sMessage);

    // The first step is to break up the topics into 3 groups.
    // forward: If we are in the subscriber's pathToRoot (to prevent loops)
    // dontForward: If we are already the parent
    // askPolicy: everyone else (after the policy is asked, these will be placed into forward/dontForward appropriately)

    // this is the list of topics that aren't a loop or already a child
    ArrayList<Topic> forward = new ArrayList<Topic>(); // leave these in the message
    ArrayList<Topic> dontForward = new ArrayList<Topic>(); // take these out, and maybe send a response message for these, the old policy just drops these, We should probably send a response as long as sMessage.getId() != NO_ACK_REQUIRED
    ArrayList<Topic> askPolicy = new ArrayList<Topic>(); // ask the policy, and take these out if the policy accepts them

    for (Topic topic : sMessage.getTopics()) {
      TopicManager tmanager = registry.get(topic);

      if (tmanager != null) {
        List<Id> path = tmanager.getPathToRoot();

        if (path.contains(sMessage.getSubscriber().getId())) {
          logger.atInfo().log(() -> {
            String s = "Rejecting subscribe message from " +
                    sMessage.getSubscriber() + " for topic " + sMessage.getTopic() +
                    " because we are on the subscriber's path to the root:";
            for (Id id : path) {
              s+=id+",";
            }
            return s;
          });
          forward.add(topic);
          continue;
        }

        // check if child is already there
        if (tmanager.getChildren().contains(sMessage.getSubscriber())){
          dontForward.add(topic);
          continue;
        }
      }

      askPolicy.add(topic);
    }

    List<Topic> accepted; // these are the messages that the policy accepted

    logger.trace("handleForwardSubscribeMessage() here 2 {}", sMessage);
    if (!askPolicy.isEmpty()) {

      // see if the policy will allow us to take on this child
      accepted = policy.allowSubscribe(this, sMessage.getSubscriber(), new ArrayList<Topic>(askPolicy), sMessage.getContent());

      // askPolicy is now the rejected
      askPolicy.removeAll(accepted);

      dontForward.addAll(accepted);

      // we only acually add if the node is alive
      // Why is this the right policy?  Maybe we are incorrect about the liveness of the node, but
      // we don't want anycasts to bounce all over the ring until they hit every node,
      // thus, if we were going to accept the node, then we drop that topic here, and forward the rest
      List<Topic> newTopics = new ArrayList<Topic>();
      if (endpoint.isAlive(sMessage.getSubscriber())) {
        for (Topic topic : accepted) {

        // the ones that are returned:
        //    a) call addChild()
        //    b) put into the SubscribeAckMessage
        // the rejected:
        //   leave in the sMessage

          logger.trace("Hijacking subscribe message from {} for topic {}", sMessage.getSubscriber(), topic);

          // if so, add the child
          if (addChildHelper(topic, sMessage.getSubscriber())) {
            // the child implicitly created a topic, need to subscribe
            newTopics.add(topic);
          }
        }

        // the topic is new to us, so we need to subscribe
        subscribe(newTopics, null, maintenancePolicy.implicitSubscribe(newTopics), null);
      } else { // isAlive
        logger.warn("Dropping subscribe message for dead {} {}", sMessage.getSubscriber(), accepted);
        accepted.clear();
      }
    } else {
      accepted = askPolicy; // also empty
    }

    // just a nicer name for it
    List<Topic> rejected = askPolicy;

    forward.addAll(rejected);

    List<Topic> toReturn; // which topics to include in the subscribeAck

    // this block chooses to set toReturn to only the new topics if this were due to maintenance,
    // because we don't need to ack the topics the node was already part of
    if (sMessage.getId() == MAINTENANCE_ID) {
      toReturn = accepted; // to update the rootToPath
    } else {
      toReturn = dontForward; // because the subscriber doesn't know he's already joined
    }

    logger.trace("handleForwardSubscribeMessage() here 3 {}", sMessage);


    // NOTE: We need the isAlive() check, otherwise the tmanager (below) may be null, if we
    // were to create teh tmanager just for this node.
    if (!toReturn.isEmpty() && endpoint.isAlive(sMessage.getSubscriber())) {
      // we send a confirmation back to the child

      // build all of the pathToRoot[]
      List<List<Id>> paths = new ArrayList<List<Id>>(toReturn.size());
      for (Topic topic : toReturn) {
        TopicManager tmanager = registry.get(topic);
        paths.add(tmanager.getPathToRoot());
      }

      endpoint.route(null, new SubscribeAckMessage(localHandle, toReturn, paths, sMessage.getId()), sMessage.getSubscriber());
    }

    // otherwise, we are effectively rejecting the child
    sMessage.removeTopics(dontForward);

    logger.trace("Rejecting subscribe message from {} for topic(s) {}", sMessage.getSubscriber(), sMessage.getTopics());

    if (sMessage.isEmpty()) {
      // there are no more topics in the message, we handled them all
      logger.trace("handleForwardSubscribeMessage() returning false here 85");
      return false; // the buck stops here, cause all requests are filled
    }

    // add the local node to the visited list
    sMessage.addVisited(endpoint.getLocalNodeHandle());

    logger.trace("handleForwardSubscribeMessage() here 4 {}", sMessage);

    // there are Topics left that we didn't accept:
    // a) for each that we have a manager, call directAnycast, this is the old way to have good management of trees
    // b) for topics that we don't have a manager, split them up based on the underlieing overlay's router and route them that way, this is the new version for multi-subscription

    List<Topic> noManager = new ArrayList<Topic>(); // these are the topics we don't have a manager for
    List<Topic> failed = new ArrayList<Topic>(); // these are the topics that have exhausted the tree (they visited everyone and were rejected)

    Iterator<Topic> topicIterator = sMessage.getTopics().iterator();
    while (topicIterator.hasNext()) {
      Topic topic = topicIterator.next();

      // the topic can change on the SubscribeMessage when we accept some of the items
      TopicManager manager = registry.get(topic);

      // we have nothing to do with this message
      if (manager == null) {
        noManager.add(topic);
      } else {
        topicIterator.remove(); // in case we just forward the message
        AnycastMessage aMessage = sMessage.copy(Collections.singletonList(topic), sMessage.getRawContent());// clone but with just the 1 topic, make sure to copy toVisit/visited/content

        // allow the policy to select the order in which the nodes are visited
        policy.directAnycast(aMessage, manager.getParent(), manager.getChildren());

        // reset the source of the message to be us
        aMessage.setSource(endpoint.getLocalNodeHandle());

        // get the next hop
        NodeHandle handle = aMessage.getNext();

        // make sure that the next node is alive
        while ((handle != null) && (!endpoint.isAlive(handle))) {
          handle = aMessage.getNext();
        }

        if (handle == null || !endpoint.isAlive(handle)) handle = null;

        logger.trace("Forwarding anycast message for topic {}on to {}", aMessage.getTopic(), handle);

        if (handle == null) {
          logger.debug("Anycast {} failed.", aMessage);

          // if it's a subscribe message, send a subscribe failed message back
          // as a courtesy
          logger.trace("Sending SubscribeFailedMessage to {} for topic {}", sMessage.getSubscriber(), topic);

          failed.add(topic);

        } else {
          logger.trace("handleForwardSubscribeMessage() routing {} to {}", aMessage, handle);
          endpoint.route(aMessage.getTopic().getId(), aMessage, handle);
        }
      }
    }
    logger.trace("handleForwardSubscribeMessage() here 5 {}", sMessage);

    // handle the multi-subscription
    HashMap<NodeHandle, List<Topic>> manifest = subscriptions.buildManifests(noManager);
    if (manifest.containsKey(localHandle)) {
      // we are the root, but the policy would let us accept him
      List<Topic> theTopics = manifest.remove(localHandle);
      sMessage.removeTopics(theTopics);
      for (Topic topic : theTopics) {
        failed.add(topic);
      }
    }
    logger.trace("handleForwardSubscribeMessage() here 6 {}", sMessage);

    endpoint.route(null,
        new SubscribeFailedMessage(localHandle, failed, sMessage.getId()),
        sMessage.getSubscriber());

    if (manifest.keySet().size() == 1) {
      logger.trace("handleForwardSubscribeMessage() returning true at this location!!! {}", sMessage);
      return true; // forward the message
    }
    for (NodeHandle nextHop : manifest.keySet()) {
      List<Topic> theTopics = manifest.get(nextHop);
      if (theTopics != null) { // this should probably never happen
        AnycastMessage aMessage = sMessage.copy(theTopics, convert(policy.divideContent(theTopics, sMessage.getContent()))); // use the copy constructor again
        endpoint.route(aMessage.getTopic().getId(), aMessage, nextHop);
      }
    }

    return false;
  }

  /**
   * This method is called on the application at the destination node for the given id.
   *
   * @param id The destination id of the message
   * @param message The message being sent
   */
  public void deliver(Id id, Message message) {
    logger.trace("Deliver called with {} {}", id, message);

    if (message instanceof AnycastMessage) {
      AnycastMessage aMessage = (AnycastMessage) message;

      // if we are the recipient to someone else's subscribe, then we should have processed
      // this message in the forward() method.
      // Otherwise, we received our own subscribe message, which means that we are
      // the root
      if (aMessage.getSource().equals(localHandle)) {
        if (aMessage instanceof SubscribeMessage) {
          subscriptions.ownSubscribeDelivered((SubscribeMessage) message);
        } else {
          logger.warn("WARNING : Anycast failed at Root for Topic {} was generated by us  msg= {}", aMessage.getTopic(), aMessage);
          logger.info("{}: AnycastMessage ANYCASTFAILEDHOPS {} {}", endpoint.getId(), aMessage.getVisitedSize(), aMessage.getContent());

          // We send a anycast failure message
          // TODO: use a faster constructor when the content is raw-serialized
          AnycastFailureMessage aFailMsg = new AnycastFailureMessage(endpoint.getLocalNodeHandle(), aMessage.getTopic(), aMessage.getContent());
          endpoint.route(null, aFailMsg, aMessage.getInitialRequestor());
        }
      } else {
        // here, we have had a subscribe message delivered, which means that we are the root, but
        // our policy says that we cannot take on this child
        if (aMessage instanceof SubscribeMessage) {
          SubscribeMessage sMessage = (SubscribeMessage) aMessage;
          logger.debug("Sending SubscribeFailedMessage (at root) to {}", sMessage.getSubscriber());

          endpoint.route(null,
                         new SubscribeFailedMessage(localHandle, sMessage.getTopics(), sMessage.getId()),
                         sMessage.getSubscriber());
        } else {
          logger.warn("WARNING : Anycast failed at Root for Topic {} not generated by us  msg= {}", aMessage.getTopic(), aMessage);
          logger.info("{}: AnycastMessage ANYCASTFAILEDHOPS {} {}", endpoint.getId(), aMessage.getVisitedSize(), aMessage.getContent());

          // We send an anycast failure message
          AnycastFailureMessage aFailMsg = new AnycastFailureMessage(endpoint.getLocalNodeHandle(), aMessage.getTopic(), aMessage.getContent());
          endpoint.route(null, aFailMsg, aMessage.getInitialRequestor());
        }
      }
    } else if (message instanceof SubscribeAckMessage) {
      subscriptions.subscribeAckReceived((SubscribeAckMessage) message);
    } else if (message instanceof SubscribeLostMessage) {
      SubscribeLostMessage slMessage = (SubscribeLostMessage) message;

      subscriptions.lostMessageReceived(slMessage);
    } else if (message instanceof SubscribeFailedMessage) {
      SubscribeFailedMessage sfMessage = (SubscribeFailedMessage) message;

      subscriptions.failedMessageReceived(sfMessage);
    } else if (message instanceof PublishRequestMessage) {
      PublishRequestMessage prMessage = (PublishRequestMessage) message;
      TopicManager manager = registry.get(prMessage.getTopic());

      logger.trace("Received publish request message with data {} for topic {}", prMessage.getContent(), prMessage.getTopic());

      // if message is for a non-existant topic, drop it on the floor (we are the root, after all)
      // otherwise, turn it into a publish message, and forward it on
      if (manager == null) {
        logger.debug("Received publish request message for non-existent topic {} - dropping on floor.", prMessage.getTopic());
      } else {
        deliver(prMessage.getTopic().getId(), new PublishMessage(prMessage.getSource(), prMessage.getTopic(), prMessage.getContent()));
      }
    } else if (message instanceof PublishMessage) {
      PublishMessage pMessage = (PublishMessage) message;
      TopicManager manager = registry.get(pMessage.getTopic());

      logger.trace("Received publish message with data {} for topic {}", pMessage.getContent(), pMessage.getTopic());

      // if we don't know about this topic, or this message did
      // not come from our parent, send an unsubscribe message
      // otherwise, we deliver the message to all clients and forward the
      // message to all children
      if ((manager != null) && ((manager.getParent() == null) || (manager.getParent().equals(pMessage.getSource())))) {
        pMessage.setSource(localHandle);

        Collection<ScribeMultiClient> clients = manager.getClients();

        // We also do an upcall intermediateNode() to allow implicit subscribers to change state in the message
        policy.intermediateNode(pMessage);

        for (ScribeMultiClient client : clients) {
          logger.trace("Delivering publish message with data {} for topic {} to client {}", pMessage.getContent(), pMessage.getTopic(), client);
          client.deliver(pMessage.getTopic(), pMessage.getContent());
        }

        Collection<NodeHandle> handles = new ArrayList<NodeHandle>(manager.getChildren());

        for (NodeHandle handle : handles) {
          logger.trace("Forwarding publish message with data {} for topic {} to child {}", pMessage.getContent(), pMessage.getTopic(), handle);
          endpoint.route(null, new PublishMessage(endpoint.getLocalNodeHandle(), pMessage.getTopic(), pMessage.getContent()), handle);
        }
      } else {
        logger.warn("Received unexpected publish message from {} for unknown topic {}", pMessage.getSource(), pMessage.getTopic());

        endpoint.route(null, new UnsubscribeMessage(localHandle, Collections.singletonList(pMessage.getTopic())), pMessage.getSource());
      }
    } else if (message instanceof UnsubscribeMessage) {
      UnsubscribeMessage uMessage = (UnsubscribeMessage) message;
      List<Topic> topics = uMessage.getTopics();
      NodeHandle source = uMessage.getSource();

      logger.atDebug().log(() -> {
        String s;
        if (topics.size() == 1) {
          s = " for topic " + topics.get(0).toString();
        } else {
          s = " for "+topics.size()+" topics.";
        }
        return "Received unsubscribe message from " +
            source + s;
      });

      for (Topic topic : topics) {
        removeChild(topic, source, false);
      }
    } else if (message instanceof DropMessage) {
      DropMessage dMessage = (DropMessage) message;
      logger.debug("Received drop message from {} for topic {}", dMessage.getSource(), dMessage.getTopic());

      TopicManager manager = registry.get(dMessage.getTopic());

      if (manager != null) {
        if ((manager.getParent() != null) && manager.getParent().equals(dMessage.getSource())) {
          // we set the parent to be null, and then send out another subscribe message
          setParent(dMessage.getTopic(), null, null);
          Collection<ScribeMultiClient> clients = manager.getClients();

          subscriptions.sendSubscribe(dMessage.getTopic(), null,
              maintenancePolicy.implicitSubscribe(Collections.singletonList(dMessage.getTopic())),
              null);
        } else {
          logger.warn("Received unexpected drop message from non-parent {} for topic {} - ignoring", dMessage.getSource(), dMessage.getTopic());
        }
      } else {
        logger.warn("Received unexpected drop message from {} for unknown topic {} - ignoring", dMessage.getSource(), dMessage.getTopic());
      }
    } else if (message instanceof MaintenanceMessage) {
      logger.debug("Received maintenance message");
      maintenancePolicy.doMaintenance(this);
    } else if (message instanceof AnycastFailureMessage) {
      AnycastFailureMessage aFailMsg = (AnycastFailureMessage) message;
      logger.debug("Received anycast failure message from {} for topic {}", aFailMsg.getSource(), aFailMsg.getTopic());
      recvAnycastFail(aFailMsg.getTopic(), aFailMsg.getSource(), aFailMsg.getContent());
    } else {
      logger.warn("Received unknown message {} - dropping on floor.", message);
    }
  }

  protected RawScribeContent convert(ScribeContent content) {
    return SubscriptionManager.convert(content);
  }

  /**
   * Called when a Node's liveness changes
   *
   */
  public void nodeHandleUpdate(NodeHandle handle, Object arg) {
    if (arg.equals(NodeHandle.DECLARED_DEAD)) {
      ArrayList<Topic> wasChildOfTopics = new ArrayList<Topic>(getTopicsByChild(handle));
      for (Topic topic : wasChildOfTopics) {
        removeChild(topic,handle); // TODO: check which messages removeChild() sends here
        logger.debug("Child {} for topic {} has died - removing.", handle, topic);
      }
      ArrayList<Topic> wasParentOfTopics = new ArrayList<Topic>(getTopicsByParent(handle));
      for (Topic topic : wasParentOfTopics) {
        logger.debug("Parent {} for topic {} has died - removing.", handle, topic);
        setParent(topic, null, null);
      }

      maintenancePolicy.nodeFaulty(this, handle,
          wasParentOfTopics,
          wasChildOfTopics);

    }
  }

  /**
   * This method is invoked to inform the application that the given node has either joined or left
   * the neighbor set of the local node, as the set would be returned by the neighborSet call.
   *
   * @param handle The handle that has joined/left
   * @param joined Whether the node has joined or left
   */
  public void update(NodeHandle handle, boolean joined) {
    logger.info("update({}, {})", handle, joined);

    if (joined) {
      // NOTE: we could clean this up to use roots/pending, but this impl seems pretty safe
      List<Topic> notRoot = new ArrayList<Topic>();
      for(TopicManager manager : (new ArrayList<TopicManager>(registry.managers()))) {
        Topic topic = manager.getTopic();
        // check if new guy is root, we were old root, then subscribe
        if (!isRoot(topic) && manager.getParent() == null){ // maybe we are already subscribing?
          // send subscribe message
          notRoot.add(topic);
        }
      }
      if (!notRoot.isEmpty()) {
        maintenancePolicy.noLongerRoot(this, notRoot);
      }
    }
  }


  @Override
  public String toString() {
    return "ScribeImpl["+localHandle+"]";
  }

  public void destroy() {
    if (environment.getSelectorManager().isSelectorThread()) {
      logger.info("Destroying {}", this);
      registry.destroy();
    } else {
      environment.getSelectorManager().invoke(() -> destroy());
    }
  }

  public Endpoint getEndpoint() {
    return endpoint;
  }

  public void setContentDeserializer(ScribeContentDeserializer deserializer) {
    contentDeserializer = deserializer;
  }

  public ScribeContentDeserializer getContentDeserializer() {
    return contentDeserializer;
  }

  public Collection<Topic> getTopics() {
    return registry.topics();
  }

  public List<Id> getPathToRoot(Topic topic) {
    return registry.getPathToRoot(topic);
  }
}
