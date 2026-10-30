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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.commonapi.RouteMessage;
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

  /**
   * the parent side: accepts subscribers as children and drops them
   */
  private final ChildManager children;

  /**
   * walks anycasts through the trees
   */
  private final AnycastHandler anycasts;

  /**
   * pushes published content down the trees
   */
  private final Publisher publisher;

  /**
   * reacts to dead parents and children, drops and root changes
   */
  private final TreeRepair repair;

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

    ScribeConfig config = ScribeConfig.fromParameters(environment.getParameters());
    MAINTENANCE_INTERVAL = config.maintenanceInterval();
    MESSAGE_TIMEOUT = config.messageTimeout();
    this.instance = instance;
    this.endpoint = node.buildEndpoint(this, instance);
    // until the application sets its own, content cannot be read
    this.contentDeserializer = (buf, ep, contentType) -> {
      throw new IOException("No ScribeContentDeserializer set to read content of type " + contentType);
    };
    this.endpoint.setDeserializer(new ScribeMessageDeserializer(endpoint, () -> contentDeserializer, instance, logger));
    this.policy = policy;
    this.maintenancePolicy = maintenancePolicy;
    this.localHandle = endpoint.getLocalNodeHandle();
    this.registry = new TopicRegistry(endpoint, localHandle, this, logger);
    this.subscriptions = new SubscriptionManager(endpoint, localHandle, node, registry, this::getPolicy, maintenancePolicy, this, MESSAGE_TIMEOUT, logger);
    this.children = new ChildManager(endpoint, localHandle, registry, this::getPolicy, maintenancePolicy, this, subscriptions, logger);
    this.anycasts = new AnycastHandler(endpoint, localHandle, registry, this::getPolicy, logger);
    this.publisher = new Publisher(endpoint, localHandle, registry, this::getPolicy, logger);
    this.repair = new TreeRepair(registry, subscriptions, children, maintenancePolicy, this, logger);

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
    doSubscribe(Collections.singletonList(topic), client, content, hint);
  }
  public void subscribe(Collection<Topic> theTopics, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    doSubscribe(theTopics, client, content, hint);
  }

  /**
   * This method prevents re-subscription to topics you are already a part of.
   * However it allows subscription if you were the root, but there is now a root.
   */
  protected void doSubscribe(Collection<Topic> theTopics, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
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
    publisher.publish(topic, content);
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
    anycasts.anycast(topic, content, hint);
  }

  /**
   * Adds a child to the given topic
   *
   * @param topic The topic to add the child to
   * @param child The child to add
   */
  public void addChild(Topic topic, NodeHandle child) {
    children.addChild(topic, child);
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
   * @return true if we need to subscribe to this topic because implicitly subscribing
   */
  protected boolean addChildHelper(Topic topic, NodeHandle child) {
    return children.addChildHelper(topic, child);
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
    children.removeChild(topic, child, sendDrop);
  }

  public Collection<Topic> getTopicsByClient(ScribeMultiClient client) {
    return registry.getTopicsByClient(client);
  }

  protected void recvAnycastFail(Topic topic, NodeHandle failedAtNode,
      ScribeContent content) {
    anycasts.recvAnycastFail(topic, failedAtNode, content);
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
      // if it's a subscribe message, we must handle it differently
      if (internalMessage instanceof SubscribeMessage) {
        SubscribeMessage sMessage = (SubscribeMessage) internalMessage;
        return handleForwardSubscribeMessage(sMessage);
      }
      return anycasts.forward(message, aMessage);
    }

    return true;
  }

  /**
   * @return true if it needs to be forward, false if we handled it
   */
  protected boolean handleForwardSubscribeMessage(SubscribeMessage sMessage) {
    return children.handleForwardSubscribeMessage(sMessage);
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
          anycasts.failedAtRoot(aMessage, true);
        }
      } else {
        // here, we have had a subscribe message delivered, which means that we are the root, but
        // our policy says that we cannot take on this child
        if (aMessage instanceof SubscribeMessage) {
          children.subscribeDeliveredAtRoot((SubscribeMessage) aMessage);
        } else {
          anycasts.failedAtRoot(aMessage, false);
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
      PublishMessage pMessage = publisher.publishRequestReceived((PublishRequestMessage) message);
      if (pMessage != null) {
        deliver(pMessage.getTopic().getId(), pMessage);
      }
    } else if (message instanceof PublishMessage) {
      publisher.publishReceived((PublishMessage) message);
    } else if (message instanceof UnsubscribeMessage) {
      children.unsubscribeReceived((UnsubscribeMessage) message);
    } else if (message instanceof DropMessage) {
      repair.dropReceived((DropMessage) message);
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

  /**
   * Called when a Node's liveness changes
   *
   */
  public void nodeHandleUpdate(NodeHandle handle, Object arg) {
    repair.nodeHandleUpdate(handle, arg);
  }

  /**
   * This method is invoked to inform the application that the given node has either joined or left
   * the neighbor set of the local node, as the set would be returned by the neighborSet call.
   *
   * @param handle The handle that has joined/left
   * @param joined Whether the node has joined or left
   */
  public void update(NodeHandle handle, boolean joined) {
    repair.update(handle, joined);
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
