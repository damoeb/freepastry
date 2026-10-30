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

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.scribe.maintenance.MaintainableScribe;
import rice.p2p.scribe.maintenance.ScribeMaintenancePolicy;
import rice.p2p.scribe.messaging.AnycastFailureMessage;
import rice.p2p.scribe.messaging.AnycastMessage;
import rice.p2p.scribe.messaging.DropMessage;
import rice.p2p.scribe.messaging.MaintenanceMessage;
import rice.p2p.scribe.messaging.PublishMessage;
import rice.p2p.scribe.messaging.PublishRequestMessage;
import rice.p2p.scribe.messaging.ScribeProtocolMessage;
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
    logger = environment.getLogManager().getLogger(ScribeImpl.class, instance);

    ScribeConfig config = ScribeConfig.fromParameters(environment.getParameters());
    MAINTENANCE_INTERVAL = config.maintenanceInterval();
    MESSAGE_TIMEOUT = config.messageTimeout();
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

  @Override
  public Environment getEnvironment() {
    return environment;
  }

  /**
   * Returns the current policy for this scribe object
   *
   * @return The current policy for this scribe
   */
  @Override
  public ScribePolicy getPolicy() {
    return policy;
  }

  /**
   * Sets the current policy for this scribe object
   *
   * @param policy The current policy for this scribe
   */
  @Override
  public void setPolicy(ScribePolicy policy) {
    this.policy = policy;
  }

  @Override
  public Collection<ScribeMultiClient> getClientsByTopic(Topic topic) {
    return registry.getClientsByTopic(topic);
  }

  @Override
  public Collection<NodeHandle> getChildrenOfTopic(Topic topic) {
    return registry.getChildrenOfTopic(topic);
  }


  /**
   * Returns the parent for a given topic
   *
   * @param topic The topic to return the parent of
   * @return The parent of the topic
   */
  @Override
  public NodeHandle getParent(Topic topic) {
    return registry.getParent(topic);
  }

  /**
   * Returns whether or not this Scribe is the root for the given topic
   *
   * @param topic The topic in question
   * @return Whether or not we are currently the root
   */
  @Override
  public boolean isRoot(Topic topic) {
    return registry.isRoot(topic);
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
  @Override
  public void subscribe(Topic topic, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    doSubscribe(Collections.singletonList(topic), client, content, hint);
  }
  @Override
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
  @Override
  public void publish(Topic topic, ScribeContent content) {
    publisher.publish(topic, content);
  }

  /**
   * Anycasts the given content to a member of the given topic
   *
   * @param topic The topic to anycast to
   * @param content The content to anycast
   */
  @Override
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
  @Override
  public void addChild(Topic topic, NodeHandle child) {
    children.addChild(topic, child);
  }

  /**
   * Removes a child from the given topic
   *
   * @param topic The topic to remove the child from
   * @param child The child to remove
   */
  @Override
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

  protected void recvAnycastFail(Topic topic, NodeHandle failedAtNode,
      ScribeContent content) {
    anycasts.recvAnycastFail(topic, failedAtNode, content);
  }

  protected void addToAllParents(Topic t, NodeHandle parent) {
    registry.addToAllParents(t, parent);
  }

  public boolean allParentsContains(Topic t, NodeHandle parent) {
    return registry.allParentsContains(t, parent);
  }

  public boolean allParentsContainsParent(NodeHandle parent) {
    return registry.allParentsContainsParent(parent);
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
  @Override
  public boolean forward(final RouteMessage message) {

    Message internalMessage;
    try {
      internalMessage = message.getMessage(endpoint.getDeserializer());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    logger.trace("Forward called with {} {}", internalMessage, internalMessage.getClass().getName());
    if(internalMessage instanceof ScribeMessage scribeMessage) {
      policy.intermediateNode(scribeMessage);
    }

    if (internalMessage instanceof AnycastMessage aMessage) {

      // get the topic manager associated with this topic
      if (aMessage.getTopic() == null) {
        throw new RuntimeException("topic is null!");
      }
      // if it's a subscribe message, we must handle it differently
      if (internalMessage instanceof SubscribeMessage sMessage) {
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
  @Override
  public void deliver(Id id, Message message) {
    logger.trace("Deliver called with {} {}", id, message);

    if (!(message instanceof ScribeProtocolMessage scribeMessage)) {
      logger.warn("Received unknown message {} - dropping on floor.", message);
      return;
    }
    switch (scribeMessage) {
      case AnycastMessage aMessage -> deliverAnycast(aMessage);
      case SubscribeAckMessage saMessage -> subscriptions.subscribeAckReceived(saMessage);
      case SubscribeLostMessage slMessage -> subscriptions.lostMessageReceived(slMessage);
      case SubscribeFailedMessage sfMessage -> subscriptions.failedMessageReceived(sfMessage);
      case PublishRequestMessage prMessage -> {
        PublishMessage pMessage = publisher.publishRequestReceived(prMessage);
        if (pMessage != null) {
          deliver(pMessage.getTopic().id(), pMessage);
        }
      }
      case PublishMessage pMessage -> publisher.publishReceived(pMessage);
      case UnsubscribeMessage uMessage -> children.unsubscribeReceived(uMessage);
      case DropMessage dMessage -> repair.dropReceived(dMessage);
      case MaintenanceMessage mMessage -> {
        logger.debug("Received maintenance message");
        maintenancePolicy.doMaintenance(this);
      }
      case AnycastFailureMessage aFailMsg -> {
        logger.debug("Received anycast failure message from {} for topic {}", aFailMsg.getSource(), aFailMsg.getTopic());
        recvAnycastFail(aFailMsg.getTopic(), aFailMsg.getSource(), aFailMsg.getContent());
      }
    }
  }

  private void deliverAnycast(AnycastMessage aMessage) {
    // if we are the recipient to someone else's subscribe, then we should have processed
    // this message in the forward() method.
    // Otherwise, we received our own subscribe message, which means that we are
    // the root
    if (aMessage.getSource().equals(localHandle)) {
      if (aMessage instanceof SubscribeMessage sMessage) {
        subscriptions.ownSubscribeDelivered(sMessage);
      } else {
        anycasts.failedAtRoot(aMessage, true);
      }
    } else {
      // here, we have had a subscribe message delivered, which means that we are the root, but
      // our policy says that we cannot take on this child
      if (aMessage instanceof SubscribeMessage sMessage) {
        children.subscribeDeliveredAtRoot(sMessage);
      } else {
        anycasts.failedAtRoot(aMessage, false);
      }
    }
  }

  /**
   * Called when a Node's liveness changes
   *
   */
  @Override
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
  @Override
  public void update(NodeHandle handle, boolean joined) {
    repair.update(handle, joined);
  }


  @Override
  public String toString() {
    return "ScribeImpl["+localHandle+"]";
  }

  @Override
  public void destroy() {
    if (environment.getSelectorManager().isSelectorThread()) {
      logger.info("Destroying {}", this);
      registry.destroy();
    } else {
      environment.getSelectorManager().invoke(() -> destroy());
    }
  }

  @Override
  public Endpoint getEndpoint() {
    return endpoint;
  }

  @Override
  public void setContentDeserializer(ScribeContentDeserializer deserializer) {
    contentDeserializer = deserializer;
  }

  @Override
  public Collection<Topic> getTopics() {
    return registry.topics();
  }
}
