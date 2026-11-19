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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.maintenance.MaintainableScribe;
import rice.p2p.scribe.maintenance.ScribeMaintenancePolicy;
import rice.p2p.scribe.messaging.AnycastMessage;
import rice.p2p.scribe.messaging.DropMessage;
import rice.p2p.scribe.messaging.SubscribeAckMessage;
import rice.p2p.scribe.messaging.SubscribeFailedMessage;
import rice.p2p.scribe.messaging.SubscribeMessage;
import rice.p2p.scribe.messaging.UnsubscribeMessage;

/**
 * The parent side of Scribe: decides, with the policy, which subscribers to take on as children while
 * their subscribe messages pass through, passes the rest on, and adds and drops children.
 */
final class ChildManager {

  private final Endpoint endpoint;

  private final NodeHandle localHandle;

  private final TopicRegistry registry;

  private final Supplier<ScribePolicy> policy;

  private final ScribeMaintenancePolicy maintenancePolicy;

  /**
   * handed to the policy, and used to subscribe to topics that a new child implicitly creates
   */
  private final Scribe scribe;

  private final SubscriptionManager subscriptions;

  private final Logger logger;

  ChildManager(Endpoint endpoint, NodeHandle localHandle, TopicRegistry registry, Supplier<ScribePolicy> policy,
      ScribeMaintenancePolicy maintenancePolicy, Scribe scribe, SubscriptionManager subscriptions, Logger logger) {
    this.endpoint = endpoint;
    this.localHandle = localHandle;
    this.registry = registry;
    this.policy = policy;
    this.maintenancePolicy = maintenancePolicy;
    this.scribe = scribe;
    this.subscriptions = subscriptions;
    this.logger = logger;
  }

  /**
   * Adds a child to the given topic
   *
   * @param topic The topic to add the child to
   * @param child The child to add
   */
  void addChild(Topic topic, NodeHandle child) {
    if (addChildHelper(topic, child)) {
      // a new TopicManager was created
      scribe.subscribe(Collections.singletonList(topic), null, maintenancePolicy.implicitSubscribe(Collections.singletonList(topic)), null);
    }

    // note, there is a bit of a synchronization issue between this call and the addChildHelper call, but I don't want
    // to be holding a lock when calling addChildHelper, because it calls in to user code
    TopicManager manager = registry.getOrCreate(topic);

    // we send a confirmation back to the child
    endpoint.route(null, new SubscribeAckMessage(localHandle,
        Collections.singletonList(topic),
        Collections.singletonList(manager.getPathToRoot()), MaintainableScribe.MAINTENANCE_ID), child);
  }

  /**
   * Adds a child to the given topic and notifies the policy and the topic's clients.
   *
   * @param topic The topic
   * @param child The child to add
   * @return true if we need to subscribe to this topic because implicitly subscribing
   */
  boolean addChildHelper(Topic topic, NodeHandle child) {
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
    policy.get().childAdded(topic, child);

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
   * @param sendDrop Whether or not to send a drop message to the chil
   */
  void removeChild(Topic topic, NodeHandle child, boolean sendDrop) {
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
          registry.removeFromAllParents(topic, parent);
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

      if (sendDrop && endpoint.isAlive(child)) {
        logger.debug("Informing child {} that he has been dropped from topic {}", child, topic);

        // now, we tell the child that he has been dropped
        endpoint.route(null, new DropMessage(localHandle, topic), child);
      }

      // and lastly notify the policy and all of the clients
      policy.get().childRemoved(topic, child);

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

  /**
   * This is complicated because the SubscribeMessage may have many topics to subscribe to at once.
   * Also, for every topic we don't accept, we must branch the SubscribeMessage based on the routing table.
   *
   *
   * @return true if it needs to be forward, false if we handled it
   */
  boolean handleForwardSubscribeMessage(SubscribeMessage sMessage) {
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
      accepted = policy.get().selectAcceptedTopics(scribe, sMessage.getSubscriber(), new ArrayList<Topic>(askPolicy), sMessage.getContent());

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
        scribe.subscribe(newTopics, null, maintenancePolicy.implicitSubscribe(newTopics), null);
      } else { // isAlive
        logger.warn("Dropping subscribe message for dead {} {}", sMessage.getSubscriber(), accepted);
        accepted.clear();
      }
    } else {
      accepted = askPolicy; // also empty
    }

    List<Topic> toReturn; // which topics to include in the subscribeAck

    // this block chooses to set toReturn to only the new topics if this were due to maintenance,
    // because we don't need to ack the topics the node was already part of
    if (sMessage.getId() == MaintainableScribe.MAINTENANCE_ID) {
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
        policy.get().directAnycast(aMessage, manager.getParent(), manager.getChildren());

        // reset the source of the message to be us
        aMessage.setSource(endpoint.getLocalNodeHandle());

        // get the next hop
        NodeHandle handle = aMessage.getNext();

        // make sure that the next node is alive
        while ((handle != null) && !endpoint.isAlive(handle)) {
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
          endpoint.route(aMessage.getTopic().id(), aMessage, handle);
        }
      }
    }
    logger.trace("handleForwardSubscribeMessage() here 5 {}", sMessage);

    // handle the multi-subscription
    Map<NodeHandle, List<Topic>> manifest = subscriptions.buildManifests(noManager);
    if (manifest.containsKey(localHandle)) {
      // we are the root, but the policy would let us accept him
      List<Topic> theTopics = manifest.remove(localHandle);
      sMessage.removeTopics(theTopics);
      for (Topic topic : theTopics) {
        failed.add(topic);
      }
    }
    logger.trace("handleForwardSubscribeMessage() here 6 {}", sMessage);

    if (!failed.isEmpty()) {
      endpoint.route(null,
          new SubscribeFailedMessage(localHandle, failed, sMessage.getId()),
          sMessage.getSubscriber());
    }

    if (manifest.keySet().size() == 1) {
      logger.trace("handleForwardSubscribeMessage() returning true at this location!!! {}", sMessage);
      return true; // forward the message
    }
    for (NodeHandle nextHop : manifest.keySet()) {
      List<Topic> theTopics = manifest.get(nextHop);
      if (theTopics != null) { // this should probably never happen
        AnycastMessage aMessage = sMessage.copy(theTopics, policy.get().divideContent(theTopics, sMessage.getContent())); // use the copy constructor again
        endpoint.route(aMessage.getTopic().id(), aMessage, nextHop);
      }
    }

    return false;
  }

  /**
   * A subscribe message was delivered to us, which means that we are the root, but our policy says that
   * we cannot take on this child.
   */
  void subscribeDeliveredAtRoot(SubscribeMessage sMessage) {
    logger.debug("Sending SubscribeFailedMessage (at root) to {}", sMessage.getSubscriber());

    endpoint.route(null,
                   new SubscribeFailedMessage(localHandle, sMessage.getTopics(), sMessage.getId()),
                   sMessage.getSubscriber());
  }

  /**
   * A child left some of our topics.
   */
  void unsubscribeReceived(UnsubscribeMessage uMessage) {
    List<Topic> topics = uMessage.topics();
    NodeHandle source = uMessage.source();

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
  }
}
