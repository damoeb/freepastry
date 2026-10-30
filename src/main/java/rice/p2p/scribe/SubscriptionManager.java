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
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.scribe.maintenance.MaintainableScribe;
import rice.p2p.scribe.maintenance.ScribeMaintenancePolicy;
import rice.p2p.scribe.messaging.SubscribeAckMessage;
import rice.p2p.scribe.messaging.SubscribeFailedMessage;
import rice.p2p.scribe.messaging.SubscribeLostMessage;
import rice.p2p.scribe.messaging.SubscribeMessage;
import rice.p2p.scribe.messaging.UnsubscribeMessage;

/**
 * The subscriber side of Scribe: joins and leaves topic trees, tracks each outstanding subscribe until it
 * is acked, fails or times out (the subscribe-lost timer), and adopts the parent that acks it.
 */
final class SubscriptionManager {

  private final Endpoint endpoint;

  private final NodeHandle localHandle;

  private final Node node;

  private final TopicRegistry registry;

  private final Supplier<ScribePolicy> policy;

  private final ScribeMaintenancePolicy maintenancePolicy;

  /**
   * handed to the maintenance policy's callbacks
   */
  private final MaintainableScribe scribe;

  /**
   * the timeout for a subscribe message
   */
  private final int messageTimeout;

  private final Logger logger;

  /**
   * The hashtable of outstanding lost messags keyed by the UID of the SubscribeMessage
   */
  private HashMap<Integer, SubscribeLostMessage> subscribeLostMessages = new HashMap<Integer, SubscribeLostMessage>();

  /**
   * the next unique id
   */
  private int id = Integer.MIN_VALUE;
  private final Object idLock = new Object();

  SubscriptionManager(Endpoint endpoint, NodeHandle localHandle, Node node, TopicRegistry registry, Supplier<ScribePolicy> policy,
      ScribeMaintenancePolicy maintenancePolicy, MaintainableScribe scribe, int messageTimeout, Logger logger) {
    this.endpoint = endpoint;
    this.localHandle = localHandle;
    this.node = node;
    this.registry = registry;
    this.policy = policy;
    this.maintenancePolicy = maintenancePolicy;
    this.scribe = scribe;
    this.messageTimeout = messageTimeout;
    this.logger = logger;
  }

  /**
   * This method prevents re-subscription to topics you are already a part of.
   * However it allows subscription if you were the root, but there is now a root.
   *
   * @param theTopics
   * @param client
   * @param content
   * @param hint
   */
  void doSubscribe(Collection<Topic> theTopics, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    logger.trace("Subscribing client {} to {}.", client, theTopics);

    List<Topic> toSubscribe = new ArrayList<Topic>();
    List<Topic> alreadySubscribed = new ArrayList<Topic>();

    synchronized(registry.lock()) {
      for (Topic topic : theTopics) {
        TopicManager manager = registry.get(topic);

        // if we don't know about this topic, subscribe
        // otherwise, we simply add the client to the list
        if (manager == null) {
          manager = registry.create(topic);
          toSubscribe.add(topic);
        } else {
          if ((manager.getParent() == null) && (! registry.isRoot(topic))) {
            toSubscribe.add(topic);
          } else {
            // else, no need to subscribe
            alreadySubscribed.add(topic);
          }
        }
        manager.addClient(client);
      }
    }

    // we may need to make this call on the Selector thread for better consistency
    if (client != null) {
      if (!alreadySubscribed.isEmpty()) client.subscribeSuccess(alreadySubscribed);
    }

    if (toSubscribe.isEmpty()) return;

    sendSubscribe(toSubscribe, client, content, hint);
  }

  /**
   * This code:
   * for each Topic:
   *   1) removes the client from the TopicManager
   *   2)
   */
  void unsubscribe(Collection<Topic> topicsToUnsubscribe, ScribeMultiClient client) {
    logger.trace("Unsubscribing client {} from topic {}", client, registry);
    HashMap<NodeHandle, List<Topic>> needToUnsubscribe = new HashMap<NodeHandle, List<Topic>>();

    synchronized(registry.lock()) {
      for (Topic topic : topicsToUnsubscribe) {
        TopicManager manager = registry.get(topic);

        if (manager != null) {

          NodeHandle parent = manager.getParent();

          // if this is the last client and there are no children,
          // then we unsubscribe from the topic
          if (manager.removeClient(client)) {
            logger.info("Removing TopicManager for topic: {}", topic);

            registry.remove(topic);

            // After we remove the topicManager we must call updateParents() to remove the parent from the parent dat structure
            registry.removeFromAllParents(topic, parent);

            if (parent != null) {
              List<Topic> theTopics = needToUnsubscribe.get(parent);
              if (theTopics == null) {
                theTopics = new ArrayList<Topic>();
                needToUnsubscribe.put(parent, theTopics);
              }
              theTopics.add(topic);
            }
          }
        } else {
          logger.warn("Attempt to unsubscribe client {} from unknown topic {}", client, topic);
        }
      }
    }

    for (NodeHandle parent : needToUnsubscribe.keySet()) {
      endpoint.route(null, new UnsubscribeMessage(localHandle, needToUnsubscribe.get(parent)), parent);
    }
  }

  /**
   * Internal methods for sending a subscribe message
   */
  void sendSubscribe(Topic topic, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    sendSubscribe(Collections.singletonList(topic), client, content, hint);
  }


  void sendSubscribe(List<Topic> topics, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    // remember that we're subscribing
    registry.markPending(topics);

    // choose the UID
    int theId;
    synchronized(idLock) {
      id++;
      if (id == MaintainableScribe.MAINTENANCE_ID) id++;
      theId = id;
    }

    // sort the topics
    if (topics.size() > 1) {
      Collections.sort(topics);
    }

    logger.atTrace().log(() -> "sendSubscribe("+topics+","+client+","+content+","+hint+") theId:"+theId);
    // schedule a LostMessage
    SubscribeLostMessage slm = new SubscribeLostMessage(localHandle, topics, theId, client);
    CancellableTask task = endpoint.scheduleMessage(slm, messageTimeout);
    slm.putTask(task);
    subscribeLostMessages.put(theId, slm);

    if (hint == null) {
      // break them into groups based on the next hop
      HashMap<NodeHandle, List<Topic>> manifest = buildManifests(topics);

      for (NodeHandle nextHop : manifest.keySet()) {
        List<Topic> theTopics = manifest.get(nextHop);
        SubscribeMessage msg = new SubscribeMessage(localHandle, theTopics, theId, policy.get().divideContent(theTopics, content));

        // if I'm the 1st Replica, then route direct (to prevent problem with consistency)  IE, I know he
        // is the root for this message, even if he doesn't.  Make sure that if it is re-routed,
        // then the RM actually gets forwarded (in the first hop: NodeHandle is the target; in the second hop: nodeId is the next hop)

        // NOTE: this may have to be further thought through and subdivided
        NodeHandleSet set = endpoint.replicaSet(msg.getTopic().getId(), 2);
        if (set.size() > 1 && set.getHandle(1) == localHandle) {
          endpoint.route(null, msg, nextHop);
        } else {
          endpoint.route(msg.getTopic().getId(), msg, nextHop);
        }
      }
    } else {
      // use the hint
      SubscribeMessage msg = new SubscribeMessage(localHandle, topics, theId, content);

      // see if hint is my direct neighbor, if so, route only directly, so that he will accept the connection even
      // though he may still be joining
      NodeHandleSet set = endpoint.replicaSet(msg.getTopic().getId(), 2);
      if (set.size() > 1 && set.getHandle(1) == localHandle) {
        endpoint.route(null, msg, hint);
      } else {
        endpoint.route(msg.getTopic().getId(), msg, hint);
      }
    }
  }


  /**
   * This checks the underlieing overlay for the next hop for each of a
   * number of topics and groups them accordingly.  Uses the Local NodeHandle
   * for topics rooted here.
   *
   * Maintains the order of the Topics
   *
   * @return
   */
  HashMap<NodeHandle, List<Topic>> buildManifests(List<Topic> topics) {
    HashMap<NodeHandle, List<Topic>> manifest = new HashMap<NodeHandle, List<Topic>>();

    for (Topic topic : topics) {
      NodeHandleSet handleSet = endpoint.replicaSet(topic.getId(), 1);
      if (handleSet.size() == 0) {
        handleSet = endpoint.localLookup(topic.getId(), 1, false);
      }

      // no next hop = we are the next hop
      NodeHandle handle = null;
      if (handleSet.size() > 0) handle = handleSet.getHandle(0);

      if (handle == null) {
        handle = this.localHandle;
        if (!registry.isRoot(topic)) {
          if (logger.isWarnEnabled()) {
            logger.warn("buildManifests("+topics+") did not receive a next hop for topic "+topic+" but we are not the root of the topic. isRoot = "+registry.isRoot(topic));
            // print handleSet
            logger.warn("handle set:"+handleSet);
            // print leafset
            logger.warn(node.printRouteState());
          }
        }
      }

      List<Topic> theTopics = manifest.get(handle);
      if (theTopics == null) {
        theTopics = new ArrayList<Topic>();
        manifest.put(handle, theTopics);
      }
      theTopics.add(topic);
    }

    if (logger.isTraceEnabled()) {
      logger.trace("buildManifest()");
      for (NodeHandle node : manifest.keySet()) {
        logger.trace("  "+node+" "+manifest.get(node));
      }
    }

    return manifest;
  }


  /**
   * Internal method which processes an ack message
   *
   * @param message The ackMessage
   */
  void ackMessageReceived(SubscribeAckMessage message) {
    logger.trace("ackMessageReceived({})", message);
    SubscribeLostMessage slm = subscribeLostMessages.get(message.getId());
    if (slm == null) {
      logger.debug("ackMessageReceived({}) for unknown id", message);
    } else {
      ScribeMultiClient multiClient = slm.getClient();
      if (multiClient != null) multiClient.subscribeSuccess(message.getTopics());
      if (slm.topicsAcked(message.getTopics())) {
        logger.trace("Removing client {} from list of outstanding for ack {}", slm.getClient(), message.getId());
        subscribeLostMessages.remove(message.getId()).cancel();
      } else {
         logger.atTrace().log(() -> {
           Collection<Topic> topics = slm.getTopics();
           int numTopics = topics.size();
           return "Still waiting for SubscribeAck from "+
               (numTopics == 1 ?
                   " topic "+topics.iterator().next() +"." :
                   numTopics+" topics.");
         });
      }
    }
  }


  /**
   * Internal method which processes a subscribe failed message
   *
   * @param message THe lost message
   */
  void failedMessageReceived(SubscribeFailedMessage message) {
    SubscribeLostMessage slm = subscribeLostMessages.get(message.getId());
    if (slm == null) {
      logger.warn("received unexpected subscribe failed message, ignoring:{}", message);
      return;
    }
    // only remove it if this message covers everything
    if (slm.topicsAcked(message.getTopics())) {
      subscribeLostMessages.remove(message.getId()).cancel();
    }

    ScribeMultiClient client = slm.getClient();

    logger.trace("Telling client {} about FAILURE for outstanding ack {}", client, message.getId());

    if (client != null) {
      client.subscribeFailed(message.getTopics());
    } else {
      maintenancePolicy.subscribeFailed(scribe, message.getTopics());
    }
  }


  /**
   * Internal method which processes a subscribe lost message
   *
   * @param message THe lost message
   */
  void lostMessageReceived(SubscribeLostMessage message) {
    SubscribeLostMessage slm = subscribeLostMessages.remove(message.getId());
    ScribeMultiClient client = slm.getClient();

    logger.trace("Telling client {} about LOSS for outstanding ack {}", client, message.getId());

    ArrayList<Topic> failedTopics = new ArrayList<Topic>();
    for (Topic topic : message.getTopics()) {
      NodeHandle parent = registry.getParent(topic);
      if (!registry.isRoot(topic) && (parent == null))
        failedTopics.add(topic);
    }

    if (!failedTopics.isEmpty()) {
      if (client != null) {
        client.subscribeFailed(failedTopics);
      } else {
        maintenancePolicy.subscribeFailed(scribe, failedTopics);
      }
    }
  }


  /**
   * Our own subscribe message was delivered to us, so we are the root of its topics.
   */
  void ownSubscribeDelivered(SubscribeMessage sMessage) {
    if (sMessage.isEmpty()) return; // we already handled the subscribe message

    SubscribeLostMessage slm = subscribeLostMessages.get(sMessage.getId());
    if (slm != null) {
      ScribeMultiClient multiClient = slm.getClient();
      if (multiClient != null) multiClient.subscribeSuccess(sMessage.getTopics());
      // I'm not sure what to do here, because we should be the root, but if we aren't?  I suspect it is a bug.
      if (slm.topicsAcked(sMessage.getTopics())) {
        logger.trace("Removing client {} from list of outstanding for ack {}", slm.getClient(), sMessage.getId());
        subscribeLostMessages.remove(sMessage.getId()).cancel();
      }
    }
    for (Topic topic : sMessage.getTopics()) {
      if (!registry.isRoot(topic)) {
        logger.warn("Received our own subscribe message {} for topic {} - we are not the root.", sMessage, sMessage.getTopic());
      }
    }
    logger.debug("Received our own subscribe message {} for topic {} - we are the root.", sMessage, sMessage.getTopic());
  }

  /**
   * A parent accepted us: adopt it (unsubscribing from a previous parent), or reject the ack if we
   * don't need it.
   */
  void subscribeAckReceived(SubscribeAckMessage saMessage) {
    // store the former parents/topics here so we can send Unsubscribe messages
    HashMap<NodeHandle, List<Topic>> needToUnsubscribe = new HashMap<NodeHandle, List<Topic>>();

    if (! endpoint.isAlive(saMessage.getSource())) {
      logger.warn("Dropping subscribe ack message from dead node:{} for topics {}", saMessage.getSource(), saMessage.getTopics());
      return;
    }

    Iterator<List<Id>> i = saMessage.getPathsToRoot().iterator();
    for (Topic topic : saMessage.getTopics()) {
      List<Id> pathToRoot = i.next();
      TopicManager manager = registry.get(topic);

      logger.debug("Received subscribe ack message from {} for topic {}", saMessage.getSource(), topic);

      ackMessageReceived(saMessage);

      // if we're the root, reject the ack message, except for the hack to implement a centralized solution with self overlays for each node (i.e everyone is a root)
      if (registry.isRoot(topic)) {
        logger.debug("Received unexpected subscribe ack message (we are the root) from {} for topic {}", saMessage.getSource(), topic);
        List<Topic> topics = needToUnsubscribe.get(saMessage.getSource());
        if (topics == null) topics = new ArrayList<Topic>();
        topics.add(topic);
      } else {
        // if we don't know about this topic, then we unsubscribe
        // if we already have a parent, then this is either an errorous
        // subscribe ack, or our path to the root has changed.
        if (manager == null) {
          logger.warn("Received unexpected subscribe ack message from {} for unknown topic {}", saMessage.getSource(), topic);
          List<Topic> topics = needToUnsubscribe.get(saMessage.getSource());
          if (topics == null) topics = new ArrayList<Topic>();
          topics.add(topic);
        } else {
          if (manager.getParent() == null) {
            registry.getOrCreate(topic).setParent(saMessage.getSource(), pathToRoot);
          }

          if (!manager.getParent().equals(saMessage.getSource())) {
            logger.warn("Received somewhat unexpected subscribe ack message (already have parent {}) from {} for topic {} - the new policy is now to accept the message", manager.getParent(), saMessage.getSource(), topic);


            NodeHandle parent = manager.getParent();
            registry.getOrCreate(topic).setParent(saMessage.getSource(), pathToRoot);

            List<Topic> topics = needToUnsubscribe.get(parent);
            if (topics == null) {
              topics = new ArrayList<Topic>();
              needToUnsubscribe.put(parent, topics);
            }
            topics.add(topic);
          }
        }
      }
      for(NodeHandle source : needToUnsubscribe.keySet()) {
        endpoint.route(null, new UnsubscribeMessage(localHandle, needToUnsubscribe.get(source)), source);
      }
    }
  }

}
