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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
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
   * subscribe ids are random, so that a node that does not see the subscribe cannot guess the id its ack must carry
   */
  private final SecureRandom ids = new SecureRandom();

  /**
   * topics this node keeps state for, at most
   */
  private final int maxTopics;

  /**
   * outstanding subscribes, at most
   */
  private final int maxPendingSubscribes;

  /**
   * milliseconds, for the window in which a maintenance ack from a new parent is expected
   */
  private final LongSupplier clock;

  /**
   * until when (clock time) the heartbeat subscribes of the last maintenance round may be answered by a new parent
   */
  private long heartbeatsAnsweredUntil = Long.MIN_VALUE;

  SubscriptionManager(Endpoint endpoint, NodeHandle localHandle, Node node, TopicRegistry registry, Supplier<ScribePolicy> policy,
      ScribeMaintenancePolicy maintenancePolicy, MaintainableScribe scribe, ScribeConfig config, LongSupplier clock, Logger logger) {
    this.endpoint = endpoint;
    this.localHandle = localHandle;
    this.node = node;
    this.registry = registry;
    this.policy = policy;
    this.maintenancePolicy = maintenancePolicy;
    this.scribe = scribe;
    this.messageTimeout = config.messageTimeout();
    this.maxTopics = config.maxTopics();
    this.maxPendingSubscribes = config.maxPendingSubscribes();
    this.clock = clock;
    this.logger = logger;
  }

  /**
   * The maintenance policy is about to send heartbeat subscribes (with {@link MaintainableScribe#MAINTENANCE_ID}) to
   * our parents. A parent that no longer has us as a child may pass one on, and another node may take us on and ack
   * it: such an ack is accepted for a message timeout from now.
   */
  void maintenanceStarted() {
    heartbeatsAnsweredUntil = clock.getAsLong() + messageTimeout;
  }

  /** whether a node may take on state for one more topic */
  boolean hasRoomForTopics(int newTopics) {
    return registry.topics().size() + newTopics <= maxTopics;
  }

  /**
   * This method prevents re-subscription to topics you are already a part of.
   * However it allows subscription if you were the root, but there is now a root.
   *
   */
  void doSubscribe(Collection<Topic> theTopics, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    logger.trace("Subscribing client {} to {}.", client, theTopics);

    List<Topic> toSubscribe = new ArrayList<Topic>();
    List<Topic> alreadySubscribed = new ArrayList<Topic>();
    List<Topic> refused = new ArrayList<Topic>();

    synchronized(registry.lock()) {
      for (Topic topic : theTopics) {
        TopicTreeState manager = registry.get(topic);

        // if we don't know about this topic, subscribe
        // otherwise, we simply add the client to the list
        if (manager == null) {
          if (!hasRoomForTopics(1)) {
            refused.add(topic);
            continue;
          }
          manager = registry.create(topic);
          toSubscribe.add(topic);
        } else {
          if ((manager.getParent() == null) && ! registry.isRoot(topic)) {
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
    if (!refused.isEmpty()) {
      logger.warn("Not subscribing to {} new topics: this node already keeps {} topics (p2p_scribe_max_topics)", refused.size(), maxTopics);
      if (client != null) client.subscribeFailed(refused);
    }

    if (toSubscribe.isEmpty()) return;

    sendSubscribe(toSubscribe, client, content, hint);
  }

  /**
   * This code:
   * for each Topic:
   *   1) removes the client from the TopicTreeState
   *   2)
   */
  void unsubscribe(Collection<Topic> topicsToUnsubscribe, ScribeMultiClient client) {
    logger.trace("Unsubscribing client {} from topic {}", client, registry);
    HashMap<NodeHandle, List<Topic>> needToUnsubscribe = new HashMap<NodeHandle, List<Topic>>();

    synchronized(registry.lock()) {
      for (Topic topic : topicsToUnsubscribe) {
        TopicTreeState manager = registry.get(topic);

        if (manager != null) {

          NodeHandle parent = manager.getParent();

          // if this is the last client and there are no children,
          // then we unsubscribe from the topic
          if (manager.removeClient(client)) {
            logger.info("Removing TopicTreeState for topic: {}", topic);

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
    if (subscribeLostMessages.size() >= maxPendingSubscribes) {
      // refuse rather than forget an outstanding one; implicit subscribes (no client) are retried by tree repair
      logger.warn("Not subscribing to {}: {} subscribes are outstanding (p2p_scribe_max_pending_subscribes)", topics, maxPendingSubscribes);
      if (client != null) client.subscribeFailed(topics);
      return;
    }

    // remember that we're subscribing
    registry.markPending(topics);

    // choose the UID: random and not in use, so an ack can be bound to this subscribe
    int theId;
    do {
      theId = ids.nextInt();
    } while (theId == MaintainableScribe.MAINTENANCE_ID || subscribeLostMessages.containsKey(theId));
    final int subscribeId = theId;

    // sort the topics
    if (topics.size() > 1) {
      Collections.sort(topics);
    }

    logger.atTrace().log(() -> "sendSubscribe("+topics+","+client+","+content+","+hint+") id:"+subscribeId);
    // schedule a LostMessage
    SubscribeLostMessage slm = new SubscribeLostMessage(localHandle, topics, subscribeId, client);
    CancellableTask task = endpoint.scheduleMessage(slm, messageTimeout);
    slm.putTask(task);
    subscribeLostMessages.put(subscribeId, slm);

    if (hint == null) {
      // break them into groups based on the next hop
      Map<NodeHandle, List<Topic>> manifest = buildManifests(topics);

      for (NodeHandle nextHop : manifest.keySet()) {
        List<Topic> theTopics = manifest.get(nextHop);
        SubscribeMessage msg = new SubscribeMessage(localHandle, theTopics, subscribeId, policy.get().divideContent(theTopics, content));

        // if I'm the 1st Replica, then route direct (to prevent problem with consistency)  IE, I know he
        // is the root for this message, even if he doesn't.  Make sure that if it is re-routed,
        // then the RM actually gets forwarded (in the first hop: NodeHandle is the target; in the second hop: nodeId is the next hop)

        // NOTE: this may have to be further thought through and subdivided
        NodeHandleSet set = endpoint.replicaSet(msg.getTopic().id(), 2);
        if (set.size() > 1 && localHandle.equals(set.getHandle(1))) {
          endpoint.route(null, msg, nextHop);
        } else {
          endpoint.route(msg.getTopic().id(), msg, nextHop);
        }
      }
    } else {
      // use the hint
      SubscribeMessage msg = new SubscribeMessage(localHandle, topics, subscribeId, content);

      // see if hint is my direct neighbor, if so, route only directly, so that he will accept the connection even
      // though he may still be joining
      NodeHandleSet set = endpoint.replicaSet(msg.getTopic().id(), 2);
      if (set.size() > 1 && localHandle.equals(set.getHandle(1))) {
        endpoint.route(null, msg, hint);
      } else {
        endpoint.route(msg.getTopic().id(), msg, hint);
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
   */
  Map<NodeHandle, List<Topic>> buildManifests(List<Topic> topics) {
    HashMap<NodeHandle, List<Topic>> manifest = new HashMap<NodeHandle, List<Topic>>();

    for (Topic topic : topics) {
      NodeHandleSet handleSet = endpoint.replicaSet(topic.id(), 1);
      if (handleSet.size() == 0) {
        handleSet = endpoint.localLookup(topic.id(), 1, false);
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
   * Reports the asked topics of an ack to the client of its subscribe and stops waiting for them.
   *
   * @param slm the outstanding subscribe the ack answers
   * @param asked the topics of the ack that the subscribe asked for
   */
  private void ackMessageReceived(SubscribeAckMessage message, SubscribeLostMessage slm, List<Topic> asked) {
    logger.trace("ackMessageReceived({})", message);
    ScribeMultiClient multiClient = slm.getClient();
    if (multiClient != null) multiClient.subscribeSuccess(asked);
    if (slm.topicsAcked(asked)) {
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


  /**
   * Internal method which processes a subscribe failed message. Only the topics that the outstanding subscribe with
   * the message's id still waits for count; the rest of the message is ignored.
   *
   * @param message THe lost message
   */
  void failedMessageReceived(SubscribeFailedMessage message) {
    SubscribeLostMessage slm = subscribeLostMessages.get(message.getId());
    if (slm == null) {
      logger.warn("received unexpected subscribe failed message, ignoring:{}", message);
      return;
    }
    List<Topic> failed = new ArrayList<Topic>(message.getTopics());
    failed.retainAll(slm.getTopics());
    if (failed.isEmpty()) {
      logger.info("received subscribe failed message for topics its subscribe does not wait for, ignoring:{}", message);
      return;
    }
    // only remove it if this message covers everything
    if (slm.topicsAcked(failed)) {
      subscribeLostMessages.remove(message.getId()).cancel();
    }

    ScribeMultiClient client = slm.getClient();

    logger.trace("Telling client {} about FAILURE for outstanding ack {}", client, message.getId());

    if (client != null) {
      client.subscribeFailed(failed);
    } else {
      maintenancePolicy.subscribeFailed(scribe, failed);
    }
  }


  /**
   * Internal method which processes a subscribe lost message
   *
   * @param message THe lost message
   */
  void lostMessageReceived(SubscribeLostMessage message) {
    SubscribeLostMessage slm = subscribeLostMessages.remove(message.getId());
    if (slm == null) {
      logger.debug("received subscribe lost message for an already completed subscribe, ignoring:{}", message);
      return;
    }
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
   * A node acked a subscribe of ours: adopt it as the parent of each topic the ack is an answer to (unsubscribing from
   * a previous parent), and tell it to drop us for the others.
   *
   * <p>An ack answers a topic if
   * <ul>
   *   <li>it comes from the topic's current parent (nothing changes), or</li>
   *   <li>its id is that of an outstanding subscribe of ours that still waits for the topic (subscribe ids are
   *       random), or</li>
   *   <li>it carries {@link MaintainableScribe#MAINTENANCE_ID}, which binds it to no request, and the topic has an
   *       outstanding subscribe (SplitStream's root takes a subscriber on with {@code addChild}) or a heartbeat of the
   *       last maintenance round is still within its message timeout.</li>
   * </ul>
   * An ack nobody asked for no longer replaces the parent: it was enough to make any node the parent of any other.
   * Which node may take us on is not checked (any node on the subscribe's path may); that the ack's source is the
   * node that sent it is checked by {@code ScribeImpl} against the authenticated previous hop.
   */
  void subscribeAckReceived(SubscribeAckMessage saMessage) {
    NodeHandle source = saMessage.getSource();
    // store the former parents/topics here so we can send Unsubscribe messages
    HashMap<NodeHandle, List<Topic>> needToUnsubscribe = new HashMap<NodeHandle, List<Topic>>();

    if (! endpoint.isAlive(source)) {
      logger.warn("Dropping subscribe ack message from dead node:{} for topics {}", source, saMessage.getTopics());
      return;
    }

    // decide which topics were asked for before the outstanding subscribe forgets them
    SubscribeLostMessage slm = saMessage.getId() == MaintainableScribe.MAINTENANCE_ID ? null : subscribeLostMessages.get(saMessage.getId());
    List<Topic> asked = new ArrayList<Topic>();
    for (Topic topic : saMessage.getTopics()) {
      if (isAnswer(topic, saMessage.getId(), slm)) {
        asked.add(topic);
      }
    }
    // the ack covers all of its topics, so it is processed once, not once per topic
    if (slm != null && !asked.isEmpty()) {
      ackMessageReceived(saMessage, slm, asked);
    }

    Iterator<List<Id>> iterator = saMessage.getPathsToRoot().iterator();
    for (Topic topic : saMessage.getTopics()) {
      List<Id> pathToRoot = iterator.next();
      TopicTreeState manager = registry.get(topic);

      logger.debug("Received subscribe ack message from {} for topic {}", source, topic);

      // if we're the root, reject the ack message, except for the hack to implement a centralized solution with self overlays for each node (i.e everyone is a root)
      if (registry.isRoot(topic)) {
        logger.debug("Received unexpected subscribe ack message (we are the root) from {} for topic {}", source, topic);
        needToUnsubscribe.computeIfAbsent(source, k -> new ArrayList<Topic>()).add(topic);
      } else if (manager == null) {
        // if we don't know about this topic, then we unsubscribe
        logger.warn("Received unexpected subscribe ack message from {} for unknown topic {}", source, topic);
        needToUnsubscribe.computeIfAbsent(source, k -> new ArrayList<Topic>()).add(topic);
      } else if (source.equals(manager.getParent())) {
        // our parent confirms us, nothing changes
      } else if (!asked.contains(topic)) {
        logger.info("Dropping subscribe ack from {} for topic {} that answers no subscribe of ours (id {}), parent stays {}", source, topic, saMessage.getId(), manager.getParent());
        needToUnsubscribe.computeIfAbsent(source, k -> new ArrayList<Topic>()).add(topic);
      } else {
        NodeHandle parent = manager.getParent();
        registry.getOrCreate(topic).setParent(source, pathToRoot);
        if (parent != null) {
          // our path to the root has changed
          logger.info("Replacing parent {} with {} for topic {}, which answered our subscribe", parent, source, topic);
          needToUnsubscribe.computeIfAbsent(parent, k -> new ArrayList<Topic>()).add(topic);
        }
      }
    }

    for (NodeHandle node : needToUnsubscribe.keySet()) {
      endpoint.route(null, new UnsubscribeMessage(localHandle, needToUnsubscribe.get(node)), node);
    }
  }

  /**
   * @param slm the outstanding subscribe with the ack's id, or null
   * @return whether an ack with this id answers a subscribe of ours for the topic
   */
  private boolean isAnswer(Topic topic, int ackId, SubscribeLostMessage slm) {
    if (ackId != MaintainableScribe.MAINTENANCE_ID) {
      return slm != null && slm.getTopics().contains(topic);
    }
    for (SubscribeLostMessage outstanding : subscribeLostMessages.values()) {
      if (outstanding.getTopics().contains(topic)) {
        return true;
      }
    }
    return registry.getParent(topic) != null && clock.getAsLong() < heartbeatsAnsweredUntil;
  }

}
