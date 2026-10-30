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
import java.util.function.Supplier;

import org.slf4j.Logger;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.messaging.PublishMessage;
import rice.p2p.scribe.messaging.PublishRequestMessage;
import rice.p2p.scribe.messaging.UnsubscribeMessage;

/**
 * Publish/multicast: sends a publish request to a topic's root, which pushes the content down the tree
 * to every member's clients.
 */
final class Publisher {

  private final Endpoint endpoint;

  private final NodeHandle localHandle;

  private final TopicRegistry registry;

  private final Supplier<ScribePolicy> policy;

  private final Logger logger;

  Publisher(Endpoint endpoint, NodeHandle localHandle, TopicRegistry registry, Supplier<ScribePolicy> policy, Logger logger) {
    this.endpoint = endpoint;
    this.localHandle = localHandle;
    this.registry = registry;
    this.policy = policy;
    this.logger = logger;
  }

  void publish(Topic topic, ScribeContent content) {
    logger.trace("Publishing content {} to topic {}", content, topic);

    endpoint.route(topic.getId(), new PublishRequestMessage(localHandle, topic, content), null);
  }

  /**
   * A publish request reached us, the root.
   *
   * @return the publish message to deliver locally, or null if we don't know the topic
   */
  PublishMessage publishRequestReceived(PublishRequestMessage prMessage) {
    TopicManager manager = registry.get(prMessage.getTopic());

    logger.trace("Received publish request message with data {} for topic {}", prMessage.getContent(), prMessage.getTopic());

    // if message is for a non-existant topic, drop it on the floor (we are the root, after all)
    // otherwise, turn it into a publish message, and forward it on
    if (manager == null) {
      logger.debug("Received publish request message for non-existent topic {} - dropping on floor.", prMessage.getTopic());
      return null;
    }
    return new PublishMessage(prMessage.getSource(), prMessage.getTopic(), prMessage.getContent());
  }

  /**
   * Hands a published message to our clients and pushes it on to our children.
   */
  void publishReceived(PublishMessage pMessage) {
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
      policy.get().intermediateNode(pMessage);

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
  }
}
