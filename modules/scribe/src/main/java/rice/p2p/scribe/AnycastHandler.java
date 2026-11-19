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

import java.util.Collection;
import java.util.function.Supplier;

import org.slf4j.Logger;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.scribe.messaging.AnycastFailureMessage;
import rice.p2p.scribe.messaging.AnycastMessage;

/**
 * Anycast: walks an anycast message through a topic's tree, in the order the policy chooses, until one
 * of the visited nodes' clients accepts it, and reports back to the requestor when nobody does.
 */
final class AnycastHandler {

  private final Endpoint endpoint;

  private final NodeHandle localHandle;

  private final TopicRegistry registry;

  private final Supplier<ScribePolicy> policy;

  private final Logger logger;

  AnycastHandler(Endpoint endpoint, NodeHandle localHandle, TopicRegistry registry, Supplier<ScribePolicy> policy, Logger logger) {
    this.endpoint = endpoint;
    this.localHandle = localHandle;
    this.registry = registry;
    this.policy = policy;
    this.logger = logger;
  }

  void anycast(Topic topic, ScribeContent content, NodeHandle hint) {
    logger.trace("Anycasting content {} to topic {} with hint {}", content, topic, hint);

    AnycastMessage aMessage = new AnycastMessage(localHandle, topic, content);

    policy.get().directAnycast(aMessage, registry.getParent(topic), registry.getChildrenOfTopic(topic));

    if (hint == null || localHandle.equals(hint)) {
      // There is a bug in Freepastry where if a message is routed with a hint
      // equal to itself then even if the node is not the destimation for the id
      // field, the deliver() method will be called

      endpoint.route(topic.id(), aMessage, null);
    } else {

      endpoint.route(topic.id(), aMessage, hint);
    }
  }

  /**
   * An anycast (that is not a subscribe) passes through us: offer it to our clients, else send it on to
   * the next node to visit.
   *
   * @param message the route message that carries aMessage
   * @return Whether or not to forward the message further
   */
  boolean forward(RouteMessage message, AnycastMessage aMessage) {
    TopicManager manager = registry.get(aMessage.getTopic());

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
        if (registry.isRoot(aMessage.getTopic())) {
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
        !localHandle.equals(message.getNextHopHandle())) {
      logger.trace("DEBUG: Anycast message.forward(2), before returning true");
      return true;
    }

    logger.trace("Rejecting anycast message from {} for topic {}", aMessage.getSource(), aMessage.getTopic());

    // add the local node to the visited list
    aMessage.addVisited(endpoint.getLocalNodeHandle());

    // allow the policy to select the order in which the nodes are visited
    policy.get().directAnycast(aMessage, manager.getParent(), manager.getChildren());

    // reset the source of the message to be us
    aMessage.setSource(endpoint.getLocalNodeHandle());

    // get the next hop
    NodeHandle handle = aMessage.removeNextToVisit();

    // make sure that the next node is alive
    while ((handle != null) && !endpoint.isAlive(handle)) {
      handle = aMessage.removeNextToVisit();
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

  /**
   * An anycast (that is not a subscribe) reached the root without being accepted: tell the requestor.
   *
   * @param generatedByUs whether the local node sent the anycast
   */
  void failedAtRoot(AnycastMessage aMessage, boolean generatedByUs) {
    logger.warn("WARNING : Anycast failed at Root for Topic {} " + (generatedByUs ? "was generated by us" : "not generated by us") + "  msg= {}", aMessage.getTopic(), aMessage);
    logger.info("{}: AnycastMessage ANYCASTFAILEDHOPS {} {}", endpoint.getId(), aMessage.getVisitedSize(), aMessage.getContent());

    // We send an anycast failure message
    // TODO: use a faster constructor when the content is raw-serialized
    AnycastFailureMessage aFailMsg = new AnycastFailureMessage(endpoint.getLocalNodeHandle(), aMessage.getTopic(), aMessage.getContent());
    endpoint.route(null, aFailMsg, aMessage.getInitialRequestor());
  }

  void recvAnycastFail(Topic topic, NodeHandle failedAtNode, ScribeContent content) {
    logger.debug("received anycast failure message from {} for topic {}", failedAtNode, topic);
    policy.get().anycastFailed(topic, failedAtNode, content);
  }
}
