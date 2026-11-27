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
import java.util.List;

import org.slf4j.Logger;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.maintenance.MaintainableScribe;
import rice.p2p.scribe.maintenance.ScribeMaintenancePolicy;
import rice.p2p.scribe.messaging.DropMessage;

/**
 * Repairs the local part of the trees when they break: a parent or child dies, a parent drops us, or a
 * new neighbour takes over as the root of some of our topics. The maintenance policy decides how to
 * re-subscribe.
 */
final class TreeRepair {

  private final TopicRegistry registry;

  private final SubscriptionManager subscriptions;

  private final ChildAdmission children;

  private final ScribeMaintenancePolicy maintenancePolicy;

  /**
   * handed to the maintenance policy's callbacks
   */
  private final MaintainableScribe scribe;

  private final Logger logger;

  TreeRepair(TopicRegistry registry, SubscriptionManager subscriptions, ChildAdmission children,
      ScribeMaintenancePolicy maintenancePolicy, MaintainableScribe scribe, Logger logger) {
    this.registry = registry;
    this.subscriptions = subscriptions;
    this.children = children;
    this.maintenancePolicy = maintenancePolicy;
    this.scribe = scribe;
    this.logger = logger;
  }

  /**
   * Called when a Node's liveness changes
   *
   */
  void nodeHandleUpdate(NodeHandle handle, Object arg) {
    if (arg.equals(NodeHandle.DECLARED_DEAD)) {
      ArrayList<Topic> wasChildOfTopics = new ArrayList<Topic>(registry.getTopicsByChild(handle));
      for (Topic topic : wasChildOfTopics) {
        children.removeChild(topic, handle, true); // removeChild() unsubscribes from the parent if this was the last child, and sends a DropMessage to the child only if it is still alive (it was just declared dead, so normally not)
        logger.debug("Child {} for topic {} has died - removing.", handle, topic);
      }
      ArrayList<Topic> wasParentOfTopics = new ArrayList<Topic>(registry.getTopicsByParent(handle));
      for (Topic topic : wasParentOfTopics) {
        logger.debug("Parent {} for topic {} has died - removing.", handle, topic);
        registry.getOrCreate(topic).setParent(null, null);
      }

      maintenancePolicy.nodeFaulty(scribe, handle,
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
  void update(NodeHandle handle, boolean joined) {
    logger.info("update({}, {})", handle, joined);

    if (joined) {
      // NOTE: we could clean this up to use roots/pending, but this impl seems pretty safe
      List<Topic> notRoot = new ArrayList<Topic>();
      for(TopicTreeState manager : new ArrayList<TopicTreeState>(registry.managers())) {
        Topic topic = manager.getTopic();
        // check if new guy is root, we were old root, then subscribe
        if (!registry.isRoot(topic) && manager.getParent() == null){ // maybe we are already subscribing?
          // send subscribe message
          notRoot.add(topic);
        }
      }
      if (!notRoot.isEmpty()) {
        maintenancePolicy.noLongerRoot(scribe, notRoot);
      }
    }
  }

  /**
   * Our parent dropped us: forget it and subscribe again.
   */
  void dropReceived(DropMessage dMessage) {
    logger.debug("Received drop message from {} for topic {}", dMessage.getSource(), dMessage.getTopic());

    TopicTreeState manager = registry.get(dMessage.getTopic());

    if (manager != null) {
      if ((manager.getParent() != null) && manager.getParent().equals(dMessage.getSource())) {
        // we set the parent to be null, and then send out another subscribe message
        registry.getOrCreate(dMessage.getTopic()).setParent(null, null);

        subscriptions.sendSubscribe(dMessage.getTopic(), null,
            maintenancePolicy.implicitSubscribe(Collections.singletonList(dMessage.getTopic())),
            null);
      } else {
        logger.warn("Received unexpected drop message from non-parent {} for topic {} - ignoring", dMessage.getSource(), dMessage.getTopic());
      }
    } else {
      logger.warn("Received unexpected drop message from {} for unknown topic {} - ignoring", dMessage.getSource(), dMessage.getTopic());
    }
  }
}
