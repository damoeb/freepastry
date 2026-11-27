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
import java.util.List;

import rice.environment.Environment;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.messaging.AnycastMessage;
import rice.p2p.scribe.messaging.ScribeMessage;

/**
 * This interface represents a policy for Scribe, which is asked whenever a
 * child is about to be added or removed, or when the the local node is about to be implicitly
 * subscribed to a new topic.
 *
 * @version $Id: ScribePolicy.java 4098 2008-02-13 09:36:33Z jeffh $
 * @author Alan Mislove
 */
public interface ScribePolicy {

  /**
   * This method is called when the newChild is about to become our child, and the policy should
   * return whether or not the child should be allowed to become our child. If the length of
   * children and clients is both 0, allowing the child to join will have the effect of implicitly
   * subscribing this node the the given topic.
   *
   * Return the topics that you are willing to accept, for example by removing the others from topics.
   *
   * Here is some example code:
   *
   * <pre>
   * Iterator&lt;Topic&gt; i = topics.iterator();
   * while (i.hasNext()) {
   *   Topic topic = i.next();
   *   if (!accept(topic)) { // your decision on what to do for the topic
   *     i.remove();
   *   }
   * }
   * return topics;
   * </pre>
   *
   * Or, to accept all:
   * <pre>
   * return topics;
   * </pre>
   *
   * Some calls that are likely useful are:
   * <ul>
   *   <li>scribe.getChildrenOfTopic(topic)</li>
   *   <li>scribe.getClientsByTopic(topic)</li>
   * </ul>
   *
   * If only some of the topics are accepted, the content may need to be modified.
   * This method <i>is</i> allowed to modify the content.
   *
   * @param scribe the Scribe that is making the request
   * @param source the subscriber
   * @param topics the topics that are requested
   * @param content the content that came with the message
   * @return the list that is accepted
   */
  public List<Topic> selectAcceptedTopics(Scribe scribe, NodeHandle source, List<Topic> topics, ScribeContent content);

  /**
   * This method is called when an anycast is received which is not satisfied at the local node.
   * This method should add both the parent and child nodes to the anycast's to-search list, but
   * this method allows different policies concerning the order of the adding as well as selectively
   * adding nodes.
   *
   * @param message The anycast message in question
   * @param parent Our current parent for this message's topic
   * @param children Our current children for this message's topic
   */
  public void directAnycast(AnycastMessage message, NodeHandle parent, Collection<NodeHandle> children);

  /**
   * Informs this policy that a child was added to a topic - the topic is free to ignore this
   * upcall if it doesn't care.
   *
   * @param topic The topic the child was added to
   * @param child The child that was added
   */
  public void childAdded(Topic topic, NodeHandle child);

  /**
   * Informs this policy that a child was removed from a topic - the topic is free to ignore this
   * upcall if it doesn't care.
   *
   * @param topic The topic the child was removed from
   * @param child The child that was removed
   */
  public void childRemoved(Topic topic, NodeHandle child);

  /**
   * This notifies us when we receive a failure for a anycast
   */
  public void anycastFailed(Topic topic, NodeHandle failedAtNode, ScribeContent content);

  /**
   * This is invoked whenever this message arrives on any overlay node, this gives the ScribeMultiClient's power
   * to tap into some datastructures they might wanna edit
   */
  public void intermediateNode(ScribeMessage message);

  /**
   * This method is called when the ScribeImpl splits a SubscribeMessage into multiple parts.
   *
   * If you modify the content, you must make a copy, as the same content will be passed in for the other
   * divisions of the SubscribeMessage.  ScribeContent's are not naturally copyable, so Scribe cannot
   * make a copy apriori.
   *
   * @param theTopics topics going to a particular location
   * @param content the content that may need to be divided
   * @return the content if not changed, a new ScribeContent if changed
   */
  public ScribeContent divideContent(List<Topic> theTopics, ScribeContent content);

  /**
   * Called at the root of a topic when a publish request arrives, before the content goes down the tree. The
   * request was routed over several hops, so {@code claimedPublisher} is the publisher's own claim and not
   * authenticated: any node can publish to any topic, and name any node as the publisher. A policy that
   * restricts publishing must check something in the content it can verify, a signature. Signed topics (publisher
   * authentication, verified at the root and again at every node on the way down) are planned to hook in here.
   *
   * @param topic the topic
   * @param claimedPublisher the node the request names as its publisher, not authenticated
   * @param content the content to publish
   * @return whether to publish the content; the default publishes everything
   */
  default boolean acceptPublishRequest(Topic topic, NodeHandle claimedPublisher, ScribeContent content) {
    return true;
  }

  /**
   * The default policy for Scribe, which always allows new children to join and adds children in
   * the order in which they are provided, implicitly providing a depth-first search.
   *
   * @version $Id: ScribePolicy.java 4098 2008-02-13 09:36:33Z jeffh $
   * @author amislove
   */
  public static class DefaultScribePolicy implements ScribePolicy {
    private final Environment environment;
    public DefaultScribePolicy(Environment env) {
      environment = env;
    }
    /**
     * Accepts all topics.
     *
     * @param scribe the Scribe that is making the request
     * @param source the subscriber
     * @param topics the topics that are requested
     * @param content the content that came with the message
     * @return the topics to accept
     */
    @Override
    public List<Topic> selectAcceptedTopics(Scribe scribe, NodeHandle source, List<Topic> topics, ScribeContent content) {
      return topics;
    }

    /**
     * Adds the children in random order at the front of the to-search list and the parent at the
     * end, which implements a randomized depth-first search.
     *
     * @param message The anycast message in question
     * @param parent Our current parent for this message's topic
     * @param theChildren Our current children for this message's topic
     */
    @Override
    public void directAnycast(AnycastMessage message, NodeHandle parent, Collection<NodeHandle> theChildren) {
      if (parent != null) {
        message.addLast(parent);
      }

      ArrayList<NodeHandle> children = new ArrayList<NodeHandle>(theChildren);

      // now randomize the children list
      while (!children.isEmpty()) {
        message.addFirst(
            children.remove(
                environment.getRandomSource().nextInt(
                    children.size())));
      }
    }


    /**
     * Informs this policy that a child was added to a topic - the topic is free to ignore this
     * upcall if it doesn't care.
     *
     * @param topic The topic the child was added to
     * @param child The child that was added
     */
    @Override
    public void childAdded(Topic topic, NodeHandle child) {
    }

    /**
     * Informs this policy that a child was removed from a topic - the topic is free to ignore this
     * upcall if it doesn't care.
     *
     * @param topic The topic the child was removed from
     * @param child The child that was removed
     */
    @Override
    public void childRemoved(Topic topic, NodeHandle child) {
    }
    @Override
    public void intermediateNode(ScribeMessage message) {
    }
    @Override
    public void anycastFailed(Topic topic, NodeHandle failedAtNode, ScribeContent content) {
    }
    @Override
    public ScribeContent divideContent(List<Topic> theTopics, ScribeContent content) {
      return content;
    }
  }
}
