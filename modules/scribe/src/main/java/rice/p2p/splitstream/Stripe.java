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

package rice.p2p.splitstream;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Vector;

import org.slf4j.Logger;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.Scribe;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.ScribeImpl;
import rice.p2p.scribe.ScribeMultiClient;
import rice.p2p.scribe.Topic;
import rice.selector.TimerTask;

/**
 * This class encapsulates all data about an individual stripe. It is the basic unit in the system.
 * It is rooted at the stripeId for the stripe. It is through the stripe that data is sent. It can
 * be subscribed to in which case data is recieved or it can be unsubscribed from. A stripe can have
 * some number of children and controlling this is the way that bandwidth is controlled. If a stripe
 * is dropped then it searches for a new parent.
 *
 * @version $Id: Stripe.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Ansley Post
 * @author Alan Mislove
 * @author Atul Singh
 */
public class Stripe implements ScribeMultiClient {

  /**
   * The maximum number of failed subscriptions
   */
  public final int MAX_FAILED_SUBSCRIPTION;// = 5;

  /**
   * The stripeId for this stripe
   */
  private final StripeId stripeId;

  /**
   * The topic corresponding to this stripeId
   */
  private final Topic topic;

  /**
   * The scribe object
   */
  private final Scribe scribe;

  /**
   * The list of SplitStreamClients interested in data from this client
   */
  private final Vector<SplitStreamClient> clients;

  /**
   * This stripe's channel
   */
  private final Channel channel;

  /**
   * The count of failed subscribe messages
   */
  private final Hashtable<Topic, Integer> failed;

  Logger logger;

  /**
   * how long to wait before resubscribing after too many failed subscriptions, in milliseconds
   */
  private final int retryDelay;

  /**
   * The constructor used when creating a stripe from scratch.
   *
   * @param stripeId the stripeId that this stripe will be rooted at
   * @param scribe the scribe the stripe is running on top of
   */
  public Stripe(StripeId stripeId, Scribe scribe, String instance, Channel channel, int maxFailedSubscriptions) {
    this.MAX_FAILED_SUBSCRIPTION = maxFailedSubscriptions;
    this.retryDelay = SplitStreamConfig.fromParameters(scribe.getEnvironment().getParameters()).maxFailedSubscriptionRetryDelay();
    this.stripeId = stripeId;
    this.scribe = scribe;
    logger = scribe.getEnvironment().getLogManager().getLogger(Stripe.class, instance);
    this.channel = channel;
    this.failed = new Hashtable<Topic, Integer>();
    this.clients = new Vector<SplitStreamClient>();
    this.topic = new Topic(stripeId.id());
  }

  /**
   * gets the StripeID for this stripe
   *
   * @return theStripeID
   */
  public StripeId getStripeId() {
    return stripeId;
  }

  /**
   * Adds a client to this stripe - the client will be informed whenever data arrives for this stripe
   *
   * @param client The client to add
   */
  public void subscribe(SplitStreamClient client) {
    if (! clients.contains(client)) {
      if (clients.size() == 0) {
        scribe.subscribe(topic, this, (ScribeContent) null, null);
      }

      clients.add(client);
    }
  }

  /**
   * Publishes the given data to this stripe
   *
   * @param data The data to publish
   */
  public void publish(byte[] data) {
    scribe.publish(topic, new SplitStreamContent(data));
  }

  /**
   * This method is invoked when an anycast is received for a topic which this client is interested
   * in. The client should return whether or not the anycast should continue.
   *
   * @param topic The topic the message was anycasted to
   * @param content The content which was anycasted
   * @return Whether or not the anycast should continue
   */
  @Override
  public boolean anycast(Topic topic, ScribeContent content) {
      return false;
  }

  /**
   * This method is invoked when a message is delivered for a topic this client is interested in.
   *
   * @param topic The topic the message was published to
   * @param content The content which was published
   */
  @Override
  public void deliver(Topic topic, ScribeContent content) {
    if (this.topic.equals(topic)) {
      if (content instanceof SplitStreamContent splitStreamContent) {
        byte[] data = splitStreamContent.data();

        SplitStreamClient[] clients = (SplitStreamClient[]) this.clients.toArray(new SplitStreamClient[0]);

        for (SplitStreamClient client : clients) {
          client.deliver(this, data);
        }
      } else {
        logger.warn("Received unexpected content {}", content);
      }
    } else {
      logger.warn("Received update for unexcpected topic {} content {}", topic, content);
    }
  }

  /**
   * Informs this client that a child was added to a topic in which it was interested in.
   *
   * @param topic The topic to unsubscribe from
   * @param child The child that was added
   */
  @Override
  public void childAdded(Topic topic, NodeHandle child) {
    logger.debug("childAdded({},{})", topic, child);
  }

  /**
   * Informs this client that a child was removed from a topic in which it was interested in.
   *
   * @param topic The topic to unsubscribe from
   * @param child The child that was removed
   */
  @Override
  public void childRemoved(Topic topic, NodeHandle child) {
    logger.debug("childRemoved({},{})", topic, child);
  }


  /**
   * Informs this client that subscriptions failed
   *
   * @param topics The topics that failed
   */
  @Override
  public void subscribeFailed(Collection<Topic> topics) {
    for (Topic topic : topics) {
      retrySubscribe(topic);
    }
  }

  /**
   * Informs this client that subscriptions succeeded, which resets their failure counts.
   *
   * @param topics The topics that succeeded
   */
  @Override
  public void subscribeSuccess(Collection<Topic> topics) {
    for (Topic topic : topics) {
      failed.remove(topic);
    }
  }

  /**
   * Retries the subscription after a failure
   *
   * @param topic The topic that failed
   */
  private void retrySubscribe(final Topic topic) {
    Integer count = (Integer) failed.get(topic);

    if (count == null) {
      count = Integer.valueOf(0);
    }

    if (count.intValue() < MAX_FAILED_SUBSCRIPTION) {
      count = Integer.valueOf(count.intValue() + 1);

      logger.warn("DEBUG :: Subscription failed at {} for topic {} - retrying.", channel.getLocalId(), topic);
      scribe.subscribe(topic, this, (ScribeContent) null, null);

      failed.put(topic, count);
    } else {
      // TODO: retry with a random exponential backoff instead of a fixed delay
      TimerTask resubscribeTask = new TimerTask() {
        @Override
        public void run() {
          if (getParent() == null) {
            scribe.subscribe(topic, Stripe.this, (ScribeContent) null, null);
          }
        }
      };
      scribe.getEnvironment().getSelectorManager().getTimer().schedule(resubscribeTask,
          retryDelay);
    }
  }

  /**
    * Returns a String representation of this Stripe
   *
   * @return A String representing this stripe
   */
  @Override
  public String toString() {
    return "Stripe " + stripeId;
  }

  /**
   * Utility method. Returns the list of children for this stripe.
   * @return A array of children.
   */
  public NodeHandle[] getChildren(){
    return this.scribe.getChildrenOfTopic(new Topic(this.getStripeId().id())).toArray(new NodeHandle[0]);
  }

  /**
   * Utility method. Returns the parent for this topic in the scribe tree.
   * @return Parent for this topic.
   */
  public NodeHandle getParent(){
    return ((ScribeImpl) this.scribe).getParent(new Topic(this.getStripeId().id()));
  }

  /**
   * Utility method. Checks if local node is root for this topic.
   * @return True/False depending on if local node is root for this topic
   */
  public boolean isRoot(){
    return ((ScribeImpl) this.scribe).isRoot(topic);
  }
}
