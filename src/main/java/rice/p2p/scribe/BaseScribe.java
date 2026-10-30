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

import rice.Destructable;
import rice.environment.Environment;
import rice.p2p.commonapi.NodeHandle;

/**
 * The Scribe functions whose signatures do not involve {@link ScribeContent}. {@link Scribe} adds the
 * subscribe, publish and anycast methods that carry content.
 *
 * @author Jeff Hoye
 *
 */
public interface BaseScribe extends Destructable {
  // ***************** Membership functions messages **************
  /**
   * Unsubscribes the given client from the provided topic.
   *
   * @param topic The topic to unsubscribe from
   * @param client The client to unsubscribe
   */
  public void unsubscribe(Topic topic, ScribeMultiClient client);
  public void unsubscribe(Collection<Topic> topicsToUnsubscribe, ScribeMultiClient client);

  /**
   * Adds a child to the given topic
   *
   * @param topic The topic to add the child to
   * @param child The child to add
   */
  public void addChild(Topic topic, NodeHandle child);

  /**
   * Removes a child from the given topic
   *
   * @param topic The topic to remove the child from
   * @param child The child to remove
   */
  public void removeChild(Topic topic, NodeHandle child);

  // *********************** Query functions *********************
  /**
   * Returns whether or not this Scribe is the root for the given topic
   *
   * @param topic The topic in question
   * @return Whether or not we are currently the root
   */
  public boolean isRoot(Topic topic);

  /**
   * Returns the root of the topic, if we can determine it.
   *
   * @param topic
   * @return null if beyond our knowledge range
   */
  public NodeHandle getRoot(Topic topic);

  /**
   * Returns the list of children for a given topic
   *
   * @param topic The topic to return the children of
   * @return The children of the topic
   */
  public Collection<NodeHandle> getChildrenOfTopic(Topic topic);

  /**
   * Returns the parent node for a given topic
   *
   * @param topic The topic to return the parent of
   * @return The parent of the topic
   */
  public NodeHandle getParent(Topic topic);

  /**
   * Returns the list of topics the given client is subscribed
   * to.
   *
   * @param client The client in question
   * @return The list of topics
   */
  public Collection<Topic> getTopicsByClient(ScribeMultiClient client);

  public int numChildren(Topic topic);

  Collection<ScribeMultiClient> getClientsByTopic(Topic topic);

  /**
   *  Returns true if there is a TopicManager object corresponding to this topic
   */
  public boolean containsTopic(Topic topic);

  public boolean containsChild(Topic topic, NodeHandle child);




  // ********************* Application management functions **************
  /**
   * Returns the current policy for this scribe object
   *
   * @return The current policy for this scribe
   */
  public ScribePolicy getPolicy();

  /**
   * Sets the current policy for this scribe object
   *
   * @param policy The current policy for this scribe
   */
  public void setPolicy(ScribePolicy policy);


  public Environment getEnvironment();

}
