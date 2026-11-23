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

import static rice.environment.params.ConfigChecks.requirePositive;

import rice.environment.params.Parameters;

/**
 * The parameters of a {@link ScribeImpl}. All times are in milliseconds.
 *
 * @param maintenanceInterval {@code p2p_scribe_maintenance_interval}: the time between tree
 *        maintenance runs
 * @param messageTimeout {@code p2p_scribe_message_timeout}: how long to wait for a subscribe ack
 */
/**
 * Scribe's parameters.
 *
 * @param maintenanceInterval milliseconds between maintenance rounds (heartbeat subscribes to the parents)
 * @param messageTimeout milliseconds a subscribe waits for its ack
 * @param maxTopics topics a node keeps state for
 * @param maxChildrenPerTopic children a node accepts per topic from remote subscribes
 * @param maxPendingSubscribes outstanding subscribes
 * @param maxAnycastVisited nodes an anycast may have visited before it fails
 */
public record ScribeConfig(int maintenanceInterval, int messageTimeout, int maxTopics, int maxChildrenPerTopic,
    int maxPendingSubscribes, int maxAnycastVisited) {

  /**
   * @throws IllegalArgumentException if a time or a bound is not positive
   */
  public static ScribeConfig fromParameters(Parameters parameters) {
    return new ScribeConfig(
      requirePositive("p2p_scribe_maintenance_interval", parameters.getInt("p2p_scribe_maintenance_interval")),
      requirePositive("p2p_scribe_message_timeout", parameters.getInt("p2p_scribe_message_timeout")),
      requirePositive("p2p_scribe_max_topics", parameters.getInt("p2p_scribe_max_topics")),
      requirePositive("p2p_scribe_max_children_per_topic", parameters.getInt("p2p_scribe_max_children_per_topic")),
      requirePositive("p2p_scribe_max_pending_subscribes", parameters.getInt("p2p_scribe_max_pending_subscribes")),
      requirePositive("p2p_scribe_max_anycast_visited", parameters.getInt("p2p_scribe_max_anycast_visited")));
  }
}
