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
package org.mpisws.p2p.transport.liveness;

import static rice.environment.params.ConfigChecks.nonNegative;
import static rice.environment.params.ConfigChecks.positive;

import rice.environment.params.Parameters;

/**
 * The tuning parameters of a {@link LivenessTransportLayerImpl}. All times are in milliseconds.
 *
 * @param pingDelay {@code pastry_socket_scm_ping_delay}: the base delay between ping retries; doubles
 *        with every try
 * @param pingJitter {@code pastry_socket_scm_ping_jitter}: the fraction of the ping delay added as
 *        random jitter
 * @param numPingTries {@code pastry_socket_scm_num_ping_tries}: the number of pings before a node is
 *        declared dead
 * @param backoffInitial {@code pastry_socket_scm_backoff_initial}
 * @param backoffLimit {@code pastry_socket_scm_backoff_limit}
 * @param checkDeadThrottle {@code pastry_socket_srm_check_dead_throttle}: the minimum time between
 *        liveness checks of a dead node
 * @param defaultRto {@code pastry_socket_srm_default_rto}: the retransmission timeout before an RTT
 *        has been measured
 * @param rtoUpperBound {@code pastry_socket_srm_rto_ubound}: the upper bound of the retransmission
 *        timeout
 * @param rtoLowerBound {@code pastry_socket_srm_rto_lbound}: the lower bound of the retransmission
 *        timeout
 * @param gainH {@code pastry_socket_srm_gain_h}: RTO gain of the deviation (RFC 1122)
 * @param gainG {@code pastry_socket_srm_gain_g}: RTO gain of the mean (RFC 1122)
 */
public record LivenessConfig(
    int pingDelay,
    float pingJitter,
    int numPingTries,
    int backoffInitial,
    int backoffLimit,
    int checkDeadThrottle,
    int defaultRto,
    int rtoUpperBound,
    int rtoLowerBound,
    double gainH,
    double gainG) {

  /**
   * @throws IllegalArgumentException if a ping delay or timeout is not positive, or the ping tries or
   *         the throttle are negative
   */
  public static LivenessConfig fromParameters(Parameters p) {
    return new LivenessConfig(
      positive("pastry_socket_scm_ping_delay", p.getInt("pastry_socket_scm_ping_delay")),
      p.getFloat("pastry_socket_scm_ping_jitter"),
      nonNegative("pastry_socket_scm_num_ping_tries", p.getInt("pastry_socket_scm_num_ping_tries")),
      p.getInt("pastry_socket_scm_backoff_initial"),
      p.getInt("pastry_socket_scm_backoff_limit"),
      nonNegative("pastry_socket_srm_check_dead_throttle", p.getInt("pastry_socket_srm_check_dead_throttle")),
      positive("pastry_socket_srm_default_rto", p.getInt("pastry_socket_srm_default_rto")),
      positive("pastry_socket_srm_rto_ubound", p.getInt("pastry_socket_srm_rto_ubound")),
      positive("pastry_socket_srm_rto_lbound", p.getInt("pastry_socket_srm_rto_lbound")),
      p.getDouble("pastry_socket_srm_gain_h"),
      p.getDouble("pastry_socket_srm_gain_g"));
  }

  /**
   * @return a copy of this config with the given check dead throttle
   */
  public LivenessConfig withCheckDeadThrottle(int checkDeadThrottle) {
    return new LivenessConfig(pingDelay, pingJitter, numPingTries, backoffInitial, backoffLimit, checkDeadThrottle,
        defaultRto, rtoUpperBound, rtoLowerBound, gainH, gainG);
  }
}
