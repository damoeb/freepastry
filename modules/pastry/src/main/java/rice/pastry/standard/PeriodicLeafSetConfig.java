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
package rice.pastry.standard;

import static rice.environment.params.ConfigChecks.requirePositive;

import java.util.OptionalLong;

import rice.environment.EnvironmentConfig;
import rice.environment.params.Parameters;

/**
 * The parameters of a {@link PeriodicLeafSetProtocol}. All times are in milliseconds.
 *
 * @param useOwnRandom {@code pastry_periodic_leafset_protocol_use_own_random}, optional, default false:
 *        give the protocol its own random source instead of the environment's
 * @param randomSeed {@code pastry_periodic_leafset_protocol_random_seed}, optional: the seed of that
 *        random source; empty if absent or {@code clock}
 * @param pingNeighborPeriod {@code pastry_protocol_periodicLeafSet_ping_neighbor_period}: how often the
 *        leaf set neighbors are pinged
 * @param leasePeriod {@code pastry_protocol_periodicLeafSet_lease_period}: how long a granted lease is
 *        valid
 * @param requestLeaseThrottle {@code pastry_protocol_periodicLeafSet_request_lease_throttle}: the
 *        minimum time between lease requests to the same node
 */
public record PeriodicLeafSetConfig(
    boolean useOwnRandom,
    OptionalLong randomSeed,
    int pingNeighborPeriod,
    int leasePeriod,
    int requestLeaseThrottle) {

  /**
   * @throws IllegalArgumentException if the ping period or the lease period is not positive
   */
  public static PeriodicLeafSetConfig fromParameters(Parameters p) {
    return new PeriodicLeafSetConfig(
      p.contains("pastry_periodic_leafset_protocol_use_own_random")
          && p.getBoolean("pastry_periodic_leafset_protocol_use_own_random"),
      EnvironmentConfig.seed(p, "pastry_periodic_leafset_protocol_random_seed"),
      requirePositive("pastry_protocol_periodicLeafSet_ping_neighbor_period",
          p.getInt("pastry_protocol_periodicLeafSet_ping_neighbor_period")),
      requirePositive("pastry_protocol_periodicLeafSet_lease_period", p.getInt("pastry_protocol_periodicLeafSet_lease_period")),
      p.getInt("pastry_protocol_periodicLeafSet_request_lease_throttle"));
  }
}
