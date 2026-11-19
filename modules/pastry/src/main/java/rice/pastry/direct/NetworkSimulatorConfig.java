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
package rice.pastry.direct;

import static rice.environment.params.ConfigChecks.requirePositive;

import java.util.OptionalLong;

import rice.environment.EnvironmentConfig;
import rice.environment.params.Parameters;

/**
 * The parameters of the direct (simulated) network: {@link NetworkSimulator},
 * {@link BasicNetworkSimulator}, the built-in topologies and {@link DirectPastryNodeFactory}.
 *
 * @param useOwnRandom {@code pastry_direct_use_own_random}, optional, default false: give the
 *        simulator its own random source instead of the environment's
 * @param randomSeed {@code pastry_direct_random_seed}, optional: the seed of that random source; empty
 *        if absent or {@code clock}
 * @param maxDiameter {@code pastry_direct_max_diameter}: the diameter of the sphere and euclidean
 *        topologies, in milliseconds of delay
 * @param guaranteeConsistency {@code pastry_direct_guarantee_consistency}: use the periodic, consistent
 *        leaf set protocol instead of the standard one
 */
public record NetworkSimulatorConfig(
    boolean useOwnRandom,
    OptionalLong randomSeed,
    int maxDiameter,
    boolean guaranteeConsistency) {

  /**
   * @throws IllegalArgumentException if maxDiameter is not positive
   */
  public static NetworkSimulatorConfig fromParameters(Parameters p) {
    return new NetworkSimulatorConfig(
      p.contains("pastry_direct_use_own_random") && p.getBoolean("pastry_direct_use_own_random"),
      EnvironmentConfig.seed(p, "pastry_direct_random_seed"),
      requirePositive("pastry_direct_max_diameter", p.getInt("pastry_direct_max_diameter")),
      p.getBoolean("pastry_direct_guarantee_consistency"));
  }
}
