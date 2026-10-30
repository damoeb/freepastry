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
package rice.environment;

import java.util.OptionalLong;

import rice.environment.params.Parameters;

/**
 * The parameters an {@link Environment} reads when it chooses its default helpers.
 *
 * @param useSimProcessor {@code environment_use_sim_processor}, optional, default false: run
 *        processing tasks on the selector thread, for the simulator
 * @param randomSeed {@code random_seed}: the seed of the default random source; empty for
 *        {@code clock}, which seeds it from the clock
 * @param directSimulatorStartTime {@code direct_simulator_start_time}: the start time of the simulated
 *        clock, in milliseconds since the epoch; negative for the current time
 */
public record EnvironmentConfig(boolean useSimProcessor, OptionalLong randomSeed, long directSimulatorStartTime) {

  public static EnvironmentConfig fromParameters(Parameters p) {
    return new EnvironmentConfig(
      p.contains("environment_use_sim_processor") && p.getBoolean("environment_use_sim_processor"),
      seed(p, "random_seed"),
      p.getLong("direct_simulator_start_time"));
  }

  /**
   * Reads a random seed parameter.
   *
   * @return the seed, or empty if the key is absent or {@code clock} (in any case)
   */
  public static OptionalLong seed(Parameters p, String key) {
    if (!p.contains(key) || p.getString(key).equalsIgnoreCase("clock")) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(p.getLong(key));
  }
}
