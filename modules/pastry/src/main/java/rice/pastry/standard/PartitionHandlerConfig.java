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

import static rice.environment.params.ConfigChecks.requireNonNegative;
import static rice.environment.params.ConfigChecks.requirePositive;

import rice.environment.params.Parameters;

/**
 * The parameters of a {@link PartitionHealer}. All times are in milliseconds.
 *
 * @param maxHistorySize {@code partition_handler_max_history_size}: the most departed nodes remembered
 *        as rejoin candidates
 * @param maxHistoryAge {@code partition_handler_max_history_age}: how long a departed node stays a
 *        candidate
 * @param bootstrapCheckRate {@code partition_handler_bootstrap_check_rate}: the probability, per check,
 *        of probing a bootstrap node instead of a departed node
 * @param checkInterval {@code partition_handler_check_interval}: the time between partition checks
 */
public record PartitionHandlerConfig(int maxHistorySize, int maxHistoryAge, double bootstrapCheckRate, int checkInterval) {

  /**
   * @throws IllegalArgumentException if the history size is negative or the interval is not positive
   */
  public static PartitionHandlerConfig fromParameters(Parameters parameters) {
    return new PartitionHandlerConfig(
      requireNonNegative("partition_handler_max_history_size", parameters.getInt("partition_handler_max_history_size")),
      parameters.getInt("partition_handler_max_history_age"),
      parameters.getDouble("partition_handler_bootstrap_check_rate"),
      requirePositive("partition_handler_check_interval", parameters.getInt("partition_handler_check_interval")));
  }
}
