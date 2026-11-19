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
package rice.p2p.aggregation;

import static rice.environment.params.ConfigChecks.inRange;
import static rice.environment.params.ConfigChecks.requirePositive;

import rice.environment.params.Parameters;

/**
 * The tuning parameters of an {@link AggregationImpl}, read once from the {@code p2p_aggregation_*}
 * parameters: each component comes from the key {@code p2p_aggregation_<component name>}. Times are in
 * milliseconds and sizes in bytes. The values that the setters of {@link AggregationImpl} can change at
 * runtime ({@code flushInterval}, {@code maxAggregateSize}, {@code maxObjectsInAggregate},
 * {@code expirationRenewThreshold}, {@code consolidationInterval}, {@code consolidationThreshold},
 * {@code consolidationMinObjectsInAggregate}, {@code consolidationMinComponentsAlive}) are only the
 * initial values here; {@link AggregationSettings} holds the current ones.
 */
public record AggregationConfig(
    long flushDelayAfterJoin,
    long flushStressInterval,
    long flushInterval,
    int maxAggregateSize,
    int maxObjectsInAggregate,
    int maxAggregatesPerRun,
    boolean addMissingAfterRefresh,
    int maxReaggregationPerRefresh,
    int nominalReferenceCount,
    int maxPointersPerAggregate,
    long pointerArrayLifetime,
    long aggregateGracePeriod,
    long aggrRefreshInterval,
    long aggrRefreshDelayAfterJoin,
    long expirationRenewThreshold,
    boolean monitorEnabled,
    long monitorRefreshInterval,
    long consolidationDelayAfterJoin,
    long consolidationInterval,
    long consolidationThreshold,
    int consolidationMinObjectsInAggregate,
    double consolidationMinComponentsAlive,
    int reconstructionMaxConcurrentLookups,
    boolean aggregateLogEnabled,
    long statsGranularity,
    long statsRange,
    long statsInterval,
    double jitterRange) {

  static final long SECONDS = 1000;
  static final long MINUTES = 60 * SECONDS;
  static final long HOURS = 60 * MINUTES;
  static final long DAYS = 24 * HOURS;

  /**
   * @throws IllegalArgumentException if a flush, refresh or consolidation interval or delay is not
   *         positive, or the jitter range is outside (0, 1]; the jitter needs a positive range
   */
  public static AggregationConfig fromParameters(Parameters p) {
    return new AggregationConfig(
      requirePositive("p2p_aggregation_flushDelayAfterJoin", p.getLong("p2p_aggregation_flushDelayAfterJoin")),
      requirePositive("p2p_aggregation_flushStressInterval", p.getLong("p2p_aggregation_flushStressInterval")),
      requirePositive("p2p_aggregation_flushInterval", p.getLong("p2p_aggregation_flushInterval")),
      p.getInt("p2p_aggregation_maxAggregateSize"),
      p.getInt("p2p_aggregation_maxObjectsInAggregate"),
      p.getInt("p2p_aggregation_maxAggregatesPerRun"),
      p.getBoolean("p2p_aggregation_addMissingAfterRefresh"),
      p.getInt("p2p_aggregation_maxReaggregationPerRefresh"),
      p.getInt("p2p_aggregation_nominalReferenceCount"),
      p.getInt("p2p_aggregation_maxPointersPerAggregate"),
      p.getLong("p2p_aggregation_pointerArrayLifetime"),
      p.getLong("p2p_aggregation_aggregateGracePeriod"),
      requirePositive("p2p_aggregation_aggrRefreshInterval", p.getLong("p2p_aggregation_aggrRefreshInterval")),
      requirePositive("p2p_aggregation_aggrRefreshDelayAfterJoin", p.getLong("p2p_aggregation_aggrRefreshDelayAfterJoin")),
      p.getLong("p2p_aggregation_expirationRenewThreshold"),
      p.getBoolean("p2p_aggregation_monitorEnabled"),
      p.getLong("p2p_aggregation_monitorRefreshInterval"),
      requirePositive("p2p_aggregation_consolidationDelayAfterJoin", p.getLong("p2p_aggregation_consolidationDelayAfterJoin")),
      requirePositive("p2p_aggregation_consolidationInterval", p.getLong("p2p_aggregation_consolidationInterval")),
      p.getLong("p2p_aggregation_consolidationThreshold"),
      p.getInt("p2p_aggregation_consolidationMinObjectsInAggregate"),
      p.getDouble("p2p_aggregation_consolidationMinComponentsAlive"),
      p.getInt("p2p_aggregation_reconstructionMaxConcurrentLookups"),
      p.getBoolean("p2p_aggregation_aggregateLogEnabled"),
      p.getLong("p2p_aggregation_statsGranularity"),
      p.getLong("p2p_aggregation_statsRange"),
      p.getLong("p2p_aggregation_statsInterval"),
      inRange("p2p_aggregation_jitterRange", p.getDouble("p2p_aggregation_jitterRange"), Double.MIN_VALUE, 1));
  }
}
