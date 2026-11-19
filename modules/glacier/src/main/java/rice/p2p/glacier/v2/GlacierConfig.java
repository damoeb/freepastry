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
package rice.p2p.glacier.v2;

import static rice.environment.params.ConfigChecks.inRange;
import static rice.environment.params.ConfigChecks.requirePositive;

import rice.environment.params.Parameters;
import rice.environment.random.RandomSource;

/**
 * The tuning parameters of a {@link GlacierImpl}, read once from the {@code p2p_glacier_*} parameters:
 * each component comes from the key {@code p2p_glacier_<component name>}. Times are in milliseconds.
 */
public record GlacierConfig(
    boolean logStatistics,
    boolean faultInjectionEnabled,
    long insertTimeout,
    double minFragmentsAfterInsert,
    long refreshTimeout,
    long expireNeighborsDelayAfterJoin,
    long expireNeighborsInterval,
    long neighborTimeout,
    long syncDelayAfterJoin,
    long syncMinRemainingLifetime,
    long syncMinQuietTime,
    int syncBloomFilterNumHashes,
    int syncBloomFilterBitsPerKey,
    int syncPartnersPerTrial,
    long syncInterval,
    long syncRetryInterval,
    int syncMaxFragments,
    int fragmentRequestMaxAttempts,
    long fragmentRequestTimeoutDefault,
    long fragmentRequestTimeoutMin,
    long fragmentRequestTimeoutMax,
    long fragmentRequestTimeoutDecrement,
    long manifestRequestTimeout,
    long manifestRequestInitialBurst,
    long manifestRequestRetryBurst,
    int manifestAggregationFactor,
    long handoffDelayAfterJoin,
    long handoffInterval,
    int handoffMaxFragments,
    long garbageCollectionInterval,
    int garbageCollectionMaxFragmentsPerRun,
    long localScanInterval,
    int localScanMaxFragmentsPerRun,
    double restoreMaxRequestFactor,
    int restoreMaxBoosts,
    long rateLimitedCheckInterval,
    int rateLimitedRequestsPerSecond,
    boolean enableBulkRefresh,
    long bulkRefreshProbeInterval,
    double bulkRefreshMaxProbeFactor,
    long bulkRefreshManifestInterval,
    int bulkRefreshManifestAggregationFactor,
    int bulkRefreshPatchAggregationFactor,
    long bulkRefreshPatchInterval,
    int bulkRefreshPatchRetries,
    long bucketTokensPerSecond,
    long bucketMaxBurstSize,
    double jitterRange,
    long statisticsReportInterval,
    int maxActiveRestores) {

  static final long SECONDS = 1000;
  static final long MINUTES = 60 * SECONDS;
  static final long HOURS = 60 * MINUTES;

  /**
   * @throws IllegalArgumentException if a jittered interval (sync, sync retry, handoff, local scan) or a
   *         Bloom filter size is not positive, or the jitter range is outside (0, 1]; the jitter needs a
   *         positive range
   */
  public static GlacierConfig fromParameters(Parameters parameters) {
    return new GlacierConfig(
      parameters.getBoolean("p2p_glacier_logStatistics"),
      parameters.getBoolean("p2p_glacier_faultInjectionEnabled"),
      parameters.getLong("p2p_glacier_insertTimeout"),
      parameters.getDouble("p2p_glacier_minFragmentsAfterInsert"),
      parameters.getLong("p2p_glacier_refreshTimeout"),
      parameters.getLong("p2p_glacier_expireNeighborsDelayAfterJoin"),
      parameters.getLong("p2p_glacier_expireNeighborsInterval"),
      parameters.getLong("p2p_glacier_neighborTimeout"),
      parameters.getLong("p2p_glacier_syncDelayAfterJoin"),
      parameters.getLong("p2p_glacier_syncMinRemainingLifetime"),
      parameters.getLong("p2p_glacier_syncMinQuietTime"),
      requirePositive("p2p_glacier_syncBloomFilterNumHashes", parameters.getInt("p2p_glacier_syncBloomFilterNumHashes")),
      requirePositive("p2p_glacier_syncBloomFilterBitsPerKey", parameters.getInt("p2p_glacier_syncBloomFilterBitsPerKey")),
      parameters.getInt("p2p_glacier_syncPartnersPerTrial"),
      requirePositive("p2p_glacier_syncInterval", parameters.getLong("p2p_glacier_syncInterval")),
      requirePositive("p2p_glacier_syncRetryInterval", parameters.getLong("p2p_glacier_syncRetryInterval")),
      parameters.getInt("p2p_glacier_syncMaxFragments"),
      parameters.getInt("p2p_glacier_fragmentRequestMaxAttempts"),
      parameters.getLong("p2p_glacier_fragmentRequestTimeoutDefault"),
      parameters.getLong("p2p_glacier_fragmentRequestTimeoutMin"),
      parameters.getLong("p2p_glacier_fragmentRequestTimeoutMax"),
      parameters.getLong("p2p_glacier_fragmentRequestTimeoutDecrement"),
      parameters.getLong("p2p_glacier_manifestRequestTimeout"),
      parameters.getLong("p2p_glacier_manifestRequestInitialBurst"),
      parameters.getLong("p2p_glacier_manifestRequestRetryBurst"),
      parameters.getInt("p2p_glacier_manifestAggregationFactor"),
      parameters.getLong("p2p_glacier_handoffDelayAfterJoin"),
      requirePositive("p2p_glacier_handoffInterval", parameters.getLong("p2p_glacier_handoffInterval")),
      parameters.getInt("p2p_glacier_handoffMaxFragments"),
      parameters.getLong("p2p_glacier_garbageCollectionInterval"),
      parameters.getInt("p2p_glacier_garbageCollectionMaxFragmentsPerRun"),
      requirePositive("p2p_glacier_localScanInterval", parameters.getLong("p2p_glacier_localScanInterval")),
      parameters.getInt("p2p_glacier_localScanMaxFragmentsPerRun"),
      parameters.getDouble("p2p_glacier_restoreMaxRequestFactor"),
      parameters.getInt("p2p_glacier_restoreMaxBoosts"),
      parameters.getLong("p2p_glacier_rateLimitedCheckInterval"),
      parameters.getInt("p2p_glacier_rateLimitedRequestsPerSecond"),
      parameters.getBoolean("p2p_glacier_enableBulkRefresh"),
      parameters.getLong("p2p_glacier_bulkRefreshProbeInterval"),
      parameters.getDouble("p2p_glacier_bulkRefreshMaxProbeFactor"),
      parameters.getLong("p2p_glacier_bulkRefreshManifestInterval"),
      parameters.getInt("p2p_glacier_bulkRefreshManifestAggregationFactor"),
      parameters.getInt("p2p_glacier_bulkRefreshPatchAggregationFactor"),
      parameters.getLong("p2p_glacier_bulkRefreshPatchInterval"),
      parameters.getInt("p2p_glacier_bulkRefreshPatchRetries"),
      parameters.getLong("p2p_glacier_bucketTokensPerSecond"),
      parameters.getLong("p2p_glacier_bucketMaxBurstSize"),
      inRange("p2p_glacier_jitterRange", parameters.getDouble("p2p_glacier_jitterRange"), Double.MIN_VALUE, 1),
      parameters.getLong("p2p_glacier_statisticsReportInterval"),
      parameters.getInt("p2p_glacier_maxActiveRestores"));
  }

  /**
   * @return a random interval within jitterRange of basis, so that periodic tasks of different nodes
   *         do not run in lockstep
   */
  long jitterTerm(long basis, RandomSource random) {
    return (long)((1-jitterRange)*basis) + random.nextInt((int)(2*jitterRange*basis));
  }
}
