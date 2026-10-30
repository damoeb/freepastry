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
import static rice.environment.params.ConfigChecks.positive;

import rice.environment.params.Parameters;
import rice.environment.random.RandomSource;

/**
 * The tuning parameters of a {@link GlacierImpl}, read once from the {@code p2p_glacier_*} parameters:
 * each component comes from the key {@code p2p_glacier_<component name>}. Times are in milliseconds.
 * Parameters that can be changed at runtime ({@code syncInterval}, through
 * {@link GlacierImpl#setSyncInterval(int)}) hold only the initial value here; the collaborator that uses
 * such a parameter keeps the current value.
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
    long overallRestoreTimeout,
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
  public static GlacierConfig fromParameters(Parameters p) {
    return new GlacierConfig(
      p.getBoolean("p2p_glacier_logStatistics"),
      p.getBoolean("p2p_glacier_faultInjectionEnabled"),
      p.getLong("p2p_glacier_insertTimeout"),
      p.getDouble("p2p_glacier_minFragmentsAfterInsert"),
      p.getLong("p2p_glacier_refreshTimeout"),
      p.getLong("p2p_glacier_expireNeighborsDelayAfterJoin"),
      p.getLong("p2p_glacier_expireNeighborsInterval"),
      p.getLong("p2p_glacier_neighborTimeout"),
      p.getLong("p2p_glacier_syncDelayAfterJoin"),
      p.getLong("p2p_glacier_syncMinRemainingLifetime"),
      p.getLong("p2p_glacier_syncMinQuietTime"),
      positive("p2p_glacier_syncBloomFilterNumHashes", p.getInt("p2p_glacier_syncBloomFilterNumHashes")),
      positive("p2p_glacier_syncBloomFilterBitsPerKey", p.getInt("p2p_glacier_syncBloomFilterBitsPerKey")),
      p.getInt("p2p_glacier_syncPartnersPerTrial"),
      positive("p2p_glacier_syncInterval", p.getLong("p2p_glacier_syncInterval")),
      positive("p2p_glacier_syncRetryInterval", p.getLong("p2p_glacier_syncRetryInterval")),
      p.getInt("p2p_glacier_syncMaxFragments"),
      p.getInt("p2p_glacier_fragmentRequestMaxAttempts"),
      p.getLong("p2p_glacier_fragmentRequestTimeoutDefault"),
      p.getLong("p2p_glacier_fragmentRequestTimeoutMin"),
      p.getLong("p2p_glacier_fragmentRequestTimeoutMax"),
      p.getLong("p2p_glacier_fragmentRequestTimeoutDecrement"),
      p.getLong("p2p_glacier_manifestRequestTimeout"),
      p.getLong("p2p_glacier_manifestRequestInitialBurst"),
      p.getLong("p2p_glacier_manifestRequestRetryBurst"),
      p.getInt("p2p_glacier_manifestAggregationFactor"),
      p.getLong("p2p_glacier_overallRestoreTimeout"),
      p.getLong("p2p_glacier_handoffDelayAfterJoin"),
      positive("p2p_glacier_handoffInterval", p.getLong("p2p_glacier_handoffInterval")),
      p.getInt("p2p_glacier_handoffMaxFragments"),
      p.getLong("p2p_glacier_garbageCollectionInterval"),
      p.getInt("p2p_glacier_garbageCollectionMaxFragmentsPerRun"),
      positive("p2p_glacier_localScanInterval", p.getLong("p2p_glacier_localScanInterval")),
      p.getInt("p2p_glacier_localScanMaxFragmentsPerRun"),
      p.getDouble("p2p_glacier_restoreMaxRequestFactor"),
      p.getInt("p2p_glacier_restoreMaxBoosts"),
      p.getLong("p2p_glacier_rateLimitedCheckInterval"),
      p.getInt("p2p_glacier_rateLimitedRequestsPerSecond"),
      p.getBoolean("p2p_glacier_enableBulkRefresh"),
      p.getLong("p2p_glacier_bulkRefreshProbeInterval"),
      p.getDouble("p2p_glacier_bulkRefreshMaxProbeFactor"),
      p.getLong("p2p_glacier_bulkRefreshManifestInterval"),
      p.getInt("p2p_glacier_bulkRefreshManifestAggregationFactor"),
      p.getInt("p2p_glacier_bulkRefreshPatchAggregationFactor"),
      p.getLong("p2p_glacier_bulkRefreshPatchInterval"),
      p.getInt("p2p_glacier_bulkRefreshPatchRetries"),
      p.getLong("p2p_glacier_bucketTokensPerSecond"),
      p.getLong("p2p_glacier_bucketMaxBurstSize"),
      inRange("p2p_glacier_jitterRange", p.getDouble("p2p_glacier_jitterRange"), Double.MIN_VALUE, 1),
      p.getLong("p2p_glacier_statisticsReportInterval"),
      p.getInt("p2p_glacier_maxActiveRestores"));
  }

  /**
   * @return a random interval within jitterRange of basis, so that periodic tasks of different nodes
   *         do not run in lockstep
   */
  long jitterTerm(long basis, RandomSource random) {
    return (long)((1-jitterRange)*basis) + random.nextInt((int)(2*jitterRange*basis));
  }
}
