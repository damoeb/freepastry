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

import rice.environment.params.Parameters;

/**
 * The tuning parameters of an {@link AggregationImpl}, read from the {@code p2p_aggregation_*} parameters.
 * Some of them can be changed at runtime through the setters of {@link AggregationImpl}.
 */
final class AggregationConfig {
  static final long SECONDS = 1000;
  static final long MINUTES = 60 * SECONDS;
  static final long HOURS = 60 * MINUTES;
  static final long DAYS = 24 * HOURS;

  private final long flushDelayAfterJoin;
  private final long flushStressInterval;
  private long flushInterval;

  private int maxAggregateSize;
  private int maxObjectsInAggregate;
  private final int maxAggregatesPerRun;

  private final boolean addMissingAfterRefresh;
  private final int maxReaggregationPerRefresh;
  private final int nominalReferenceCount;
  private final int maxPointersPerAggregate;
  private final long pointerArrayLifetime;
  private final long aggregateGracePeriod;

  private final long aggrRefreshInterval;
  private final long aggrRefreshDelayAfterJoin;
  private long expirationRenewThreshold;

  private final boolean monitorEnabled;
  private final long monitorRefreshInterval;

  private final long consolidationDelayAfterJoin;
  private long consolidationInterval;
  private long consolidationThreshold;
  private int consolidationMinObjectsInAggregate;
  private double consolidationMinComponentsAlive;

  private final int reconstructionMaxConcurrentLookups;

  private final boolean aggregateLogEnabled;

  private final long statsGranularity;
  private final long statsRange;
  private final long statsInterval;

  private final double jitterRange;

  AggregationConfig(Parameters p) {
    // read but unused, as before
    p.getBoolean("p2p_aggregation_logStatistics");

    flushDelayAfterJoin = p.getLong("p2p_aggregation_flushDelayAfterJoin");
    flushStressInterval = p.getLong("p2p_aggregation_flushStressInterval");
    flushInterval = p.getLong("p2p_aggregation_flushInterval");

    maxAggregateSize = p.getInt("p2p_aggregation_maxAggregateSize");
    maxObjectsInAggregate = p.getInt("p2p_aggregation_maxObjectsInAggregate");
    maxAggregatesPerRun = p.getInt("p2p_aggregation_maxAggregatesPerRun");

    addMissingAfterRefresh = p.getBoolean("p2p_aggregation_addMissingAfterRefresh");
    maxReaggregationPerRefresh = p.getInt("p2p_aggregation_maxReaggregationPerRefresh");
    nominalReferenceCount = p.getInt("p2p_aggregation_nominalReferenceCount");
    maxPointersPerAggregate = p.getInt("p2p_aggregation_maxPointersPerAggregate");
    pointerArrayLifetime = p.getLong("p2p_aggregation_pointerArrayLifetime");
    aggregateGracePeriod = p.getLong("p2p_aggregation_aggregateGracePeriod");

    aggrRefreshInterval = p.getLong("p2p_aggregation_aggrRefreshInterval");
    aggrRefreshDelayAfterJoin = p.getLong("p2p_aggregation_aggrRefreshDelayAfterJoin");
    expirationRenewThreshold = p.getLong("p2p_aggregation_expirationRenewThreshold");

    monitorEnabled = p.getBoolean("p2p_aggregation_monitorEnabled");
    monitorRefreshInterval = p.getLong("p2p_aggregation_monitorRefreshInterval");

    consolidationDelayAfterJoin = p.getLong("p2p_aggregation_consolidationDelayAfterJoin");
    consolidationInterval = p.getLong("p2p_aggregation_consolidationInterval");
    consolidationThreshold = p.getLong("p2p_aggregation_consolidationThreshold");
    consolidationMinObjectsInAggregate = p.getInt("p2p_aggregation_consolidationMinObjectsInAggregate");
    consolidationMinComponentsAlive = p.getDouble("p2p_aggregation_consolidationMinComponentsAlive");

    reconstructionMaxConcurrentLookups = p.getInt("p2p_aggregation_reconstructionMaxConcurrentLookups");

    aggregateLogEnabled = p.getBoolean("p2p_aggregation_aggregateLogEnabled");

    statsGranularity = p.getLong("p2p_aggregation_statsGranularity");
    statsRange = p.getLong("p2p_aggregation_statsRange");
    statsInterval = p.getLong("p2p_aggregation_statsInterval");

    jitterRange = p.getDouble("p2p_aggregation_jitterRange");
  }

  /**
   * @return the parameters, as the "show config" debug command prints them
   */
  String describe() {
    return
      "flushDelayAfterJoin = " + (int)(flushDelayAfterJoin / SECONDS) + " sec\n" +
      "flushInterval = " + (int)(flushInterval / SECONDS) + " sec\n" +
      "maxAggregateSize = " + maxAggregateSize + " bytes\n" +
      "maxObjectsInAggregate = " + maxObjectsInAggregate + " objects\n" +
      "maxAggregatesPerRun = " + maxAggregatesPerRun + " aggregates\n" +
      "addMissingAfterRefresh = " + addMissingAfterRefresh + "\n" +
      "nominalReferenceCount = " + nominalReferenceCount + "\n" +
      "maxPointersPerAggregate = " + maxPointersPerAggregate + "\n" +
      "pointerArrayLifetime = " + (int)(pointerArrayLifetime / DAYS) + " days\n" +
      "aggrRefreshInterval = " + (int)(aggrRefreshInterval / SECONDS) + " sec\n" +
      "aggrRefreshDelayAfterJoin = " + (int)(aggrRefreshDelayAfterJoin / SECONDS) + " sec\n" +
      "expirationRenewThreshold = " + (int)(expirationRenewThreshold / HOURS) + " hrs\n" +
      "consolidationDelayAfterJoin = " + (int)(consolidationDelayAfterJoin / SECONDS) + " sec\n" +
      "consolidationInterval = " + (int)(consolidationInterval / SECONDS) + " sec\n" +
      "consolidationThreshold = " + (int)(consolidationThreshold / HOURS) + " hrs\n" +
      "consolidationMinObjectsInAggregate = " + consolidationMinObjectsInAggregate + "\n" +
      "consolidationMinComponentsAlive = " + consolidationMinComponentsAlive + "\n";
  }

  long getFlushDelayAfterJoin() {
    return flushDelayAfterJoin;
  }

  long getFlushStressInterval() {
    return flushStressInterval;
  }

  long getFlushInterval() {
    return flushInterval;
  }

  void setFlushInterval(long flushInterval) {
    this.flushInterval = flushInterval;
  }

  int getMaxAggregateSize() {
    return maxAggregateSize;
  }

  void setMaxAggregateSize(int maxAggregateSize) {
    this.maxAggregateSize = maxAggregateSize;
  }

  int getMaxObjectsInAggregate() {
    return maxObjectsInAggregate;
  }

  void setMaxObjectsInAggregate(int maxObjectsInAggregate) {
    this.maxObjectsInAggregate = maxObjectsInAggregate;
  }

  int getMaxAggregatesPerRun() {
    return maxAggregatesPerRun;
  }

  boolean isAddMissingAfterRefresh() {
    return addMissingAfterRefresh;
  }

  int getMaxReaggregationPerRefresh() {
    return maxReaggregationPerRefresh;
  }

  int getNominalReferenceCount() {
    return nominalReferenceCount;
  }

  int getMaxPointersPerAggregate() {
    return maxPointersPerAggregate;
  }

  long getPointerArrayLifetime() {
    return pointerArrayLifetime;
  }

  long getAggregateGracePeriod() {
    return aggregateGracePeriod;
  }

  long getAggrRefreshInterval() {
    return aggrRefreshInterval;
  }

  long getAggrRefreshDelayAfterJoin() {
    return aggrRefreshDelayAfterJoin;
  }

  long getExpirationRenewThreshold() {
    return expirationRenewThreshold;
  }

  void setExpirationRenewThreshold(long expirationRenewThreshold) {
    this.expirationRenewThreshold = expirationRenewThreshold;
  }

  boolean isMonitorEnabled() {
    return monitorEnabled;
  }

  long getMonitorRefreshInterval() {
    return monitorRefreshInterval;
  }

  long getConsolidationDelayAfterJoin() {
    return consolidationDelayAfterJoin;
  }

  long getConsolidationInterval() {
    return consolidationInterval;
  }

  void setConsolidationInterval(long consolidationInterval) {
    this.consolidationInterval = consolidationInterval;
  }

  long getConsolidationThreshold() {
    return consolidationThreshold;
  }

  void setConsolidationThreshold(long consolidationThreshold) {
    this.consolidationThreshold = consolidationThreshold;
  }

  int getConsolidationMinObjectsInAggregate() {
    return consolidationMinObjectsInAggregate;
  }

  void setConsolidationMinObjectsInAggregate(int consolidationMinObjectsInAggregate) {
    this.consolidationMinObjectsInAggregate = consolidationMinObjectsInAggregate;
  }

  double getConsolidationMinComponentsAlive() {
    return consolidationMinComponentsAlive;
  }

  void setConsolidationMinComponentsAlive(double consolidationMinComponentsAlive) {
    this.consolidationMinComponentsAlive = consolidationMinComponentsAlive;
  }

  int getReconstructionMaxConcurrentLookups() {
    return reconstructionMaxConcurrentLookups;
  }

  boolean isAggregateLogEnabled() {
    return aggregateLogEnabled;
  }

  long getStatsGranularity() {
    return statsGranularity;
  }

  long getStatsRange() {
    return statsRange;
  }

  long getStatsInterval() {
    return statsInterval;
  }

  double getJitterRange() {
    return jitterRange;
  }
}
