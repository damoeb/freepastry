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

/**
 * The current tuning parameters of an {@link AggregationImpl}, shared by its helpers. They start as the
 * values of an {@link AggregationConfig}; some of them can be changed at runtime through the setters of
 * {@link AggregationImpl}.
 */
final class AggregationSettings {

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

  AggregationSettings(AggregationConfig config) {
    flushDelayAfterJoin = config.flushDelayAfterJoin();
    flushStressInterval = config.flushStressInterval();
    flushInterval = config.flushInterval();

    maxAggregateSize = config.maxAggregateSize();
    maxObjectsInAggregate = config.maxObjectsInAggregate();
    maxAggregatesPerRun = config.maxAggregatesPerRun();

    addMissingAfterRefresh = config.addMissingAfterRefresh();
    maxReaggregationPerRefresh = config.maxReaggregationPerRefresh();
    nominalReferenceCount = config.nominalReferenceCount();
    maxPointersPerAggregate = config.maxPointersPerAggregate();
    pointerArrayLifetime = config.pointerArrayLifetime();
    aggregateGracePeriod = config.aggregateGracePeriod();

    aggrRefreshInterval = config.aggrRefreshInterval();
    aggrRefreshDelayAfterJoin = config.aggrRefreshDelayAfterJoin();
    expirationRenewThreshold = config.expirationRenewThreshold();

    monitorEnabled = config.monitorEnabled();
    monitorRefreshInterval = config.monitorRefreshInterval();

    consolidationDelayAfterJoin = config.consolidationDelayAfterJoin();
    consolidationInterval = config.consolidationInterval();
    consolidationThreshold = config.consolidationThreshold();
    consolidationMinObjectsInAggregate = config.consolidationMinObjectsInAggregate();
    consolidationMinComponentsAlive = config.consolidationMinComponentsAlive();

    reconstructionMaxConcurrentLookups = config.reconstructionMaxConcurrentLookups();

    aggregateLogEnabled = config.aggregateLogEnabled();

    statsGranularity = config.statsGranularity();
    statsRange = config.statsRange();
    statsInterval = config.statsInterval();

    jitterRange = config.jitterRange();
  }

  /**
   * @return the parameters, as the "show config" debug command prints them
   */
  String describe() {
    return
      "flushDelayAfterJoin = " + (int)(flushDelayAfterJoin / AggregationConfig.SECONDS) + " sec\n" +
      "flushInterval = " + (int)(flushInterval / AggregationConfig.SECONDS) + " sec\n" +
      "maxAggregateSize = " + maxAggregateSize + " bytes\n" +
      "maxObjectsInAggregate = " + maxObjectsInAggregate + " objects\n" +
      "maxAggregatesPerRun = " + maxAggregatesPerRun + " aggregates\n" +
      "addMissingAfterRefresh = " + addMissingAfterRefresh + "\n" +
      "nominalReferenceCount = " + nominalReferenceCount + "\n" +
      "maxPointersPerAggregate = " + maxPointersPerAggregate + "\n" +
      "pointerArrayLifetime = " + (int)(pointerArrayLifetime / AggregationConfig.DAYS) + " days\n" +
      "aggrRefreshInterval = " + (int)(aggrRefreshInterval / AggregationConfig.SECONDS) + " sec\n" +
      "aggrRefreshDelayAfterJoin = " + (int)(aggrRefreshDelayAfterJoin / AggregationConfig.SECONDS) + " sec\n" +
      "expirationRenewThreshold = " + (int)(expirationRenewThreshold / AggregationConfig.HOURS) + " hrs\n" +
      "consolidationDelayAfterJoin = " + (int)(consolidationDelayAfterJoin / AggregationConfig.SECONDS) + " sec\n" +
      "consolidationInterval = " + (int)(consolidationInterval / AggregationConfig.SECONDS) + " sec\n" +
      "consolidationThreshold = " + (int)(consolidationThreshold / AggregationConfig.HOURS) + " hrs\n" +
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
