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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.persistence.CachedStorage;
import rice.persistence.PersistentStorage;
import rice.persistence.Storage;

/**
 * Collects the statistics of one reporting interval, and reports them to the registered listeners.
 */
final class GlacierStatisticsCollector {
  private final GlacierConfig config;
  private final GlacierScheduler scheduler;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierNeighborhood neighborhood;
  private final GlacierTrash trash;
  private final CachedStorage fragmentStorage;
  private final CachedStorage neighborStorage;
  private final String instance;
  private final Environment environment;
  private final Logger logger;
  private final List<GlacierStatisticsListener> listeners;
  private GlacierStatistics statistics;

  GlacierStatisticsCollector(GlacierConfig config, GlacierScheduler scheduler, GlacierTrafficShaper trafficShaper, GlacierNeighborhood neighborhood, GlacierTrash trash, CachedStorage fragmentStorage, CachedStorage neighborStorage, String instance, Environment environment, Logger logger) {
    this.config = config;
    this.scheduler = scheduler;
    this.trafficShaper = trafficShaper;
    this.neighborhood = neighborhood;
    this.trash = trash;
    this.fragmentStorage = fragmentStorage;
    this.neighborStorage = neighborStorage;
    this.instance = instance;
    this.environment = environment;
    this.logger = logger;
    this.listeners = new CopyOnWriteArrayList<>();
    this.statistics = new GlacierStatistics(GlacierTags.MAX, environment);
  }

  void countMessageSent(char tag) {
    statistics.countMessageSent(tag);
  }

  void addListener(GlacierStatisticsListener gsl) {
    listeners.add(gsl);
  }

  void removeListener(GlacierStatisticsListener gsl) {
    listeners.remove(gsl);
  }

  /**
   * @return the periodic task that reports the statistics and starts a new interval
   */
  GlacierContinuation newReportingTask() {
    return new GlacierContinuation() {
      long nextTimeout;

      @Override
      public String toString() {
        return "Statistics";
      }
      @Override
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.statisticsReportInterval();
      }
      @Override
      public void receiveResult(Object o) {
        logger.warn("STAT received object: {}", o);
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("STAT received exception: ", e);
      }
      @Override
      public long getTimeout() {
        return nextTimeout;
      }
      @Override
      public void timeoutExpired() {
        nextTimeout += config.statisticsReportInterval();

        if (!listeners.isEmpty()) {
          statistics.setNumNeighbors(neighborStorage.scan().numElements());
          statistics.setNumFragments(fragmentStorage.scan().numElements());
          statistics.setNumContinuations(scheduler.size());
          statistics.setResponsibleRange(neighborhood.getResponsibleRange());
          trafficShaper.addStatistics(statistics);

          Storage storageF = fragmentStorage.getStorage();
          if (storageF instanceof PersistentStorage persistentStorage)
            statistics.setFragmentStorageSize(persistentStorage.getTotalSize());

          trash.addStatistics(statistics);

          if (config.logStatistics())
            statistics.dump(environment.getLogManager().getLogger(GlacierStatistics.class, instance));

          // listeners are added and removed by other threads; the copy-on-write list iterates over a snapshot
          for (GlacierStatisticsListener gsl : listeners) {
            gsl.receiveStatistics(statistics);
          }
        }

        statistics = new GlacierStatistics(GlacierTags.MAX, environment);
      }
    };
  }
}
