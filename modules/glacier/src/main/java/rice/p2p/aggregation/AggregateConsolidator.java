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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.random.RandomSource;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Id;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.gc.ExpiringPastContent;

/**
 * Consolidates aggregates that hold too few objects, or too many dead ones: fetches a random selection of
 * them, stores their live objects in one new aggregate, and drops the old ones from the aggregate list.
 */
final class AggregateConsolidator {
  private final Past aggregateStore;
  private final AggregateList aggregateList;
  private final AggregateFactory aggregateFactory;
  private final AggregateStorer storer;
  private final AggregationSettings config;
  private final TimeSource timeSource;
  private final RandomSource randomSource;
  private final Logger logger;

  AggregateConsolidator(Past aggregateStore, AggregateList aggregateList, AggregateFactory aggregateFactory, AggregateStorer storer, AggregationSettings config, TimeSource timeSource, RandomSource randomSource, Logger logger) {
    this.aggregateStore = aggregateStore;
    this.aggregateList = aggregateList;
    this.aggregateFactory = aggregateFactory;
    this.storer = storer;
    this.config = config;
    this.timeSource = timeSource;
    this.randomSource = randomSource;
    this.logger = logger;
  }

  void consolidateAggregates() {
    final long now = timeSource.currentTimeMillis();

    logger.info("Looking for aggregates to consolidate");

    List<AggregateDescriptor> candidateList = findCandidates(now);
    if (candidateList.isEmpty()) {
      logger.info("No candidates for consolidation");
      return;
    }

    logger.debug("{} candidate(s) for consolidation", candidateList.size());

    final List<AggregateDescriptor> componentList = new ArrayList<AggregateDescriptor>();
    int objectsSoFar = 0;
    int bytesSoFar = 0;

    while (!candidateList.isEmpty()) {
      AggregateDescriptor adc = candidateList.remove(randomSource.nextInt(candidateList.size()));
      componentList.add(adc);
      logger.atDebug().log(() -> "Picked candidate "+adc.getKey().toStringFull()+" ("+adc.objectsAliveAt(now)+"/"+adc.getObjects().length+" objects, "+adc.bytesAliveAt(now)+" bytes alive)");
      objectsSoFar += adc.objectsAliveAt(now);
      bytesSoFar += adc.bytesAliveAt(now);

      int p = 0;
      while (p < candidateList.size()) {
        AggregateDescriptor adx = candidateList.get(p);
        if (((adx.objectsAliveAt(now) + objectsSoFar) > config.getMaxObjectsInAggregate()) ||
            ((adx.bytesAliveAt(now) + bytesSoFar) > config.getMaxAggregateSize()))
          candidateList.remove(p);
        else
          p++;
      }
    }

    if (componentList.isEmpty() || (objectsSoFar < config.getConsolidationMinObjectsInAggregate())) {
      logger.info("Not enough objects ({} found, {} required), postponing...", objectsSoFar, config.getConsolidationMinObjectsInAggregate());
      return;
    }

    logger.debug("Consolidation: Decided to consolidate {} objects from {} aggregates ({} bytes)", objectsSoFar, componentList.size(), bytesSoFar);

    new Consolidation(componentList.toArray(new AggregateDescriptor[] {}), objectsSoFar, now).start();
  }

  /**
   * An aggregate is a candidate for consolidation iff
   * <ul>
   * <li>it does not need to be refreshed yet, but has reached a certain age (to prevent consolidation of
   * 'garbage'),</li>
   * <li>at least one of its objects is still alive, and</li>
   * <li>it does not contain enough objects, or too many of them are dead.</li>
   * </ul>
   * Pointer arrays are never consolidated; they expire after some time.
   */
  private List<AggregateDescriptor> findCandidates(final long now) {
    Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
    List<AggregateDescriptor> candidateList = new ArrayList<AggregateDescriptor>();

    aggregateList.resetMarkers();
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
      if (!aggr.isMarker()) {
        aggr.setMarker(true);

        if ((aggr.getCurrentLifetime() > (now + config.getExpirationRenewThreshold())) &&
            (aggr.getCurrentLifetime() < (now + config.getConsolidationThreshold())) &&
            (aggr.objectsAliveAt(now) > 0)) {
          float fractionAlive = ((float)aggr.objectsAliveAt(now)) / aggr.getObjects().length;
          if ((aggr.getObjects().length < config.getConsolidationMinObjectsInAggregate()) ||
              (fractionAlive < config.getConsolidationMinComponentsAlive())) {
            logger.atDebug().log(() -> "Can consolidate: "+aggr.getKey().toStringFull()+", "+aggr.objectsAliveAt(now)+"/"+aggr.getObjects().length+" alive");
            candidateList.add(aggr);
          }
        }
      }
    }

    return candidateList;
  }

  /**
   * One consolidation run: fetches the chosen aggregates one after the other, then stores their live objects
   * as a new aggregate.
   */
  private final class Consolidation implements Continuation<PastContent, Exception> {
    private final AggregateDescriptor[] adc;
    private final Aggregate[] aggr;
    private final int objectsTotal;
    private final long now;
    private int currentLookup = 0;

    Consolidation(AggregateDescriptor[] adc, int objectsTotal, long now) {
      this.adc = adc;
      this.aggr = new Aggregate[adc.length];
      this.objectsTotal = objectsTotal;
      this.now = now;
    }

    void start() {
      final Id firstKey = adc[0].getKey();
      logger.atDebug().log(() -> "Consolidation: Fetching aggregate #0: "+firstKey.toStringFull());
      aggregateStore.lookup(firstKey, this);
    }

    @Override
    public void receiveResult(PastContent o) {
      if (o instanceof Aggregate aggregate) {
        aggr[currentLookup] = aggregate;
        currentLookup ++;
        if (currentLookup >= adc.length) {
          storeConsolidated();
        } else {
          logger.atDebug().log(() -> "Consolidation: Fetching aggregate #"+currentLookup+": "+adc[currentLookup].getKey().toStringFull());
          aggregateStore.lookup(adc[currentLookup].getKey(), this);
        }
      }
    }

    @Override
    public void receiveException(Exception e) {
      logger.atWarn().setCause(e).log(() -> "Exception during consolidation lookup "+adc[currentLookup].getKey().toStringFull()+": "+e+" -- aborting");
    }

    private void storeConsolidated() {
      ExpiringPastContent[] components = new ExpiringPastContent[objectsTotal];
      ObjectDescriptor[] desc = new ObjectDescriptor[objectsTotal];
      int componentIndex = 0;

      logger.info("Consolidation: All aggregates fetched OK, forming new aggregate...");

      for (int i=0; i<adc.length; i++) {
        for (int j=0; j<adc[i].getObjects().length; j++) {
          if (adc[i].getObjects()[j].isAliveAt(now)) {

            ExpiringPastContent temp = aggr[i].getComponent(j);

            components[componentIndex] = temp;
            desc[componentIndex] = adc[i].getObjects()[j];
            if (logger.isDebugEnabled()) logger.debug("  #"+componentIndex+": "+adc[i].getObjects()[j].getKey().toStringFull());
            componentIndex ++;
          } else {
            if (logger.isDebugEnabled()) logger.debug("Skipped (dead): "+adc[i].getObjects()[j].getKey().toStringFull());
          }
        }
      }

      Id[] obsoleteAggregates = new Id[adc.length];
      for (int i=0; i<adc.length; i++)
        obsoleteAggregates[i] = adc[i].getKey();

      Id[] pointers = aggregateList.getSomePointers(config.getNominalReferenceCount(), config.getMaxPointersPerAggregate(), obsoleteAggregates);
      final long aggrExpirationF = AggregateStorer.chooseAggregateLifetime(desc);

      storer.storeAggregate(aggregateFactory.buildAggregate(components, pointers), aggrExpirationF, desc, pointers, new Continuation<Boolean, Exception>() {
        @Override
        public void receiveResult(Boolean o) {
          logger.info("Consolidated Aggregate stored OK, removing old descriptors...");
          for (AggregateDescriptor aggregateDescriptor : adc) {
            if (logger.isDebugEnabled()) logger.debug("Removing "+aggregateDescriptor.getKey().toStringFull()+" ...");
            aggregateList.removeAggregateDescriptor(aggregateDescriptor);
          }

          aggregateList.writeToDisk();
          logger.info("Consolidation completed, {} objects from {} aggregates consolidated", objectsTotal, aggr.length);
        }
        @Override
        public void receiveException(Exception e) {
          logger.warn("Exception during consolidation store: e={} -- aborting", e, e);
        }
      });
    }
  }
}
