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
import rice.Futures;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Id;
import rice.p2p.past.Past;
import rice.p2p.past.gc.ExpiringPast;
import rice.p2p.past.gc.RefreshOutcomes;

/**
 * The lifetimes of the aggregates: renews aggregates that are about to expire but still hold live objects,
 * and drops expired aggregates from the aggregate list.
 */
final class AggregateExpiration {
  private final Past aggregateStore;
  private final AggregateList aggregateList;
  private final AggregationSettings config;
  private final TimeSource timeSource;
  private final Logger logger;

  AggregateExpiration(Past aggregateStore, AggregateList aggregateList, AggregationSettings config, TimeSource timeSource, Logger logger) {
    this.aggregateStore = aggregateStore;
    this.aggregateList = aggregateList;
    this.config = config;
    this.timeSource = timeSource;
    this.logger = logger;
  }

  /**
   * Removes the aggregates that expired longer than the grace period ago.
   */
  void removeDeadAggregates() {
    List<AggregateDescriptor> toRemove = new ArrayList<AggregateDescriptor>();
    Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
    long now = timeSource.currentTimeMillis();

    /* Note: Multiple keys are mapped to a single aggregate descriptor, so the same ADC
       may be returned multiple times - but we must delete it only once! */

    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggregate = (AggregateDescriptor) enumeration.nextElement();
      if (aggregate.getCurrentLifetime() < (now - config.getAggregateGracePeriod())) {
        if (!toRemove.contains(aggregate))
          toRemove.add(aggregate);
        logger.atWarn().log(() -> "Scheduling dead aggregate for removal: "+aggregate.getKey().toStringFull()+"(expired "+aggregate.getCurrentLifetime()+")");
      }
    }

    if (toRemove.size() > 0) {
      logger.info("Removing {} dead aggregates...", toRemove.size());

      for (AggregateDescriptor aggregate : toRemove)
        aggregateList.removeAggregateDescriptor(aggregate);
    }
  }

  /**
   * Renews the aggregates that expire within the renew threshold and removes the expired ones.
   */
  void refreshAggregates() {
    Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
    long now = timeSource.currentTimeMillis();
    List<AggregateDescriptor> removeList = new ArrayList<AggregateDescriptor>();
    // read (not modified) by the refresh continuation below
    final List<AggregateDescriptor> refreshAggregateList = new ArrayList<AggregateDescriptor>();
    final List<Long> refreshLifetimeList = new ArrayList<Long>();

    logger.info("Checking aggregate lifetimes");

    aggregateList.resetMarkers();
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggregate = (AggregateDescriptor) enumeration.nextElement();
      if (!aggregate.isMarker()) {
        aggregate.setMarker(true);

        boolean isBeingRefreshed = false;
        if (aggregate.getCurrentLifetime() < (now + config.getExpirationRenewThreshold())) {
          long newLifetime = AggregateStorer.chooseAggregateLifetime(aggregate.getObjects());
          if (newLifetime > aggregate.getCurrentLifetime()) {
            logger.atInfo().log(() -> "Refreshing aggregate "+aggregate.getKey().toStringFull()+", new expiration is "+newLifetime);
            isBeingRefreshed = true;

            refreshAggregateList.add(aggregate);
            refreshLifetimeList.add(Long.valueOf(newLifetime));
          }
        }

        if ((aggregate.getCurrentLifetime() < now) && !isBeingRefreshed) {
          logger.debug("Adding expired aggregate {} to remove list", aggregate.getKey());
          removeList.add(aggregate);
        }
      }
    }

    boolean deletedOne = false;
    while (!removeList.isEmpty()) {
      AggregateDescriptor aggregate = removeList.remove(0);
      logger.atInfo().log(() -> "Removing expired aggregate "+aggregate.getKey().toStringFull()+" from list");
      deletedOne = true;
      aggregateList.removeAggregateDescriptor(aggregate);
    }

    if (deletedOne)
      aggregateList.writeToDisk();

    if (!refreshAggregateList.isEmpty()) {
      logger.info("Refreshing {} aggregate(s)", refreshAggregateList.size());
      if (aggregateStore instanceof ExpiringPast gcPast) {
        renewInAggregateStore(gcPast, refreshAggregateList, refreshLifetimeList);
      } else {
        logger.debug("Aggregate store does not support GC; refreshing directly");
        for (int i=0; i<refreshAggregateList.size(); i++) {
          AggregateDescriptor aggregate = refreshAggregateList.get(i);
          long newLifetime = refreshLifetimeList.get(i).longValue();
          aggregateList.refreshAggregate(aggregate, newLifetime);
        }
      }
    }
  }

  private void renewInAggregateStore(ExpiringPast store, final List<AggregateDescriptor> refreshAggregateList, final List<Long> refreshLifetimeList) {
    Id[] ids = new Id[refreshAggregateList.size()];
    long[] lifetimes = new long[refreshAggregateList.size()];
    for (int i=0; i<refreshAggregateList.size(); i++) {
      ids[i] = refreshAggregateList.get(i).getKey();
      lifetimes[i] = refreshLifetimeList.get(i).longValue();
    }

    Futures.forward(store.refresh(ids, lifetimes).thenApply(RefreshOutcomes::toArray), new Continuation<Object[], Exception>() {
      @Override
      public void receiveResult(Object[] rawResults) {
        Object[] results = (Object[]) rawResults;
        logger.debug("Received refresh results for {} aggregates", results.length);
        int numOk = 0;

        for (int i=0; i<results.length; i++) {
          if (results[i] instanceof Boolean) {
            AggregateDescriptor aggregate = refreshAggregateList.get(i);
            long newLifetime = refreshLifetimeList.get(i).longValue();
            if (logger.isDebugEnabled()) logger.debug("Aggregate #"+i+" ("+aggregate.getKey().toStringFull()+"): OK, new lifetime is "+newLifetime);
            aggregateList.refreshAggregate(aggregate, newLifetime);
            numOk ++;
          } else {
            AggregateDescriptor aggregate = refreshAggregateList.get(i);
            Exception e = (Exception) results[i];
            if (logger.isWarnEnabled()) logger.warn("Aggregate #"+i+" ("+aggregate.getKey().toStringFull()+"): Refresh failed, e=", e);
          }
        }

        aggregateList.writeToDisk();
        logger.info("Refresh complete, {}/{} aggregates refreshed OK", numOk, results.length);
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Interface contract broken; exception {} returned directly", e, e);
      }
    });
  }
}
