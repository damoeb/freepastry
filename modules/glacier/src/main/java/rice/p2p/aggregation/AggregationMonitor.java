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

import java.util.Enumeration;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.glacier.v2.DebugContent;
import rice.p2p.past.Past;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.GCPastContentHandle;

/**
 * The monitor: a set of test objects, inserted with the "monitor add" debug command, whose lifetimes are
 * refreshed periodically and can be checked with "monitor check".
 */
final class AggregationMonitor {
  private final AggregationImpl aggregation;
  private final Past objectStore;
  private final Past aggregateStore;
  private final AggregateList aggregateList;
  private final IdFactory factory;
  private final Environment environment;
  private final AggregationSettings config;
  private final Logger logger;

  private final Vector<Id> monitorIDs;

  AggregationMonitor(AggregationImpl aggregation, Past objectStore, Past aggregateStore, AggregateList aggregateList, IdFactory factory, Environment environment, AggregationSettings config, Logger logger) {
    this.aggregation = aggregation;
    this.objectStore = objectStore;
    this.aggregateStore = aggregateStore;
    this.aggregateList = aggregateList;
    this.factory = factory;
    this.environment = environment;
    this.config = config;
    this.logger = logger;
    this.monitorIDs = new Vector<Id>();
  }

  /**
   * Refreshes the monitored objects; runs on the monitor timer.
   */
  void refreshMonitored() {
    Id[] ids = (Id[]) monitorIDs.toArray(new Id[] {});
    logger.info("Monitor: Refreshing {} objects", ids.length);
    aggregation.refresh(ids, environment.getTimeSource().currentTimeMillis() + 3 * config.getMonitorRefreshInterval(), new Continuation<Object[], Exception>() {
      @Override
      public void receiveResult(Object[] o) {
        logger.debug("Monitor: Refresh completed, result={}", o);
      }
      @Override
      public void receiveException(Exception e) {
        logger.debug("Monitor: Refresh failed, exception=", e);
      }
    });
  }

  /**
   * @return the output of a "monitor ..." debug command, or null if {@code cmd} is none (or the monitor is
   *         disabled)
   */
  String handleDebugCommand(String cmd) {
    if (cmd.startsWith("monitor remove") && config.isMonitorEnabled()) {
      String[] args = cmd.substring(15).split(" ");
      if (args.length == 1) {
        int howMany = Integer.parseInt(args[0]);

        if (howMany > monitorIDs.size())
          howMany = monitorIDs.size();

        for (int i=0; i<howMany; i++)
          monitorIDs.removeElementAt(environment.getRandomSource().nextInt(monitorIDs.size()));

        return "Removed "+howMany+" elements; "+monitorIDs.size()+" elements left";
      } else return "Syntax: monitor remove <howMany>";
    }

    if (cmd.startsWith("monitor status") && config.isMonitorEnabled()) {
      return "Monitor is "+(config.isMonitorEnabled() ? ("enabled, monitoring "+monitorIDs.size()+" objects") : "disabled");
    }

    if (cmd.startsWith("monitor ls") && config.isMonitorEnabled()) {
      StringBuilder result = new StringBuilder();
      Enumeration<Id> enumeration = monitorIDs.elements();

      while (enumeration.hasMoreElements())
        result.append(((Id)enumeration.nextElement()).toStringFull() + "\n");

      result.append(monitorIDs.size() + " object(s)");
      return result.toString();
    }

    if (cmd.startsWith("monitor check") && config.isMonitorEnabled()) {
      return check();
    }

    if (cmd.startsWith("monitor add") && config.isMonitorEnabled()) {
      String[] args = cmd.substring(12).split(" ");
      if (args.length == 6) {
        final int numFiles = Integer.parseInt(args[0]);
        final int avgBurstSize = Integer.parseInt(args[1]);
        final double sizeSkew = Double.parseDouble(args[2]);
        final int smallSize = Integer.parseInt(args[3]);
        final int largeSize = Integer.parseInt(args[4]);
        final long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(args[5]);

        new BurstInsert(numFiles, avgBurstSize, sizeSkew, smallSize, largeSize, expiration).receiveResult(Boolean.valueOf(true));
        return "In progress...";
      }

      return "Syntax: monitor add <#files> <avgBurstSize> <sizeSkew> <smallSize> <largeSize> <lifetime>";
    }

    return null;
  }

  /**
   * Looks up each monitored object in the object store, the aggregate list and the aggregate store, and
   * reports the remaining lifetimes.
   */
  private String check() {
    final StringBuffer result = new StringBuffer();
    final CompletableFuture<String> ret = new CompletableFuture<String>();

    if (monitorIDs.isEmpty())
      return "Add objects first!";

    final long now = environment.getTimeSource().currentTimeMillis();

    Continuation<PastContentHandle[], Exception> c = new Continuation<PastContentHandle[], Exception>() {
      int currentLookup = 0;
      boolean lookupInAggrStore = false;

      @Override
      public void receiveResult(PastContentHandle[] o) {
        logger.debug("Monitor: Retr {} a={} got {}", currentLookup, lookupInAggrStore, o);
        Id currentId = (Id) monitorIDs.elementAt(currentLookup);
        PastContentHandle[] handles = (PastContentHandle[]) o;
        GCPastContentHandle handle = null;
        boolean skipToNext = true;

        for (int i=0; i<handles.length; i++)
          if (handles[i] != null)
            handle = (GCPastContentHandle) handles[i];

        if (!lookupInAggrStore) {
          result.append(currentId.toStringFull() + " - OS ");
          result.append((handle==null) ? "--" : ""+(handle.getExpiration()-now));

          AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(currentId);
          if (adc != null) {
            result.append(" AD " + (adc.getCurrentLifetime() - now));

            int objDescIndex = adc.lookupNewest(currentId);
            if (objDescIndex >= 0) {
              ObjectDescriptor odc = adc.getObjects()[objDescIndex];
              result.append(" OD " + (odc.getCurrentLifetime() - now));
              lookupInAggrStore = true;
              skipToNext = false;
              aggregateStore.lookupHandles(adc.getKey(), 1, this);
            } else {
              result.append(" OD ??\n");
            }
          } else {
            result.append(" AD ??\n");
          }
        } else {
          result.append(" AS " + ((handle==null) ? "--\n" : ""+(handle.getExpiration()-now) + "\n"));
          lookupInAggrStore = false;
        }

        if (skipToNext) {
          currentLookup++;
          if (currentLookup < monitorIDs.size()) {
            logger.debug("Monitor: Continuing with element {}", currentLookup);
            objectStore.lookupHandles((Id) monitorIDs.elementAt(currentLookup), 1, this);
          } else {
            logger.debug("Monitor: Done");
            ret.complete("done");
          }
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Montior: Failed, e=", e);
        ret.complete("done");
      }
    };

    objectStore.lookupHandles((Id) monitorIDs.elementAt(0), 1, c);
    String outcome = AggregationDebugCommands.await(ret, environment);
    // the StringBuffer is synchronized, and complete() happens-after the last append
    return outcome.equals("done") ? result.toString() : result.toString() + outcome;
  }

  /**
   * Inserts the "monitor add" objects in bursts of random size, flushing after each burst.
   */
  private final class BurstInsert implements Continuation<Object, Exception> {
    private final int numFiles;
    private final int avgBurstSize;
    private final double sizeSkew;
    private final int smallSize;
    private final int largeSize;
    private final long expiration;
    private int remainingTotal;

    BurstInsert(int numFiles, int avgBurstSize, double sizeSkew, int smallSize, int largeSize, long expiration) {
      this.numFiles = numFiles;
      this.avgBurstSize = avgBurstSize;
      this.sizeSkew = sizeSkew;
      this.smallSize = smallSize;
      this.largeSize = largeSize;
      this.expiration = expiration;
      this.remainingTotal = numFiles;
    }

    @Override
    public void receiveResult(Object o) {
      if (remainingTotal > 0) {
        final int burstSize = Math.min((int)((avgBurstSize*0.3) + environment.getRandomSource().nextInt((int)(1.4*avgBurstSize))), remainingTotal);
        remainingTotal -= burstSize;
        logger.debug("Inserting burst of size {}, remaining objects: {}", burstSize, remainingTotal);
        new Burst(burstSize).receiveResult(Boolean.valueOf(true));
      } else {
        logger.info("Monitor add completed, {} objects created successfully", numFiles);
      }
    }

    @Override
    public void receiveException(Exception e) {
      logger.warn("Monitor.add aggregate insertion failed: ", e);
      receiveResult(e);
    }

    /**
     * Inserts one burst of objects one after the other, then flushes and continues with the next burst.
     */
    private final class Burst implements Continuation<Object, Exception> {
      private long remainingHere;

      Burst(int burstSize) {
        this.remainingHere = burstSize;
      }

      @Override
      public void receiveResult(Object o) {
        if (remainingHere > 0) {
          logger.debug("Continuing burst insert, {} remaining", remainingHere);
          int thisAvgSize = ((0.001*environment.getRandomSource().nextInt(1000)) < sizeSkew) ? smallSize : largeSize;
          int thisSize = (int)(0.3*thisAvgSize + environment.getRandomSource().nextInt((int)(1.4*thisAvgSize)));
          Id randomID = factory.buildRandomId(environment.getRandomSource());
          remainingHere --;
          monitorIDs.add(randomID);
          aggregation.insert(new DebugContent(randomID, false, 0, new byte[thisSize]), expiration, this);
        } else {
          logger.debug("Burst insertion complete, flushing...");
          aggregation.flush(BurstInsert.this);
        }
      }

      @Override
      public void receiveException(Exception e) {
        logger.warn("Monitor.add component insertion failed: ", e);
        receiveResult(e);
      }
    }
  }
}
