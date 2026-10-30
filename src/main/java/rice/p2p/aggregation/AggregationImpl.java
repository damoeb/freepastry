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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.aggregation.messaging.AggregationMessage;
import rice.p2p.aggregation.messaging.AggregationTimeoutMessage;
import rice.p2p.aggregation.messaging.NonAggregate;
import rice.p2p.aggregation.raw.RawAggregate;
import rice.p2p.aggregation.raw.RawAggregateFactory;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.VersioningPast;
import rice.p2p.glacier.v2.DebugContent;
import rice.p2p.glacier.v2.GlacierContentHandle;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.PastImpl;
import rice.p2p.past.gc.GCPast;
import rice.p2p.past.gc.GCPastContent;
import rice.p2p.past.gc.GCPastContentHandle;
import rice.p2p.past.gc.rawserialization.RawGCPastContent;
import rice.p2p.past.rawserialization.JavaPastContentDeserializer;
import rice.p2p.past.rawserialization.JavaPastContentHandleDeserializer;
import rice.p2p.past.rawserialization.JavaSerializedPastContent;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.rawserialization.PastContentHandleDeserializer;
import rice.p2p.past.rawserialization.RawPastContent;
import rice.p2p.util.DebugCommandHandler;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.persistence.StorageManager;

@SuppressWarnings("unchecked")
public class AggregationImpl implements GCPast, VersioningPast, Aggregation, Application, DebugCommandHandler {
  private final Past aggregateStore;
  private final StorageManager waitingList;
  private final AggregationPolicy policy;
  private final AggregateList aggregateList;
  private final Endpoint endpoint;
  private final Past objectStore;
  private final String instance;
  private final IdFactory factory;
  private final Node node;

  private final char tiFlush = 1;
  private final char tiMonitor = 2;
  private final char tiConsolidate = 3;
  private final char tiStatistics = 4;
  private final char tiExpire = 5;
  private final Hashtable<Integer, CancellableTask> timers;
  private Continuation<? super Boolean, Exception> flushWait;
  private boolean rebuildInProgress;
  private final Vector<Id> monitorIDs;
  private AggregationStatistics stats;

  private final AggregationConfig config;

  private Environment environment;
  private final Logger logger;

  private PastContentDeserializer contentDeserializer;
  private PastContentHandleDeserializer contentHandleDeserializer;

  private AggregateFactory aggregateFactory;

  public AggregationImpl(Node node, Past aggregateStore, Past objectStore, StorageManager waitingList, String configFileName, IdFactory factory, String instance) throws IOException {
    this(node, aggregateStore, objectStore, waitingList, configFileName, factory, instance, null, null);
  }

  public AggregationImpl(Node node, Past aggregateStore, Past objectStore, StorageManager waitingList, String configFileName, IdFactory factory, String instance, AggregationPolicy policy, AggregateFactory aggregateFactory) throws IOException {
    this.environment = node.getEnvironment();
    logger = environment.getLogManager().getLogger(AggregationImpl.class, instance);

    config = new AggregationConfig(environment.getParameters());

    this.aggregateFactory = aggregateFactory;
    if (this.aggregateFactory == null) {
      this.aggregateFactory = getDefaultAggregateFactory();
    }

    this.endpoint = node.buildEndpoint(this, instance);
    this.endpoint.setDeserializer((buf, type, priority, sender) -> {
      // the only message type, AggregationTimeoutMessage, is scheduled locally and never deserialized
      throw new IOException("Unknown type: " + type + " in " + AggregationImpl.this);
    });

    this.waitingList = waitingList;
    this.instance = instance;

    this.contentDeserializer = new JavaPastContentDeserializer();
    this.contentHandleDeserializer = new JavaPastContentHandleDeserializer();

    this.aggregateStore = aggregateStore;
    this.aggregateStore.setContentDeserializer((buf, endpoint, contentType) -> {
      switch(contentType) {
        case RawAggregate.TYPE:
          return new RawAggregate(buf, endpoint, contentDeserializer);
        case NonAggregate.TYPE:
          short subType = buf.readShort();
          return contentDeserializer.deserializePastContent(buf, endpoint, subType);
      }
      throw new IllegalArgumentException("Unknown Type:"+contentType);
    });

    this.aggregateStore.setContentHandleDeserializer((buf, endpoint, contentType) -> {
      switch(contentType) {
        case AggregateHandle.TYPE:
          return new AggregateHandle(buf, endpoint);
      }
      throw new IllegalArgumentException("Unknown Type:"+contentType);
    });

    this.objectStore = objectStore;
    this.node = node;
    this.timers = new Hashtable<Integer, CancellableTask>();
    this.aggregateList = new AggregateList(configFileName, getLocalNodeHandle().getId().toString(), factory, config.isAggregateLogEnabled(), instance, environment);
    this.stats = aggregateList.getStatistics(config.getStatsGranularity(), config.getStatsRange(), config.getNominalReferenceCount());
    if (policy == null) {
      this.policy = getDefaultPolicy();
    } else {
      this.policy = policy;
    }

    this.factory = factory;
    this.flushWait = null;
    this.rebuildInProgress = false;
    this.monitorIDs = new Vector<Id>();

    if (!aggregateList.readOK()) {
      logger.warn("Failed to read configuration file; aggregate list must be rebuilt!");
    } else {
      logger.atInfo().log(() -> "Aggregate list read OK -- current root: " + ((aggregateList.getRoot() == null) ? "null" : aggregateList.getRoot().toStringFull()));
    }

    removeDeadAggregates();

    addTimer(jitterTerm(config.getFlushDelayAfterJoin()), tiFlush);
    addTimer(jitterTerm(config.getAggrRefreshDelayAfterJoin()), tiExpire);
    addTimer(jitterTerm(config.getConsolidationDelayAfterJoin()), tiConsolidate);
    addTimer(config.getStatsInterval(), tiStatistics);
    if (config.isMonitorEnabled())
      addTimer(config.getMonitorRefreshInterval(), tiMonitor);
    endpoint.register();
  }

  private static AggregationPolicy getDefaultPolicy() {
    return new AggregationDefaultPolicy();
  }

  private static AggregateFactory getDefaultAggregateFactory() {
    return new RawAggregateFactory();
  }

  private long jitterTerm(long basis) {
    return (long)((1-config.getJitterRange())*basis) + environment.getRandomSource().nextInt((int)(2*config.getJitterRange()*basis));
  }

  /**
   * Schedule a timer event
   *
   * @param timeoutMsec Length of the delay (in milliseconds)
   * @param timeoutID Identifier (to distinguish between multiple timers)
   */
  private void addTimer(long timeoutMsec, char timeoutID) {
    /*
     *  We schedule an AggregationTimeoutMessage with the ID of the
     *  requested timer. This message will be delivered if the timer
     *  expires and it has not been removed in the meantime.
     */
    CancellableTask timer = endpoint.scheduleMessage(new AggregationTimeoutMessage(timeoutID, getLocalNodeHandle()), timeoutMsec);
    timers.put(Integer.valueOf(timeoutID), timer);
  }

  /**
   * Cancel a timer event that has not yet occurred
   *
   * @param timeoutID Identifier of the timer event to be cancelled
   */
  private void removeTimer(int timeoutID) {
    CancellableTask timer = (CancellableTask) timers.remove(Integer.valueOf(timeoutID));

    if (timer != null) {
      timer.cancel();
    }
  }

  private void panic(String s) throws Error {
    Error err = new Error("Panic "+s);
    logger.error("PANIC: {}", s, err);
    throw err;
  }

  /**
   * How long a debug command waits for its asynchronous result.
   */
  private static final long DEBUG_COMMAND_TIMEOUT = 2 * 60 * 1000;

  private String awaitDebugResult(CompletableFuture<String> result) {
    return DebugCommandHandler.await(result, environment.getSelectorManager(), DEBUG_COMMAND_TIMEOUT);
  }

  public String handleDebugCommand(String command) {
    if (command.indexOf(" ") < 0)
      return null;

    String requestedInstance = command.substring(0, command.indexOf(" "));
    String myInstance = "aggr."+instance.substring(instance.lastIndexOf("-") + 1);
    String cmd = command.substring(requestedInstance.length() + 1);

    if (!requestedInstance.equals(myInstance) && !requestedInstance.equals("a")) {
      String subResult = null;

      if ((subResult == null) && (aggregateStore instanceof DebugCommandHandler))
        subResult = ((DebugCommandHandler)aggregateStore).handleDebugCommand(command);
      if ((subResult == null) && (objectStore instanceof DebugCommandHandler))
        subResult = ((DebugCommandHandler)objectStore).handleDebugCommand(command);

      return subResult;
    }

    logger.info("Debug command: {}", cmd);

    if (cmd.startsWith("status")) {
      return stats.getNumObjectsTotal()+" objects total\n"+stats.getNumObjectsAlive()+" objects alive\n"+
             stats.getNumAggregatesTotal()+" aggregates total\n"+stats.getNumPointerArrays()+" pointer arrays\n"+
             stats.getCriticalAggregates()+" critical aggregates\n"+stats.getOrphanedAggregates()+" orphaned aggregates\n";
    }

    if (cmd.startsWith("insert")) {
      int numObjects = Integer.parseInt(cmd.substring(7));
      String result = "";

      for (int i=0; i<numObjects; i++) {
        Id randomID = factory.buildRandomId(environment.getRandomSource());
        result = result + randomID.toStringFull() + "\n";
        insert(
          new DebugContent(randomID, false, 0, new byte[] {}),
          environment.getTimeSource().currentTimeMillis() + 120*AggregationConfig.SECONDS,
          new Continuation<Boolean[], Exception>() {
            public void receiveResult(Boolean[] o) {
            }
            public void receiveException(Exception e) {
            }
          }
        );
      }

      return result + numObjects + " object(s) created\n";
    }

    if (cmd.startsWith("show config")) {
      return config.describe();
    }

    if (cmd.startsWith("ls")) {
      Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
      StringBuilder result = new StringBuilder();
      int numAggr = 0, numObj = 0;

      long now = environment.getTimeSource().currentTimeMillis();
      if (cmd.indexOf("-r") < 0)
        now = 0;

      aggregateList.recalculateReferenceCounts(null);
      aggregateList.resetMarkers();
      while (enumeration.hasMoreElements()) {
        AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
        if (!aggr.isMarker()) {
          result.append("***" + aggr.getKey().toStringFull() + " (" + aggr.getObjects().length + " obj, " +
                   aggr.getPointers().length + " ptr, " + aggr.getReferenceCount() + " ref, exp=" +
                   (aggr.getCurrentLifetime() - now) + ")\n");
          for (int i=0; i<aggr.getObjects().length; i++)
            result.append("    #"+i+" "+
              aggr.getObjects()[i].getKey().toStringFull()+"v"+aggr.getObjects()[i].getVersion() +
              ", lt=" + (aggr.getObjects()[i].getCurrentLifetime()-now) +
              ", rt=" + (aggr.getObjects()[i].getRefreshedLifetime()-now) +
              ", size=" + aggr.getObjects()[i].getSize() + " bytes\n");
          for (int i=0; i<aggr.getPointers().length; i++)
            result.append("    Ref "+aggr.getPointers()[i].toStringFull()+"\n");
          result.append("\n");
          aggr.setMarker(true);
          numAggr ++;
          numObj += aggr.getObjects().length;
        }
      }

      result.append(numAggr + " aggregate(s), " + numObj + " object(s)");

      return result.toString();
    }

    if (cmd.startsWith("write list")) {
      aggregateList.writeToDisk();
      return "Done, new root is "+((aggregateList.getRoot()==null) ? "null" : aggregateList.getRoot().toStringFull());
    }

    if ((cmd.length() >= 5) && cmd.substring(0, 5).equals("reset")) {
      final CompletableFuture<String> ret = new CompletableFuture<String>();

      reset(new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {
          ret.complete("result("+o+")");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return awaitDebugResult(ret);
    }

    if (cmd.startsWith("flush")) {
      final CompletableFuture<String> ret = new CompletableFuture<String>();

      flush(new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {
          ret.complete("result("+o+")");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return awaitDebugResult(ret);
    }

    if (cmd.startsWith("get root")) {
      return "root="+((aggregateList.getRoot()==null) ? "null" : aggregateList.getRoot().toStringFull());
    }

    if (cmd.startsWith("set root")) {
      final CompletableFuture<String> ret = new CompletableFuture<String>();
      setHandle(factory.buildIdFromToString(cmd.substring(9)), new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {
          ret.complete("result("+o+")");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return awaitDebugResult(ret);
    }

    if (cmd.startsWith("lookup")) {
      Id id = factory.buildIdFromToString(cmd.substring(7));

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      lookup(id, false, new Continuation<PastContent, Exception>() {
        public void receiveResult(PastContent o) {
          ret.complete("result("+o+")");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return "lookup("+id+")="+awaitDebugResult(ret);
    }

    if (cmd.startsWith("handles")) {
      String args = cmd.substring(8);
      Id id = factory.buildIdFromToString(args.substring(args.indexOf(' ') + 1));
      int max = Integer.parseInt(args.substring(0, args.indexOf(' ')));

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      lookupHandles(id, max, new Continuation<PastContentHandle[], Exception>() {
        public void receiveResult(PastContentHandle[] o) {
          if (o instanceof PastContentHandle[]) {
            PastContentHandle[] oA = (PastContentHandle[]) o;
            String text = "";
            for (int i=0; i<oA.length; i++)
              text = text + "#"+i+" "+oA[i]+"\n";
            ret.complete(text + oA.length + " handle(s) returned\n");
          } else ret.complete("result("+o+") -- no handles returned!");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return "Handles("+max+","+id+"):\n"+awaitDebugResult(ret);
    }

    if (cmd.startsWith("refresh all")) {
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(cmd.substring(12));
      TreeSet<Id> ids = new TreeSet<Id>();
      String result;

      aggregateList.resetMarkers();

      Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
      while (enumeration.hasMoreElements()) {
        AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
        if (!aggr.isMarker()) {
          aggr.setMarker(true);
          for (int i=0; i<aggr.getObjects().length; i++)
            ids.add(aggr.getObjects()[i].getKey());
        }
      }

      if (!ids.isEmpty()) {
        Id[] allIds = (Id[]) ids.toArray(new Id[] {});
        result = "Refreshing " + allIds.length + " keys...\n";

        for (int i=0; i<allIds.length; i++)
          result = result + "#" + i + " " + allIds[i].toStringFull() + "\n";

        final CompletableFuture<String> ret = new CompletableFuture<String>();
        refresh(allIds, expiration, new Continuation<Object[], Exception>() {
          public void receiveResult(Object[] o) {
            ret.complete("result("+o+")");
          };
          public void receiveException(Exception e) {
            ret.complete("exception("+e+")");
          }
        });

        result = result + awaitDebugResult(ret);
      } else result = "Aggregate list is empty; nothing to refresh!";

      return result;
    }

    if (cmd.startsWith("refresh")) {
      String args = cmd.substring(8);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String keyArg = args.substring(0, args.lastIndexOf(' '));

      Id id = factory.buildIdFromToString(keyArg);
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(expirationArg);

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      refresh(new Id[] { id }, expiration, new Continuation<Object[], Exception>() {
        public void receiveResult(Object[] o) {
          ret.complete("result("+o+")");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return "refresh("+id+", "+expiration+")="+awaitDebugResult(ret);
    }

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
      final StringBuffer result = new StringBuffer();
      final CompletableFuture<String> ret = new CompletableFuture<String>();

      if (monitorIDs.isEmpty())
        return "Add objects first!";

      final long now = environment.getTimeSource().currentTimeMillis();

      Continuation<PastContentHandle[], Exception> c = new Continuation<PastContentHandle[], Exception>() {
        int currentLookup = 0;
        boolean lookupInAggrStore = false;

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
        public void receiveException(Exception e) {
          logger.warn("Montior: Failed, e=", e);
          ret.complete("done");
        }
      };

      objectStore.lookupHandles((Id) monitorIDs.elementAt(0), 1, c);
      String outcome = awaitDebugResult(ret);
      // the StringBuffer is synchronized, and complete() happens-after the last append
      return outcome.equals("done") ? result.toString() : result.toString() + outcome;
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

        Continuation<Object, Exception> c = new Continuation<Object, Exception>() {
          int remainingTotal = numFiles;
          public void receiveResult(Object o) {
            if (remainingTotal > 0) {
              final int burstSize = Math.min((int)((avgBurstSize*0.3) + environment.getRandomSource().nextInt((int)(1.4*avgBurstSize))), remainingTotal);
              final Continuation<Object, Exception> outerContinuation = this;
              remainingTotal -= burstSize;
              logger.debug("Inserting burst of size {}, remaining objects: {}", burstSize, remainingTotal);
              Continuation<Object, Exception> c2 = new Continuation<Object, Exception>() {
                long remainingHere = burstSize;
                public void receiveResult(Object o) {
                  if (remainingHere > 0) {
                    logger.debug("Continuing burst insert, {} remaining", remainingHere);
                    int thisAvgSize = ((0.001*environment.getRandomSource().nextInt(1000)) < sizeSkew) ? smallSize : largeSize;
                    int thisSize = (int)(0.3*thisAvgSize + environment.getRandomSource().nextInt((int)(1.4*thisAvgSize)));
                    Id randomID = factory.buildRandomId(environment.getRandomSource());
                    remainingHere --;
                    monitorIDs.add(randomID);
                    insert(new DebugContent(randomID, false, 0, new byte[thisSize]), expiration, this);
                  } else {
                    logger.debug("Burst insertion complete, flushing...");
                    flush(outerContinuation);
                  }
                }
                public void receiveException(Exception e) {
                  logger.warn("Monitor.add component insertion failed: ", e);
                  receiveResult(e);
                }
              };

              c2.receiveResult(Boolean.valueOf(true));
            } else {
              logger.info("Monitor add completed, {} objects created successfully", numFiles);
            }
          }
          public void receiveException(Exception e) {
            logger.warn("Monitor.add aggregate insertion failed: ", e);
            receiveResult(e);
          }
        };

        c.receiveResult(Boolean.valueOf(true));
        return "In progress...";
      }

      return "Syntax: monitor add <#files> <avgBurstSize> <sizeSkew> <smallSize> <largeSize> <lifetime>";
    }

    if (cmd.startsWith("killall")) {
      String args = cmd.substring(8);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String keyArg = args.substring(0, args.lastIndexOf(' '));

      Id id = factory.buildIdFromToString(keyArg);
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(expirationArg);

      AggregateDescriptor aggr = (AggregateDescriptor) aggregateList.getADC(id);
      if (aggr != null) {
        aggregateList.setAggregateLifetime(aggr, Math.min(aggr.getCurrentLifetime(), expiration));
        for (int i=0; i<aggr.getObjects().length; i++) {
          aggregateList.setObjectCurrentLifetime(aggr, i, Math.min(aggr.getObjects()[i].getCurrentLifetime(), expiration));
          aggregateList.setObjectRefreshedLifetime(aggr, i, Math.min(aggr.getObjects()[i].getRefreshedLifetime(), expiration));
        }
        return "OK";
      }

      return "Aggregate "+id+" not found in aggregate list";
    }

    if (cmd.startsWith("waiting")) {
      Iterator<Id> iter = waitingList.scan().getIterator();
      String result = "";

      result = result + waitingList.scan().numElements()+ " object(s) waiting\n";

      while (iter.hasNext()) {
        Id thisId = (Id) iter.next();
        result = result + thisId.toStringFull()+" "+waitingList.getMetadata(thisId)+"\n";
      }

      return result;
    }

    if (cmd.startsWith("vlookup")) {
      String[] vkeyS = cmd.substring(8).split("v");
      Id key = factory.buildIdFromToString(vkeyS[0]);
      long version = Long.parseLong(vkeyS[1]);

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      lookup(key, version, new Continuation<PastContent, Exception>() {
        public void receiveResult(PastContent o) {
          ret.complete("result("+o+")");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return "vlookup("+key+"v"+version+")="+awaitDebugResult(ret);
    }

    return null;
  }

  private void removeDeadAggregates() {
    List<AggregateDescriptor> toRemove = new ArrayList<AggregateDescriptor>();
    Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
    long now = environment.getTimeSource().currentTimeMillis();

    /* Note: Multiple keys are mapped to a single aggregate descriptor, so the same ADC
       may be returned multiple times - but we must delete it only once! */

    while (enumeration.hasMoreElements()) {
      AggregateDescriptor adc = (AggregateDescriptor) enumeration.nextElement();
      if (adc.getCurrentLifetime() < (now - config.getAggregateGracePeriod())) {
        if (!toRemove.contains(adc))
          toRemove.add(adc);
        logger.atWarn().log(() -> "Scheduling dead aggregate for removal: "+adc.getKey().toStringFull()+"(expired "+adc.getCurrentLifetime()+")");
      }
    }

    if (toRemove.size() > 0) {
      logger.info("Removing {} dead aggregates...", toRemove.size());

      for (AggregateDescriptor adc : toRemove)
        aggregateList.removeAggregateDescriptor(adc);
    }
  }

  private void storeAggregate(final Aggregate aggr, final long expiration, final ObjectDescriptor[] desc, final Id[] pointers, final Continuation<? super Boolean, Exception> command) {
    logger.debug("storeAggregate() schedules content hash computation...");
    endpoint.process(() -> {
      logger.debug("storeAggregate() starts working on content hash...");
      return factory.buildId(aggr.getContentHash());
    }, new Continuation<Id, Exception>() {
      public void receiveResult(Id o) {
        if (o instanceof Id) {
          aggr.setId((Id) o);
          logger.atInfo().log(() -> "Storing aggregate, CH="+aggr.getId()+", expiration="+expiration+" (rel "+(expiration-environment.getTimeSource().currentTimeMillis())+") with "+desc.length+" objects:");
          for (int j=0; j<desc.length; j++)
            logger.info("#{}: {}", j, desc[j]);

          Continuation<Boolean[], Exception> c = new Continuation<Boolean[], Exception>() {
            public void receiveResult(Boolean[] o) {
              AggregateDescriptor adc = new AggregateDescriptor(
                aggr.getId(),
                expiration,
                desc,
                pointers
              );

              if (o instanceof Boolean[]) {
                aggregateList.addAggregateDescriptor(adc);
                aggregateList.setRoot(aggr.getId());
                aggregateList.writeToDisk();
                logger.debug("Aggregate inserted successfully");
                command.receiveResult(Boolean.valueOf(true));
              } else {
                logger.warn("Unexpected result in aggregate insert (commit): {}", (Object) o);
                command.receiveException(new AggregationException("Unexpected result (commit): "+o));
              }
            }
            public void receiveException(Exception e) {
              command.receiveException(e);
            }
          };

          if (aggregateStore instanceof GCPast)
            ((GCPast)aggregateStore).insert(aggr, expiration, c);
          else
            aggregateStore.insert(aggr, c);
        } else {
          logger.warn("storeAggregate() cannot determine content hash, received {}", o);
          command.receiveException(new AggregationException("storeAggregate() cannot determine content hash"));
        }
      }
      public void receiveException(Exception e) {
        logger.warn("storeAggregate() cannot determine content hash, exception ", e);
        command.receiveException(e);
      }
    });
  }

  private void flushComplete(Object o) {
    if (flushWait != null) {
      Continuation<? super Boolean, Exception> c = flushWait;
      flushWait = null;
      if (o instanceof Exception)
        c.receiveException((Exception) o);
      else
        c.receiveResult((Boolean) o);
    }
  }

  private void formAggregates(final Continuation<? super Boolean, Exception> command) {
    if (flushWait != null) {
      logger.info("Flush in progress... daisy-chaining continuation");
      final Continuation<? super Boolean, Exception> parent = flushWait;
      flushWait = new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {
          logger.info("Daisy-chain receiveResult(), restarting {}", command);
          parent.receiveResult(o);
          formAggregates(command);
        }
        public void receiveException(Exception e) {
          logger.info("Daisy-chain receiveException(), restarting {}", command);
          parent.receiveException(e);
          formAggregates(command);
        }
      };
      return;
    }

    flushWait = command;

    IdSet waitingKeys = waitingList.scan();
    if (waitingKeys.numElements() == 0) {
      logger.info("NO BINS TO PACK");
      flushComplete(Boolean.valueOf(true));
      return;
    }

    logger.info("BIN PACKING STARTED");

    List<ObjectDescriptor> currentAggregate = new ArrayList<ObjectDescriptor>();
    List<ObjectDescriptor[]> aggregates = new ArrayList<ObjectDescriptor[]>();
    List<Id> deletionVector = new ArrayList<Id>();
    Iterator<Id> iter = waitingKeys.getIterator();
    long currentAggregateSize = 0;
    int currentObjectsInAggregate = 0;

    while (true) {
      ObjectDescriptor thisObject = null;
      boolean mustAddObject = false;

      if (aggregates.size() >= config.getMaxAggregatesPerRun())
        break;

      while (iter.hasNext()) {
        Id thisId = (Id) iter.next();
        thisObject = (ObjectDescriptor) waitingList.getMetadata(thisId);
        if (thisObject != null) {
          thisObject = new ObjectDescriptor(
            thisObject.getKey(), thisObject.getVersion(), thisObject.getCurrentLifetime(),
            thisObject.getRefreshedLifetime(), thisObject.getSize()
          );
          if ((((currentAggregateSize + thisObject.getSize()) <= config.getMaxAggregateSize()) || currentAggregate.isEmpty()) && (currentObjectsInAggregate < config.getMaxObjectsInAggregate())) {
            currentAggregateSize += thisObject.getSize();
            currentObjectsInAggregate ++;
            currentAggregate.add(thisObject);
          } else {
            mustAddObject = true;
            break;
          }
        } else {
          logger.atWarn().log(() -> "Metadata in waiting object "+thisId.toStringFull()+" appears to be damaged. Scheduling for deletion...");
          deletionVector.add(thisId);
        }
      }

      int numObjectsInAggregate = currentAggregate.size();
      if (numObjectsInAggregate < 1) {
        logger.warn("Waiting list seems to consist entirely of damaged objects -- please remove!");
        flushComplete(Boolean.valueOf(true));
        return;
      }

      ObjectDescriptor[] desc = new ObjectDescriptor[numObjectsInAggregate];
      for (int i=0; i<numObjectsInAggregate; i++) {
        desc[i] = currentAggregate.get(i);
        logger.debug("#{}: {} {} bytes", i, desc[i].getKey(), desc[i].getSize());
      }

      aggregates.add(desc);
      currentAggregate.clear();
      currentObjectsInAggregate = 0;
      currentAggregateSize = 0;

      if (mustAddObject) {
        currentAggregate.add(thisObject);
        currentAggregateSize += thisObject.getSize();
      } else {
        if (!iter.hasNext())
          break;
      }
    }

    for (final Id thisId : deletionVector) {
      logger.atInfo().log(() -> "Deleting object "+thisId.toStringFull()+" from waiting list (broken metadata)");
      waitingList.unstore(thisId, new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {
          logger.debug("Successfully deleted: {}", thisId);
        }
        public void receiveException(Exception e) {
          logger.warn("Cannot delete: {}, e=", thisId, e);
        }
      });
    }

    Continuation.MultiContinuation<Object[]> c = new Continuation.MultiContinuation<Object[]>(new Continuation<Object[], Exception>() {
      public void receiveResult(Object[] o) {
        flushComplete(Boolean.valueOf(true));
      }
      public void receiveException(Exception e) {
        flushComplete(e);
      }
    }, aggregates.size());

    for (int i=0; i<aggregates.size(); i++) {
      final ObjectDescriptor[] desc = aggregates.get(i);
      final GCPastContent[] obj = new GCPastContent[desc.length];
      final long aggrExpirationF = chooseAggregateLifetime(desc, environment.getTimeSource().currentTimeMillis(), 0);
      final Continuation<Object, Exception> thisContinuation = c.getSubContinuation(i);
      final int iF = i;

      logger.debug("Retrieving #{}.0: {}", i, desc[0].getKey());
      waitingList.getObject(new VersionKey(desc[0].getKey(), desc[0].getVersion()), new Continuation<Serializable, Exception>() {
        int currentQuery = 0;
        public void receiveResult(Serializable o) {
          if ((o!=null) && (o instanceof GCPastContent)) {
            obj[currentQuery++] = (GCPastContent) o;
            if (currentQuery < desc.length) {
              logger.debug("Retrieving #{}.{}: {}", iF, currentQuery, desc[currentQuery].getKey());
              waitingList.getObject(new VersionKey(desc[currentQuery].getKey(), desc[currentQuery].getVersion()), this);
            } else {
              Id[] pointers = aggregateList.getSomePointers(config.getNominalReferenceCount(), config.getMaxPointersPerAggregate(), null);
              storeAggregate(aggregateFactory.buildAggregate(obj, pointers), aggrExpirationF, desc, pointers, new Continuation<Boolean, Exception>() {
                public void receiveResult(Boolean o) {
                  final Continuation.MultiContinuation<Object[]> c2 = new Continuation.MultiContinuation<Object[]>(thisContinuation, desc.length);
                  for (int i=0; i<desc.length; i++) {
                    final Continuation<Object, Exception> c2s = c2.getSubContinuation(i);
                    waitingList.unstore(new VersionKey(desc[i].getKey(), desc[i].getVersion()), new Continuation<Boolean, Exception>() {
                      public void receiveResult(Boolean o) {
                        c2s.receiveResult(o);
                      }
                      public void receiveException(Exception e) {
                        logger.warn("Exception while unstoring aggregate component: ", e);
                        c2s.receiveException(e);
                      }
                    });
                  }
                }
                public void receiveException(Exception e) {
                  logger.warn("Exception while storing new aggregate: ", e);
                  thisContinuation.receiveException(e);
                }
              });
            }
          } else {
            logger.warn("Aggregation cannot retrieve {} (found o={})", desc[currentQuery].getKey(), o);
            thisContinuation.receiveException(new AggregationException("Cannot retrieve object from waiting list: "+desc[currentQuery].getKey()));
          }
        }
        public void receiveException(Exception e) {
          logger.atWarn().log(() -> "Exception while building aggregate: "+e);
          thisContinuation.receiveException(e);
        }
      });
    }
  }

  private long chooseAggregateLifetime(ObjectDescriptor[] components, long now, long currentLifetime) {
    long maxLifetime = 0;

    for (int i=0; i<components.length; i++)
      if (components[i].getRefreshedLifetime() > maxLifetime)
        maxLifetime = components[i].getRefreshedLifetime();

    return maxLifetime;
  }

  private void refreshAggregates() {
    Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
    long now = environment.getTimeSource().currentTimeMillis();
    List<AggregateDescriptor> removeList = new ArrayList<AggregateDescriptor>();
    // read (not modified) by the refresh continuation below
    final List<AggregateDescriptor> refreshAggregateList = new ArrayList<AggregateDescriptor>();
    final List<Long> refreshLifetimeList = new ArrayList<Long>();

    logger.info("Checking aggregate lifetimes");

    aggregateList.resetMarkers();
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
      if (!aggr.isMarker()) {
        aggr.setMarker(true);

        boolean isBeingRefreshed = false;
        if (aggr.getCurrentLifetime() < (now + config.getExpirationRenewThreshold())) {
          long newLifetime = chooseAggregateLifetime(aggr.getObjects(), now, aggr.getCurrentLifetime());
          if (newLifetime > aggr.getCurrentLifetime()) {
            logger.atInfo().log(() -> "Refreshing aggregate "+aggr.getKey().toStringFull()+", new expiration is "+newLifetime);
            isBeingRefreshed = true;

            refreshAggregateList.add(aggr);
            refreshLifetimeList.add(Long.valueOf(newLifetime));
          }
        }

        if ((aggr.getCurrentLifetime() < now) && !isBeingRefreshed) {
          logger.debug("Adding expired aggregate {} to remove list", aggr.getKey());
          removeList.add(aggr);
        }
      }
    }

    boolean deletedOne = false;
    while (!removeList.isEmpty()) {
      AggregateDescriptor aggr = removeList.remove(0);
      logger.atInfo().log(() -> "Removing expired aggregate "+aggr.getKey().toStringFull()+" from list");
      deletedOne = true;
      aggregateList.removeAggregateDescriptor(aggr);
    }

    if (deletedOne)
      aggregateList.writeToDisk();

    if (!refreshAggregateList.isEmpty()) {
      logger.info("Refreshing {} aggregate(s)", refreshAggregateList.size());
      if (aggregateStore instanceof GCPast) {
        Id[] ids = new Id[refreshAggregateList.size()];
        long[] lifetimes = new long[refreshAggregateList.size()];
        for (int i=0; i<refreshAggregateList.size(); i++) {
          ids[i] = refreshAggregateList.get(i).getKey();
          lifetimes[i] = refreshLifetimeList.get(i).longValue();
        }

        ((GCPast)aggregateStore).refresh(ids, lifetimes, new Continuation<Object[], Exception>() {
          public void receiveResult(Object[] o) {
            Object[] results = (Object[]) o;
            logger.debug("Received refresh results for {} aggregates", results.length);
            int numOk = 0;

            for (int i=0; i<results.length; i++) {
              if (results[i] instanceof Boolean) {
                AggregateDescriptor aggr = refreshAggregateList.get(i);
                long newLifetime = refreshLifetimeList.get(i).longValue();
                if (logger.isDebugEnabled()) logger.debug("Aggregate #"+i+" ("+aggr.getKey().toStringFull()+"): OK, new lifetime is "+newLifetime);
                aggregateList.refreshAggregate(aggr, newLifetime);
                numOk ++;
              } else {
                AggregateDescriptor aggr = refreshAggregateList.get(i);
                Exception e = (Exception) results[i];
                if (logger.isWarnEnabled()) logger.warn("Aggregate #"+i+" ("+aggr.getKey().toStringFull()+"): Refresh failed, e=", e);
              }
            }

            aggregateList.writeToDisk();
            logger.info("Refresh complete, {}/{} aggregates refreshed OK", numOk, results.length);
          }
          public void receiveException(Exception e) {
            logger.warn("Interface contract broken; exception {} returned directly", e, e);
          }
        });
      } else {
        logger.debug("Aggregate store does not support GC; refreshing directly");
        for (int i=0; i<refreshAggregateList.size(); i++) {
          AggregateDescriptor aggr = refreshAggregateList.get(i);
          long newLifetime = refreshLifetimeList.get(i).longValue();
          aggregateList.refreshAggregate(aggr, newLifetime);
        }
      }
    }
  }

  private void consolidateAggregates() {
    final long now = environment.getTimeSource().currentTimeMillis();
    Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
    List<AggregateDescriptor> candidateList = new ArrayList<AggregateDescriptor>();

    logger.info("Looking for aggregates to consolidate");

    aggregateList.resetMarkers();
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
      if (!aggr.isMarker()) {
        aggr.setMarker(true);

        /* An aggregate is a candidate for consolidation iff
              - it does not need to be refreshed yet, but has reached a certain age
                (to prevent consolidation of 'garbage'),
              - at least one of its objects is still alive, and
              - it does not contain enough objects, or too many of them are dead
           Pointer arrays are never consolidated; they expire after some time.
        */

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

    if (candidateList.isEmpty()) {
      logger.info("No candidates for consolidation");
      return;
    }

    logger.debug("{} candidate(s) for consolidation", candidateList.size());

    final List<AggregateDescriptor> componentList = new ArrayList<AggregateDescriptor>();
    int objectsSoFar = 0;
    int bytesSoFar = 0;

    while (!candidateList.isEmpty()) {
      AggregateDescriptor adc = candidateList.remove(environment.getRandomSource().nextInt(candidateList.size()));
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

    final AggregateDescriptor[] adc = (AggregateDescriptor[]) componentList.toArray(new AggregateDescriptor[] {});
    final Aggregate[] aggr = new Aggregate[adc.length];
    final int objectsTotal = objectsSoFar;
    final Id firstKey = adc[0].getKey();

    logger.atDebug().log(() -> "Consolidation: Fetching aggregate #0: "+firstKey.toStringFull());
    aggregateStore.lookup(firstKey, new Continuation<PastContent, Exception>() {
      int currentLookup = 0;
      public void receiveResult(PastContent o) {
        if (o instanceof Aggregate) {
          aggr[currentLookup] = (Aggregate) o;
          currentLookup ++;
          if (currentLookup >= componentList.size()) {
            RawGCPastContent[] components = new RawGCPastContent[objectsTotal];
            ObjectDescriptor[] desc = new ObjectDescriptor[objectsTotal];
            int componentIndex = 0;

            logger.info("Consolidation: All aggregates fetched OK, forming new aggregate...");

            for (int i=0; i<adc.length; i++) {
              for (int j=0; j<adc[i].getObjects().length; j++) {
                if (adc[i].getObjects()[j].isAliveAt(now)) {

                  GCPastContent temp = aggr[i].getComponent(j);

                  components[componentIndex] = (RawGCPastContent)temp;
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
            final long aggrExpirationF = chooseAggregateLifetime(desc, environment.getTimeSource().currentTimeMillis(), 0);

            storeAggregate(aggregateFactory.buildAggregate(components, pointers), aggrExpirationF, desc, pointers, new Continuation<Boolean, Exception>() {
              public void receiveResult(Boolean o) {
                logger.info("Consolidated Aggregate stored OK, removing old descriptors...");
                for (AggregateDescriptor aggregateDescriptor : adc) {
                  if (logger.isDebugEnabled()) logger.debug("Removing "+aggregateDescriptor.getKey().toStringFull()+" ...");
                  aggregateList.removeAggregateDescriptor(aggregateDescriptor);
                }

                aggregateList.writeToDisk();
                logger.info("Consolidation completed, {} objects from {} aggregates consolidated", objectsTotal, aggr.length);
              }
              public void receiveException(Exception e) {
                logger.warn("Exception during consolidation store: e={} -- aborting", e, e);
              }
            });
          } else {
            logger.atDebug().log(() -> "Consolidation: Fetching aggregate #"+currentLookup+": "+adc[currentLookup].getKey().toStringFull());
            aggregateStore.lookup(adc[currentLookup].getKey(), this);
          }
        }
      }
      public void receiveException(Exception e) {
        logger.atWarn().setCause(e).log(() -> "Exception during consolidation lookup "+adc[currentLookup].getKey().toStringFull()+": "+e+" -- aborting");
      }
    });
  }

  private void reconnectTree() {

    if (rebuildInProgress) {
      logger.info("Skipping connectivity check (rebuild in progress)");
      return;
    }

    logger.info("Checking for disconnections");

    Id[] disconnected = aggregateList.getSomePointers(1, config.getMaxPointersPerAggregate(), null);
    if (disconnected.length < 2) {
      Id newRoot = (disconnected.length == 1) ? disconnected[0] : null;
      Id currentRoot = aggregateList.getRoot();
      if (((newRoot == null) && (currentRoot != null)) || ((newRoot != null) && (currentRoot == null)) ||
          ((newRoot != null) && (currentRoot != null) && !newRoot.equals(currentRoot)))
        aggregateList.setRoot(newRoot);
      logger.info("No aggregates disconnected (n={})", disconnected.length);
      logger.atDebug().log(() -> "root="+((aggregateList.getRoot() == null) ? "null" : aggregateList.getRoot().toStringFull()));
      return;
    }

    logger.info("Found {} disconnected aggregates; inserting pointer array", disconnected.length);
    storeAggregate(
      aggregateFactory.buildAggregate(new GCPastContent[] {}, disconnected),
      environment.getTimeSource().currentTimeMillis() + config.getPointerArrayLifetime(),
      new ObjectDescriptor[] {},
      disconnected,
      new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {
          logger.debug("Successfully inserted pointer array");
        }
        public void receiveException(Exception e) {
          logger.warn("Error while inserting pointer array: ", e);
        }
      }
    );
  }

  private void timerExpired(char timerID) {
    logger.atDebug().log(() -> "TIMER EXPIRED: #" + (int) timerID);

    switch (timerID) {
      case tiFlush :
      {
        logger.info("Scheduled flush, waiting list: {}", waitingList.getSize());

        formAggregates(new Continuation<Boolean, Exception>() {
          public void receiveResult(Boolean o) {
            logger.debug("Scheduled flush: Success (o={})", o);
          }
          public void receiveException(Exception e) {
            logger.warn("Scheduled flush: Failure (e={})", e, e);
          }
        });

        logger.atInfo().log(() -> "Waiting list: "+waitingList.getSize()+" Scan: "+getNumObjectsWaiting()+" Max: "+(config.getMaxObjectsInAggregate() * config.getMaxAggregatesPerRun()));

        if (getNumObjectsWaiting() >= (config.getMaxObjectsInAggregate() * config.getMaxAggregatesPerRun())) {
          logger.info("Retrying later");
          addTimer(jitterTerm(config.getFlushStressInterval()), tiFlush);
        } else {
          logger.info("OK, waiting for next deadline");
          addTimer(jitterTerm(config.getFlushInterval()), tiFlush);
        }

        break;
      }
      case tiExpire :
      {
        refreshAggregates();
        reconnectTree();
        addTimer(jitterTerm(config.getAggrRefreshInterval()), tiExpire);
        break;
      }
      case tiConsolidate :
      {
        consolidateAggregates();
        addTimer(jitterTerm(config.getConsolidationInterval()), tiConsolidate);
        break;
      }
      case tiMonitor :
      {
        Id[] ids = (Id[]) monitorIDs.toArray(new Id[] {});
        logger.info("Monitor: Refreshing {} objects", ids.length);
        refresh(ids, environment.getTimeSource().currentTimeMillis() + 3 * config.getMonitorRefreshInterval(), new Continuation<Object[], Exception>() {
          public void receiveResult(Object[] o) {
            logger.debug("Monitor: Refresh completed, result={}", o);
          }
          public void receiveException(Exception e) {
            logger.debug("Monitor: Refresh failed, exception=", e);
          }
        });

        addTimer(config.getMonitorRefreshInterval(), tiMonitor);
        break;
      }
      case tiStatistics :
      {
        stats = aggregateList.getStatistics(config.getStatsGranularity(), config.getStatsRange(), config.getNominalReferenceCount());
        stats.dump(environment.getLogManager().getLogger(AggregationStatistics.class, instance));
        addTimer(config.getStatsInterval(), tiStatistics);
        break;
      }
      default:
      {
        panic("Unknown timer expired: " + (int) timerID);
      }
    }
  }

  private void refreshInObjectStore(Id[] ids, long[] expirations, Continuation<Object, Exception> command) {
    if (objectStore instanceof GCPast) {
      ((GCPast)objectStore).refresh(ids, expirations, command);
    } else {
      // same contract as GCPast.refresh: one result per id
      Object[] result = new Object[ids.length];
      Arrays.fill(result, Boolean.TRUE);
      command.receiveResult(result);
    }
  }

  public void refresh(Id[] ids, long expiration, Continuation<? super Object[], Exception> command) {
    long[] expirations = new long[ids.length];
    Arrays.fill(expirations, expiration);
    refresh(ids, expirations, command);
  }

  public void refresh(final Id[] ids, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    if (ids.length < 1) {
      command.receiveResult(new Boolean[] {});
      return;
    }

    logger.info("Refreshing {} keys", ids.length);

    refreshInObjectStore(ids, expirations, new Continuation<Object, Exception>() {
      Object[] result;

      public void receiveResult(Object o) {
        if (o instanceof Object[]) {
          result = (Object[]) o;
        } else {
          logger.warn("refresh: ObjectStore result is of incorrect type; expected Object[], got {}", o);
          result = new Object[ids.length];
          for (int i=0; i<ids.length; i++)
            result[i] = o;
        }

        refreshInAggregates();
      }
      public void receiveException(Exception e) {
        result = new Object[ids.length];
        for (int i=0; i<ids.length; i++)
          result[i] = e;
        logger.warn("", e);
        refreshInAggregates();
      }
      private void refreshInAggregates() {
        Continuation<Object[], Exception> c = new Continuation<Object[], Exception>() {
          public void receiveResult(Object[] o) {
            aggregateList.writeToDisk();
            command.receiveResult(o);
          }
          public void receiveException(Exception e) {
            logger.warn("", e);
            command.receiveException(e);
          }
        };

        refreshInternal(ids, expirations, result, c);
      }
    });
  }

  public void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    final Object result[] = new Object[ids.length];

    for (int i=0; i<ids.length; i++) {
      logger.info("Refresh({}v{}, expiration={})", ids[i], versions[i], expirations[i]);

      AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(new VersionKey(ids[i], versions[i]));
      if (adc!=null) {
        int objDescIndex = adc.lookupSpecific(ids[i], versions[i]);
        if (objDescIndex < 0) {
          result[i] = new AggregationException("Inconsistency detected in aggregate list -- try restarting the application");
        } else {
          if (adc.getObjects()[objDescIndex].getRefreshedLifetime() < expirations[i])
            aggregateList.setObjectRefreshedLifetime(adc, objDescIndex, expirations[i]);

          result[i] = Boolean.valueOf(true);
        }
      } else result[i] = new AggregationException("Not found");
    }

    if (objectStore instanceof VersioningPast) {
      ((VersioningPast)objectStore).refresh(ids, versions, expirations, new Continuation<Object[], Exception>() {
        public void receiveResult(Object[] o) {
          if (o instanceof Object[]) {
            Object[] subresult = (Object[]) o;
            for (int i=0; i<result.length; i++)
              if ((result[i] instanceof Boolean) && !(subresult[i] instanceof Boolean))
                result[i] = subresult[i];
          } else {
            Exception e = new AggregationException("Object store returns unexpected result: "+o);
            for (int i=0; i<result.length; i++)
              result[i] = e;
          }

          command.receiveResult(result);
        }
        public void receiveException(Exception e) {
          command.receiveException(e);
        }
      });
    } else {
      command.receiveResult(result);
    }
  }

  private void refreshInternal(final Id[] ids, final long[] expirations, final Object[] result, final Continuation<? super Object[], Exception> command) {
    logger.info("refreshInternal: Accepted {} keys, starting with first key...", ids.length);

    final Continuation<Object, Exception> theContinuation = new Continuation<Object, Exception>() {
      int objectsMissing = 0;
      int objectsFetched = 0;
      int currentIndex = -1;

      public void receiveResult(Object o) {
        Object lastResult = o;
        while (true) {
          if (currentIndex >= 0) {
            logger.debug("receiveResult({}) for index {}, length={}", lastResult, currentIndex, ids.length);
            if (logger.isDebugEnabled()) logger.debug("Internal refresh of "+ids[currentIndex].toStringFull()+" returned "+lastResult);
            result[currentIndex] = lastResult;
          }

          /* If we completed the entire list, return */

          currentIndex ++;
          if (currentIndex >= ids.length) {
            if (objectsMissing > 0)
              logger.warn("refresh: {}/{} objects not in aggregate list, fetched {} (max {})", objectsMissing, ids.length, objectsFetched, config.getMaxReaggregationPerRefresh());

            int nOK = 0;
            for (int i=0; i<ids.length; i++)
              if (result[i] instanceof Boolean)
                nOK ++;

            logger.info("refreshInternal: Processed {} keys, completed {}", ids.length, nOK);
              for (int i=0; i<ids.length; i++)
                if (logger.isTraceEnabled()) logger.trace(" - "+ids[i].toStringFull()+": "+result[i]);

            command.receiveResult(result);
            return;
          }

          final Id id = ids[currentIndex];
          final long expiration = expirations[currentIndex];
          logger.atInfo().log(() -> "Refresh("+id.toStringFull()+", expiration="+expiration+") started");

          /* In the common case, the aggregate is in the list, and we can simply update its lifetime there */

          AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(id);
          if (adc!=null) {
            int objDescIndex = adc.lookupNewest(id);
            if (objDescIndex < 0) {
              logger.warn("NL: Aggregate found, but object not found in aggregate?!? -- aborted");
              command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
              return;
            }

            if (adc.getObjects()[objDescIndex].getRefreshedLifetime() < expiration) {
              logger.debug("Changing expiration date from {} to {}", adc.getObjects()[objDescIndex].getRefreshedLifetime(), expiration);
              aggregateList.setObjectRefreshedLifetime(adc, objDescIndex, expiration);
            } else {
              logger.debug("Expiration is {} already, no update needed", adc.getObjects()[objDescIndex].getRefreshedLifetime());
            }

            lastResult = Boolean.valueOf(true);
            continue;
          } else {

            /* If the aggregate is not in the list, check whether it is still in the waiting list */

            IdSet waitingIds = waitingList.scan();
            Iterator<Id> iter = waitingIds.getIterator();

            while (iter.hasNext()) {
              final VersionKey vkey = (VersionKey) iter.next();
              if (vkey.getId().equals(id)) {
                ObjectDescriptor thisObject = (ObjectDescriptor) waitingList.getMetadata(vkey);
                logger.atInfo().log(() -> "Refreshing in waiting list: " + vkey.toStringFull());

                if (thisObject == null) {
                  logger.atWarn().log(() -> "Broken object in waiting list: " + vkey.toStringFull() + ", removing...");
                  final Continuation<Object, Exception> myParent = this;
                  waitingList.unstore(vkey, new Continuation<Boolean, Exception>() {
                    public void receiveResult(Boolean o) {
                      logger.atInfo().log(() -> "Broken object "+vkey.toStringFull()+" removed successfully");
                      myParent.receiveResult(new AggregationException("Object in waiting list, but broken: "+vkey.toStringFull()));
                    }
                    public void receiveException(Exception e) {
                      logger.atWarn().setCause(e).log(() -> "Cannot remove broken object "+vkey.toStringFull()+" from waiting list (exception: "+e+")");
                      myParent.receiveResult(new AggregationException("Object broken, in waiting list, and cannot remove: "+vkey.toStringFull()+" (e="+e+")"));
                    }
                  });
                  return;
                }

                if (thisObject.getRefreshedLifetime() < expiration) {
                  ObjectDescriptor newDescriptor = new ObjectDescriptor(
                    thisObject.getKey(), thisObject.getVersion(), thisObject.getCurrentLifetime(),
                    expiration, thisObject.getSize()
                  );

                  final Continuation<Object, Exception> myParent = this;
                  waitingList.setMetadata(vkey, newDescriptor, new Continuation<Boolean, Exception>() {
                    public void receiveResult(Boolean o) {
                      logger.atDebug().log(() -> "Refreshed metadata written ok for "+vkey.toStringFull());
                      myParent.receiveResult(Boolean.valueOf(true));
                    }
                    public void receiveException(Exception e) {
                      logger.atWarn().setCause(e).log(() -> "Cannot refresh waiting object "+vkey.toStringFull()+", e=");
                      myParent.receiveResult(new AggregationException("Cannot refresh waiting object "+vkey.toStringFull()+", setMetadata() failed (e="+e+")"));
                    }
                  });
                  return;
                } else {
                  logger.atDebug().log(() -> "Object found in waiting list and no update needed: "+vkey.toStringFull());
                  receiveResult(Boolean.valueOf(true));
                  return;
                }
              }
            }

            /* If the object is neither in the aggregate list nor in the waiting list, there must have
               been a failure, and we need to reaggregate this object. */

            objectsMissing ++;
            if (config.isAddMissingAfterRefresh()) {
              if (objectsFetched < config.getMaxReaggregationPerRefresh()) {
                objectsFetched ++;
                final Continuation<Object, Exception> myParent = this;
                objectStore.lookup(id, false, new Continuation<PastContent, Exception>() {
                  public void receiveResult(PastContent o) {
                    if (o instanceof PastContent) {
                      final PastContent obj = (PastContent) o;
                      logger.atWarn().log(() -> "Refresh: Found in PAST, but not in aggregate list: "+id.toStringFull());

                      long theVersion;
                      if (o instanceof GCPastContent) {
                        theVersion = ((GCPastContent)obj).getVersion();
                      } else {
                        theVersion = 0;
                      }

                      final VersionKey vkey = new VersionKey(obj.getId(), theVersion);
                      final long theVersionF = theVersion;
                      final int theSize = getSize(obj);

                      if (policy.shouldBeAggregated(obj, theSize)) {
                        if (!waitingList.exists(vkey)) {
                          logger.debug("ADDING MISSING OBJECT AFTER REFRESH: {}", obj.getId());

                          waitingList.store(vkey, new ObjectDescriptor(obj.getId(), theVersionF, expiration, expiration, theSize), obj, new Continuation<Boolean, Exception>() {
                            public void receiveResult(Boolean o) {
                              ((PastImpl) objectStore).cache(obj, new Continuation<Boolean, Exception>() {
                                public void receiveResult(Boolean o) {
                                  logger.atDebug().log(() -> "Refresh: Missing object "+id.toStringFull()+" added ok");
                                  myParent.receiveResult(Boolean.valueOf(true));
                                }
                                public void receiveException(Exception e) {
                                  logger.atWarn().setCause(e).log(() -> "Refresh: Exception while precaching object: "+id.toStringFull()+" (e="+e+")");
                                  myParent.receiveResult(Boolean.valueOf(true));
                                }
                              });
                            }
                            public void receiveException(Exception e) {
                              logger.atWarn().setCause(e).log(() -> "Refresh: Exception while refreshing aggregate: "+id.toStringFull()+" (e="+e+")");
                              myParent.receiveResult(new AggregationException("Cannot store reaggregated object in waiting list: "+id.toStringFull()));
                            }
                          });

                          return;
                        } else {
                          logger.atDebug().log(() -> "Refresh: Missing object already in waiting list: "+id.toStringFull());
                          myParent.receiveResult(Boolean.valueOf(true));
                          return;
                        }
                      }

                      logger.atDebug().log(() -> "Refresh: Missing object should not be aggregated: "+id.toStringFull());
                      myParent.receiveResult(Boolean.valueOf(true));
                      return;
                    } else {
                      logger.atWarn().log(() -> "Refresh: Cannot find refreshed object "+id.toStringFull()+", lookup returns "+o);
                      myParent.receiveException(new AggregationException("Object not found during reaggregation: "+id.toStringFull()));
                    }
                  }
                  public void receiveException(Exception e) {
                    logger.atWarn().log(() -> "Refresh: Exception received while reaggregating "+id.toStringFull()+", e="+e);
                    myParent.receiveException(e);
                  }
                });
                return;
              } else {
                logger.atDebug().log(() -> "Refresh: Limit of "+config.getMaxReaggregationPerRefresh()+" reaggregations exceeded; postponing id="+id.toStringFull());
                lastResult = Boolean.valueOf(true);
                continue;
              }
            } else {
              logger.atWarn().log(() -> "Refresh: Refreshed object not found in any aggregate: "+id.toStringFull());
              lastResult = Boolean.valueOf(true);
              continue;
            }
          }

          /* This should NEVER be reached */
        }
      }
      public void receiveException(Exception e) {
        logger.atWarn().setCause(e).log(() -> "Exception while refreshing "+ids[currentIndex].toStringFull()+", e=");
        receiveResult(e);
      }
    };

    theContinuation.receiveResult(null);
  }

  private int getSize(PastContent obj) {
    try {
      RawPastContent rpc;
      if (obj instanceof RawPastContent) {
        rpc = (RawPastContent)obj;
      } else {
        rpc = new JavaSerializedPastContent(obj);
      }
      SimpleOutputBuffer buf = new SimpleOutputBuffer();
      buf.writeShort(rpc.getType());
      rpc.serialize(buf);
      return buf.getWritten();
    } catch (IOException ioe) {
      logger.atWarn().log(() -> "Cannot serialize object, size unknown: "+ioe);
    }

    return 0;
  }

  public Serializable getHandle() {
    return aggregateList.getRoot();
  }

  public void setHandle(Serializable handle, Continuation<? super Boolean, Exception> command) {
    logger.info("setHandle({})", handle);

    if (!(handle instanceof Id)) {
      command.receiveException(new AggregationException("Illegal handle"));
      return;
    }

    if (aggregateList.getADC((Id) handle) != null) {
      logger.info("Rebuild: Handle {} is already covered by current root", handle);
      command.receiveResult(Boolean.valueOf(true));
      return;
    }

    aggregateList.setRoot((Id) handle);
    rebuildAggregateList(command);
  }

  private void rebuildRecursive(final Id fromKey, final List<Id> keysInProgress, final List<Id> keysPostponed, final List<Id> keysDone, final Continuation<? super Boolean, Exception> command) {
    keysInProgress.add(fromKey);

    logger.atInfo().log(() -> "Rebuild: Fetching handles for aggregate " + fromKey.toStringFull());
    aggregateStore.lookupHandles(fromKey, 999, new Continuation<PastContentHandle[], Exception>() {
      public void receiveResult(PastContentHandle[] o) {
        logger.debug("Got handles for {}", fromKey);

        if (o instanceof PastContentHandle[]) {
          PastContentHandle[] pch = (PastContentHandle[]) o;
          PastContentHandle bestHandle = null;

          for (PastContentHandle pastContentHandle : pch) {
            if ((pastContentHandle == null) || ((pastContentHandle instanceof GCPastContentHandle) && (((GCPastContentHandle)pastContentHandle).getVersion() != 0)))
              continue;

            if (bestHandle == null)
              bestHandle = pastContentHandle;
          }

          if (bestHandle != null) {
            final PastContentHandle thisHandle = bestHandle;
            final Continuation<PastContentHandle[], Exception> outerContinuation = this;

            logger.debug("Fetching {}", thisHandle);
            aggregateStore.fetch(thisHandle, new Continuation<PastContent, Exception>() {
              public void receiveResult(PastContent o) {
                if (o instanceof Aggregate) {
                  keysInProgress.remove(fromKey);
                  keysDone.add(fromKey);

                  logger.atInfo().log(() -> "Rebuild: Got aggregate " + fromKey.toStringFull());

                  Aggregate aggr = (Aggregate) o;
                  ObjectDescriptor[] objects = new ObjectDescriptor[aggr.numComponents()];
                  long aggregateExpiration = (thisHandle instanceof GCPastContentHandle) ? ((GCPastContentHandle)thisHandle).getExpiration() : GCPast.INFINITY_EXPIRATION;

                  for (int i=0; i<aggr.numComponents(); i++) {
                    objects[i] = new ObjectDescriptor(
                      aggr.getComponent(i).getId(),
                      aggr.getComponent(i).getVersion(),
                      aggregateExpiration,
                      aggregateExpiration,
                      getSize(aggr.getComponent(i))
                    );

                    final GCPastContent objData = aggr.getComponent(i);
                    logger.debug("Checking whether {}v{} is in object store...", objData.getId(), objData.getVersion());
                    objectStore.lookupHandles(objData.getId(), 1, new Continuation<PastContentHandle[], Exception>() {
                      public void receiveResult(PastContentHandle[] o) {
                        PastContentHandle[] result = (o instanceof PastContentHandle[]) ? ((PastContentHandle[])o) : new PastContentHandle[] {};
                        logger.atDebug().log(() -> "Handles for "+objData.getId()+"v"+objData.getVersion()+": "+result+" ("+result.length+", PCH="+(o instanceof PastContentHandle[])+")");
                        boolean gotOne = false;
                        for (int i=0; i<result.length; i++) {
                          if (result[i] != null) {
                            if (logger.isDebugEnabled()) logger.debug("Have v"+((GCPastContentHandle)result[i]).getVersion());
                            if (((GCPastContentHandle)result[i]).getVersion() >= objData.getVersion())
                              gotOne = true;
                          }
                        }

                        if (gotOne) {
                          logger.debug("Got it");
                        } else {
                          logger.debug("Ain't got it... reinserting");
                          objectStore.insert(objData, new Continuation<Boolean[], Exception>() {
                            public void receiveResult(Boolean[] o) {
                              logger.debug("Reinsert {}v{} ok, result={}", objData.getId(), objData.getVersion(), o);
                            }
                            public void receiveException(Exception e) {
                              logger.debug("Reinsert {}v{} failed, exception=", objData.getId(), objData.getVersion(), e);
                            }
                          });
                        }
                      }
                      public void receiveException(Exception e) {
                        logger.debug("Cannot retrieve handles for object {}v{} to be restored; e=", objData.getId(), objData.getVersion(), e);
                      }
                    });
                  }

                  aggregateList.addAggregateDescriptor(new AggregateDescriptor(fromKey, aggregateExpiration, objects, aggr.getPointers()));

                  Id[] pointers = aggr.getPointers();
                  int numAdded = 0;
                  if (pointers != null) {
                    for (Id pointer : pointers) {
                      if (pointer instanceof Id) {
                        Id thisPointer = pointer;
                        if (!keysDone.contains(thisPointer) && !keysPostponed.contains(thisPointer) && !keysInProgress.contains(thisPointer)) {
                          if (keysInProgress.size() >= config.getReconstructionMaxConcurrentLookups())
                            keysPostponed.add(thisPointer);
                          else
                            rebuildRecursive(thisPointer, keysInProgress, keysPostponed, keysDone, command);

                          numAdded ++;
                        }
                      }
                    }
                  }

                  logger.debug("Rebuild: Added {} keys, now {} in progress, {} postponed and {} done", numAdded, keysInProgress.size(), keysPostponed.size(), keysDone.size());

                  if (!keysInProgress.isEmpty() || !keysPostponed.isEmpty()) {
                    logger.info("Rebuild: {} keys in progress, {} postponed, {} done", keysInProgress.size(), keysPostponed.size(), keysDone.size());

                    while ((keysInProgress.size() < config.getReconstructionMaxConcurrentLookups()) && (keysPostponed.size() > 0)) {
                      Id nextKey = keysPostponed.get(0);
                      logger.atDebug().log(() -> "Rebuild: Resuming lookup for postponed key "+nextKey.toStringFull());
                      keysPostponed.remove(nextKey);
                      rebuildRecursive(nextKey, keysInProgress, keysPostponed, keysDone, command);
                    }
                  } else {
                    aggregateList.writeToDisk();
                    rebuildInProgress = false;
                    logger.info("Rebuild: Completed; {} aggregates checked", keysDone.size());
                    command.receiveResult(Boolean.valueOf(true));
                  }
                } else {
                  receiveException(new AggregationException("Fetch failed: "+fromKey+", returned "+o));
                }
              }
              public void receiveException(Exception e) {
                outerContinuation.receiveException(e);
              }
            });
          } else {
            receiveException(new AggregationException("LookupHandles did not return any valid handles for "+fromKey));
          }
        } else {
          receiveException(new AggregationException("LookupHandles for "+fromKey+" failed, returned o="+o));
        }
      }
      public void receiveException(Exception e) {
        logger.warn("Rebuild: Exception ", e);
        keysInProgress.remove(fromKey);
        keysDone.add(fromKey);

        if (keysInProgress.isEmpty() && keysPostponed.isEmpty()) {
          rebuildInProgress = false;
          if (aggregateList.isEmpty()) {
            command.receiveException(new AggregationException("Cannot read root aggregate! -- retry later"));
          } else {
            aggregateList.writeToDisk();
            command.receiveResult(Boolean.valueOf(true));
          }
        }

        while ((keysInProgress.size() < config.getReconstructionMaxConcurrentLookups()) && (keysPostponed.size() > 0)) {
          Id nextKey = keysPostponed.get(0);
          logger.atDebug().log(() -> "Rebuild: Resuming lookup for postponed key "+nextKey.toStringFull());
          keysPostponed.remove(nextKey);
          rebuildRecursive(nextKey, keysInProgress, keysPostponed, keysDone, command);
        }
      }
    });
  }

  private void rebuildAggregateList(final Continuation<? super Boolean, Exception> command) {
    // the lookups' continuations may run on different threads (a store can complete on the calling thread, others
    // on the selector), so keep the synchronization these had as Vectors
    List<Id> keysInProgress = Collections.synchronizedList(new ArrayList<Id>());
    List<Id> keysPostponed = Collections.synchronizedList(new ArrayList<Id>());
    List<Id> keysDone = Collections.synchronizedList(new ArrayList<Id>());

    logger.info("rebuildAggregateList({})", aggregateList.getRoot());
    if (aggregateList.getRoot() == null) {
      logger.warn("rebuildAggregateList invoked while rootKey is null");
      command.receiveException(new AggregationException("Set handle first!"));
      return;
    }

    rebuildInProgress = true;
    rebuildRecursive(aggregateList.getRoot(), keysInProgress, keysPostponed, keysDone, command);
  }

  public void insert(final PastContent obj, final Continuation<? super Boolean[], Exception> command) {
    insert(obj, INFINITY_EXPIRATION, command);
  }

  public void insert(final PastContent obj, final long lifetime, final Continuation<? super Boolean[], Exception> command) {

    long theVersion;
    if (obj instanceof GCPastContent) {
      theVersion = ((GCPastContent)obj).getVersion();
    } else {
      theVersion = 0;
    }

    final VersionKey vkey = new VersionKey(obj.getId(), theVersion);
    final long theVersionF = theVersion;
    final int theSize = getSize(obj);

    if (policy.shouldBeAggregated(obj, theSize)) {
      logger.info("AGGREGATE INSERT: {} version={} size={} class={}", obj.getId(), theVersion, theSize, obj.getClass().getName());

      if (objectStore instanceof GCPast)
        ((GCPast)objectStore).insert(obj, lifetime, command);
      else
        objectStore.insert(obj, command);

      waitingList.store(vkey, new ObjectDescriptor(obj.getId(), theVersionF, lifetime, lifetime, theSize), obj, new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {
        }
        public void receiveException(Exception e) {
          logger.warn("Exception while storing aggregate: {} (e={})", obj.getId(), e, e);
        }
      });
    } else {
      logger.info("INSERT WITHOUT AGGREGATION: {} version={} size={} class={}", obj.getId(), theVersionF, theSize, obj.getClass().getName());

      Continuation<Boolean[], Exception> c = new Continuation<Boolean[], Exception>() {
        boolean otherSucceeded = false;
        boolean otherFailed = false;

        public void receiveResult(Boolean[] o) {
          logger.debug("INSERT {} receiveResult({}), otherSucc={} otherFail={}", obj.getId(), o, otherSucceeded, otherFailed);
          if (otherSucceeded) {
            if (!otherFailed) {
              logger.debug("--reporting Success");
              command.receiveResult(new Boolean[] { Boolean.valueOf(true) });
            }
          } else {
            otherSucceeded = true;
          }
        }
        public void receiveException(Exception e) {
          logger.debug("INSERT {} receiveException({}), otherSucc={} otherFail={}", obj.getId(), e, otherSucceeded, otherFailed);
          logger.debug("--reporting Failure");
          command.receiveException(e);
          otherFailed = true;
        }
      };

      if (objectStore instanceof GCPast)
        ((GCPast)objectStore).insert(obj, lifetime, c);
      else
        objectStore.insert(obj, c);

      if (aggregateStore instanceof GCPast)
        ((GCPast)aggregateStore).insert(new NonAggregate(obj), lifetime, c);
      else
        aggregateStore.insert(new NonAggregate(obj), c);
    }
  }

  private void retrieveObjectFromAggregate(final AggregateDescriptor adc, final int objDescIndex, final Continuation<? super GCPastContent, Exception> command) {
    aggregateStore.lookup(adc.getKey(), new Continuation<PastContent, Exception>() {
      public void receiveResult(PastContent o) {
        if (o instanceof Aggregate) {
          final Aggregate aggr = (Aggregate) o;
          endpoint.process(() -> factory.buildId(aggr.getContentHash()), new Continuation<Id, Exception>() {
            public void receiveResult(Id o) {
              if (o instanceof Id) {
                Id aggrNominalKey = (Id) o;

                if (!aggrNominalKey.equals(adc.getKey())) {
                  logger.warn("Cannot validate aggregate {}, hash={}", adc.getKey(), aggrNominalKey);
                  command.receiveException(new AggregationException("Cannot validate aggregate -- retry?"));
                  return;
                }

                logger.debug("Object {} (#{}) successfully retrieved from {}", adc.getObjects()[objDescIndex].getKey(), objDescIndex, adc.getKey());

                objectStore.insert(aggr.getComponent(objDescIndex), new Continuation<Boolean[], Exception>() {
                  public void receiveResult(Boolean[] o) {}
                  public void receiveException(Exception e) {}
                });

                command.receiveResult(aggr.getComponent(objDescIndex));
              } else {
                logger.warn("retrieveObjectFromAggregate cannot determine content hash, received {}", o);
                command.receiveException(new AggregationException("retrieveObjectFromAggregate cannot determine content hash"));
              }
            }
            public void receiveException(Exception e) {
              logger.warn("retrieveObjectFromAggregate cannot determine content hash, exception ", e);
              command.receiveException(e);
            }
          });
        } else {
          logger.warn("retrieveObjectFromAggregate failed; receiveResult({})", o);
          command.receiveResult(null);
        }
      }
      public void receiveException(Exception e) {
        logger.warn("retrieveObjectFromAggregate failed; receiveException({})", e, e);
        command.receiveException(e);
      }
    });
  }

  public void lookup(final Id id, boolean cache, final Continuation<? super PastContent, Exception> command) {
    logger.info("lookup({}, cache={})", id, cache);

    objectStore.lookup(id, cache, new Continuation<PastContent, Exception>() {
      public void receiveResult(PastContent o) {
        if (o != null) {
          logger.debug("NL: Found in PAST: {}", id);
          command.receiveResult(o);
        } else {
          AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(id);
          if (adc!=null) {
            logger.debug("NL: Must retrieve from aggregate");

            int objDescIndex = adc.lookupNewest(id);
            if (objDescIndex < 0) {
              logger.warn("NL: Aggregate found, but object not found in aggregate?!? -- aborted");
              command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
              return;
            }

            retrieveObjectFromAggregate(adc, objDescIndex, command);

          } else {
            logger.warn("NL: LOOKUP FAILED, OBJECT NOT FOUND: {}", id);
            command.receiveResult(null);
          }
        }
      }
      public void receiveException(Exception e) {
        command.receiveException(e);
      }
    });
  }

  public void lookup(final Id id, final long version, final Continuation<? super PastContent, Exception> command) {
    logger.info("lookup({}, version={})", id, version);

    AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(new VersionKey(id, version));
    if (adc!=null) {
      logger.debug("VL: Retrieving from aggregate");

      int objDescIndex = adc.lookupSpecific(id, version);
      if (objDescIndex < 0) {
        logger.warn("VL: Aggregate found, but object not found in aggregate?!? -- aborted");
        command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
        return;
      }

      retrieveObjectFromAggregate(adc, objDescIndex, command);
    } else {
      logger.debug("VL: Not found in aggregate list: {}v{}", id, version);
      if (aggregateStore instanceof VersioningPast) {
        VersioningPast vaggr = (VersioningPast) aggregateStore;
        vaggr.lookup(id, version, new Continuation<PastContent, Exception>() {
          public void receiveResult(PastContent o) {
            if (o != null) {
              logger.debug("VL: Found in Aggregate.VersioningPAST: {}v{}", id, version);
              command.receiveResult(o);
            } else {
              logger.debug("VL: Not found in Aggregate.VersioningPAST: {}v{}", id, version);
              if (objectStore instanceof VersioningPast) {
                VersioningPast vpast = (VersioningPast) objectStore;
                vpast.lookup(id, version, new Continuation<PastContent, Exception>() {
                  public void receiveResult(PastContent o) {
                    if (o != null) {
                      logger.debug("VL: Found in Object.VersioningPAST: {}v{}", id, version);
                      command.receiveResult(o);
                    } else {
                      logger.warn("VL: LOOKUP FAILED, OBJECT NOT FOUND: {}v{}", id, version);
                      command.receiveResult(null);
                    }
                  }
                  public void receiveException(Exception e) {
                    command.receiveException(e);
                  }
                });
              } else {
                logger.debug("VL: Object store does not support versioning");
                command.receiveException(new AggregationException("Cannot find "+id+"v"+version+" -- try rebuilding aggregate list?"));
              }
            }
          }
          public void receiveException(Exception e) {
            logger.warn("Aggregate.VersioningPAST returned exception for {}v{}: ", id, version, e);
            command.receiveException(new AggregationException("Aggregate.VersioningPAST returned exception for "+id+"v"+version+": "+e));
          }
        });
      } else {
        logger.debug("VL: Aggregate store does not support versioning");
        if (objectStore instanceof VersioningPast) {
          VersioningPast vpast = (VersioningPast) objectStore;
          vpast.lookup(id, version, new Continuation<PastContent, Exception>() {
            public void receiveResult(PastContent o) {
              if (o != null) {
                logger.debug("VL: Found in Object.VersioningPAST: {}v{}", id, version);
                command.receiveResult(o);
              } else {
                logger.warn("VL: LOOKUP FAILED, OBJECT NOT FOUND: {}v{}", id, version);
                command.receiveResult(null);
              }
            }
            public void receiveException(Exception e) {
              command.receiveException(e);
            }
          });
        } else {
          logger.debug("VL: Object store does not support versioning");
          command.receiveResult(null);
        }
      }
    }
  }

  public void lookup(Id id, Continuation<? super PastContent, Exception> command) {
    lookup(id, true, command);
  }

  public void lookupHandles(final Id id, final long version, final int max, final Continuation<? super PastContentHandle[], Exception> command) {
    ((VersioningPast)aggregateStore).lookupHandles(id, version, max, command);
  }

  public void lookupHandle(final Id id, final NodeHandle handle, final Continuation<? super PastContentHandle, Exception> command) {
    command.receiveException(new UnsupportedOperationException("LookupHandle() is not supported on Aggregation"));
  }

  public void lookupHandles(final Id id, final int max, final Continuation<? super PastContentHandle[], Exception> command) {
    logger.info("lookupHandles({},{})", id, max);
    objectStore.lookupHandles(id, max, new Continuation<PastContentHandle[], Exception>() {
      public void receiveResult(PastContentHandle[] o) {
        PastContentHandle[] result = (o instanceof PastContentHandle[]) ? ((PastContentHandle[])o) : new PastContentHandle[] {};
        boolean foundHandle = false;

        for (int i=0; i<result.length; i++)
          if (result[i] != null)
            foundHandle = true;

        if (foundHandle) {
          logger.debug("lookupHandles({},{}) handled by PAST; ret={}", id, max, o);
          command.receiveResult(o);
        } else {
          logger.info("lookupHandles({},{}) failed, ret={}", id, max, o);

          AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(id);
          if (adc!=null) {
            logger.debug("lookupHandles: Retrieving from aggregate");

            int objDescIndex = adc.lookupNewest(id);
            if (objDescIndex < 0) {
              logger.warn("lookupHandles: Aggregate found, but object not found in aggregate?!? -- aborted");
              command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
              return;
            }

            if (adc.getObjects()[objDescIndex].getRefreshedLifetime() < environment.getTimeSource().currentTimeMillis()) {
              logger.debug("Object {} exists, but has expired -- ignoring", id);
              command.receiveResult(new PastContentHandle[] { null });
              return;
            }

            retrieveObjectFromAggregate(adc, objDescIndex, new Continuation<GCPastContent, Exception>() {
              public void receiveResult(GCPastContent o) {
                logger.debug("lookupHandles: Retrieved from aggregate: {}, result={}", id, o);
                /* re-inserted implicitly by retrieveObjectFromAggregate */
                objectStore.lookupHandles(id, max, command);
              }
              public void receiveException(Exception e) {
                logger.atWarn().log(() -> "lookupHandles: Cannot retrieve from aggregate, exception "+e);
                command.receiveException(e);
              }
            });
          } else {
            logger.info("lookupHandles: {} is neither in object store nor in aggregate list", id);
            /* Note that we have to give up here... even if the object has not been
               aggregated, there is no efficient way to find out its version number.
               The user must call lookupVersion in this case. */
            command.receiveResult(new PastContentHandle[] { null });
          }
        }
      }
      public void receiveException(Exception e) {
        logger.atWarn().log(() -> "Exception in lookupHandles: "+e);
        command.receiveException(e);
      }
    });
  }

  public void fetch(PastContentHandle handle, Continuation<? super PastContent, Exception> command) {

    /* Note that we never give out any handles from the aggregate store, so this must
       be an object store handle */
    if (handle instanceof GlacierContentHandle)
      aggregateStore.fetch(handle, command);
    else
      objectStore.fetch(handle, command);
  }

  public void flush(Id id, Continuation<? super Boolean, Exception> command) {
    Iterator<Id> iter = waitingList.scan().getIterator();
    boolean objectIsWaiting = false;

    logger.info("flush({}) invoked", id);

    while (iter.hasNext()) {
      VersionKey thisKey = (VersionKey) iter.next();
      if (thisKey.getId().equals(id)) {
        objectIsWaiting = true;
        break;
      }
    }

    if (objectIsWaiting)
      formAggregates(command);
    else
      command.receiveResult(Boolean.valueOf(true));
  }

  public void flush(final Continuation<? super Boolean, Exception> command) {
    formAggregates(command);
  }

  public void rollback(Id id, Continuation<? super PastContent, Exception> command) {
    AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(id);

    if (adc!=null) {
      int objDescIndex = adc.lookupNewest(id);
      if (objDescIndex < 0) {
        logger.warn("Rollback: Aggregate found, but object not found in aggregate?!? -- aborted");
        command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
        return;
      }

      logger.debug("Rollback: Found {}v{}", adc.getObjects()[objDescIndex].getKey(), adc.getObjects()[objDescIndex].getVersion());
      retrieveObjectFromAggregate(adc, objDescIndex, command);
      return;
    }

    logger.debug("Rollback: No version of {} found", id);
    command.receiveResult(null);
  }

  public void reset(Continuation<? super Boolean, Exception> command) {
    aggregateList.clear();

    Iterator<Id> iter = waitingList.scan().getIterator();
    while (iter.hasNext()) {
      VersionKey thisKey = (VersionKey) iter.next();
      waitingList.unstore(thisKey, new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {}
        public void receiveException(Exception e) {}
      });
    }

    command.receiveResult(Boolean.valueOf(true));
  }

  public NodeHandle getLocalNodeHandle() {
    return objectStore.getLocalNodeHandle();
  }

  public int getReplicationFactor() {
    return objectStore.getReplicationFactor();
  }

  public boolean forward(RouteMessage message) {
    return true;
  }

  public void update(NodeHandle handle, boolean joined) {
  }

  public void deliver(Id id, Message message) {

    final AggregationMessage msg = (AggregationMessage) message;
    logger.debug("Received message {} with destination {} from {}", msg, id, msg.getSource().getId());

    if (msg instanceof AggregationTimeoutMessage) {

      /* TimeoutMessages are generated by the local node when a
         timeout expires. */

      AggregationTimeoutMessage gtm = (AggregationTimeoutMessage) msg;
      timerExpired((char) gtm.getUID());
      return;
    } else {
      panic("AGGREGATION ERROR - Received message " + msg + " of unknown type.");
    }
  }

  public void setFlushInterval(int flushIntervalSec) {
    config.setFlushInterval(flushIntervalSec * AggregationConfig.SECONDS);
  }

  public void setMaxAggregateSize(int maxAggregateSize) {
    config.setMaxAggregateSize(maxAggregateSize);
  }

  public void setMaxObjectsInAggregate(int maxObjectsInAggregate) {
    config.setMaxObjectsInAggregate(maxObjectsInAggregate);
  }

  public void setRenewThreshold(int expirationRenewThresholdHrs) {
    config.setExpirationRenewThreshold(expirationRenewThresholdHrs * AggregationConfig.HOURS);
  }

  public void setConsolidationInterval(long consolidationIntervalSec) {
    config.setConsolidationInterval(consolidationIntervalSec * AggregationConfig.SECONDS);
  }

  public void setConsolidationThreshold(long consolidationThresholdSec) {
    config.setConsolidationThreshold(consolidationThresholdSec * AggregationConfig.SECONDS);
  }

  public void setConsolidationMinObjectsPerAggregate(int minObjectsInAggregateArg) {
    config.setConsolidationMinObjectsInAggregate(minObjectsInAggregateArg);
  }

  public void setConsolidationMinUtilization(double minUtilization) {
    config.setConsolidationMinComponentsAlive(minUtilization);
  }

  public Past getAggregateStore() {
    return aggregateStore;
  }

  public Past getObjectStore() {
    return objectStore;
  }

  public int getNumObjectsWaiting() {
    return waitingList.scan().numElements();
  }

  public AggregationStatistics getStatistics() {
    return stats;
  }

  public String getInstance() {
    return instance;
  }

  public Environment getEnvironment() {
    return environment;
  }

  public void setContentDeserializer(PastContentDeserializer deserializer) {
    contentDeserializer = deserializer;
    objectStore.setContentDeserializer(contentDeserializer);
  }

  public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer) {
    contentHandleDeserializer = deserializer;
    objectStore.setContentHandleDeserializer(deserializer);
  }
}
