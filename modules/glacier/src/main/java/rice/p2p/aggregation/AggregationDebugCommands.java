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

import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.glacier.v2.DebugContent;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.util.DebugCommandHandler;
import rice.persistence.StorageManager;

/**
 * The debug commands of an {@link AggregationImpl}. Commands for another instance are passed on to the
 * aggregate store and the object store.
 */
final class AggregationDebugCommands {
  /**
   * How long a debug command waits for its asynchronous result.
   */
  private static final long DEBUG_COMMAND_TIMEOUT = 2 * 60 * 1000;

  private final AggregationImpl aggregation;
  private final Past aggregateStore;
  private final Past objectStore;
  private final AggregateList aggregateList;
  private final StorageManager waitingList;
  private final AggregationMonitor monitor;
  private final IdFactory factory;
  private final Environment environment;
  private final AggregationSettings config;
  private final String instance;
  private final Logger logger;

  AggregationDebugCommands(AggregationImpl aggregation, Past aggregateStore, Past objectStore, AggregateList aggregateList, StorageManager waitingList, AggregationMonitor monitor, IdFactory factory, Environment environment, AggregationSettings config, String instance, Logger logger) {
    this.aggregation = aggregation;
    this.aggregateStore = aggregateStore;
    this.objectStore = objectStore;
    this.aggregateList = aggregateList;
    this.waitingList = waitingList;
    this.monitor = monitor;
    this.factory = factory;
    this.environment = environment;
    this.config = config;
    this.instance = instance;
    this.logger = logger;
  }

  static String await(CompletableFuture<String> result, Environment environment) {
    return DebugCommandHandler.await(result, environment.getSelectorManager(), DEBUG_COMMAND_TIMEOUT);
  }

  private String awaitDebugResult(CompletableFuture<String> result) {
    return await(result, environment);
  }

  /**
   * @return a Continuation that completes {@code ret} with "result(...)" or "exception(...)"
   */
  private static <R> Continuation<R, Exception> reportTo(final CompletableFuture<String> ret) {
    return new Continuation<R, Exception>() {
      public void receiveResult(R o) {
        ret.complete("result("+o+")");
      }
      public void receiveException(Exception e) {
        ret.complete("exception("+e+")");
      }
    };
  }

  String handleDebugCommand(String command) {
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
      AggregationStatistics stats = aggregation.getStatistics();
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
        aggregation.insert(
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
      return list(cmd);
    }

    if (cmd.startsWith("write list")) {
      aggregateList.writeToDisk();
      return "Done, new root is "+((aggregateList.getRoot()==null) ? "null" : aggregateList.getRoot().toStringFull());
    }

    if ((cmd.length() >= 5) && cmd.substring(0, 5).equals("reset")) {
      final CompletableFuture<String> ret = new CompletableFuture<String>();
      aggregation.reset(AggregationDebugCommands.<Boolean>reportTo(ret));
      return awaitDebugResult(ret);
    }

    if (cmd.startsWith("flush")) {
      final CompletableFuture<String> ret = new CompletableFuture<String>();
      aggregation.flush(AggregationDebugCommands.<Boolean>reportTo(ret));
      return awaitDebugResult(ret);
    }

    if (cmd.startsWith("get root")) {
      return "root="+((aggregateList.getRoot()==null) ? "null" : aggregateList.getRoot().toStringFull());
    }

    if (cmd.startsWith("set root")) {
      final CompletableFuture<String> ret = new CompletableFuture<String>();
      aggregation.setHandle(factory.buildIdFromToString(cmd.substring(9)), AggregationDebugCommands.<Boolean>reportTo(ret));
      return awaitDebugResult(ret);
    }

    if (cmd.startsWith("lookup")) {
      Id id = factory.buildIdFromToString(cmd.substring(7));

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      aggregation.lookup(id, false, AggregationDebugCommands.<PastContent>reportTo(ret));

      return "lookup("+id+")="+awaitDebugResult(ret);
    }

    if (cmd.startsWith("handles")) {
      return handles(cmd);
    }

    if (cmd.startsWith("refresh all")) {
      return refreshAll(cmd);
    }

    if (cmd.startsWith("refresh")) {
      String args = cmd.substring(8);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String keyArg = args.substring(0, args.lastIndexOf(' '));

      Id id = factory.buildIdFromToString(keyArg);
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(expirationArg);

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      aggregation.refresh(new Id[] { id }, expiration, AggregationDebugCommands.<Object[]>reportTo(ret));

      return "refresh("+id+", "+expiration+")="+awaitDebugResult(ret);
    }

    String monitorResult = monitor.handleDebugCommand(cmd);
    if (monitorResult != null)
      return monitorResult;

    if (cmd.startsWith("killall")) {
      return killAll(cmd);
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
      aggregation.lookup(key, version, AggregationDebugCommands.<PastContent>reportTo(ret));

      return "vlookup("+key+"v"+version+")="+awaitDebugResult(ret);
    }

    return null;
  }

  private String list(String cmd) {
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

  private String handles(String cmd) {
    String args = cmd.substring(8);
    Id id = factory.buildIdFromToString(args.substring(args.indexOf(' ') + 1));
    int max = Integer.parseInt(args.substring(0, args.indexOf(' ')));

    final CompletableFuture<String> ret = new CompletableFuture<String>();
    aggregation.lookupHandles(id, max, new Continuation<PastContentHandle[], Exception>() {
      public void receiveResult(PastContentHandle[] o) {
        if (o instanceof PastContentHandle[]) {
          PastContentHandle[] oA = (PastContentHandle[]) o;
          String text = "";
          for (int i=0; i<oA.length; i++)
            text = text + "#"+i+" "+oA[i]+"\n";
          ret.complete(text + oA.length + " handle(s) returned\n");
        } else ret.complete("result("+Arrays.toString(o)+") -- no handles returned!");
      }
      public void receiveException(Exception e) {
        ret.complete("exception("+e+")");
      }
    });

    return "Handles("+max+","+id+"):\n"+awaitDebugResult(ret);
  }

  private String refreshAll(String cmd) {
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
      aggregation.refresh(allIds, expiration, AggregationDebugCommands.<Object[]>reportTo(ret));

      result = result + awaitDebugResult(ret);
    } else result = "Aggregate list is empty; nothing to refresh!";

    return result;
  }

  private String killAll(String cmd) {
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
}
