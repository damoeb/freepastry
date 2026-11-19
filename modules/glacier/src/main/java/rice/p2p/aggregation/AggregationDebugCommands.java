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
import rice.persistence.CachedStorage;

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
  private final CachedStorage waitingList;
  private final AggregationMonitor monitor;
  private final IdFactory factory;
  private final Environment environment;
  private final AggregationSettings config;
  private final String instance;
  private final Logger logger;

  AggregationDebugCommands(AggregationImpl aggregation, Past aggregateStore, Past objectStore, AggregateList aggregateList, CachedStorage waitingList, AggregationMonitor monitor, IdFactory factory, Environment environment, AggregationSettings config, String instance, Logger logger) {
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
  private static <R> Continuation<R, Exception> reportTo(final CompletableFuture<String> reply) {
    return new Continuation<R, Exception>() {
      @Override
      public void receiveResult(R result) {
        reply.complete("result("+result+")");
      }
      @Override
      public void receiveException(Exception e) {
        reply.complete("exception("+e+")");
      }
    };
  }

  String handleDebugCommand(String command) {
    if (command.indexOf(" ") < 0)
      return null;

    String requestedInstance = command.substring(0, command.indexOf(" "));
    String myInstance = "aggr."+instance.substring(instance.lastIndexOf("-") + 1);
    String instanceFreeCommand = command.substring(requestedInstance.length() + 1);

    if (!requestedInstance.equals(myInstance) && !requestedInstance.equals("a")) {
      String subResult = null;

      if ((subResult == null) && (aggregateStore instanceof DebugCommandHandler debugCommandHandler))
        subResult = debugCommandHandler.handleDebugCommand(command);
      if ((subResult == null) && (objectStore instanceof DebugCommandHandler debugCommandHandler))
        subResult = debugCommandHandler.handleDebugCommand(command);

      return subResult;
    }

    logger.info("Debug command: {}", instanceFreeCommand);

    if (instanceFreeCommand.startsWith("status")) {
      AggregationStatistics stats = aggregation.getStatistics();
      return stats.getNumObjectsTotal()+" objects total\n"+stats.getNumObjectsAlive()+" objects alive\n"+
             stats.getNumAggregatesTotal()+" aggregates total\n"+stats.getNumPointerArrays()+" pointer arrays\n"+
             stats.getCriticalAggregates()+" critical aggregates\n"+stats.getOrphanedAggregates()+" orphaned aggregates\n";
    }

    if (instanceFreeCommand.startsWith("insert")) {
      int numObjects = Integer.parseInt(instanceFreeCommand.substring(7));
      String result = "";

      for (int i=0; i<numObjects; i++) {
        Id randomID = factory.buildRandomId(environment.getRandomSource());
        result = result + randomID.toStringFull() + "\n";
        aggregation.insert(
          new DebugContent(randomID, false, 0, new byte[] {}),
          environment.getTimeSource().currentTimeMillis() + 120*AggregationConfig.SECONDS,
          new Continuation<Boolean[], Exception>() {
            @Override
            public void receiveResult(Boolean[] results) {
            }
            @Override
            public void receiveException(Exception e) {
            }
          }
        );
      }

      return result + numObjects + " object(s) created\n";
    }

    if (instanceFreeCommand.startsWith("show config")) {
      return config.describe();
    }

    if (instanceFreeCommand.startsWith("ls")) {
      return list(instanceFreeCommand);
    }

    if (instanceFreeCommand.startsWith("write list")) {
      aggregateList.writeToDisk();
      return "Done, new root is "+((aggregateList.getRoot()==null) ? "null" : aggregateList.getRoot().toStringFull());
    }

    if ((instanceFreeCommand.length() >= 5) && instanceFreeCommand.substring(0, 5).equals("reset")) {
      final CompletableFuture<String> reply = new CompletableFuture<String>();
      aggregation.reset(AggregationDebugCommands.<Boolean>reportTo(reply));
      return awaitDebugResult(reply);
    }

    if (instanceFreeCommand.startsWith("flush")) {
      final CompletableFuture<String> reply = new CompletableFuture<String>();
      aggregation.flush(AggregationDebugCommands.<Boolean>reportTo(reply));
      return awaitDebugResult(reply);
    }

    if (instanceFreeCommand.startsWith("get root")) {
      return "root="+((aggregateList.getRoot()==null) ? "null" : aggregateList.getRoot().toStringFull());
    }

    if (instanceFreeCommand.startsWith("set root")) {
      final CompletableFuture<String> reply = new CompletableFuture<String>();
      aggregation.setHandle(factory.buildIdFromString(instanceFreeCommand.substring(9)), AggregationDebugCommands.<Boolean>reportTo(reply));
      return awaitDebugResult(reply);
    }

    if (instanceFreeCommand.startsWith("lookup")) {
      Id id = factory.buildIdFromString(instanceFreeCommand.substring(7));

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      aggregation.lookup(id, false, AggregationDebugCommands.<PastContent>reportTo(reply));

      return "lookup("+id+")="+awaitDebugResult(reply);
    }

    if (instanceFreeCommand.startsWith("handles")) {
      return handles(instanceFreeCommand);
    }

    if (instanceFreeCommand.startsWith("refresh all")) {
      return refreshAll(instanceFreeCommand);
    }

    if (instanceFreeCommand.startsWith("refresh")) {
      String args = instanceFreeCommand.substring(8);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String keyArg = args.substring(0, args.lastIndexOf(' '));

      Id id = factory.buildIdFromString(keyArg);
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(expirationArg);

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      aggregation.refresh(new Id[] { id }, expiration, AggregationDebugCommands.<Object[]>reportTo(reply));

      return "refresh("+id+", "+expiration+")="+awaitDebugResult(reply);
    }

    String monitorResult = monitor.handleDebugCommand(instanceFreeCommand);
    if (monitorResult != null)
      return monitorResult;

    if (instanceFreeCommand.startsWith("killall")) {
      return killAll(instanceFreeCommand);
    }

    if (instanceFreeCommand.startsWith("waiting")) {
      Iterator<Id> iter = waitingList.scan().getIterator();
      String result = "";

      result = result + waitingList.scan().numElements()+ " object(s) waiting\n";

      while (iter.hasNext()) {
        Id thisId = (Id) iter.next();
        result = result + thisId.toStringFull()+" "+waitingList.getMetadata(thisId)+"\n";
      }

      return result;
    }

    if (instanceFreeCommand.startsWith("vlookup")) {
      @SuppressWarnings("StringSplitter") // splits debug command arguments; dropping trailing empty ones is intended
      String[] versionKeyParts = instanceFreeCommand.substring(8).split("v");
      Id key = factory.buildIdFromString(versionKeyParts[0]);
      long version = Long.parseLong(versionKeyParts[1]);

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      aggregation.lookup(key, version, AggregationDebugCommands.<PastContent>reportTo(reply));

      return "vlookup("+key+"v"+version+")="+awaitDebugResult(reply);
    }

    return null;
  }

  private String list(String command) {
    Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
    StringBuilder result = new StringBuilder();
    int numAggr = 0, numObj = 0;

    long now = environment.getTimeSource().currentTimeMillis();
    if (command.indexOf("-r") < 0)
      now = 0;

    aggregateList.recalculateReferenceCounts(null);
    aggregateList.resetMarkers();
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggregate = (AggregateDescriptor) enumeration.nextElement();
      if (!aggregate.isMarker()) {
        result.append("***" + aggregate.getKey().toStringFull() + " (" + aggregate.getObjects().length + " obj, " +
                 aggregate.getPointers().length + " ptr, " + aggregate.getReferenceCount() + " ref, exp=" +
                 (aggregate.getCurrentLifetime() - now) + ")\n");
        for (int i=0; i<aggregate.getObjects().length; i++)
          result.append("    #"+i+" "+
            aggregate.getObjects()[i].getKey().toStringFull()+"v"+aggregate.getObjects()[i].getVersion() +
            ", lt=" + (aggregate.getObjects()[i].getCurrentLifetime()-now) +
            ", rt=" + (aggregate.getObjects()[i].getRefreshedLifetime()-now) +
            ", size=" + aggregate.getObjects()[i].getSize() + " bytes\n");
        for (int i=0; i<aggregate.getPointers().length; i++)
          result.append("    Ref "+aggregate.getPointers()[i].toStringFull()+"\n");
        result.append("\n");
        aggregate.setMarker(true);
        numAggr ++;
        numObj += aggregate.getObjects().length;
      }
    }

    result.append(numAggr + " aggregate(s), " + numObj + " object(s)");

    return result.toString();
  }

  private String handles(String command) {
    String args = command.substring(8);
    Id id = factory.buildIdFromString(args.substring(args.indexOf(' ') + 1));
    int max = Integer.parseInt(args.substring(0, args.indexOf(' ')));

    final CompletableFuture<String> reply = new CompletableFuture<String>();
    aggregation.lookupHandles(id, max, new Continuation<PastContentHandle[], Exception>() {
      @Override
      public void receiveResult(PastContentHandle[] results) {
        if (results != null) {
          PastContentHandle[] handles = (PastContentHandle[]) results;
          String text = "";
          for (int i=0; i<handles.length; i++)
            text = text + "#"+i+" "+handles[i]+"\n";
          reply.complete(text + handles.length + " handle(s) returned\n");
        } else reply.complete("result("+Arrays.toString(results)+") -- no handles returned!");
      }
      @Override
      public void receiveException(Exception e) {
        reply.complete("exception("+e+")");
      }
    });

    return "Handles("+max+","+id+"):\n"+awaitDebugResult(reply);
  }

  private String refreshAll(String command) {
    long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(command.substring(12));
    TreeSet<Id> ids = new TreeSet<Id>();
    String result;

    aggregateList.resetMarkers();

    Enumeration<AggregateDescriptor> enumeration = aggregateList.elements();
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggregate = (AggregateDescriptor) enumeration.nextElement();
      if (!aggregate.isMarker()) {
        aggregate.setMarker(true);
        for (int i=0; i<aggregate.getObjects().length; i++)
          ids.add(aggregate.getObjects()[i].getKey());
      }
    }

    if (!ids.isEmpty()) {
      Id[] allIds = (Id[]) ids.toArray(new Id[] {});
      result = "Refreshing " + allIds.length + " keys...\n";

      for (int i=0; i<allIds.length; i++)
        result = result + "#" + i + " " + allIds[i].toStringFull() + "\n";

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      aggregation.refresh(allIds, expiration, AggregationDebugCommands.<Object[]>reportTo(reply));

      result = result + awaitDebugResult(reply);
    } else result = "Aggregate list is empty; nothing to refresh!";

    return result;
  }

  private String killAll(String command) {
    String args = command.substring(8);
    String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
    String keyArg = args.substring(0, args.lastIndexOf(' '));

    Id id = factory.buildIdFromString(keyArg);
    long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(expirationArg);

    AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.getAggregateDescriptor(id);
    if (aggregate != null) {
      aggregateList.setAggregateLifetime(aggregate, Math.min(aggregate.getCurrentLifetime(), expiration));
      for (int i=0; i<aggregate.getObjects().length; i++) {
        aggregateList.setObjectCurrentLifetime(aggregate, i, Math.min(aggregate.getObjects()[i].getCurrentLifetime(), expiration));
        aggregateList.setObjectRefreshedLifetime(aggregate, i, Math.min(aggregate.getObjects()[i].getRefreshedLifetime(), expiration));
      }
      return "OK";
    }

    return "Aggregate "+id+" not found in aggregate list";
  }
}
