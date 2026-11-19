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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Id;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.ExpiringPast;
import rice.p2p.past.gc.ExpiringPastContent;
import rice.p2p.past.gc.ExpiringPastContentHandle;

/**
 * The tree of aggregates that pointers link to the root (the handle): rebuilds the aggregate list by walking
 * the tree from a new root, and keeps the tree connected by inserting pointer arrays for disconnected
 * aggregates.
 */
final class AggregateTree {
  /** Asks for the handles of all replicas of an aggregate. */
  private static final int MAX_HANDLES = 999;

  private final Past aggregateStore;
  private final Past objectStore;
  private final AggregateList aggregateList;
  private final AggregateStorer storer;
  private final AggregateFactory aggregateFactory;
  private final AggregationSettings config;
  private final TimeSource timeSource;
  private final Logger logger;

  private boolean rebuildInProgress;

  AggregateTree(Past aggregateStore, Past objectStore, AggregateList aggregateList, AggregateStorer storer, AggregateFactory aggregateFactory, AggregationSettings config, TimeSource timeSource, Logger logger) {
    this.aggregateStore = aggregateStore;
    this.objectStore = objectStore;
    this.aggregateList = aggregateList;
    this.storer = storer;
    this.aggregateFactory = aggregateFactory;
    this.config = config;
    this.timeSource = timeSource;
    this.logger = logger;
    this.rebuildInProgress = false;
  }

  void setHandle(Id handle, Continuation<? super Boolean, Exception> command) {
    logger.info("setHandle({})", handle);

    if (handle == null) {
      command.receiveException(new AggregationException("Illegal handle"));
      return;
    }

    if (aggregateList.getAggregateDescriptor(handle) != null) {
      logger.info("Rebuild: Handle {} is already covered by current root", handle);
      command.receiveResult(Boolean.valueOf(true));
      return;
    }

    aggregateList.setRoot(handle);
    rebuildAggregateList(command);
  }

  /**
   * Inserts a pointer array if more than one aggregate is disconnected from the tree, or else makes the only
   * disconnected aggregate the root.
   */
  void reconnectTree() {

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
    storer.storeAggregate(
      aggregateFactory.buildAggregate(new ExpiringPastContent[] {}, disconnected),
      timeSource.currentTimeMillis() + config.getPointerArrayLifetime(),
      new ObjectDescriptor[] {},
      disconnected,
      new Continuation<Boolean, Exception>() {
        @Override
        public void receiveResult(Boolean result) {
          logger.debug("Successfully inserted pointer array");
        }
        @Override
        public void receiveException(Exception e) {
          logger.warn("Error while inserting pointer array: ", e);
        }
      }
    );
  }

  private void rebuildAggregateList(final Continuation<? super Boolean, Exception> command) {
    logger.info("rebuildAggregateList({})", aggregateList.getRoot());
    if (aggregateList.getRoot() == null) {
      logger.warn("rebuildAggregateList invoked while rootKey is null");
      command.receiveException(new AggregationException("Set handle first!"));
      return;
    }

    rebuildInProgress = true;
    new Rebuild(command).fetch(aggregateList.getRoot());
  }

  /**
   * One rebuild of the aggregate list: fetches the aggregates reachable from the root, at most
   * {@code reconstructionMaxConcurrentLookups} at a time, and adds them to the aggregate list.
   */
  private final class Rebuild {
    // the lookups' continuations may run on different threads (a store can complete on the calling thread, others
    // on the selector), so keep the synchronization these had as Vectors
    private final List<Id> keysInProgress = Collections.synchronizedList(new ArrayList<Id>());
    private final List<Id> keysPostponed = Collections.synchronizedList(new ArrayList<Id>());
    private final List<Id> keysDone = Collections.synchronizedList(new ArrayList<Id>());
    private final Continuation<? super Boolean, Exception> command;
    // a lookup that completes on the calling thread finishes the run inside the loop that started it
    private final AtomicBoolean finished = new AtomicBoolean();

    Rebuild(Continuation<? super Boolean, Exception> command) {
      this.command = command;
    }

    void fetch(final Id fromKey) {
      keysInProgress.add(fromKey);
      startLookup(fromKey);
    }

    /** Starts the lookup of a key that is already in {@code keysInProgress}. */
    private void startLookup(final Id fromKey) {
      logger.atInfo().log(() -> "Rebuild: Fetching handles for aggregate " + fromKey.toStringFull());
      aggregateStore.lookupHandles(fromKey, MAX_HANDLES, new Continuation<PastContentHandle[], Exception>() {
        @Override
        public void receiveResult(PastContentHandle[] results) {
          logger.debug("Got handles for {}", fromKey);
          handlesReceived(fromKey, results);
        }
        @Override
        public void receiveException(Exception e) {
          failed(fromKey, e);
        }
      });
    }

    private void handlesReceived(final Id fromKey, PastContentHandle[] results) {
      if (results == null) {
        failed(fromKey, new AggregationException("LookupHandles for "+fromKey+" failed, returned o="+Arrays.toString(results)));
        return;
      }

      PastContentHandle bestHandle = null;
      for (PastContentHandle pastContentHandle : results) {
        if ((pastContentHandle == null) || ((pastContentHandle instanceof ExpiringPastContentHandle gcPastContentHandle) && (gcPastContentHandle.getVersion() != 0)))
          continue;

        if (bestHandle == null)
          bestHandle = pastContentHandle;
      }

      if (bestHandle == null) {
        failed(fromKey, new AggregationException("LookupHandles did not return any valid handles for "+fromKey));
        return;
      }

      final PastContentHandle thisHandle = bestHandle;
      logger.debug("Fetching {}", thisHandle);
      aggregateStore.fetch(thisHandle, new Continuation<PastContent, Exception>() {
        @Override
        public void receiveResult(PastContent result) {
          if (result instanceof Aggregate aggregate) {
            aggregateFetched(fromKey, thisHandle, aggregate);
          } else {
            failed(fromKey, new AggregationException("Fetch failed: "+fromKey+", returned "+result));
          }
        }
        @Override
        public void receiveException(Exception e) {
          failed(fromKey, e);
        }
      });
    }

    private void aggregateFetched(Id fromKey, PastContentHandle thisHandle, Aggregate aggr) {
      keysInProgress.remove(fromKey);
      keysDone.add(fromKey);

      logger.atInfo().log(() -> "Rebuild: Got aggregate " + fromKey.toStringFull());

      ObjectDescriptor[] objects = new ObjectDescriptor[aggr.numComponents()];
      long aggregateExpiration = (thisHandle instanceof ExpiringPastContentHandle gcPastContentHandle) ? gcPastContentHandle.getExpiration() : ExpiringPast.INFINITY_EXPIRATION;

      for (int i=0; i<aggr.numComponents(); i++) {
        objects[i] = new ObjectDescriptor(
          aggr.getComponent(i).getId(),
          aggr.getComponent(i).getVersion(),
          aggregateExpiration,
          aggregateExpiration,
          AggregateStorer.getSize(aggr.getComponent(i), logger)
        );

        restoreInObjectStore(aggr.getComponent(i));
      }

      aggregateList.addAggregateDescriptor(new AggregateDescriptor(fromKey, aggregateExpiration, objects, aggr.getPointers()));

      Id[] pointers = aggr.getPointers();
      int numAdded = 0;
      // register all lookups before starting any, so one that completes at once cannot see the run as done
      List<Id> toStart = new ArrayList<>();
      if (pointers != null) {
        for (Id pointer : pointers) {
          if (pointer != null) {
            Id thisPointer = pointer;
            if (!keysDone.contains(thisPointer) && !keysPostponed.contains(thisPointer) && !keysInProgress.contains(thisPointer)) {
              if (keysInProgress.size() >= config.getReconstructionMaxConcurrentLookups())
                keysPostponed.add(thisPointer);
              else {
                keysInProgress.add(thisPointer);
                toStart.add(thisPointer);
              }

              numAdded ++;
            }
          }
        }
      }
      toStart.forEach(this::startLookup);

      logger.debug("Rebuild: Added {} keys, now {} in progress, {} postponed and {} done", numAdded, keysInProgress.size(), keysPostponed.size(), keysDone.size());

      if (!keysInProgress.isEmpty() || !keysPostponed.isEmpty()) {
        logger.info("Rebuild: {} keys in progress, {} postponed, {} done", keysInProgress.size(), keysPostponed.size(), keysDone.size());
        resumePostponed();
      } else if (finished.compareAndSet(false, true)) {
        aggregateList.writeToDisk();
        rebuildInProgress = false;
        logger.info("Rebuild: Completed; {} aggregates checked", keysDone.size());
        command.receiveResult(Boolean.valueOf(true));
      }
    }

    /**
     * Reinserts a component of a rebuilt aggregate into the object store, unless the object store has that
     * version (or a newer one) already.
     */
    private void restoreInObjectStore(final ExpiringPastContent objData) {
      logger.debug("Checking whether {}v{} is in object store...", objData.getId(), objData.getVersion());
      objectStore.lookupHandles(objData.getId(), 1, new Continuation<PastContentHandle[], Exception>() {
        @Override
        public void receiveResult(PastContentHandle[] rawHandles) {
          PastContentHandle[] result = (rawHandles != null) ? ((PastContentHandle[])rawHandles) : new PastContentHandle[] {};
          logger.atDebug().log(() -> "Handles for "+objData.getId()+"v"+objData.getVersion()+": "+Arrays.toString(result)+" ("+result.length+", PCH="+(rawHandles != null)+")");
          boolean gotOne = false;
          for (int i=0; i<result.length; i++) {
            if (result[i] != null) {
              if (logger.isDebugEnabled()) logger.debug("Have v"+((ExpiringPastContentHandle)result[i]).getVersion());
              if (((ExpiringPastContentHandle)result[i]).getVersion() >= objData.getVersion())
                gotOne = true;
            }
          }

          if (gotOne) {
            logger.debug("Got it");
          } else {
            logger.debug("Ain't got it... reinserting");
            objectStore.insert(objData, new Continuation<Boolean[], Exception>() {
              @Override
              public void receiveResult(Boolean[] results) {
                logger.debug("Reinsert {}v{} ok, result={}", objData.getId(), objData.getVersion(), results);
              }
              @Override
              public void receiveException(Exception e) {
                logger.debug("Reinsert {}v{} failed, exception=", objData.getId(), objData.getVersion(), e);
              }
            });
          }
        }
        @Override
        public void receiveException(Exception e) {
          logger.debug("Cannot retrieve handles for object {}v{} to be restored; e=", objData.getId(), objData.getVersion(), e);
        }
      });
    }

    private void failed(Id fromKey, Exception e) {
      logger.warn("Rebuild: Exception ", e);
      keysInProgress.remove(fromKey);
      keysDone.add(fromKey);

      if (keysInProgress.isEmpty() && keysPostponed.isEmpty() && finished.compareAndSet(false, true)) {
        rebuildInProgress = false;
        if (aggregateList.isEmpty()) {
          command.receiveException(new AggregationException("Cannot read root aggregate! -- retry later"));
        } else {
          aggregateList.writeToDisk();
          command.receiveResult(Boolean.valueOf(true));
        }
      }

      resumePostponed();
    }

    private void resumePostponed() {
      while ((keysInProgress.size() < config.getReconstructionMaxConcurrentLookups()) && (keysPostponed.size() > 0)) {
        Id nextKey = keysPostponed.get(0);
        logger.atDebug().log(() -> "Rebuild: Resuming lookup for postponed key "+nextKey.toStringFull());
        keysPostponed.remove(nextKey);
        fetch(nextKey);
      }
    }
  }
}
