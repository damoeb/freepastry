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
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import rice.Continuation;
import rice.Futures;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdSet;
import rice.p2p.glacier.VersionKey;
import rice.p2p.past.gc.ExpiringPastContent;
import rice.persistence.CachedStorage;

/**
 * Forms aggregates from the objects in the waiting list: packs them into bins, builds and stores one aggregate
 * per bin, and removes the aggregated objects from the waiting list. Only one flush runs at a time; a flush
 * requested meanwhile is daisy-chained and starts when the running one completes.
 */
final class AggregateFlusher {
  private final CachedStorage waitingList;
  private final AggregateList aggregateList;
  private final AggregateFactory aggregateFactory;
  private final AggregateStorer storer;
  private final AggregationSettings config;
  private final Logger logger;

  private Continuation<? super Boolean, Exception> flushWait;

  AggregateFlusher(CachedStorage waitingList, AggregateList aggregateList, AggregateFactory aggregateFactory, AggregateStorer storer, AggregationSettings config, Logger logger) {
    this.waitingList = waitingList;
    this.aggregateList = aggregateList;
    this.aggregateFactory = aggregateFactory;
    this.storer = storer;
    this.config = config;
    this.logger = logger;
    this.flushWait = null;
  }

  void formAggregates(final Continuation<? super Boolean, Exception> command) {
    if (flushWait != null) {
      logger.info("Flush in progress... daisy-chaining continuation");
      final Continuation<? super Boolean, Exception> parent = flushWait;
      flushWait = new Continuation<Boolean, Exception>() {
        @Override
        public void receiveResult(Boolean result) {
          logger.info("Daisy-chain receiveResult(), restarting {}", command);
          parent.receiveResult(result);
          formAggregates(command);
        }
        @Override
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

    List<Id> deletionVector = new ArrayList<Id>();
    List<ObjectDescriptor[]> aggregates = packBins(waitingKeys, deletionVector);
    if (aggregates == null) {
      logger.warn("Waiting list seems to consist entirely of damaged objects -- please remove!");
      deleteDamagedObjects(deletionVector);
      flushComplete(Boolean.valueOf(true));
      return;
    }

    deleteDamagedObjects(deletionVector);

    Continuation.MultiContinuation<Object[]> multiContinuation = new Continuation.MultiContinuation<Object[]>(new Continuation<Object[], Exception>() {
      @Override
      public void receiveResult(Object[] results) {
        flushComplete(Boolean.valueOf(true));
      }
      @Override
      public void receiveException(Exception e) {
        flushComplete(e);
      }
    }, aggregates.size());

    for (int i=0; i<aggregates.size(); i++)
      new AggregateBuilder(i, aggregates.get(i), multiContinuation.getSubContinuation(i)).start();
  }

  private void flushComplete(Object result) {
    if (flushWait != null) {
      Continuation<? super Boolean, Exception> continuation = flushWait;
      flushWait = null;
      if (result instanceof Exception exception)
        continuation.receiveException(exception);
      else
        continuation.receiveResult((Boolean) result);
    }
  }

  /**
   * Packs the waiting objects into bins, one per aggregate, and collects the keys of objects with damaged
   * metadata in {@code deletionVector}.
   *
   * @return the bins, or null if the first bin would be empty because all objects are damaged
   */
  List<ObjectDescriptor[]> packBins(IdSet waitingKeys, List<Id> deletionVector) {
    List<ObjectDescriptor> currentAggregate = new ArrayList<ObjectDescriptor>();
    List<ObjectDescriptor[]> aggregates = new ArrayList<ObjectDescriptor[]>();
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
      if (numObjectsInAggregate < 1)
        return null;

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
        currentObjectsInAggregate ++;
      } else {
        if (!iter.hasNext())
          break;
      }
    }

    return aggregates;
  }

  private void deleteDamagedObjects(List<Id> deletionVector) {
    for (final Id thisId : deletionVector) {
      logger.atInfo().log(() -> "Deleting object "+thisId.toStringFull()+" from waiting list (broken metadata)");
      Futures.forward(waitingList.unstore(thisId), new Continuation<Boolean, Exception>() {
        @Override
        public void receiveResult(Boolean result) {
          logger.debug("Successfully deleted: {}", thisId);
        }
        @Override
        public void receiveException(Exception e) {
          logger.warn("Cannot delete: {}, e=", thisId, e);
        }
      });
    }
  }

  /**
   * Builds one aggregate: fetches its components from the waiting list one after the other, stores the
   * aggregate, and then removes the components from the waiting list.
   */
  private final class AggregateBuilder implements Continuation<Object, Exception> {
    private final int index;
    private final ObjectDescriptor[] desc;
    private final ExpiringPastContent[] contents;
    private final long aggrExpiration;
    private final Continuation<Object, Exception> command;
    private int currentQuery = 0;

    AggregateBuilder(int index, ObjectDescriptor[] desc, Continuation<Object, Exception> command) {
      this.index = index;
      this.desc = desc;
      this.contents = new ExpiringPastContent[desc.length];
      this.aggrExpiration = AggregateStorer.chooseAggregateLifetime(desc);
      this.command = command;
    }

    void start() {
      logger.debug("Retrieving #{}.0: {}", index, desc[0].getKey());
      Futures.forward(waitingList.getObject(new VersionKey(desc[0].getKey(), desc[0].getVersion())), this);
    }

    @Override
    public void receiveResult(Object result) {
      if ((result!=null) && (result instanceof ExpiringPastContent gcPastContent)) {
        contents[currentQuery++] = gcPastContent;
        if (currentQuery < desc.length) {
          logger.debug("Retrieving #{}.{}: {}", index, currentQuery, desc[currentQuery].getKey());
          Futures.forward(waitingList.getObject(new VersionKey(desc[currentQuery].getKey(), desc[currentQuery].getVersion())), this);
        } else {
          storeAggregate();
        }
      } else {
        logger.warn("Aggregation cannot retrieve {} (found o={})", desc[currentQuery].getKey(), result);
        command.receiveException(new AggregationException("Cannot retrieve object from waiting list: "+desc[currentQuery].getKey()));
      }
    }

    @Override
    public void receiveException(Exception e) {
      logger.atWarn().log(() -> "Exception while building aggregate: "+e);
      command.receiveException(e);
    }

    private void storeAggregate() {
      Id[] pointers = aggregateList.getSomePointers(config.getNominalReferenceCount(), config.getMaxPointersPerAggregate(), null);
      storer.storeAggregate(aggregateFactory.buildAggregate(contents, pointers), aggrExpiration, desc, pointers, new Continuation<Boolean, Exception>() {
        @Override
        public void receiveResult(Boolean result) {
          unstoreComponents();
        }
        @Override
        public void receiveException(Exception e) {
          logger.warn("Exception while storing new aggregate: ", e);
          command.receiveException(e);
        }
      });
    }

    private void unstoreComponents() {
      final Continuation.MultiContinuation<Object[]> multiContinuation = new Continuation.MultiContinuation<Object[]>(command, desc.length);
      for (int i=0; i<desc.length; i++) {
        final Continuation<Object, Exception> subContinuation = multiContinuation.getSubContinuation(i);
        Futures.forward(waitingList.unstore(new VersionKey(desc[i].getKey(), desc[i].getVersion())), new Continuation<Boolean, Exception>() {
          @Override
          public void receiveResult(Boolean result) {
            subContinuation.receiveResult(result);
          }
          @Override
          public void receiveException(Exception e) {
            logger.warn("Exception while unstoring aggregate component: ", e);
            subContinuation.receiveException(e);
          }
        });
      }
    }
  }
}
