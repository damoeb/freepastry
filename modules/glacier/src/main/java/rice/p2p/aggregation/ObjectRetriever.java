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

import org.slf4j.Logger;
import rice.Continuation;
import rice.Futures;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.VersioningPast;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.ExpiringPastContent;

/**
 * Looks objects up: in the object store first, and in their aggregate (found through the aggregate list) if
 * the object store does not have them. An object retrieved from an aggregate is reinserted into the object
 * store.
 */
final class ObjectRetriever {
  private final Past objectStore;
  private final Past aggregateStore;
  private final AggregateList aggregateList;
  private final Endpoint endpoint;
  private final IdFactory factory;
  private final TimeSource timeSource;
  private final Logger logger;

  ObjectRetriever(Past objectStore, Past aggregateStore, AggregateList aggregateList, Endpoint endpoint, IdFactory factory, TimeSource timeSource, Logger logger) {
    this.objectStore = objectStore;
    this.aggregateStore = aggregateStore;
    this.aggregateList = aggregateList;
    this.endpoint = endpoint;
    this.factory = factory;
    this.timeSource = timeSource;
    this.logger = logger;
  }

  void lookup(final Id id, boolean cache, final Continuation<? super PastContent, Exception> command) {
    logger.info("lookup({}, cache={})", id, cache);

    objectStore.lookup(id, cache, new Continuation<PastContent, Exception>() {
      @Override
      public void receiveResult(PastContent result) {
        if (result != null) {
          logger.debug("NL: Found in PAST: {}", id);
          command.receiveResult(result);
        } else {
          lookupInAggregate(id, command);
        }
      }
      @Override
      public void receiveException(Exception e) {
        command.receiveException(e);
      }
    });
  }

  private void lookupInAggregate(Id id, Continuation<? super PastContent, Exception> command) {
    AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.getAggregateDescriptor(id);
    if (aggregate!=null) {
      logger.debug("NL: Must retrieve from aggregate");

      int objDescIndex = aggregate.lookupNewest(id);
      if (objDescIndex < 0) {
        logger.warn("NL: Aggregate found, but object not found in aggregate?!? -- aborted");
        command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
        return;
      }

      retrieveObjectFromAggregate(aggregate, objDescIndex, command);

    } else {
      logger.warn("NL: LOOKUP FAILED, OBJECT NOT FOUND: {}", id);
      command.receiveResult(null);
    }
  }

  void lookup(final Id id, final long version, final Continuation<? super PastContent, Exception> command) {
    logger.info("lookup({}, version={})", id, version);

    AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.getAggregateDescriptor(new VersionKey(id, version));
    if (aggregate!=null) {
      logger.debug("VL: Retrieving from aggregate");

      int objDescIndex = aggregate.lookupSpecific(id, version);
      if (objDescIndex < 0) {
        logger.warn("VL: Aggregate found, but object not found in aggregate?!? -- aborted");
        command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
        return;
      }

      retrieveObjectFromAggregate(aggregate, objDescIndex, command);
      return;
    }

    logger.debug("VL: Not found in aggregate list: {}v{}", id, version);
    if (!(aggregateStore instanceof VersioningPast versioningPast)) {
      logger.debug("VL: Aggregate store does not support versioning");
      lookupVersionInObjectStore(id, version, command, false);
      return;
    }

    versioningPast.lookup(id, version, new Continuation<PastContent, Exception>() {
      @Override
      public void receiveResult(PastContent result) {
        if (result != null) {
          logger.debug("VL: Found in Aggregate.VersioningPAST: {}v{}", id, version);
          command.receiveResult(result);
        } else {
          logger.debug("VL: Not found in Aggregate.VersioningPAST: {}v{}", id, version);
          lookupVersionInObjectStore(id, version, command, true);
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Aggregate.VersioningPAST returned exception for {}v{}: ", id, version, e);
        command.receiveException(new AggregationException("Aggregate.VersioningPAST returned exception for "+id+"v"+version+": "+e));
      }
    });
  }

  /**
   * Looks up a specific version in the object store.
   *
   * @param failIfUnsupported whether to report an exception (rather than a null result) if the object store
   *          does not support versioning
   */
  private void lookupVersionInObjectStore(final Id id, final long version, final Continuation<? super PastContent, Exception> command, boolean failIfUnsupported) {
    if (!(objectStore instanceof VersioningPast versioningPast)) {
      logger.debug("VL: Object store does not support versioning");
      if (failIfUnsupported)
        command.receiveException(new AggregationException("Cannot find "+id+"v"+version+" -- try rebuilding aggregate list?"));
      else
        command.receiveResult(null);
      return;
    }

    versioningPast.lookup(id, version, new Continuation<PastContent, Exception>() {
      @Override
      public void receiveResult(PastContent result) {
        if (result != null) {
          logger.debug("VL: Found in Object.VersioningPAST: {}v{}", id, version);
          command.receiveResult(result);
        } else {
          logger.warn("VL: LOOKUP FAILED, OBJECT NOT FOUND: {}v{}", id, version);
          command.receiveResult(null);
        }
      }
      @Override
      public void receiveException(Exception e) {
        command.receiveException(e);
      }
    });
  }

  void lookupHandles(final Id id, final int max, final Continuation<? super PastContentHandle[], Exception> command) {
    logger.info("lookupHandles({},{})", id, max);
    objectStore.lookupHandles(id, max, new Continuation<PastContentHandle[], Exception>() {
      @Override
      public void receiveResult(PastContentHandle[] results) {
        PastContentHandle[] result = (results != null) ? ((PastContentHandle[])results) : new PastContentHandle[] {};
        boolean foundHandle = false;

        for (int i=0; i<result.length; i++)
          if (result[i] != null)
            foundHandle = true;

        if (foundHandle) {
          logger.debug("lookupHandles({},{}) handled by PAST; ret={}", id, max, results);
          command.receiveResult(results);
        } else {
          logger.info("lookupHandles({},{}) failed, ret={}", id, max, results);
          lookupHandlesInAggregate(id, max, command);
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.atWarn().log(() -> "Exception in lookupHandles: "+e);
        command.receiveException(e);
      }
    });
  }

  /**
   * Retrieves the object from its aggregate, which reinserts it into the object store, and then looks up its
   * handles in the object store again.
   */
  private void lookupHandlesInAggregate(final Id id, final int max, final Continuation<? super PastContentHandle[], Exception> command) {
    AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.getAggregateDescriptor(id);
    if (aggregate==null) {
      logger.info("lookupHandles: {} is neither in object store nor in aggregate list", id);
      /* Note that we have to give up here... even if the object has not been
         aggregated, there is no efficient way to find out its version number.
         The user must call lookupVersion in this case. */
      command.receiveResult(new PastContentHandle[] { null });
      return;
    }

    logger.debug("lookupHandles: Retrieving from aggregate");

    int objDescIndex = aggregate.lookupNewest(id);
    if (objDescIndex < 0) {
      logger.warn("lookupHandles: Aggregate found, but object not found in aggregate?!? -- aborted");
      command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
      return;
    }

    if (aggregate.getObjects()[objDescIndex].getRefreshedLifetime() < timeSource.currentTimeMillis()) {
      logger.debug("Object {} exists, but has expired -- ignoring", id);
      command.receiveResult(new PastContentHandle[] { null });
      return;
    }

    retrieveObjectFromAggregate(aggregate, objDescIndex, new Continuation<ExpiringPastContent, Exception>() {
      @Override
      public void receiveResult(ExpiringPastContent result) {
        logger.debug("lookupHandles: Retrieved from aggregate: {}, result={}", id, result);
        /* re-inserted implicitly by retrieveObjectFromAggregate */
        objectStore.lookupHandles(id, max, command);
      }
      @Override
      public void receiveException(Exception e) {
        logger.atWarn().log(() -> "lookupHandles: Cannot retrieve from aggregate, exception "+e);
        command.receiveException(e);
      }
    });
  }

  /**
   * Fetches the aggregate, validates its content hash (on the processing thread), reinserts the object into
   * the object store and returns it.
   */
  private void retrieveObjectFromAggregate(final AggregateDescriptor aggregateDescriptor, final int objDescIndex, final Continuation<? super ExpiringPastContent, Exception> command) {
    aggregateStore.lookup(aggregateDescriptor.getKey(), new Continuation<PastContent, Exception>() {
      @Override
      public void receiveResult(PastContent result) {
        if (result instanceof Aggregate aggregate) {
          validateAndExtract(aggregateDescriptor, objDescIndex, aggregate, command);
        } else {
          logger.warn("retrieveObjectFromAggregate failed; receiveResult({})", result);
          command.receiveResult(null);
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("retrieveObjectFromAggregate failed; receiveException({})", e, e);
        command.receiveException(e);
      }
    });
  }

  private void validateAndExtract(final AggregateDescriptor aggregate, final int objDescIndex, final Aggregate fetchedAggregate, final Continuation<? super ExpiringPastContent, Exception> command) {
    Futures.forward(endpoint.process(() -> factory.buildId(fetchedAggregate.getContentHash())), new Continuation<Id, Exception>() {
      @Override
      public void receiveResult(Id result) {
        if (result != null) {
          Id aggrNominalKey = (Id) result;

          if (!aggrNominalKey.equals(aggregate.getKey())) {
            logger.warn("Cannot validate aggregate {}, hash={}", aggregate.getKey(), aggrNominalKey);
            command.receiveException(new AggregationException("Cannot validate aggregate -- retry?"));
            return;
          }

          logger.debug("Object {} (#{}) successfully retrieved from {}", aggregate.getObjects()[objDescIndex].getKey(), objDescIndex, aggregate.getKey());

          objectStore.insert(fetchedAggregate.getComponent(objDescIndex), new Continuation<Boolean[], Exception>() {
            @Override
            public void receiveResult(Boolean[] results) {}
            @Override
            public void receiveException(Exception e) {}
          });

          command.receiveResult(fetchedAggregate.getComponent(objDescIndex));
        } else {
          logger.warn("retrieveObjectFromAggregate cannot determine content hash, received {}", result);
          command.receiveException(new AggregationException("retrieveObjectFromAggregate cannot determine content hash"));
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("retrieveObjectFromAggregate cannot determine content hash, exception ", e);
        command.receiveException(e);
      }
    }, endpoint.getEnvironment());
  }
}
