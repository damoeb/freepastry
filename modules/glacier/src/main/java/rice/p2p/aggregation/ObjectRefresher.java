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
import java.util.Iterator;

import org.slf4j.Logger;
import rice.Continuation;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.VersioningPast;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastImpl;
import rice.p2p.past.gc.ExpiringPast;
import rice.p2p.past.gc.ExpiringPastContent;
import rice.persistence.CachedStorage;

/**
 * Extends the lifetimes of objects: in the object store, and in the aggregate list or the waiting list,
 * whichever holds the object. An object found in neither is reaggregated from the object store (up to
 * {@code maxReaggregationPerRefresh} per refresh).
 */
final class ObjectRefresher {
  /**
   * Returned by the steps of a {@link RefreshRun} that complete asynchronously (or abort the run).
   */
  private static final Object PENDING = new Object();

  private final Past objectStore;
  private final CachedStorage waitingList;
  private final AggregateList aggregateList;
  private final AggregationPolicy policy;
  private final AggregationSettings config;
  private final Logger logger;

  ObjectRefresher(Past objectStore, CachedStorage waitingList, AggregateList aggregateList, AggregationPolicy policy, AggregationSettings config, Logger logger) {
    this.objectStore = objectStore;
    this.waitingList = waitingList;
    this.aggregateList = aggregateList;
    this.policy = policy;
    this.config = config;
    this.logger = logger;
  }

  void refresh(final Id[] ids, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    if (ids.length < 1) {
      command.receiveResult(new Boolean[] {});
      return;
    }

    logger.info("Refreshing {} keys", ids.length);

    refreshInObjectStore(ids, expirations, new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object o) {
        Object[] result;
        if (o instanceof Object[] array) {
          result = array;
        } else {
          logger.warn("refresh: ObjectStore result is of incorrect type; expected Object[], got {}", o);
          result = new Object[ids.length];
          for (int i=0; i<ids.length; i++)
            result[i] = o;
        }

        refreshInAggregates(ids, expirations, result, command);
      }
      @Override
      public void receiveException(Exception e) {
        Object[] result = new Object[ids.length];
        for (int i=0; i<ids.length; i++)
          result[i] = e;
        logger.warn("", e);
        refreshInAggregates(ids, expirations, result, command);
      }
    });
  }

  void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    final Object result[] = new Object[ids.length];

    for (int i=0; i<ids.length; i++) {
      logger.info("Refresh({}v{}, expiration={})", ids[i], versions[i], expirations[i]);

      AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getAggregateDescriptor(new VersionKey(ids[i], versions[i]));
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

    if (objectStore instanceof VersioningPast versioningPast) {
      versioningPast.refresh(ids, versions, expirations, new Continuation<Object[], Exception>() {
        @Override
        public void receiveResult(Object[] o) {
          if (o != null) {
            for (int i=0; i<result.length; i++) {
              if (!(result[i] instanceof Boolean))
                continue;

              if (i >= o.length)
                result[i] = new AggregationException("Object store returned "+o.length+" results for "+result.length+" ids");
              else if (!(o[i] instanceof Boolean))
                result[i] = o[i];
            }
          } else {
            Exception e = new AggregationException("Object store returns unexpected result: "+Arrays.toString(o));
            for (int i=0; i<result.length; i++)
              result[i] = e;
          }

          command.receiveResult(result);
        }
        @Override
        public void receiveException(Exception e) {
          command.receiveException(e);
        }
      });
    } else {
      command.receiveResult(result);
    }
  }

  private void refreshInObjectStore(Id[] ids, long[] expirations, Continuation<Object, Exception> command) {
    if (objectStore instanceof ExpiringPast gcPast) {
      gcPast.refresh(ids, expirations, command);
    } else {
      // same contract as ExpiringPast.refresh: one result per id
      Object[] result = new Object[ids.length];
      Arrays.fill(result, true);
      command.receiveResult(result);
    }
  }

  private void refreshInAggregates(Id[] ids, long[] expirations, Object[] result, final Continuation<? super Object[], Exception> command) {
    Continuation<Object[], Exception> c = new Continuation<Object[], Exception>() {
      @Override
      public void receiveResult(Object[] o) {
        aggregateList.writeToDisk();
        command.receiveResult(o);
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("", e);
        command.receiveException(e);
      }
    };

    logger.info("refreshInternal: Accepted {} keys, starting with first key...", ids.length);
    new RefreshRun(ids, expirations, result, c).receiveResult(null);
  }

  /**
   * Refreshes the objects one after the other in the aggregate list or the waiting list. Each step either
   * returns its result right away, or returns {@link #PENDING} and later reports to this continuation.
   */
  private final class RefreshRun implements Continuation<Object, Exception> {
    private final Id[] ids;
    private final long[] expirations;
    private final Object[] result;
    private final Continuation<? super Object[], Exception> command;
    private int objectsMissing = 0;
    private int objectsFetched = 0;
    private int currentIndex = -1;

    RefreshRun(Id[] ids, long[] expirations, Object[] result, Continuation<? super Object[], Exception> command) {
      this.ids = ids;
      this.expirations = expirations;
      this.result = result;
      this.command = command;
    }

    @Override
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
          complete();
          return;
        }

        lastResult = refreshObject(ids[currentIndex], expirations[currentIndex]);
        @SuppressWarnings("ReferenceEquality") // PENDING is a sentinel instance
        boolean pending = lastResult == PENDING;
        if (pending)
          return;
      }
    }

    @Override
    public void receiveException(Exception e) {
      logger.atWarn().setCause(e).log(() -> "Exception while refreshing "+ids[currentIndex].toStringFull()+", e=");
      receiveResult(e);
    }

    private void complete() {
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
    }

    private Object refreshObject(final Id id, final long expiration) {
      logger.atInfo().log(() -> "Refresh("+id.toStringFull()+", expiration="+expiration+") started");

      /* In the common case, the aggregate is in the list, and we can simply update its lifetime there */

      AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getAggregateDescriptor(id);
      if (adc!=null)
        return refreshInAggregateList(adc, id, expiration);

      /* If the aggregate is not in the list, check whether it is still in the waiting list */

      Iterator<Id> iter = waitingList.scan().getIterator();
      while (iter.hasNext()) {
        final VersionKey vkey = (VersionKey) iter.next();
        if (vkey.id().equals(id))
          return refreshInWaitingList(vkey, expiration);
      }

      /* If the object is neither in the aggregate list nor in the waiting list, there must have
         been a failure, and we need to reaggregate this object. */

      objectsMissing ++;
      if (!config.isAddMissingAfterRefresh()) {
        logger.atWarn().log(() -> "Refresh: Refreshed object not found in any aggregate: "+id.toStringFull());
        return Boolean.valueOf(true);
      }

      if (objectsFetched >= config.getMaxReaggregationPerRefresh()) {
        logger.atDebug().log(() -> "Refresh: Limit of "+config.getMaxReaggregationPerRefresh()+" reaggregations exceeded; postponing id="+id.toStringFull());
        return Boolean.valueOf(true);
      }

      objectsFetched ++;
      reaggregate(id, expiration);
      return PENDING;
    }

    private Object refreshInAggregateList(AggregateDescriptor adc, Id id, long expiration) {
      int objDescIndex = adc.lookupNewest(id);
      if (objDescIndex < 0) {
        logger.warn("NL: Aggregate found, but object not found in aggregate?!? -- aborted");
        command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
        return PENDING;
      }

      if (adc.getObjects()[objDescIndex].getRefreshedLifetime() < expiration) {
        logger.debug("Changing expiration date from {} to {}", adc.getObjects()[objDescIndex].getRefreshedLifetime(), expiration);
        aggregateList.setObjectRefreshedLifetime(adc, objDescIndex, expiration);
      } else {
        logger.debug("Expiration is {} already, no update needed", adc.getObjects()[objDescIndex].getRefreshedLifetime());
      }

      return Boolean.valueOf(true);
    }

    private Object refreshInWaitingList(final VersionKey vkey, long expiration) {
      ObjectDescriptor thisObject = (ObjectDescriptor) waitingList.getMetadata(vkey);
      logger.atInfo().log(() -> "Refreshing in waiting list: " + vkey.toStringFull());

      if (thisObject == null) {
        logger.atWarn().log(() -> "Broken object in waiting list: " + vkey.toStringFull() + ", removing...");
        waitingList.unstore(vkey, new Continuation<Boolean, Exception>() {
          @Override
          public void receiveResult(Boolean o) {
            logger.atInfo().log(() -> "Broken object "+vkey.toStringFull()+" removed successfully");
            RefreshRun.this.receiveResult(new AggregationException("Object in waiting list, but broken: "+vkey.toStringFull()));
          }
          @Override
          public void receiveException(Exception e) {
            logger.atWarn().setCause(e).log(() -> "Cannot remove broken object "+vkey.toStringFull()+" from waiting list (exception: "+e+")");
            RefreshRun.this.receiveResult(new AggregationException("Object broken, in waiting list, and cannot remove: "+vkey.toStringFull()+" (e="+e+")"));
          }
        });
        return PENDING;
      }

      if (thisObject.getRefreshedLifetime() >= expiration) {
        logger.atDebug().log(() -> "Object found in waiting list and no update needed: "+vkey.toStringFull());
        return Boolean.valueOf(true);
      }

      ObjectDescriptor newDescriptor = new ObjectDescriptor(
        thisObject.getKey(), thisObject.getVersion(), thisObject.getCurrentLifetime(),
        expiration, thisObject.getSize()
      );

      waitingList.setMetadata(vkey, newDescriptor, new Continuation<Boolean, Exception>() {
        @Override
        public void receiveResult(Boolean o) {
          logger.atDebug().log(() -> "Refreshed metadata written ok for "+vkey.toStringFull());
          RefreshRun.this.receiveResult(Boolean.valueOf(true));
        }
        @Override
        public void receiveException(Exception e) {
          logger.atWarn().setCause(e).log(() -> "Cannot refresh waiting object "+vkey.toStringFull()+", e=");
          RefreshRun.this.receiveResult(new AggregationException("Cannot refresh waiting object "+vkey.toStringFull()+", setMetadata() failed (e="+e+")"));
        }
      });
      return PENDING;
    }

    /**
     * Looks the object up in the object store and adds it to the waiting list again.
     */
    private void reaggregate(final Id id, final long expiration) {
      objectStore.lookup(id, false, new Continuation<PastContent, Exception>() {
        @Override
        public void receiveResult(PastContent o) {
          if (o != null) {
            addMissingObject(id, expiration, o);
          } else {
            logger.atWarn().log(() -> "Refresh: Cannot find refreshed object "+id.toStringFull()+", lookup returns "+o);
            RefreshRun.this.receiveException(new AggregationException("Object not found during reaggregation: "+id.toStringFull()));
          }
        }
        @Override
        public void receiveException(Exception e) {
          logger.atWarn().log(() -> "Refresh: Exception received while reaggregating "+id.toStringFull()+", e="+e);
          RefreshRun.this.receiveException(e);
        }
      });
    }

    private void addMissingObject(final Id id, long expiration, final PastContent obj) {
      logger.atWarn().log(() -> "Refresh: Found in PAST, but not in aggregate list: "+id.toStringFull());

      long theVersion;
      if (obj instanceof ExpiringPastContent gcPastContent) {
        theVersion = gcPastContent.getVersion();
      } else {
        theVersion = 0;
      }

      final VersionKey vkey = new VersionKey(obj.getId(), theVersion);
      final int theSize = AggregateStorer.getSize(obj, logger);

      if (!policy.shouldBeAggregated(obj, theSize)) {
        logger.atDebug().log(() -> "Refresh: Missing object should not be aggregated: "+id.toStringFull());
        receiveResult(Boolean.valueOf(true));
        return;
      }

      if (waitingList.exists(vkey)) {
        logger.atDebug().log(() -> "Refresh: Missing object already in waiting list: "+id.toStringFull());
        receiveResult(Boolean.valueOf(true));
        return;
      }

      logger.debug("ADDING MISSING OBJECT AFTER REFRESH: {}", obj.getId());

      waitingList.store(vkey, new ObjectDescriptor(obj.getId(), theVersion, expiration, expiration, theSize), obj, new Continuation<Boolean, Exception>() {
        @Override
        public void receiveResult(Boolean o) {
          if (!(objectStore instanceof PastImpl pastImpl)) {
            /* Precaching only speeds up later lookups; other Past implementations simply skip it */
            RefreshRun.this.receiveResult(Boolean.valueOf(true));
            return;
          }
          pastImpl.cache(obj, new Continuation<Boolean, Exception>() {
            @Override
            public void receiveResult(Boolean o) {
              logger.atDebug().log(() -> "Refresh: Missing object "+id.toStringFull()+" added ok");
              RefreshRun.this.receiveResult(Boolean.valueOf(true));
            }
            @Override
            public void receiveException(Exception e) {
              logger.atWarn().setCause(e).log(() -> "Refresh: Exception while precaching object: "+id.toStringFull()+" (e="+e+")");
              RefreshRun.this.receiveResult(Boolean.valueOf(true));
            }
          });
        }
        @Override
        public void receiveException(Exception e) {
          logger.atWarn().setCause(e).log(() -> "Refresh: Exception while refreshing aggregate: "+id.toStringFull()+" (e="+e+")");
          RefreshRun.this.receiveResult(new AggregationException("Cannot store reaggregated object in waiting list: "+id.toStringFull()));
        }
      });
    }
  }
}
