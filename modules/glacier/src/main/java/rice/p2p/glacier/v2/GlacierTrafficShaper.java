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
package rice.p2p.glacier.v2;

import java.util.Enumeration;
import java.util.Hashtable;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;

/**
 * Limits the rate at which Glacier starts fragment retrievals and refreshes. Jobs wait in a queue keyed
 * by the object or fragment they are for; a periodic task releases a few of them per second while there
 * are bandwidth tokens left and not too many restores are active. Incoming data messages consume tokens.
 */
final class GlacierTrafficShaper {
  /** Estimated size of a data message without its fragments and manifests. */
  private static final long DATA_MESSAGE_OVERHEAD_BYTES = 1000;
  /** Estimated manifest bytes per fragment. */
  private static final int MANIFEST_BYTES_PER_FRAGMENT = 21;

  private final GlacierConfig config;
  private final int numFragments;
  private final Environment environment;
  private final Logger logger;
  private final Hashtable<Id, Continuation<Object, Exception>> pendingTraffic;
  private final int[] numActiveRestores;
  private long tokenBucket;
  private long bucketLastUpdated;
  private long bucketMin;
  private long bucketMax;
  private long bucketConsumed;
  private int rateLimitedRequestsPerSecond;
  private long bucketTokensPerSecond;
  private long bucketMaxBurstSize;

  GlacierTrafficShaper(GlacierConfig config, int numFragments, Environment environment, Logger logger) {
    this.config = config;
    this.numFragments = numFragments;
    this.environment = environment;
    this.logger = logger;
    this.pendingTraffic = new Hashtable<Id, Continuation<Object, Exception>>();
    this.numActiveRestores = new int[1];
    this.numActiveRestores[0] = 0;
    this.tokenBucket = 0;
    this.bucketLastUpdated = environment.getTimeSource().currentTimeMillis();
    this.rateLimitedRequestsPerSecond = config.rateLimitedRequestsPerSecond();
    this.bucketTokensPerSecond = config.bucketTokensPerSecond();
    this.bucketMaxBurstSize = config.bucketMaxBurstSize();
  }

  /**
   * Queues a job; the traffic shaper calls its receiveResult() when it may run.
   *
   * @return the job that was queued under this key before, if any
   */
  Continuation<Object, Exception> enqueue(Id key, Continuation<Object, Exception> job) {
    return pendingTraffic.put(key, job);
  }

  boolean isPending(Id key) {
    return pendingTraffic.containsKey(key);
  }

  int numPending() {
    return pendingTraffic.size();
  }

  void restoreStarted() {
    synchronized (numActiveRestores) {
      numActiveRestores[0] ++;
    }
  }

  void restoreFinished() {
    synchronized (numActiveRestores) {
      numActiveRestores[0] --;
    }
  }

  int getActiveRestores() {
    return numActiveRestores[0];
  }

  int getRateLimit() {
    return rateLimitedRequestsPerSecond;
  }

  void setRateLimit(int rps) {
    this.rateLimitedRequestsPerSecond = rps;
  }

  /**
   * Takes the size of an incoming data message out of the token bucket.
   */
  void charge(GlacierDataMessage gdm) {
    long thisSize = DATA_MESSAGE_OVERHEAD_BYTES;
    updateTokenBucket();
    for (int i=0; i<gdm.numKeys(); i++) {
      if (gdm.getFragment(i) != null)
        thisSize += gdm.getFragment(i).getPayload().length;
      if (gdm.getManifest(i) != null)
        thisSize += numFragments * MANIFEST_BYTES_PER_FRAGMENT;
    }

    tokenBucket -= thisSize;
    bucketConsumed += thisSize;
    if (bucketMin > tokenBucket)
      bucketMin = tokenBucket;

    logger.debug("Token bucket contains {} tokens (consumed {})", tokenBucket, thisSize);
  }

  void updateTokenBucket() {
    final long now = environment.getTimeSource().currentTimeMillis();
    final long contentsBefore = tokenBucket;

    while (bucketLastUpdated < now) {
      bucketLastUpdated += GlacierConfig.SECONDS/10;
      tokenBucket += bucketTokensPerSecond/10;
      if (tokenBucket > bucketMaxBurstSize)
        tokenBucket = bucketMaxBurstSize;
    }

    if (bucketMax < tokenBucket)
      bucketMax = tokenBucket;

    logger.atDebug().log(() -> "Token bucket contains "+tokenBucket+" tokens (added "+(tokenBucket-contentsBefore)+")");
  }

  /**
   * Reports the bucket and queue state, and starts a new reporting interval for the bucket minimum,
   * maximum and consumption.
   */
  void addStatistics(GlacierStatistics statistics) {
    statistics.setPendingRequests(pendingTraffic.size());
    statistics.setActiveFetches(numActiveRestores[0]);
    statistics.setBucketMin(bucketMin);
    statistics.setBucketMax(bucketMax);
    statistics.setBucketConsumed(bucketConsumed);
    bucketMin = tokenBucket;
    bucketMax = tokenBucket;
    bucketConsumed = 0;
  }

  /**
   * @return the periodic task that releases queued jobs
   */
  GlacierContinuation newShaperTask() {
    return new GlacierContinuation() {
      long nextTimeout;

      @Override
      public String toString() {
        return "Traffic shaper";
      }
      @Override
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.rateLimitedCheckInterval();
      }
      @Override
      public void receiveResult(Object o) {
        logger.warn("TS received object: {}", o);
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("TS received exception: ", e);
      }
      @Override
      public long getTimeout() {
        return nextTimeout;
      }
      @Override
      public void timeoutExpired() {
        /* Use relative timeout to avoid backlog! */
        nextTimeout = environment.getTimeSource().currentTimeMillis() + (1 * GlacierConfig.SECONDS);

        if (pendingTraffic.isEmpty()) {
          logger.debug("Traffic shaper: Idle");
          nextTimeout += config.rateLimitedCheckInterval();
          return;
        }

        int numCurrentRestores = 0;
        synchronized (numActiveRestores) {
          numCurrentRestores = numActiveRestores[0];
        }

        logger.info("Traffic shaper: {} jobs waiting ({} active jobs, {} tokens)", pendingTraffic.size(), numCurrentRestores, tokenBucket);

        updateTokenBucket();
        if ((numCurrentRestores < config.maxActiveRestores()) && (tokenBucket>0)) {
          for (int i=0; i<rateLimitedRequestsPerSecond; i++) {
            if (!pendingTraffic.isEmpty()) {
              Enumeration<Id> keys = pendingTraffic.keys();
              Object thisKey = (Object) keys.nextElement();
              logger.debug("Sending request {}", thisKey);
              Continuation<Object, Exception> c = pendingTraffic.remove(thisKey);
              c.receiveResult(Boolean.valueOf(true));
            }
          }
        }
      }
    };
  }
}
