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

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.glacier.v2.messaging.GlacierTimeoutMessage;

/**
 * Keeps Glacier's pending {@link GlacierContinuation}s by UID, and drives their timeouts with a single
 * self-scheduled {@link GlacierTimeoutMessage}. Runs on the selector thread.
 */
final class GlacierContinuationRegistry {
  /** Terminated continuations removed per pass; a full pass triggers another one. */
  private static final int MAX_DELETES_PER_PASS = 100;

  private final Endpoint endpoint;
  private final Environment environment;
  private final Logger logger;
  private final ConcurrentHashMap<Integer, GlacierContinuation> continuations;
  private long nextContinuationTimeout;
  private int nextUID;
  private CancellableTask timer;

  GlacierContinuationRegistry(Endpoint endpoint, Environment environment, Logger logger) {
    this.endpoint = endpoint;
    this.environment = environment;
    this.logger = logger;
    this.continuations = new ConcurrentHashMap<>();
    this.nextContinuationTimeout = -1;
    this.nextUID = 0;
    this.timer = null;
  }

  /**
   * @return the continuation waiting for responses with this UID, or null
   */
  GlacierContinuation get(int uid) {
    return continuations.get(Integer.valueOf(uid));
  }

  int size() {
    return continuations.size();
  }

  void addContinuation(GlacierContinuation gc) {
    int thisUID = getUID();
    gc.setup(thisUID);
    continuations.put(Integer.valueOf(thisUID), gc);
    gc.init();

    long thisTimeout = gc.getTimeout();
    long now = environment.getTimeSource().currentTimeMillis();

    if ((nextContinuationTimeout == -1) || (thisTimeout < nextContinuationTimeout)) {
      if (nextContinuationTimeout != -1)
        cancelTimer();

      nextContinuationTimeout = thisTimeout;
      if (nextContinuationTimeout > now)
        setTimer((int)(nextContinuationTimeout - now));
      else
        timerExpired();
    }
  }

  int getUID() {
    return nextUID++;
  }

  /**
   * Schedule a timer event
   *
   * @param timeoutMsec Length of the delay (in milliseconds)
   */
  private void setTimer(int timeoutMsec) {
    cancelTimer();
    timer = endpoint.scheduleMessage(new GlacierTimeoutMessage(0, endpoint.getLocalNodeHandle()), timeoutMsec);
  }

  /**
   * Cancel a timer event that has not yet occurred
   */
  private void cancelTimer() {
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
  }

  void timerExpired() {
    logger.debug("Timer expired");
    // Cancelling is a no-op for a timer that fired; it stops a second chain when timerExpired() is called directly.
    cancelTimer();
    nextContinuationTimeout = -1;

    long earliestTimeout;
    int numDelete;

    do {
      long now = environment.getTimeSource().currentTimeMillis();
      int[] deleteList = new int[MAX_DELETES_PER_PASS];
      numDelete = 0;
      earliestTimeout = -1;

      logger.debug("Timer run at {}", now);

      for (GlacierContinuation gc : continuations.values()) {
        long currentTimeout = gc.getTimeout();

        if (!gc.hasTerminated() && currentTimeout < (now + 1*GlacierConfig.SECONDS)) {
          logger.debug("Timer: Resuming [{}]", gc);
          gc.syncTimeoutExpired();
          if (!gc.hasTerminated() && (gc.getTimeout() <= currentTimeout))
            panic("Continuation does not set new timeout: "+gc);
        }

        if (!gc.hasTerminated()) {
          if ((earliestTimeout == -1) || (gc.getTimeout() < earliestTimeout))
            earliestTimeout = gc.getTimeout();
        } else {
          if (numDelete < MAX_DELETES_PER_PASS)
            deleteList[numDelete++] = gc.getMyUID();
        }
      }

      if (numDelete > 0) {
        logger.debug("Deleting {} expired continuations", numDelete);
        for (int i=0; i<numDelete; i++)
          continuations.remove(Integer.valueOf(deleteList[i]));
      }

    } while ((numDelete == MAX_DELETES_PER_PASS) || ((earliestTimeout >= 0) && (earliestTimeout < environment.getTimeSource().currentTimeMillis())));

    if (earliestTimeout >= 0) {
      logger.debug("Next timeout is at {}", earliestTimeout);
      nextContinuationTimeout = earliestTimeout;
      setTimer((int)Math.max(earliestTimeout - environment.getTimeSource().currentTimeMillis(), 1*GlacierConfig.SECONDS));
    } else logger.debug("No more timeouts");
  }

  /**
   * This method is called when Glacier encounters a fatal error
   *
   * @param s Message describing the error, which is logged
   * @throws Error always
   */
  private void panic(String s) throws Error {
    logger.error("PANIC: {}", s);
    throw new Error("Panic");
  }
}
