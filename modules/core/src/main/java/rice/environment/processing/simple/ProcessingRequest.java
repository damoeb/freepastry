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
/*
 * Created on Aug 9, 2005
 */
package rice.environment.processing.simple;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.Executable;
import rice.environment.logging.LogManager;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.selector.SelectorLoop;

/**
 * Scheduling with a lower priority number will be executed first (is higher Priority)
 *
 * @author Jeff Hoye
 */
public class ProcessingRequest<R> implements Runnable,
    Comparable<ProcessingRequest<?>>, Cancellable {
  private final CompletableFuture<R> future = new CompletableFuture<R>();
  Executable<R, ? extends Exception> r;
  // set by the selector or a caller, read by the processing thread
  private volatile boolean cancelled = false;
  private volatile boolean running = false;

  TimeSource timeSource;
  SelectorLoop selectorManager;
  Logger logger;
  int priority = 0;
  long seq;

  public ProcessingRequest(Executable<R, ? extends Exception> executable, int priority, long seq,
      LogManager logging, TimeSource timeSource, SelectorLoop selectorManager) {
    this.r = executable;

    logger = logging.getLogger(getClass(), null);
    this.timeSource = timeSource;
    this.selectorManager = selectorManager;
    this.priority = priority;
    this.seq = seq;
  }

  /**
   * @return completed on the selector thread with the result of the task, or the exception it threw
   */
  public CompletableFuture<R> future() {
    return future;
  }

  public void returnResult(R r) {
    future.complete(r);
  }

  public void returnError(Exception e) {
    // unchecked exceptions thrown by the task are delivered here as well
    future.completeExceptionally(e);
  }

  public int getPriority() {
    return priority;
  }

  @Override
  public int compareTo(ProcessingRequest<?> request) {
    if (priority == request.getPriority()) {
      if (seq > request.seq) return 1;
      return -1;
    }
    if (priority > request.getPriority()) return 1;
    return -1;
  }

  @Override
  public void run() {
    if (cancelled) return;
    running = true;
    logger.trace("COUNT: Starting execution of {}", this);
    try {
      long start = timeSource.currentTimeMillis();
      final R result = r.execute();
      logger.atTrace().log(() -> "QT: " + (timeSource.currentTimeMillis() - start) + " "
          + r.toString());

      selectorManager.invoke(new Runnable() {
        @Override
        public void run() {
          returnResult(result);
        }

        @Override
        public String toString() {
          return "return ProcessingRequest for " + r;
        }
      });
    } catch (final Exception e) {
      selectorManager.invoke(new Runnable() {
        @Override
        public void run() {
          returnError(e);
        }

        @Override
        public String toString() {
          return "return ProcessingRequest for " + r;
        }
      });
    }
    logger.trace("COUNT: Done execution of {}", this);
  }

  @Override
  public boolean cancel() {
    cancelled = true;
    return !running;
  }
}
