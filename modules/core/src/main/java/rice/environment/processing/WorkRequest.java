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
package rice.environment.processing;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;
import rice.p2p.commonapi.Cancellable;
import rice.selector.SelectorLoop;

/**
 * A piece of blocking (disk) IO that a {@link Processor} implementation queues for its blocking IO thread: it
 * runs the work there and completes its {@link #future()} on the selector thread. Callers use
 * {@link Processor#processBlockingIO(Callable, SelectorLoop)}.
 *
 * @author Jeff Hoye
 */
public class WorkRequest<R> implements Runnable, Cancellable {
  private final @Nullable Callable<R> work;
  private final @Nullable SelectorLoop selectorManager;
  private final CompletableFuture<R> future = new CompletableFuture<R>();

  protected volatile boolean cancelled = false;
  private volatile boolean running = false;

  /**
   * @param work the work; its toString() describes the request
   * @param selectorLoop the selector whose thread completes the future
   */
  public WorkRequest(Callable<R> work, SelectorLoop selectorLoop) {
    this.work = Objects.requireNonNull(work, "work");
    this.selectorManager = Objects.requireNonNull(selectorLoop, "selectorLoop");
  }

  /**
   * Only for requests that never run, such as WorkQueue's STOP sentinel.
   */
  public WorkRequest() {
    this.work = null;
    this.selectorManager = null;
  }

  /**
   * @return completed with the result of the work, or the exception it threw
   */
  public CompletableFuture<R> future() {
    return future;
  }

  public void returnResult(R r) {
    future.complete(r);
  }

  public void returnError(Exception e) {
    future.completeExceptionally(e);
  }

  @Override
  public void run() {
    if (cancelled) return;
    running = true;
    Callable<R> task = Objects.requireNonNull(work, "a sentinel request can't run");
    SelectorLoop selector = Objects.requireNonNull(selectorManager, "a sentinel request can't run");

    try {
      final R result = task.call();
      selector.invoke(new Runnable() {
        @Override
        public void run() {
          returnResult(result);
        }

        @Override
        public String toString() {
          return "invc result of " + WorkRequest.this;
        }
      });
    } catch (final Exception e) {
      selector.invoke(new Runnable() {
        @Override
        public void run() {
          returnError(e);
        }

        @Override
        public String toString() {
          return "invc error of " + WorkRequest.this;
        }
      });
    }
  }

  @Override
  public boolean cancel() {
    cancelled = true;
    return !running;
  }

  @Override
  public String toString() {
    return String.valueOf(work);
  }
}
