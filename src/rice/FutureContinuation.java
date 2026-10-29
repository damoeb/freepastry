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
package rice;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Bridges {@link Continuation} callbacks and {@link CompletableFuture}s, so that new code can use futures
 * with the existing callback based API.
 * <p>
 * As a Continuation, this completes its {@link #future()} with the first result or exception it receives;
 * later calls are ignored, just like a second {@code complete()} on a future.
 * <pre>
 *   // callback API -&gt; future
 *   CompletableFuture&lt;PastContent&gt; f = FutureContinuation.call(c -&gt; past.lookup(id, c));
 *
 *   // future -&gt; callback API, delivered on the selector thread
 *   FutureContinuation.forward(f, continuation, environment.getSelectorManager()::invoke);
 * </pre>
 * Note: the future's dependent stages run on the thread that completes it, which for most FreePastry
 * callbacks is the selector thread. Don't block in them; use the {@code *Async} variants to move work
 * elsewhere. Conversely, never block on the future (e.g. {@code get()}/{@code join()}) <em>on</em> the selector
 * thread while the result is still pending: it can only be delivered by the selector thread, so that
 * deadlocks.
 *
 * @param <R> the result type
 * @param <E> the exception type of the Continuation
 */
public class FutureContinuation<R, E extends Exception> implements Continuation<R, E> {

  private final CompletableFuture<R> future;

  /**
   * Creates a Continuation that completes a new future.
   */
  public FutureContinuation() {
    this(new CompletableFuture<R>());
  }

  /**
   * Creates a Continuation that completes the given future.
   */
  public FutureContinuation(CompletableFuture<R> future) {
    this.future = Objects.requireNonNull(future, "future");
  }

  /**
   * @return the future completed by this Continuation
   */
  public CompletableFuture<R> future() {
    return future;
  }

  public void receiveResult(R result) {
    future.complete(result);
  }

  public void receiveException(E exception) {
    future.completeExceptionally(exception == null ? new NullPointerException("receiveException(null)") : exception);
  }

  /**
   * Starts an asynchronous operation that reports to a Continuation, and returns a future for its outcome.
   * A RuntimeException thrown by {@code operation} itself also completes the future (exceptionally).
   *
   * @param operation starts the operation, passing the given Continuation to the callback API
   * @return a future completed by that Continuation
   */
  public static <R> CompletableFuture<R> call(Consumer<? super Continuation<R, Exception>> operation) {
    FutureContinuation<R, Exception> c = new FutureContinuation<R, Exception>();
    try {
      operation.accept(c);
    } catch (RuntimeException re) {
      c.future.completeExceptionally(re);
    }
    return c.future;
  }

  /**
   * Delivers the outcome of {@code stage} to {@code continuation}, on the thread that completes the stage
   * (or the calling thread, if it already has).
   *
   * @return a stage that completes after the continuation was called, exceptionally if the continuation threw
   */
  public static <R> CompletionStage<Void> forward(CompletionStage<? extends R> stage,
      final Continuation<? super R, Exception> continuation) {
    Objects.requireNonNull(continuation, "continuation");
    return stage.handle((r, t) -> {
      deliver(r, t, continuation);
      return null;
    });
  }

  /**
   * Delivers the outcome of {@code stage} to {@code continuation} via {@code executor}; pass
   * {@code selectorManager::invoke} to call it on the selector thread, as most FreePastry code expects.
   *
   * @return a stage that completes after the continuation was called, exceptionally if the continuation threw
   */
  public static <R> CompletionStage<Void> forward(CompletionStage<? extends R> stage,
      final Continuation<? super R, Exception> continuation, Executor executor) {
    Objects.requireNonNull(continuation, "continuation");
    Objects.requireNonNull(executor, "executor");
    return stage.handleAsync((r, t) -> {
      deliver(r, t, continuation);
      return null;
    }, executor);
  }

  private static <R> void deliver(R result, Throwable t, Continuation<? super R, Exception> continuation) {
    if (t == null) {
      continuation.receiveResult(result);
    } else {
      continuation.receiveException(toException(t));
    }
  }

  /**
   * Converts the failure of a future into the Exception a Continuation expects: unwraps the
   * {@link CompletionException}/{@link ExecutionException} wrappers that dependent stages and {@code get()} add,
   * and wraps a non-Exception Throwable (an Error) in an ExecutionException.
   */
  public static Exception toException(Throwable t) {
    while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
      t = t.getCause();
    }
    if (t instanceof Exception) return (Exception)t;
    return new ExecutionException(t);
  }
}
