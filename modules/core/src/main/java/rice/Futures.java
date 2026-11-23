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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.selector.SelectorLoop;
import rice.selector.Timer;
import rice.selector.TimerTask;

/**
 * Helpers for the {@link CompletableFuture}s of FreePastry's data path, and the rules that keep them on the
 * selector thread.
 * <p>
 * FreePastry runs its protocol code on one thread, the selector thread. A future keeps that contract when:
 * <ol>
 * <li>it is completed only on the selector thread (sources that hop threads complete it inside
 * {@link SelectorLoop#invoke(Runnable)}), so that its dependent stages run there too;</li>
 * <li>dependent stages use the methods without {@code Async}; an {@code *Async} method is only called with
 * {@link SelectorLoop#executor()}, never with the default executor ({@code ForkJoinPool.commonPool()});</li>
 * <li>timeouts run on the selector's timer ({@link #timeout}), never on {@code orTimeout},
 * {@code completeOnTimeout} or {@code delayedExecutor}, which use a real-time JDK thread and break the
 * simulator;</li>
 * <li>nobody blocks on a pending future on the selector thread: only the selector can complete it, so that
 * deadlocks. Code off the selector waits with {@link #await}, the only place that calls {@code get()};</li>
 * <li>a terminal stage ends with {@link #reportUnhandled}, so that an exception thrown in a callback still reaches
 * the {@code ExceptionStrategy}, as it does when a {@link Continuation} throws on the selector thread;</li>
 * <li>a failure is unwrapped with {@link #unwrap} before it is tested with {@code instanceof}: dependent stages
 * wrap it in a {@link CompletionException}.</li>
 * </ol>
 * The architecture test of the tutorials module enforces rules 2 to 4 for all main code.
 */
public final class Futures {

  private static final Logger LOG = LoggerFactory.getLogger(Futures.class);

  private Futures() {
  }

  /**
   * Starts an operation of a {@link Continuation} based API and returns a future for its outcome; see
   * {@link FutureContinuation#call}.
   *
   * @param operation starts the operation, passing the given Continuation to the callback API
   * @return a future completed by that Continuation, on the thread that calls it
   */
  public static <R extends @Nullable Object> CompletableFuture<R> call(
      Consumer<? super Continuation<R, Exception>> operation) {
    return FutureContinuation.call(operation);
  }

  /**
   * Delivers the outcome of {@code stage} to a {@link Continuation}, on the thread that completes the stage (or
   * the calling thread, if it already has): the boundary between future based code and code that still takes a
   * Continuation. A failure is unwrapped first. An exception thrown by the continuation is reported with
   * {@link #reportUnhandled}, as if the continuation had thrown on the selector thread.
   *
   * @param stage the stage
   * @param continuation receives its value or failure
   * @param environment whose {@code ExceptionStrategy} gets what the continuation throws
   */
  public static <R extends @Nullable Object> void forward(CompletionStage<? extends R> stage,
      Continuation<? super R, Exception> continuation, Environment environment) {
    reportUnhandled(FutureContinuation.forward(stage, continuation), environment);
  }

  /**
   * Removes the {@link CompletionException} and {@link ExecutionException} wrappers that dependent stages,
   * {@code join()} and {@code get()} put around the failure of a future.
   *
   * @param throwable a failure
   * @return the original failure
   */
  public static Throwable unwrap(Throwable throwable) {
    Throwable cause = throwable;
    while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  /**
   * Waits at most {@code timeoutMillis} for the value of {@code future}. This is the only place in FreePastry's
   * main code that blocks on a future.
   *
   * @param future the future
   * @param selector the selector that completes the future
   * @param timeoutMillis how long to wait
   * @return the value
   * @throws IllegalStateException if called on the selector thread while the future is not done: the selector
   *     can't complete it while it waits, so that would deadlock
   * @throws ExecutionException if the future failed, with the failure as cause; a cancellation (such as an
   *     {@code EnvironmentDestroyedException}) is thrown as it is
   * @throws TimeoutException if the future is not done in time
   * @throws InterruptedException if interrupted while waiting
   */
  public static <T extends @Nullable Object> T await(CompletableFuture<T> future, SelectorLoop selector,
      long timeoutMillis) throws ExecutionException, TimeoutException, InterruptedException {
    checkNotBlockingSelector(future, selector);
    try {
      return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (CancellationException cancellation) {
      throw originalCancellation(cancellation);
    }
  }

  /**
   * Waits for the value of {@code future}, without a timeout; prefer
   * {@link #await(CompletableFuture, SelectorLoop, long)}.
   *
   * @param future the future
   * @param selector the selector that completes the future
   * @return the value
   * @throws IllegalStateException if called on the selector thread while the future is not done
   * @throws ExecutionException if the future failed, with the failure as cause
   * @throws InterruptedException if interrupted while waiting
   */
  public static <T extends @Nullable Object> T await(CompletableFuture<T> future, SelectorLoop selector)
      throws ExecutionException, InterruptedException {
    checkNotBlockingSelector(future, selector);
    try {
      return future.get();
    } catch (CancellationException cancellation) {
      throw originalCancellation(cancellation);
    }
  }

  /**
   * {@code get()} reports a cancellation with a new CancellationException whose cause is the one the future
   * failed with, such as an {@code EnvironmentDestroyedException}; return that.
   */
  private static CancellationException originalCancellation(CancellationException cancellation) {
    if (cancellation.getCause() instanceof CancellationException original) return original;
    return cancellation;
  }

  private static void checkNotBlockingSelector(CompletableFuture<?> future, SelectorLoop selector) {
    if (!future.isDone() && selector.isSelectorThread()) {
      throw new IllegalStateException("can't wait for a pending future on the selector thread, only it can complete it");
    }
  }

  /**
   * Fails {@code future} with the exception from {@code timeoutException} if it is not done after
   * {@code timeoutMillis}, measured by the selector's timer (virtual time in the simulator). The timer task is
   * cancelled once the future completes.
   *
   * @param future the future
   * @param timeoutMillis the timeout
   * @param timer the selector's timer, see {@link SelectorLoop#getTimer()}
   * @param timeoutException creates the failure, on the selector thread
   * @return {@code future}
   */
  public static <T extends @Nullable Object> CompletableFuture<T> timeout(final CompletableFuture<T> future,
      long timeoutMillis, Timer timer, final Supplier<? extends Throwable> timeoutException) {
    if (future.isDone()) return future;
    final TimerTask timeoutTask = new TimerTask() {
      @Override
      public void run() {
        future.completeExceptionally(timeoutException.get());
      }

      @Override
      public String toString() {
        return "timeout of " + future;
      }
    };
    timer.schedule(timeoutTask, timeoutMillis);
    var _ = future.whenComplete((_, _) -> timeoutTask.cancel());
    return future;
  }

  /**
   * Cancels {@code request} when {@code future} is cancelled (including when it fails with an
   * {@code EnvironmentDestroyedException}, a CancellationException): connects a future to the
   * {@link Cancellable} of the operation behind it. Register it on the future that is handed out, not on an
   * intermediate stage: cancelling a dependent stage does not cancel its source.
   *
   * @param future the future handed out
   * @param request the operation that completes it
   * @return {@code future}
   */
  public static <T extends @Nullable Object> CompletableFuture<T> onCancel(CompletableFuture<T> future,
      final Cancellable request) {
    Objects.requireNonNull(request, "request");
    var _ = future.whenComplete((_, throwable) -> {
      if (throwable instanceof CancellationException) {
        request.cancel();
      }
    });
    return future;
  }

  /**
   * Ends a chain of stages in protocol code: a failure that reaches it was not handled, so it goes to the
   * environment's {@code ExceptionStrategy} with the selector as source, as an exception thrown by a
   * {@link Continuation} on the selector thread does. A cancellation (also of a destroyed environment) is
   * expected and only logged.
   *
   * @param stage the terminal stage
   * @param environment whose {@code ExceptionStrategy} gets the failure
   */
  public static void reportUnhandled(CompletionStage<?> stage, final Environment environment) {
    Objects.requireNonNull(environment, "environment");
    var _ = stage.whenComplete((_, throwable) -> {
      if (throwable == null) return;
      Throwable cause = unwrap(throwable);
      if (cause instanceof CancellationException) {
        LOG.debug("cancelled: {}", cause.toString());
        return;
      }
      environment.getExceptionStrategy().handleException(environment.getSelectorManager(), cause);
    });
  }

  /**
   * Decides, from the outcomes so far, whether a fan-out is done (see {@link #quorum}).
   *
   * @param <T> the value type of the operations
   */
  @FunctionalInterface
  public interface QuorumCheck<T extends @Nullable Object> {
    /**
     * @param outcomes one entry per operation, in the order of the list given to {@link #quorum}; null where
     *     the operation has not completed yet
     * @return whether the fan-out is done
     * @throws Exception to fail the fan-out with this exception; it is done then as well
     */
    boolean isDone(List<@Nullable Outcome<T>> outcomes) throws Exception;
  }

  /**
   * Waits for all {@code operations} and collects their outcomes, the failures included: unlike
   * {@code CompletableFuture.allOf}, a failed operation does not fail the result. This is the default behaviour
   * of {@code Continuation.MultiContinuation}, except that an empty list completes at once (a
   * MultiContinuation for zero results never completes).
   *
   * @param operations the futures of the operations
   * @return the outcomes, in the order of {@code operations}; completed on the thread that completes the last
   *     operation
   */
  public static <T extends @Nullable Object> CompletableFuture<List<Outcome<T>>> collect(
      List<? extends CompletionStage<? extends T>> operations) {
    return quorum(operations, Futures::allDone, Futures::withoutPending);
  }

  /**
   * Completes once {@code isDone} says so: the fan-out of {@code Continuation.MultiContinuation} with an
   * overridden {@code isDone()} and {@code getResult()}, such as PAST's insert that is done once enough replicas
   * stored the object. {@code isDone} is called after each outcome that comes in (and once at the start if
   * {@code operations} is empty); outcomes after the fan-out is done are ignored.
   *
   * @param operations the futures of the operations
   * @param isDone decides from the outcomes so far whether the fan-out is done; if it throws, the result fails
   *     with that exception
   * @param result builds the result from the outcomes, null where an operation has not completed yet
   * @return the result; completed on the thread that completes the operation that made it done
   */
  public static <T extends @Nullable Object, R extends @Nullable Object> CompletableFuture<R> quorum(
      List<? extends CompletionStage<? extends T>> operations, QuorumCheck<T> isDone,
      Function<? super List<@Nullable Outcome<T>>, ? extends R> result) {
    Fanout<T, R> fanout = new Fanout<T, R>(operations.size(), isDone, result);
    if (operations.isEmpty()) {
      fanout.check();
    }
    for (int i = 0; i < operations.size(); i++) {
      final int index = i;
      var _ = operations.get(i).whenComplete((value, throwable) -> fanout.receive(index, Outcome.<T>of(value, throwable)));
    }
    return fanout.result;
  }

  private static <T extends @Nullable Object> boolean allDone(List<@Nullable Outcome<T>> outcomes) {
    for (Outcome<T> outcome : outcomes) {
      if (outcome == null) return false;
    }
    return true;
  }

  private static <T extends @Nullable Object> List<Outcome<T>> withoutPending(List<@Nullable Outcome<T>> outcomes) {
    List<Outcome<T>> complete = new ArrayList<Outcome<T>>(outcomes.size());
    for (Outcome<T> outcome : outcomes) {
      complete.add(Objects.requireNonNull(outcome, "pending outcome"));
    }
    return Collections.unmodifiableList(complete);
  }

  /** The state of one {@link #quorum}. */
  private static final class Fanout<T extends @Nullable Object, R extends @Nullable Object> {
    final CompletableFuture<R> result = new CompletableFuture<R>();
    private final List<@Nullable Outcome<T>> outcomes;
    private final List<@Nullable Outcome<T>> outcomesView;
    private final QuorumCheck<T> isDone;
    private final Function<? super List<@Nullable Outcome<T>>, ? extends R> resultBuilder;
    private boolean done;

    Fanout(int operationCount, QuorumCheck<T> isDone,
        Function<? super List<@Nullable Outcome<T>>, ? extends R> resultBuilder) {
      this.outcomes = new ArrayList<@Nullable Outcome<T>>(Collections.nCopies(operationCount, (Outcome<T>) null));
      this.outcomesView = Collections.unmodifiableList(outcomes);
      this.isDone = isDone;
      this.resultBuilder = resultBuilder;
    }

    void receive(int index, Outcome<T> outcome) {
      synchronized (this) {
        if (done || outcomes.get(index) != null) return;
        outcomes.set(index, outcome);
      }
      check();
    }

    void check() {
      Outcome<R> decision;
      synchronized (this) {
        if (done) return;
        try {
          if (!isDone.isDone(outcomesView)) return;
          decision = new Outcome.Success<R>(resultBuilder.apply(outcomesView));
        } catch (Exception exception) {
          decision = new Outcome.Failure<R>(exception);
        }
        done = true;
      }
      // complete outside the lock: the dependent stages run now, on this thread
      switch (decision) {
        case Outcome.Success<R> success -> result.complete(success.value());
        case Outcome.Failure<R> failure -> result.completeExceptionally(failure.cause());
      }
    }
  }
}
