package rice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.environment.EnvironmentDestroyedException;
import rice.environment.time.simulated.DirectTimeSource;
import rice.selector.SelectorLoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the future helpers of {@link Futures} and {@link rice.selector.SelectorLoop#executor()}, in particular on
 * which thread they complete.
 */
public class FuturesTest {

  static final long WAIT_MILLIS = 5000;

  private final List<Environment> environments = new ArrayList<Environment>();

  @AfterEach
  void destroyEnvironments() {
    for (Environment environment : environments) {
      environment.destroy();
    }
  }

  private Environment environment() {
    Environment environment = new Environment();
    environments.add(environment);
    return environment;
  }

  /** Runs {@code task} on the selector thread and waits for it. */
  static void onSelector(SelectorLoop selector, Runnable task) throws Exception {
    CompletableFuture<Void> ran = new CompletableFuture<Void>();
    selector.invoke(() -> {
      try {
        task.run();
        ran.complete(null);
      } catch (Throwable throwable) {
        ran.completeExceptionally(throwable);
      }
    });
    ran.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  void unwrapRemovesCompletionAndExecutionWrappers() {
    IOException cause = new IOException("disk");
    assertSame(cause, Futures.unwrap(new CompletionException(new ExecutionException(cause))));
    assertSame(cause, Futures.unwrap(cause));
    CompletionException withoutCause = new CompletionException("no cause", null);
    assertSame(withoutCause, Futures.unwrap(withoutCause));
  }

  @Test
  void executorRunsOnTheSelectorThread() throws Exception {
    Environment environment = environment();
    SelectorLoop selector = environment.getSelectorManager();
    CompletableFuture<Boolean> onSelector = CompletableFuture.supplyAsync(selector::isSelectorThread, selector.executor());
    assertTrue(onSelector.get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
  }

  @Test
  void awaitReturnsTheValueOffTheSelector() throws Exception {
    Environment environment = environment();
    CompletableFuture<String> future = new CompletableFuture<String>();
    environment.getSelectorManager().invoke(() -> future.complete("value"));
    assertEquals("value", Futures.await(future, environment.getSelectorManager(), WAIT_MILLIS));
  }

  @Test
  void awaitTimesOut() {
    Environment environment = environment();
    assertThrows(TimeoutException.class,
        () -> Futures.await(new CompletableFuture<String>(), environment.getSelectorManager(), 10));
  }

  @Test
  void awaitReportsTheFailure() {
    Environment environment = environment();
    IOException cause = new IOException("disk");
    ExecutionException thrown = assertThrows(ExecutionException.class,
        () -> Futures.await(CompletableFuture.failedFuture(cause), environment.getSelectorManager(), WAIT_MILLIS));
    assertSame(cause, thrown.getCause());
  }

  @Test
  void awaitThrowsTheCancellationOfADestroyedEnvironmentAsItIs() {
    Environment environment = environment();
    CompletableFuture<String> destroyed = CompletableFuture.failedFuture(new EnvironmentDestroyedException());
    assertThrows(EnvironmentDestroyedException.class, () -> Futures.await(destroyed, environment.getSelectorManager(), WAIT_MILLIS));
    assertThrows(EnvironmentDestroyedException.class, () -> Futures.await(destroyed, environment.getSelectorManager()));
  }

  @Test
  void awaitRefusesToBlockTheSelector() throws Exception {
    Environment environment = environment();
    SelectorLoop selector = environment.getSelectorManager();
    AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
    AtomicReference<String> doneValue = new AtomicReference<String>();
    onSelector(selector, () -> {
      try {
        Futures.await(new CompletableFuture<String>(), selector, WAIT_MILLIS);
      } catch (Throwable throwable) {
        thrown.set(throwable);
      }
      try {
        // a done future doesn't block, so that is allowed
        doneValue.set(Futures.await(CompletableFuture.completedFuture("done"), selector));
      } catch (Exception e) {
        thrown.set(e);
      }
    });
    assertInstanceOf(IllegalStateException.class, thrown.get());
    assertEquals("done", doneValue.get());
  }

  @Test
  void timeoutFailsThePendingFutureOnTheSelectorThread() throws Exception {
    Environment environment = environment();
    SelectorLoop selector = environment.getSelectorManager();
    CompletableFuture<String> future = new CompletableFuture<String>();
    AtomicBoolean failedOnSelector = new AtomicBoolean();
    onSelector(selector, () -> {
      var _ = Futures.timeout(future, 20, selector.getTimer(), () -> new IOException("timed out"));
    });
    CompletableFuture<Void> observed = future.handle((value, throwable) -> {
      failedOnSelector.set(selector.isSelectorThread());
      return null;
    });
    observed.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    ExecutionException thrown = assertThrows(ExecutionException.class, () -> future.get());
    assertInstanceOf(IOException.class, thrown.getCause());
    assertTrue(failedOnSelector.get());
  }

  @Test
  void timeoutIsCancelledByTheResult() throws Exception {
    Environment environment = environment();
    SelectorLoop selector = environment.getSelectorManager();
    CompletableFuture<String> future = new CompletableFuture<String>();
    onSelector(selector, () -> {
      var _ = Futures.timeout(future, 20, selector.getTimer(), () -> new IOException("timed out"));
      future.complete("in time");
    });
    Thread.sleep(100);
    assertEquals("in time", future.get());
  }

  @Test
  void timeoutRunsOnVirtualTimeInTheSimulator() throws Exception {
    Environment environment = Environment.directEnvironment();
    environments.add(environment);
    SelectorLoop selector = environment.getSelectorManager();
    DirectTimeSource time = (DirectTimeSource) environment.getTimeSource();
    CompletableFuture<String> future = new CompletableFuture<String>();
    List<Boolean> failedOnSelector = new CopyOnWriteArrayList<Boolean>();
    onSelector(selector, () -> {
      var _ = Futures.timeout(future, 1000, selector.getTimer(), () -> new IOException("timed out"))
          .whenComplete((_, _) -> failedOnSelector.add(selector.isSelectorThread()));
    });
    // real time passes, virtual time does not: no timeout
    Thread.sleep(100);
    assertFalse(future.isDone());
    onSelector(selector, () -> time.setTime(time.currentTimeMillis() + 1000));
    ExecutionException thrown = assertThrows(ExecutionException.class, () -> future.get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    assertInstanceOf(IOException.class, thrown.getCause());
    assertEquals(List.of(true), failedOnSelector);
  }

  @Test
  void onCancelCancelsTheRequest() {
    AtomicBoolean requestCancelled = new AtomicBoolean();
    CompletableFuture<String> future = Futures.onCancel(new CompletableFuture<String>(), () -> {
      requestCancelled.set(true);
      return true;
    });
    future.cancel(false);
    assertTrue(requestCancelled.get());
  }

  @Test
  void onCancelCancelsTheRequestWhenTheEnvironmentIsDestroyed() {
    AtomicBoolean requestCancelled = new AtomicBoolean();
    CompletableFuture<String> future = Futures.onCancel(new CompletableFuture<String>(), () -> {
      requestCancelled.set(true);
      return true;
    });
    future.completeExceptionally(new EnvironmentDestroyedException());
    assertTrue(requestCancelled.get());
  }

  @Test
  void onCancelIgnoresResultsAndFailures() {
    AtomicBoolean requestCancelled = new AtomicBoolean();
    CompletableFuture<String> succeeded = Futures.onCancel(new CompletableFuture<String>(), () -> {
      requestCancelled.set(true);
      return true;
    });
    succeeded.complete("x");
    CompletableFuture<String> failed = Futures.onCancel(new CompletableFuture<String>(), () -> {
      requestCancelled.set(true);
      return true;
    });
    failed.completeExceptionally(new IOException());
    assertFalse(requestCancelled.get());
  }

  /** Records what reaches the ExceptionStrategy. */
  static final class Reported {
    final List<Object> sources = new CopyOnWriteArrayList<Object>();
    final List<Throwable> throwables = new CopyOnWriteArrayList<Throwable>();
    final CountDownLatch first = new CountDownLatch(1);

    Reported(Environment environment) {
      environment.setExceptionStrategy((source, throwable) -> {
        sources.add(source);
        throwables.add(throwable);
        first.countDown();
      });
    }
  }

  @Test
  void reportUnhandledHandsTheUnwrappedFailureToTheExceptionStrategy() {
    Environment environment = environment();
    Reported reported = new Reported(environment);
    IllegalStateException bug = new IllegalStateException("bug in a callback");
    CompletableFuture<String> source = new CompletableFuture<String>();
    Futures.reportUnhandled(source.thenAccept(value -> { throw bug; }), environment);
    source.complete("x");
    assertEquals(List.of(bug), reported.throwables);
    assertEquals(List.<Object>of(environment.getSelectorManager()), reported.sources);
  }

  @Test
  void reportUnhandledIgnoresResultsAndCancellations() {
    Environment environment = environment();
    Reported reported = new Reported(environment);
    Futures.reportUnhandled(CompletableFuture.completedFuture("x"), environment);
    Futures.reportUnhandled(CompletableFuture.failedFuture(new CancellationException()), environment);
    Futures.reportUnhandled(CompletableFuture.failedFuture(new EnvironmentDestroyedException()), environment);
    assertTrue(reported.throwables.isEmpty());
  }

  /** Records what a Continuation receives. */
  static final class Recorder<R> implements Continuation<R, Exception> {
    final List<Object> received = new ArrayList<Object>();

    @Override
    public void receiveResult(R result) {
      received.add("result:" + result);
    }

    @Override
    public void receiveException(Exception exception) {
      received.add(exception);
    }
  }

  @Test
  void forwardDeliversTheUnwrappedOutcome() {
    Environment environment = environment();
    Recorder<Integer> results = new Recorder<Integer>();
    Futures.forward(CompletableFuture.completedFuture(1), results, environment);
    IOException cause = new IOException("disk");
    CompletableFuture<Integer> source = new CompletableFuture<Integer>();
    Recorder<Integer> failures = new Recorder<Integer>();
    Futures.forward(source.thenApply(value -> value + 1), failures, environment);
    source.completeExceptionally(cause);
    assertEquals(List.of("result:1"), results.received);
    assertEquals(List.of(cause), failures.received);
  }

  @Test
  void forwardReportsWhatTheContinuationThrows() {
    Environment environment = environment();
    Reported reported = new Reported(environment);
    IllegalStateException bug = new IllegalStateException("bug in receiveResult");
    Futures.forward(CompletableFuture.completedFuture("x"), new Continuation<String, Exception>() {
      @Override
      public void receiveResult(String result) {
        throw bug;
      }

      @Override
      public void receiveException(Exception exception) {
      }
    }, environment);
    assertEquals(List.of(bug), reported.throwables);
  }

  @Test
  void forwardWithoutAnEnvironmentReportsToTheSelectorItRunsOn() throws Exception {
    Environment environment = environment();
    Reported reported = new Reported(environment);
    IllegalStateException bug = new IllegalStateException("bug in receiveResult");
    CompletableFuture<String> source = new CompletableFuture<String>();
    Futures.forward(source, new Continuation<String, Exception>() {
      @Override
      public void receiveResult(String result) {
        throw bug;
      }

      @Override
      public void receiveException(Exception exception) {
      }
    });
    environment.getSelectorManager().invoke(() -> source.complete("x"));
    assertTrue(reported.first.await(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    assertEquals(List.of(bug), reported.throwables);
    assertEquals(List.<Object>of(environment.getSelectorManager()), reported.sources);
  }

  @Test
  void callCompletesWhereTheContinuationIsCalled() throws Exception {
    Environment environment = environment();
    SelectorLoop selector = environment.getSelectorManager();
    List<Continuation<String, Exception>> started = new CopyOnWriteArrayList<Continuation<String, Exception>>();
    CompletableFuture<Boolean> deliveredOnSelector = Futures.<String>call(started::add).thenApply(value -> selector.isSelectorThread());
    selector.invoke(() -> started.get(0).receiveResult("x"));
    assertTrue(deliveredOnSelector.get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
  }
}
