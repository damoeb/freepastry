package rice.environment;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Destroying an {@link Environment} fails the futures {@linkplain Environment#track tracked} by it.
 */
public class EnvironmentPendingFuturesTest {

  static final long WAIT_MILLIS = 5000;

  /** Waits for the future and returns the exception it failed with, as it is. */
  static Throwable failureOf(CompletableFuture<?> future) throws Exception {
    return future.handle((value, throwable) -> throwable).get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  void destroyFailsThePendingFuturesOnTheSelectorThread() throws Exception {
    Environment environment = new Environment();
    CompletableFuture<String> pending = environment.track(new CompletableFuture<String>());
    List<Boolean> failedOnSelector = new CopyOnWriteArrayList<Boolean>();
    var _ = pending.whenComplete((_, _) -> failedOnSelector.add(environment.getSelectorManager().isSelectorThread()));

    environment.destroy();

    Throwable failure = failureOf(pending);
    assertInstanceOf(EnvironmentDestroyedException.class, failure);
    assertEquals("environment destroyed", failure.getMessage());
    assertEquals(List.of(true), failedOnSelector);
  }

  @Test
  void completedFuturesAreForgotten() {
    Environment environment = new Environment();
    try {
      CompletableFuture<String> completed = environment.track(new CompletableFuture<String>());
      CompletableFuture<String> failed = environment.track(new CompletableFuture<String>());
      var _ = environment.track(CompletableFuture.completedFuture("already done"));
      assertEquals(2, environment.pendingFutureCount());
      completed.complete("x");
      failed.completeExceptionally(new IllegalStateException());
      assertEquals(0, environment.pendingFutureCount(), "no leak of completed futures");
    } finally {
      environment.destroy();
    }
  }

  @Test
  void aFutureTrackedAfterDestroyFailsAtOnce() throws Exception {
    Environment environment = new Environment();
    CompletableFuture<String> before = environment.track(new CompletableFuture<String>());
    environment.destroy();
    assertInstanceOf(EnvironmentDestroyedException.class, failureOf(before));

    CompletableFuture<String> after = environment.track(new CompletableFuture<String>());
    assertTrue(after.isCompletedExceptionally());
    assertInstanceOf(EnvironmentDestroyedException.class, failureOf(after));
  }

  @Test
  void aCompletedFutureIsNotChanged() throws Exception {
    Environment environment = new Environment();
    CompletableFuture<String> completed = environment.track(new CompletableFuture<String>());
    completed.complete("in time");
    environment.destroy();
    Thread.sleep(50);
    assertEquals("in time", completed.get());
  }

  @Test
  void destroyingTheSimulatorFailsItsPendingFutures() throws Exception {
    Environment environment = Environment.directEnvironment();
    CompletableFuture<String> pending = environment.track(new CompletableFuture<String>());
    environment.destroy();
    assertInstanceOf(EnvironmentDestroyedException.class, failureOf(pending));
  }
}
