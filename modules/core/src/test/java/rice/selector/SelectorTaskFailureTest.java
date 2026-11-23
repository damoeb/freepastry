package rice.selector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;

/**
 * A task on the selector thread that throws costs that task, not the node: the default ExceptionStrategy used to
 * destroy the Environment (and every node in it) for any exception that left an invocation or a timer task. Only an
 * error of the virtual machine (out of memory, a stack overflow) or a class that fails to link still destroys it.
 */
class SelectorTaskFailureTest {
  private static final long TIMEOUT_SECONDS = 10;

  private final Environment env = new Environment();

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("an invocation that throws does not stop the invocations queued with it, nor later ones")
  void invocationThatThrows() throws Exception {
    SelectorLoop selector = env.getSelectorManager();
    CompletableFuture<Void> queuedWithIt = new CompletableFuture<>();
    CountDownLatch queued = new CountDownLatch(1);
    // queue both in one round of invocations
    selector.invoke(() -> {
      try {
        queued.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    selector.invoke(() -> {
      throw new IllegalStateException("a bug in one task");
    });
    selector.invoke(() -> queuedWithIt.complete(null));
    queued.countDown();
    queuedWithIt.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertSelectorStillRuns();
  }

  @Test
  @DisplayName("a timer task that throws does not stop the timer tasks due with it, nor later ones")
  void timerTaskThatThrows() throws Exception {
    SelectorLoop selector = env.getSelectorManager();
    CompletableFuture<Void> dueWithIt = new CompletableFuture<>();
    selector.invoke(() -> {
      // both due at once, in the same round
      selector.schedule(new TimerTask() {
        @Override
        public void run() {
          throw new IllegalStateException("a bug in one timer");
        }
      }, 0);
      selector.schedule(new TimerTask() {
        @Override
        public void run() {
          dueWithIt.complete(null);
        }
      }, 0);
    });
    dueWithIt.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertSelectorStillRuns();
  }

  @Test
  @DisplayName("a periodic timer task that throws is dropped, other periodic tasks keep running")
  void periodicTaskThatThrows() throws Exception {
    SelectorLoop selector = env.getSelectorManager();
    CountDownLatch otherRuns = new CountDownLatch(5);
    int[] failingRuns = new int[1];
    selector.invoke(() -> {
      selector.schedule(new TimerTask() {
        @Override
        public void run() {
          failingRuns[0]++;
          throw new IllegalStateException("a bug in a periodic timer");
        }
      }, 0, 10);
      selector.schedule(new TimerTask() {
        @Override
        public void run() {
          otherRuns.countDown();
        }
      }, 0, 10);
    });
    assertTrue(otherRuns.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "the other periodic task keeps running");
    assertSelectorStillRuns();
    CompletableFuture<Integer> failed = new CompletableFuture<>();
    selector.invoke(() -> failed.complete(failingRuns[0]));
    assertEquals(1, failed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), "the failing periodic task ran once");
  }

  @Test
  @DisplayName("an error of the virtual machine still destroys the environment")
  void virtualMachineErrorDestroys() throws Exception {
    SelectorLoop selector = env.getSelectorManager();
    selector.invoke(() -> {
      throw new InternalError("the virtual machine is broken");
    });
    selector.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    assertFalse(selector.isAlive(), "the selector thread stopped");
  }

  private void assertSelectorStillRuns() throws Exception {
    SelectorLoop selector = env.getSelectorManager();
    CompletableFuture<Void> later = new CompletableFuture<>();
    selector.invoke(() -> later.complete(null));
    try {
      later.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new AssertionError("a later invocation never ran: the environment was destroyed", e);
    }
    assertTrue(selector.isAlive(), "the selector thread still runs");
  }
}
