package rice.environment.processing;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import rice.Futures;
import rice.environment.Environment;
import rice.selector.SelectorLoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The future methods of {@link Processor}: they complete on the selector thread, with the exception the task
 * threw, and cancelling the future before the task starts keeps it from running.
 */
public class ProcessorFutureTest {

  static final long WAIT_MILLIS = 5000;

  private Environment environment;

  @AfterEach
  void destroy() {
    if (environment != null) environment.destroy();
  }

  private Processor processor() {
    return environment.getProcessor();
  }

  private SelectorLoop selector() {
    return environment.getSelectorManager();
  }

  /** What completed the future, and whether on the selector thread. */
  record Completion(Object outcome, boolean onSelector) {
  }

  private CompletableFuture<Completion> completionOf(CompletableFuture<?> future) {
    return future.handle((value, throwable) -> new Completion(throwable == null ? value : Futures.unwrap(throwable),
        selector().isSelectorThread()));
  }

  @Test
  void processCompletesOnTheSelectorThread() throws Exception {
    environment = new Environment();
    CountDownLatch registered = new CountDownLatch(1);
    CompletableFuture<String> future = processor().process(() -> {
      registered.await();
      return "done";
    }, selector(), environment.getTimeSource(), environment.getLogManager());
    CompletableFuture<Completion> completion = completionOf(future);
    registered.countDown();
    assertEquals(new Completion("done", true), completion.get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
  }

  @Test
  void processFailsWithTheExceptionOfTheTask() throws Exception {
    environment = new Environment();
    CountDownLatch registered = new CountDownLatch(1);
    IOException failure = new IOException("task failed");
    CompletableFuture<String> future = processor().process(() -> {
      registered.await();
      throw failure;
    }, selector(), environment.getTimeSource(), environment.getLogManager());
    CompletableFuture<Completion> completion = completionOf(future);
    registered.countDown();
    Completion completed = completion.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertSame(failure, completed.outcome());
    assertTrue(completed.onSelector());
  }

  @Test
  void cancellingBeforeTheTaskStartsKeepsItFromRunning() throws Exception {
    environment = new Environment();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CompletableFuture<String> blocker = processor().process(() -> {
      started.countDown();
      release.await();
      return "blocker";
    }, selector(), environment.getTimeSource(), environment.getLogManager());
    assertTrue(started.await(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    List<String> ran = new CopyOnWriteArrayList<String>();
    CompletableFuture<String> cancelled = processor().process(() -> {
      ran.add("cancelled task");
      return "cancelled";
    }, selector(), environment.getTimeSource(), environment.getLogManager());
    assertTrue(cancelled.cancel(false));
    release.countDown();
    assertEquals("blocker", blocker.get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    // the processing thread is single, so a task after the cancelled one shows it was skipped
    assertEquals("after", processor().process(() -> "after", selector(), environment.getTimeSource(), environment.getLogManager())
        .get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    assertTrue(ran.isEmpty());
  }

  @Test
  void blockingIOCompletesOnTheSelectorThread() throws Exception {
    environment = new Environment();
    CountDownLatch registered = new CountDownLatch(1);
    List<Boolean> ranOnSelector = new CopyOnWriteArrayList<Boolean>();
    CompletableFuture<String> future = processor().processBlockingIO(() -> {
      registered.await();
      ranOnSelector.add(selector().isSelectorThread());
      return "read";
    }, selector());
    CompletableFuture<Completion> completion = completionOf(future);
    IOException failure = new IOException("disk");
    CompletableFuture<Completion> failed = completionOf(processor().processBlockingIO(() -> {
      throw failure;
    }, selector()));
    registered.countDown();
    assertEquals(new Completion("read", true), completion.get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    assertEquals(new Completion(failure, true), failed.get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    assertEquals(List.of(false), ranOnSelector);
  }

  @Test
  void theSimulatorCompletesOnTheSelectorInOrder() throws Exception {
    environment = Environment.directEnvironment(1);
    List<String> order = new CopyOnWriteArrayList<String>();
    CompletableFuture<Void> done = new CompletableFuture<Void>();
    selector().invoke(() -> {
      CompletableFuture<String> first = processor().process(() -> "1", selector(), environment.getTimeSource(), environment.getLogManager());
      CompletableFuture<String> io = processor().processBlockingIO(() -> "io", selector());
      CompletableFuture<String> second = processor().process(() -> "2", selector(), environment.getTimeSource(), environment.getLogManager());
      for (CompletableFuture<String> future : List.of(first, io, second)) {
        var _ = future.thenAccept(value -> order.add(value + " on selector " + selector().isSelectorThread()));
      }
      assertFalse(first.isDone(), "the simulator runs the task later, on the selector");
      order.add("submitted");
      var _ = second.thenRun(() -> done.complete(null));
    });
    done.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("submitted", "1 on selector true", "io on selector true", "2 on selector true"), order);
  }
}
