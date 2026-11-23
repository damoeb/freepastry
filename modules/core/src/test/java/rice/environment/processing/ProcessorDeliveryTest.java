package rice.environment.processing;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.selector.SelectorLoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins where and when the {@link Processor} delivers: the task runs off the selector (on the selector in the
 * simulator), its result or exception arrives on the selector thread, and a request cancelled before it starts
 * delivers nothing.
 */
public class ProcessorDeliveryTest {

  static final long WAIT_MILLIS = 5000;

  private Environment environment;

  @AfterEach
  void destroy() {
    if (environment != null) environment.destroy();
  }

  /** What a delivery looked like. */
  record Delivery(Object outcome, boolean onSelector) {
  }

  /** Completes a future with the first delivery, and records on which thread it came. */
  static final class Recorder<R> implements Continuation<R, Exception> {
    final CompletableFuture<Delivery> delivered = new CompletableFuture<Delivery>();
    final SelectorLoop selector;

    Recorder(SelectorLoop selector) {
      this.selector = selector;
    }

    @Override
    public void receiveResult(R result) {
      delivered.complete(new Delivery(result, selector.isSelectorThread()));
    }

    @Override
    public void receiveException(Exception exception) {
      delivered.complete(new Delivery(exception, selector.isSelectorThread()));
    }

    Delivery await() throws Exception {
      return delivered.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

  private Processor processor() {
    return environment.getProcessor();
  }

  private SelectorLoop selector() {
    return environment.getSelectorManager();
  }

  @Test
  void theResultArrivesOnTheSelectorThread() throws Exception {
    environment = new Environment();
    List<Boolean> ranOnSelector = new CopyOnWriteArrayList<Boolean>();
    Recorder<String> recorder = new Recorder<String>(selector());
    processor().process(() -> {
      ranOnSelector.add(selector().isSelectorThread());
      return "done";
    }, recorder, selector(), environment.getTimeSource(), environment.getLogManager());
    assertEquals(new Delivery("done", true), recorder.await());
    assertEquals(List.of(false), ranOnSelector, "the task runs off the selector");
  }

  @Test
  void theExceptionArrivesOnTheSelectorThread() throws Exception {
    environment = new Environment();
    IOException failure = new IOException("task failed");
    Recorder<String> recorder = new Recorder<String>(selector());
    processor().process(() -> {
      throw failure;
    }, recorder, selector(), environment.getTimeSource(), environment.getLogManager());
    Delivery delivery = recorder.await();
    assertSame(failure, delivery.outcome());
    assertTrue(delivery.onSelector());
  }

  @Test
  void aRequestCancelledBeforeItStartsDeliversNothing() throws Exception {
    environment = new Environment();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Recorder<String> blocker = new Recorder<String>(selector());
    processor().process(() -> {
      started.countDown();
      release.await();
      return "blocker";
    }, blocker, selector(), environment.getTimeSource(), environment.getLogManager());
    assertTrue(started.await(WAIT_MILLIS, TimeUnit.MILLISECONDS));

    List<String> ran = new CopyOnWriteArrayList<String>();
    Recorder<String> cancelledRecorder = new Recorder<String>(selector());
    Cancellable request = processor().process(() -> {
      ran.add("cancelled task");
      return "cancelled";
    }, cancelledRecorder, selector(), environment.getTimeSource(), environment.getLogManager());
    assertTrue(request.cancel(), "not started yet");
    release.countDown();

    assertEquals("blocker", blocker.await().outcome());
    // let a delivery of the cancelled request come through, if there were one
    CompletableFuture<Void> selectorRound = new CompletableFuture<Void>();
    Thread.sleep(50);
    selector().invoke(() -> selectorRound.complete(null));
    selectorRound.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertTrue(ran.isEmpty());
    assertFalse(cancelledRecorder.delivered.isDone());
  }

  @Test
  void blockingIOResultAndExceptionArriveOnTheSelectorThread() throws Exception {
    environment = new Environment();
    List<Boolean> ranOnSelector = new CopyOnWriteArrayList<Boolean>();
    Recorder<String> results = new Recorder<String>(selector());
    processor().processBlockingIO(new WorkRequest<String>(results, selector()) {
      @Override
      public String doWork() {
        ranOnSelector.add(selector().isSelectorThread());
        return "read";
      }
    });
    IOException failure = new IOException("disk");
    Recorder<String> failures = new Recorder<String>(selector());
    processor().processBlockingIO(new WorkRequest<String>(failures, selector()) {
      @Override
      public String doWork() throws IOException {
        throw failure;
      }
    });
    assertEquals(new Delivery("read", true), results.await());
    assertEquals(new Delivery(failure, true), failures.await());
    assertEquals(List.of(false), ranOnSelector, "blocking IO runs off the selector");
  }

  @Test
  void theSimulatorRunsTasksOnTheSelectorInOrder() throws Exception {
    environment = Environment.directEnvironment(1);
    List<String> order = new CopyOnWriteArrayList<String>();
    Recorder<String> first = new Recorder<String>(selector());
    Recorder<String> second = new Recorder<String>(selector());
    Recorder<String> io = new Recorder<String>(selector());
    CompletableFuture<Void> submitted = new CompletableFuture<Void>();
    selector().invoke(() -> {
      processor().process(() -> {
        order.add("task 1 on selector " + selector().isSelectorThread());
        return "1";
      }, first, selector(), environment.getTimeSource(), environment.getLogManager());
      processor().processBlockingIO(new WorkRequest<String>(io, selector()) {
        @Override
        public String doWork() {
          order.add("io on selector " + selector().isSelectorThread());
          return "io";
        }
      });
      processor().process(() -> {
        order.add("task 2 on selector " + selector().isSelectorThread());
        return "2";
      }, second, selector(), environment.getTimeSource(), environment.getLogManager());
      order.add("submitted");
      submitted.complete(null);
    });
    submitted.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertEquals(new Delivery("1", true), first.await());
    assertEquals(new Delivery("io", true), io.await());
    assertEquals(new Delivery("2", true), second.await());
    assertEquals(List.of("submitted", "task 1 on selector true", "io on selector true", "task 2 on selector true"), order);
  }
}
