package rice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;
import rice.Continuation.MultiContinuation;
import rice.environment.Environment;
import rice.selector.SelectorLoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Characterises {@link Futures#collect} and {@link Futures#quorum} against the {@link MultiContinuation} they
 * replace: the same scenario runs through both, and both must deliver the same result after the same event, on
 * the same thread.
 */
public class FuturesFanoutTest {

  /** One sub-operation completes: with a value, or with an exception if {@code outcome} is one. */
  record Event(int index, Object outcome) {
  }

  /** What one side delivered, and after which event. */
  static final class Delivery {
    Object result;
    Exception failure;
    int afterEvent = -1;
    int deliveries;
    boolean onSelector;

    void result(Object value, int event, boolean isSelectorThread) {
      result = value;
      afterEvent = event;
      deliveries++;
      onSelector = isSelectorThread;
    }

    void failure(Exception exception, int event, boolean isSelectorThread) {
      failure = exception;
      afterEvent = event;
      deliveries++;
      onSelector = isSelectorThread;
    }
  }

  /** The PAST insert rule: done once half of the replicas stored the object, failed once that can't happen. */
  static final double THRESHOLD = 0.5;

  static boolean quorumReached(boolean[] haveResult, Object[] result) throws Exception {
    int successes = 0;
    for (int i = 0; i < haveResult.length; i++) {
      if (haveResult[i] && Boolean.TRUE.equals(result[i])) successes++;
    }
    if (successes >= THRESHOLD * haveResult.length) return true;
    for (boolean have : haveResult) {
      if (!have) return false;
    }
    throw new IOException("only " + successes + " successes");
  }

  static Boolean[] successes(Object[] result) {
    Boolean[] successes = new Boolean[result.length];
    for (int i = 0; i < result.length; i++) {
      successes[i] = (result[i] == null) || Boolean.TRUE.equals(result[i]);
    }
    return successes;
  }

  /** The outcomes in the shape of MultiContinuation's Object[]: the value, or the exception. */
  static Object[] asResultArray(List<Outcome<Object>> outcomes) {
    Object[] result = new Object[outcomes.size()];
    for (int i = 0; i < result.length; i++) {
      Outcome<Object> outcome = outcomes.get(i);
      if (outcome == null) continue;
      result[i] = switch (outcome) {
        case Outcome.Success<Object> success -> success.value();
        case Outcome.Failure<Object> failure -> failure.cause();
      };
    }
    return result;
  }

  static boolean[] arrived(List<Outcome<Object>> outcomes) {
    boolean[] arrived = new boolean[outcomes.size()];
    for (int i = 0; i < arrived.length; i++) arrived[i] = outcomes.get(i) != null;
    return arrived;
  }

  /**
   * Runs the events through a MultiContinuation and through futures, on the calling thread.
   *
   * @param withQuorum use the PAST insert rule instead of waiting for all
   * @return the MultiContinuation's delivery and the future's
   */
  static Delivery[] run(int operationCount, List<Event> events, boolean withQuorum, BooleanSupplier isSelectorThread) {
    final Delivery[] deliveries = {new Delivery(), new Delivery()};
    final int[] currentEvent = {-1};

    Continuation<Object, Exception> parent = new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        deliveries[0].result(result, currentEvent[0], isSelectorThread.getAsBoolean());
      }

      @Override
      public void receiveException(Exception exception) {
        deliveries[0].failure(exception, currentEvent[0], isSelectorThread.getAsBoolean());
      }
    };
    MultiContinuation<Object> multiContinuation = withQuorum
        ? new MultiContinuation<Object>(parent, operationCount) {
            @Override
            public boolean isDone() throws Exception {
              return quorumReached(haveResult, result);
            }

            @Override
            public Object getResult() {
              return successes(result);
            }
          }
        : new MultiContinuation<Object>(parent, operationCount);
    List<Continuation<Object, Exception>> subContinuations = new ArrayList<Continuation<Object, Exception>>();
    for (int i = 0; i < operationCount; i++) subContinuations.add(multiContinuation.getSubContinuation(i));

    List<CompletableFuture<Object>> operations = new ArrayList<CompletableFuture<Object>>();
    for (int i = 0; i < operationCount; i++) operations.add(new CompletableFuture<Object>());
    CompletableFuture<?> fanout = withQuorum
        ? Futures.<Object, Boolean[]>quorum(operations,
            outcomes -> quorumReached(arrived(outcomes), asResultArray(outcomes)),
            outcomes -> successes(asResultArray(outcomes)))
        : Futures.<Object>collect(operations).thenApply(FuturesFanoutTest::asResultArray);
    var _ = fanout.whenComplete((value, throwable) -> {
      if (throwable == null) {
        deliveries[1].result(value, currentEvent[0], isSelectorThread.getAsBoolean());
      } else {
        deliveries[1].failure((Exception) Futures.unwrap(throwable), currentEvent[0], isSelectorThread.getAsBoolean());
      }
    });

    for (int i = 0; i < events.size(); i++) {
      currentEvent[0] = i;
      Event event = events.get(i);
      if (event.outcome() instanceof Exception exception) {
        subContinuations.get(event.index()).receiveException(exception);
        operations.get(event.index()).completeExceptionally(exception);
      } else {
        subContinuations.get(event.index()).receiveResult(event.outcome());
        operations.get(event.index()).complete(event.outcome());
      }
    }
    return deliveries;
  }

  static void assertSameDelivery(Delivery[] deliveries) {
    Delivery multi = deliveries[0];
    Delivery future = deliveries[1];
    assertEquals(multi.afterEvent, future.afterEvent, "delivered after the same event");
    assertEquals(multi.deliveries, future.deliveries, "delivered as often");
    assertEquals(multi.onSelector, future.onSelector, "delivered on the same thread");
    if (multi.result instanceof Object[] multiResult) {
      assertTrue(Arrays.equals(multiResult, (Object[]) future.result),
          Arrays.toString(multiResult) + " vs " + Arrays.toString((Object[]) future.result));
    } else {
      assertEquals(multi.result, future.result);
    }
    if (multi.failure == null) {
      assertEquals(null, future.failure);
    } else {
      assertEquals(multi.failure.getClass(), future.failure.getClass());
      assertEquals(multi.failure.getMessage(), future.failure.getMessage());
    }
  }

  @Test
  void collectWaitsForAllAndKeepsTheOrderOfTheOperations() {
    Delivery[] deliveries = run(3, List.of(new Event(2, "c"), new Event(0, "a"), new Event(1, "b")), false, ON_TEST_THREAD);
    assertSameDelivery(deliveries);
    assertEquals(2, deliveries[1].afterEvent);
    assertTrue(Arrays.equals(new Object[] {"a", "b", "c"}, (Object[]) deliveries[1].result));
  }

  @Test
  void collectKeepsFailuresAsEntries() {
    IOException failure = new IOException("replica down");
    Delivery[] deliveries = run(3, List.of(new Event(0, "a"), new Event(1, failure), new Event(2, "c")), false, ON_TEST_THREAD);
    assertSameDelivery(deliveries);
    assertSame(failure, ((Object[]) deliveries[1].result)[1]);
  }

  @Test
  void collectIgnoresASecondOutcomeOfTheSameOperation() {
    Delivery[] deliveries = run(2, List.of(new Event(0, "a"), new Event(0, "again"), new Event(1, "b")), false, ON_TEST_THREAD);
    assertSameDelivery(deliveries);
    assertTrue(Arrays.equals(new Object[] {"a", "b"}, (Object[]) deliveries[1].result));
  }

  @Test
  void collectKeepsNullValues() {
    List<Outcome<String>> outcomes = Futures.<String>collect(List.of(CompletableFuture.<String>completedFuture(null))).join();
    assertEquals(List.of(new Outcome.Success<String>(null)), outcomes);
  }

  @Test
  void quorumCompletesEarlyOnceEnoughSucceeded() {
    Delivery[] deliveries = run(4, List.of(new Event(3, true), new Event(1, new IOException("down")), new Event(0, true),
        new Event(2, true)), true, ON_TEST_THREAD);
    assertSameDelivery(deliveries);
    assertEquals(2, deliveries[1].afterEvent, "done after the second success");
    assertEquals(1, deliveries[1].deliveries, "the late outcome is ignored");
    // a missing outcome counts as success, a failure as no success
    assertTrue(Arrays.equals(new Boolean[] {true, false, true, true}, (Boolean[]) deliveries[1].result));
  }

  @Test
  void quorumFailsWithTheExceptionOfTheCheck() {
    Delivery[] deliveries = run(3, List.of(new Event(0, false), new Event(1, new IOException("down")), new Event(2, true)),
        true, ON_TEST_THREAD);
    assertSameDelivery(deliveries);
    assertEquals(IOException.class, deliveries[1].failure.getClass());
    assertEquals("only 1 successes", deliveries[1].failure.getMessage());
  }

  @Test
  void bothDeliverOnTheSelectorThreadWhenTheOperationsCompleteThere() throws Exception {
    Environment environment = new Environment();
    try {
      SelectorLoop selector = environment.getSelectorManager();
      CompletableFuture<Delivery[]> ran = new CompletableFuture<Delivery[]>();
      selector.invoke(() -> ran.complete(run(2, List.of(new Event(1, "b"), new Event(0, "a")), false, selector::isSelectorThread)));
      Delivery[] deliveries = ran.get(5, TimeUnit.SECONDS);
      assertSameDelivery(deliveries);
      assertTrue(deliveries[1].onSelector);
    } finally {
      environment.destroy();
    }
  }

  @Test
  void anEmptyCollectCompletesAtOnceUnlikeAMultiContinuation() {
    Delivery[] deliveries = run(0, List.of(), false, ON_TEST_THREAD);
    assertEquals(0, deliveries[0].deliveries, "a MultiContinuation for zero results never completes");
    assertEquals(1, deliveries[1].deliveries, "collect() of nothing completes at once");
    assertEquals(0, ((Object[]) deliveries[1].result).length);
  }

  @Test
  void anEmptyQuorumAsksTheCheckOnce() {
    CompletableFuture<String> decided = Futures.<String, String>quorum(List.of(), outcomes -> true, outcomes -> "decided");
    assertEquals("decided", decided.join());
    CompletableFuture<String> pending = Futures.<String, String>quorum(List.of(), outcomes -> false, outcomes -> "never");
    assertFalse(pending.isDone());
  }

  /** For the tests that run on the test thread: nothing runs on a selector there. */
  static final BooleanSupplier ON_TEST_THREAD = () -> false;
}
