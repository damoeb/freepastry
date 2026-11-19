package rice.pastry.pns;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import rice.FutureContinuation;
import rice.p2p.commonapi.Cancellable;

class PendingRequestsTest {
  private final PendingRequests<String, Integer> target = new PendingRequests<>();

  /** A timeout that remembers being cancelled. */
  private static final class Timeout implements Cancellable {
    final AtomicBoolean cancelled = new AtomicBoolean();

    @Override
    public boolean cancel() {
      cancelled.set(true);
      return true;
    }
  }

  @Test
  void completeAnswersEveryWaiterOfTheKeyAndCancelsTheirTimeouts() throws Exception {
    FutureContinuation<Integer, Exception> first = new FutureContinuation<>();
    FutureContinuation<Integer, Exception> second = new FutureContinuation<>();
    FutureContinuation<Integer, Exception> other = new FutureContinuation<>();
    Timeout timeout = new Timeout();
    target.add("a", first, timeout);
    target.add("a", second, new Timeout());
    target.add("b", other, new Timeout());

    target.complete("a", 7);

    assertEquals(7, first.future().getNow(null));
    assertEquals(7, second.future().getNow(null));
    assertTrue(timeout.cancelled.get());
    assertFalse(other.future().isDone());
  }

  @Test
  void aRemovedWaiterIsNotAnsweredAndItsTimeoutIsCancelled() {
    FutureContinuation<Integer, Exception> continuation = new FutureContinuation<>();
    Timeout timeout = new Timeout();
    target.add("a", continuation, timeout);

    assertTrue(target.remove("a", continuation));
    target.complete("a", 7);

    assertFalse(continuation.future().isDone());
    assertTrue(timeout.cancelled.get());
  }

  @Test
  void removingAWaiterThatIsNotWaitingReportsFalse() {
    FutureContinuation<Integer, Exception> continuation = new FutureContinuation<>();
    target.add("a", continuation, new Timeout());
    target.complete("a", 7);

    assertFalse(target.remove("a", continuation));
    assertFalse(target.remove("b", continuation));
  }
}
