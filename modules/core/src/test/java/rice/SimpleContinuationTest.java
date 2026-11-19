package rice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.Continuation.SimpleContinuation;

class SimpleContinuationTest {

  private final List<Object> results = new ArrayList<>();

  private final SimpleContinuation testObj =
      new SimpleContinuation() {
        @Override
        public void receiveResult(Object o) {
          results.add(o);
        }
      };

  @Test
  @DisplayName("receiveException does not invoke receiveResult")
  void receiveExceptionIsNotTreatedAsResult() {
    testObj.receiveException(new IllegalStateException("boom"));

    assertTrue(results.isEmpty());
  }

  @Test
  @DisplayName("receiveResult still reaches the subclass handler")
  void receiveResultIsDelivered() {
    testObj.receiveResult("ok");

    assertEquals(List.of("ok"), results);
  }
}
