package rice.p2p.aggregation;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.Continuation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InsertJoinTest {

  private final List<Object> reported = new ArrayList<>();
  private final InsertJoin testObj = new InsertJoin(new Continuation<Boolean[], Exception>() {
    @Override
    public void receiveResult(Boolean[] o) {
      reported.add(o);
    }

    @Override
    public void receiveException(Exception e) {
      reported.add(e);
    }
  });

  @Test
  @DisplayName("reports success once, after both inserts succeeded")
  void reportsSuccessAfterBoth() {
    testObj.receiveResult(new Boolean[] {true});
    assertTrue(reported.isEmpty());

    testObj.receiveResult(new Boolean[] {true});

    assertEquals(1, reported.size());
    assertTrue(reported.get(0) instanceof Boolean[]);
  }

  @Test
  @DisplayName("reports a failure once, even if both inserts fail")
  void reportsTheFirstFailureOnly() {
    Exception first = new Exception("first");

    testObj.receiveException(first);
    testObj.receiveException(new Exception("second"));

    assertEquals(List.of(first), reported);
  }

  @Test
  @DisplayName("reports only the failure if one insert succeeded and the other failed")
  void reportsFailureAfterSuccess() {
    Exception failure = new Exception("failed");

    testObj.receiveResult(new Boolean[] {true});
    testObj.receiveException(failure);

    assertEquals(List.of(failure), reported);
  }

  @Test
  @DisplayName("does not report success if one insert failed before the other succeeded")
  void reportsNoSuccessAfterFailure() {
    Exception failure = new Exception("failed");

    testObj.receiveException(failure);
    testObj.receiveResult(new Boolean[] {true});

    assertEquals(List.of(failure), reported);
  }
}
