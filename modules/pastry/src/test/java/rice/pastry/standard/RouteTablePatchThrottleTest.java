package rice.pastry.standard;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The throttle of route table patches (design 3f): once per hop and throttle time, and a bounded memory of hops. */
class RouteTablePatchThrottleTest {
  private static final long THROTTLE_MILLIS = 5_000;
  private static final long NOW_MILLIS = 1_000_000;

  @Test
  @DisplayName("a hop gets one patch per throttle time")
  void oncePerThrottleTime() {
    RouteTablePatchThrottle<String> target = new RouteTablePatchThrottle<>(THROTTLE_MILLIS);

    assertTrue(target.mayPatch("hop", NOW_MILLIS));
    assertFalse(target.mayPatch("hop", NOW_MILLIS + THROTTLE_MILLIS - 1));
    assertTrue(target.mayPatch("hop", NOW_MILLIS + THROTTLE_MILLIS));
  }

  @Test
  @DisplayName("routing for many distinct previous hops does not make the throttle remember them all")
  void theMemoryOfHopsIsBounded() {
    RouteTablePatchThrottle<Integer> target = new RouteTablePatchThrottle<>(THROTTLE_MILLIS);
    int hops = 50_000;

    for (int n = 0; n < hops; n++) {
      target.mayPatch(n, NOW_MILLIS + n);
    }

    assertTrue(target.size() <= 10_000, "remembers " + target.size() + " hops");
  }
}
