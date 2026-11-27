package rice.pastry.standard;

import java.util.HashMap;
import java.util.Map;

/**
 * Remembers when a route table patch (a row sent to a previous hop that lacked an entry) last went to each previous
 * hop, so that one goes to a hop at most once per {@code throttleMillis}: blasting rows can cause a feedback loop.
 */
final class RouteTablePatchThrottle<K> {
  private final long throttleMillis;
  private final Map<K, Long> lastPatchMillis = new HashMap<>();

  RouteTablePatchThrottle(long throttleMillis) {
    this.throttleMillis = throttleMillis;
  }

  /**
   * @return true, and remembers the time, if no patch went to the hop within the throttle time
   */
  synchronized boolean mayPatch(K previousHop, long nowMillis) {
    Long lastMillis = lastPatchMillis.get(previousHop);
    if (lastMillis != null && lastMillis > nowMillis - throttleMillis) return false;
    lastPatchMillis.put(previousHop, nowMillis);
    return true;
  }

  /** The previous hops remembered. */
  synchronized int size() {
    return lastPatchMillis.size();
  }
}
