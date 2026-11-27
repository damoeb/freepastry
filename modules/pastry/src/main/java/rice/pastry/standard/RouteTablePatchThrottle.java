package rice.pastry.standard;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Remembers when a route table patch (a row sent to a previous hop that lacked an entry) last went to each previous
 * hop, so that one goes to a hop at most once per {@code throttleMillis}: blasting rows can cause a feedback loop. It
 * remembers at most {@code maxHops} hops (design 3f); beyond them the least recently patched one is forgotten, which
 * costs at most an early patch to it.
 */
final class RouteTablePatchThrottle<K> {
  /** the hops remembered when none is configured, as {@code pastry_budget_max_peers} */
  static final int DEFAULT_MAX_HOPS = 10_000;

  private final long throttleMillis;
  private final Map<K, Long> lastPatchMillis;

  RouteTablePatchThrottle(long throttleMillis) {
    this(throttleMillis, DEFAULT_MAX_HOPS);
  }

  /** @throws IllegalArgumentException if maxHops is not positive */
  RouteTablePatchThrottle(long throttleMillis, int maxHops) {
    if (maxHops <= 0) throw new IllegalArgumentException("maxHops must be positive: " + maxHops);
    this.throttleMillis = throttleMillis;
    this.lastPatchMillis = new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<K, Long> eldest) {
        return size() > maxHops;
      }
    };
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
