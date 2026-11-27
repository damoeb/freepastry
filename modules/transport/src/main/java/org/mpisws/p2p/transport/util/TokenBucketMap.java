package org.mpisws.p2p.transport.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One {@link TokenBucket} per key (a remote peer or a {@link PrefixKey}), all with the same rate and burst, in a map
 * bounded to {@code maxKeys} entries. When the map is full, the least recently used key is forgotten; its bucket starts
 * full when the key comes back, so an attacker gains from that only by spreading over more keys than the map holds.
 *
 * <p>Not thread-safe: used on the selector thread only.
 */
public final class TokenBucketMap<K> {
  private final double tokensPerSecond;
  private final double burst;
  private final LinkedHashMap<K, TokenBucket> buckets;

  /**
   * @throws IllegalArgumentException if the rate is not positive, the burst is below 1 or maxKeys is not positive
   */
  public TokenBucketMap(double tokensPerSecond, double burst, int maxKeys) {
    if (maxKeys <= 0) throw new IllegalArgumentException("maxKeys must be positive: " + maxKeys);
    TokenBucket.validate(tokensPerSecond, burst);
    this.tokensPerSecond = tokensPerSecond;
    this.burst = burst;
    this.buckets = new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<K, TokenBucket> eldest) {
        return size() > maxKeys;
      }
    };
  }

  /**
   * Takes one token from the bucket of this key.
   *
   * @return true if the key had a token
   */
  public boolean tryTake(K key, long nowMillis) {
    TokenBucket bucket = buckets.get(key);
    if (bucket == null) {
      bucket = new TokenBucket(tokensPerSecond, burst, nowMillis);
      buckets.put(key, bucket);
    }
    return bucket.tryTake(nowMillis);
  }

  /** The number of keys that have a bucket. */
  public int size() {
    return buckets.size();
  }
}
