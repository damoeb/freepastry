package org.mpisws.p2p.transport.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TokenBucketTest {
  private static final long START_MILLIS = 1_000_000;

  @Test
  @DisplayName("a new bucket gives its burst at once, then nothing until time passes")
  void startsFullAndEmpties() {
    TokenBucket bucket = new TokenBucket(10, 3, START_MILLIS);

    assertTrue(bucket.tryTake(START_MILLIS));
    assertTrue(bucket.tryTake(START_MILLIS));
    assertTrue(bucket.tryTake(START_MILLIS));
    assertFalse(bucket.tryTake(START_MILLIS));
  }

  @Test
  @DisplayName("an empty bucket refills at its rate and never beyond its burst")
  void refillsAtRateUpToBurst() {
    TokenBucket bucket = new TokenBucket(10, 3, START_MILLIS);
    for (int i = 0; i < 3; i++) bucket.tryTake(START_MILLIS);

    assertFalse(bucket.tryTake(START_MILLIS + 99), "less than one token after 99 ms at 10/s");
    assertTrue(bucket.tryTake(START_MILLIS + 100), "one token after 100 ms at 10/s");
    assertEquals(3, bucket.availableTokens(START_MILLIS + 60_000), 1e-9);
    assertTrue(bucket.isFull(START_MILLIS + 60_000));
  }

  @Test
  @DisplayName("a clock that goes backwards refills nothing")
  void clockGoingBackwardsRefillsNothing() {
    TokenBucket bucket = new TokenBucket(10, 1, START_MILLIS);
    assertTrue(bucket.tryTake(START_MILLIS));

    assertFalse(bucket.tryTake(START_MILLIS - 10_000));
    assertTrue(bucket.tryTake(START_MILLIS + 100));
  }

  @Test
  @DisplayName("a rate that is not positive or a burst below one is refused")
  void refusesBadSettings() {
    assertThrows(IllegalArgumentException.class, () -> new TokenBucket(0, 1, START_MILLIS));
    assertThrows(IllegalArgumentException.class, () -> new TokenBucket(Double.NaN, 1, START_MILLIS));
    assertThrows(IllegalArgumentException.class, () -> new TokenBucket(1, 0.5, START_MILLIS));
    assertThrows(IllegalArgumentException.class, () -> new TokenBucketMap<String>(1, 1, 0));
  }

  @Test
  @DisplayName("each key has its own bucket, and the map forgets the least recently used key beyond its bound")
  void mapKeepsOneBucketPerKeyBounded() {
    TokenBucketMap<String> buckets = new TokenBucketMap<>(1, 1, 2);

    assertTrue(buckets.tryTake("a", START_MILLIS));
    assertFalse(buckets.tryTake("a", START_MILLIS));
    assertTrue(buckets.tryTake("b", START_MILLIS), "another key is not affected");
    assertTrue(buckets.tryTake("c", START_MILLIS));

    assertEquals(2, buckets.size());
    assertTrue(buckets.tryTake("a", START_MILLIS), "the forgotten key starts with a full bucket");
  }
}
