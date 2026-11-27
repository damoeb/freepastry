package org.mpisws.p2p.transport.util;

/**
 * A token bucket: it holds at most {@code burst} tokens, starts full, and refills at {@code tokensPerSecond}. Each
 * {@link #tryTake(long)} takes one token if there is one.
 *
 * <p>The caller passes the current time, read from the environment's {@code TimeSource}, so that tests can drive the
 * bucket with a manual clock. A clock that goes backwards refills nothing. Not thread-safe: every bucket of the socket
 * stack is used on the selector thread only.
 */
public final class TokenBucket {
  private static final double MILLIS_PER_SECOND = 1000.0;

  private final double tokensPerMilli;
  private final double burst;
  private double tokens;
  private long lastRefillMillis;

  /**
   * @param tokensPerSecond the refill rate, positive
   * @param burst the most tokens the bucket holds, at least 1
   * @param nowMillis the current time; the bucket starts full
   * @throws IllegalArgumentException if the rate is not positive or the burst is below 1
   */
  public TokenBucket(double tokensPerSecond, double burst, long nowMillis) {
    validate(tokensPerSecond, burst);
    this.tokensPerMilli = tokensPerSecond / MILLIS_PER_SECOND;
    this.burst = burst;
    this.tokens = burst;
    this.lastRefillMillis = nowMillis;
  }

  /** @throws IllegalArgumentException if the rate is not positive or the burst is below 1 */
  static void validate(double tokensPerSecond, double burst) {
    if (!(tokensPerSecond > 0)) throw new IllegalArgumentException("tokensPerSecond must be positive: " + tokensPerSecond);
    if (!(burst >= 1)) throw new IllegalArgumentException("burst must be at least 1: " + burst);
  }

  /**
   * Takes one token.
   *
   * @return true if there was a token, false if the bucket is empty (nothing is taken then)
   */
  public boolean tryTake(long nowMillis) {
    refill(nowMillis);
    if (tokens < 1) return false;
    tokens -= 1;
    return true;
  }

  /** The tokens in the bucket now, after refilling; fractions count toward the next token. */
  public double availableTokens(long nowMillis) {
    refill(nowMillis);
    return tokens;
  }

  /** Whether the bucket is full: it has refilled every token taken, so forgetting it loses nothing. */
  public boolean isFull(long nowMillis) {
    return availableTokens(nowMillis) >= burst;
  }

  private void refill(long nowMillis) {
    if (nowMillis <= lastRefillMillis) return;
    tokens = Math.min(burst, tokens + (nowMillis - lastRefillMillis) * tokensPerMilli);
    lastRefillMillis = nowMillis;
  }

  @Override
  public String toString() {
    return "TokenBucket[" + tokens + "/" + burst + ", " + tokensPerMilli * MILLIS_PER_SECOND + "/s]";
  }
}
