package org.mpisws.p2p.transport.priority;

import java.util.function.Function;
import java.util.function.Predicate;
import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.limitsockets.SocketPinPolicy;
import org.mpisws.p2p.transport.util.PrefixKey;
import org.mpisws.p2p.transport.util.TokenBucket;
import org.mpisws.p2p.transport.util.TokenBucketMap;

/**
 * Decides whether a complete message of a peer is delivered now (design 3d). A peer is the key
 * {@link PeerBudgets#peerKey} gives its socket. Each peer has a token bucket, each network
 * prefix one more that all its peers share, and one selector pass delivers at most
 * {@link PeerBudgetConfig#maxMessagesPerPass()} messages. A reader whose message is not admitted holds it and stops
 * reading its socket until {@link #resumeLater} runs it again: TCP backpressure slows the sender, nothing is dropped and
 * the other peers go on. Pinned peers (leaf set, routing table) have an extra bucket of
 * {@code (pinnedRateFactor - 1)} times the rate and burst and do not wait for their prefix's bucket, so a flood from
 * their network does not throttle them; their allowance is never zero.
 *
 * <p>Selector thread only.
 */
final class PeerReadBudget {
  /** {@link #admit}: deliver the message now */
  static final long DELIVER_NOW = 0;
  /** {@link #admit}: the pass is full, ask again in the next selector pass */
  static final long NEXT_PASS = -1;

  private final @Nullable PeerBudgetConfig config;
  private final ReadScheduler scheduler;
  private final @Nullable TokenBucketMap<Object> peerBuckets;
  private final @Nullable TokenBucketMap<PrefixKey> prefixBuckets;
  private final @Nullable TokenBucketMap<Object> pinnedExtraBuckets;
  private SocketPinPolicy<Object> pinPolicy = () -> peer -> false;
  private Function<Object, @Nullable PrefixKey> prefixOf = peer -> null;

  private int messagesThisPass;
  private boolean isPassResetScheduled;
  /** the pin policy's answer for this pass, asked for only when a peer's bucket is empty */
  private @Nullable Predicate<Object> pinnedThisPass;

  private PeerReadBudget(@Nullable PeerBudgetConfig config, ReadScheduler scheduler) {
    this.config = config;
    this.scheduler = scheduler;
    if (config == null) {
      peerBuckets = null;
      prefixBuckets = null;
      pinnedExtraBuckets = null;
    } else {
      peerBuckets = new TokenBucketMap<>(config.messageRatePerPeerPerSecond(), config.messageBurstPerPeer(), config.maxPeers());
      prefixBuckets = new TokenBucketMap<>(config.messageRatePerPrefixPerSecond(), config.messageBurstPerPrefix(), config.maxPeers());
      double extra = config.pinnedRateFactor() - 1;
      pinnedExtraBuckets = extra * config.messageBurstPerPeer() >= 1
          ? new TokenBucketMap<>(extra * config.messageRatePerPeerPerSecond(), extra * config.messageBurstPerPeer(), config.maxPeers())
          : null;
    }
  }

  /** A budget that admits every message at once. */
  static PeerReadBudget unlimited() {
    return new PeerReadBudget(null, ReadScheduler.NONE);
  }

  static PeerReadBudget of(PeerBudgetConfig config, ReadScheduler scheduler) {
    return new PeerReadBudget(config, scheduler);
  }

  void setPolicy(SocketPinPolicy<Object> pinPolicy, Function<Object, @Nullable PrefixKey> prefixOf) {
    this.pinPolicy = pinPolicy;
    this.prefixOf = prefixOf;
  }

  /**
   * Takes a message of the peer from the budget.
   *
   * @return {@link #DELIVER_NOW} if the message may be delivered (its tokens are taken), {@link #NEXT_PASS} if this
   *         pass delivered enough, else the millis after which a token is there
   */
  long admit(Object peer) {
    PeerBudgetConfig config = this.config;
    TokenBucketMap<Object> peerBuckets = this.peerBuckets;
    TokenBucketMap<PrefixKey> prefixBuckets = this.prefixBuckets;
    if (config == null || peerBuckets == null || prefixBuckets == null) return DELIVER_NOW;
    if (messagesThisPass >= config.maxMessagesPerPass()) return NEXT_PASS;
    long now = scheduler.nowMillis();
    TokenBucket peerBucket = peerBuckets.bucket(peer, now);
    PrefixKey prefix = prefixOf.apply(peer);
    TokenBucket prefixBucket = prefix == null ? null : prefixBuckets.bucket(prefix, now);
    boolean hasPeerToken = peerBucket.availableTokens(now) >= 1;
    boolean hasPrefixToken = prefixBucket == null || prefixBucket.availableTokens(now) >= 1;
    if (hasPeerToken && hasPrefixToken) {
      peerBucket.tryTake(now);
      if (prefixBucket != null) prefixBucket.tryTake(now);
      return delivered();
    }
    if (isPinned(peer)) {
      if (hasPeerToken) {
        peerBucket.tryTake(now);
        return delivered();
      }
      TokenBucketMap<Object> pinnedExtraBuckets = this.pinnedExtraBuckets;
      if (pinnedExtraBuckets == null) return peerBucket.millisUntilToken(now);
      TokenBucket extraBucket = pinnedExtraBuckets.bucket(peer, now);
      if (extraBucket.tryTake(now)) return delivered();
      return Math.min(peerBucket.millisUntilToken(now), extraBucket.millisUntilToken(now));
    }
    long wait = peerBucket.millisUntilToken(now);
    if (prefixBucket != null) wait = Math.max(wait, prefixBucket.millisUntilToken(now));
    return Math.max(1, wait);
  }

  /** Runs the task again when {@link #admit} said so: in the next pass or after the delay. */
  void resumeLater(long wait, Runnable task) {
    if (wait == NEXT_PASS) {
      scheduler.nextPass(task);
    } else {
      scheduler.after(wait, task);
    }
  }

  private long delivered() {
    messagesThisPass++;
    if (!isPassResetScheduled) {
      isPassResetScheduled = true;
      // runs before the readers that wait for the next pass: they were scheduled after it
      scheduler.nextPass(this::startPass);
    }
    return DELIVER_NOW;
  }

  private void startPass() {
    messagesThisPass = 0;
    isPassResetScheduled = false;
    pinnedThisPass = null;
  }

  private boolean isPinned(Object peer) {
    Predicate<Object> pinned = pinnedThisPass;
    if (pinned == null) {
      pinned = pinPolicy.pinnedIdentifiers();
      pinnedThisPass = pinned;
      if (!isPassResetScheduled) {
        isPassResetScheduled = true;
        scheduler.nextPass(this::startPass);
      }
    }
    return pinned.test(peer);
  }

  /** The messages delivered in this pass, for tests. */
  int messagesThisPass() {
    return messagesThisPass;
  }
}
