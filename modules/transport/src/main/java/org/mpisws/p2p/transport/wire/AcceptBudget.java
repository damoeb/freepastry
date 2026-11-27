package org.mpisws.p2p.transport.wire;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.util.PrefixKey;
import org.mpisws.p2p.transport.util.TokenBucket;
import org.mpisws.p2p.transport.util.TokenBucketMap;

/**
 * Decides whether the wire layer keeps a socket it just accepted (C1, design 3a). It refuses one when the node already
 * has {@code transport_wire_max_handshaking_sockets} sockets in the handshake, when the network prefix of the peer has
 * {@code transport_wire_max_handshaking_sockets_per_prefix}, when the prefix used up its accept bucket, or when all
 * peers together used up the global one. A peer's prefix is checked before the global bucket, so that one network that
 * floods empties its own bucket and not the one every other network shares.
 *
 * <p>The caller passes the time of the environment's {@code TimeSource}. Synchronized, though used on the selector
 * thread: a socket may end its handshake from another thread.
 */
final class AcceptBudget {
  private final int maxHandshakingSocketsPerPrefix;
  private final int maxHandshakingSockets;
  private final PrefixKey.Lengths prefixLengths;
  /** How many accepted sockets from each prefix are still in the handshake; at most maxHandshakingSockets keys. */
  private final Map<PrefixKey, Integer> handshakingSocketCountByPrefix = new HashMap<>();
  private int handshakingSocketCount = 0;
  private final TokenBucketMap<PrefixKey> acceptBucketsByPrefix;
  private final TokenBucket acceptBucket;

  AcceptBudget(WireConfig config, long nowMillis) {
    this.maxHandshakingSocketsPerPrefix = config.maxHandshakingSocketsPerPrefix();
    this.maxHandshakingSockets = config.maxHandshakingSockets();
    this.prefixLengths = config.prefixLengths();
    this.acceptBucketsByPrefix = new TokenBucketMap<>(config.acceptRatePerPrefixPerSecond(), config.acceptBurstPerPrefix(), config.maxBudgetPrefixes());
    this.acceptBucket = new TokenBucket(config.acceptRatePerSecond(), config.acceptBurst(), nowMillis);
  }

  /**
   * Counts a newly accepted socket from this address into the handshake, unless a limit refuses it.
   *
   * @return null if the socket may stay, else why it must be closed
   */
  synchronized @Nullable String startHandshake(InetAddress remoteAddress, long nowMillis) {
    if (handshakingSocketCount >= maxHandshakingSockets) {
      return "the node has " + maxHandshakingSockets + " sockets in the handshake";
    }
    PrefixKey prefix = prefixLengths.of(remoteAddress);
    int handshakingSockets = handshakingSocketCountByPrefix.getOrDefault(prefix, 0);
    if (handshakingSockets >= maxHandshakingSocketsPerPrefix) {
      return prefix + " has " + maxHandshakingSocketsPerPrefix + " sockets in the handshake";
    }
    if (!acceptBucketsByPrefix.tryTake(prefix, nowMillis)) {
      return prefix + " opened sockets faster than its accept budget";
    }
    if (!acceptBucket.tryTake(nowMillis)) {
      return "all peers opened sockets faster than the accept budget of the node";
    }
    handshakingSocketCountByPrefix.put(prefix, handshakingSockets + 1);
    handshakingSocketCount++;
    return null;
  }

  /** An incoming socket from this address, counted by {@link #startHandshake}, finished its handshake or closed. */
  synchronized void handshakeEnded(InetAddress remoteAddress) {
    PrefixKey prefix = prefixLengths.of(remoteAddress);
    Integer handshakingSockets = handshakingSocketCountByPrefix.get(prefix);
    if (handshakingSockets == null) return;
    if (handshakingSockets > 1) {
      handshakingSocketCountByPrefix.put(prefix, handshakingSockets - 1);
    } else {
      handshakingSocketCountByPrefix.remove(prefix);
    }
    handshakingSocketCount--;
  }

  /** The sockets of all peers in the handshake. */
  synchronized int getHandshakingSocketCount() {
    return handshakingSocketCount;
  }
}
