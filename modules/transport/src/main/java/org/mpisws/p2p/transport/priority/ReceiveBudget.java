package org.mpisws.p2p.transport.priority;

import java.util.HashMap;
import java.util.Map;
import org.mpisws.p2p.transport.limitsockets.SocketPinPolicy;

/**
 * Caps the memory that the {@link BigMessageReader}s of one node hold for messages that are still arriving (design
 * 3e), so that many sockets which each announce a big message cannot exhaust the heap together, and one peer cannot
 * hold the whole budget: a peer may hold at most {@code max(minShare, messageFloor, total / (2 × peers holding
 * memory))}, where the message floor is the largest BIG message, so that a peer can always finish one message when the
 * total has room. A pinned peer (leaf set, routing table) may always hold its minimum share, even beyond the total;
 * the total is therefore exceeded by at most {@code minShare} per pinned peer.
 *
 * <p>Selector thread only.
 */
final class ReceiveBudget {
  private final long totalBytes;
  private final long minShareBytes;
  private final long messageFloorBytes;
  private final boolean hasShares;
  private final Map<Object, Long> reservedByPeer = new HashMap<>();
  private long reservedBytes;
  private SocketPinPolicy<Object> pinPolicy = () -> peer -> false;

  /**
   * @param totalBytes the memory all peers may hold together
   * @param minShareBytes the memory any peer may hold if the total allows it, and a pinned peer always
   * @param messageFloorBytes the memory any peer may hold if the total allows it: the largest BIG message
   */
  ReceiveBudget(long totalBytes, long minShareBytes, long messageFloorBytes) {
    this(totalBytes, minShareBytes, messageFloorBytes, true);
  }

  private ReceiveBudget(long totalBytes, long minShareBytes, long messageFloorBytes, boolean hasShares) {
    this.totalBytes = totalBytes;
    this.minShareBytes = Math.min(minShareBytes, totalBytes);
    this.messageFloorBytes = Math.min(messageFloorBytes, totalBytes);
    this.hasShares = hasShares;
  }

  /** A budget with one total for all peers together and no per-peer shares, as before design 3e. */
  static ReceiveBudget withoutShares(long totalBytes) {
    return new ReceiveBudget(totalBytes, 0, 0, false);
  }

  void setPinPolicy(SocketPinPolicy<Object> pinPolicy) {
    this.pinPolicy = pinPolicy;
  }

  /**
   * @return false, and reserves nothing, if {@code bytes} more would exceed the peer's share or the total
   */
  boolean tryReserve(Object peer, long bytes) {
    long held = reservedByPeer.getOrDefault(peer, 0L);
    int peersHoldingMemory = reservedByPeer.size() + (held == 0 ? 1 : 0);
    boolean fits = held + bytes <= shareBytes(peersHoldingMemory) && reservedBytes + bytes <= totalBytes;
    // the pin policy is asked only when it matters
    if (!fits && !(held + bytes <= minShareBytes && pinPolicy.pinnedIdentifiers().test(peer))) return false;
    reservedByPeer.put(peer, held + bytes);
    reservedBytes += bytes;
    return true;
  }

  /** Gives back what {@link #tryReserve} took for the peer. */
  void release(Object peer, long bytes) {
    if (bytes == 0) return;
    long held = reservedByPeer.getOrDefault(peer, 0L) - bytes;
    if (held <= 0) {
      reservedByPeer.remove(peer);
    } else {
      reservedByPeer.put(peer, held);
    }
    reservedBytes -= bytes;
  }

  /** The most a peer may hold while this many peers hold memory. */
  long shareBytes(int peersHoldingMemory) {
    if (!hasShares) return totalBytes;
    return Math.max(Math.max(minShareBytes, messageFloorBytes), totalBytes / (2L * peersHoldingMemory));
  }

  /** The bytes reserved by all peers. */
  long reserved() {
    return reservedBytes;
  }

  /** The bytes reserved by one peer. */
  long reserved(Object peer) {
    return reservedByPeer.getOrDefault(peer, 0L);
  }
}
