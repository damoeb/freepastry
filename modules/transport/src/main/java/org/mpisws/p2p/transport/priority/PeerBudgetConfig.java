package org.mpisws.p2p.transport.priority;

import static rice.environment.params.ConfigChecks.requirePositive;

import rice.environment.params.Parameters;

/**
 * The per-peer budgets of a {@link PriorityTransportLayer} (design 3d and 3e): how many messages a peer, and a network
 * prefix, may have delivered per second, how many messages one selector pass delivers, and how much memory the
 * messages that are still arriving may hold.
 *
 * @param messageRatePerPeerPerSecond {@code pastry_socket_msg_rate_per_peer_per_second}: messages a peer may have
 *        delivered per second; beyond it the layer stops reading the peer's socket until the bucket refills
 * @param messageBurstPerPeer {@code pastry_socket_msg_burst_per_peer}: messages a peer may have delivered at once
 * @param messageRatePerPrefixPerSecond {@code pastry_socket_msg_rate_per_prefix_per_second}: the same for all peers of
 *        one network prefix ({@code transport_prefix_bits_v4}/{@code _v6}) together
 * @param messageBurstPerPrefix {@code pastry_socket_msg_burst_per_prefix}
 * @param maxMessagesPerPass {@code pastry_socket_max_messages_per_pass}: messages the layer delivers in one pass of
 *        the selector loop, over all peers; the rest wait for the next pass
 * @param pinnedRateFactor {@code pastry_socket_pinned_rate_factor}: pinned peers (leaf set, routing table) get this
 *        many times the rate and burst of a peer, and do not draw on the prefix bucket; at least 1
 * @param maxPeers {@code pastry_socket_budget_max_peers}: the peers and prefixes that have a bucket; beyond it the
 *        least recently used one is forgotten
 * @param receiveMinShareBytes {@code pastry_socket_receive_min_share}: a peer may hold
 *        {@code max(minShare, budget / (2 × peers holding memory))} of the receive budget
 *        ({@link PriorityConfig#maxBigMsgInFlight()}); a pinned peer gets its minimum share even when the
 *        budget is used up
 * @param receiveMinRateBytesPerSecond {@code pastry_socket_receive_min_rate_bytes_per_second}: a BIG message must
 *        arrive at least this fast, else its socket is closed at the deadline
 * @param receiveMinDeadlineMillis {@code pastry_socket_receive_min_deadline_millis}: the shortest deadline of a BIG
 *        message
 */
public record PeerBudgetConfig(
    double messageRatePerPeerPerSecond,
    double messageBurstPerPeer,
    double messageRatePerPrefixPerSecond,
    double messageBurstPerPrefix,
    int maxMessagesPerPass,
    double pinnedRateFactor,
    int maxPeers,
    long receiveMinShareBytes,
    long receiveMinRateBytesPerSecond,
    long receiveMinDeadlineMillis) {

  /** @throws IllegalArgumentException if a value is out of range */
  public PeerBudgetConfig {
    positive("pastry_socket_msg_rate_per_peer_per_second", messageRatePerPeerPerSecond);
    atLeastOne("pastry_socket_msg_burst_per_peer", messageBurstPerPeer);
    positive("pastry_socket_msg_rate_per_prefix_per_second", messageRatePerPrefixPerSecond);
    atLeastOne("pastry_socket_msg_burst_per_prefix", messageBurstPerPrefix);
    requirePositive("pastry_socket_max_messages_per_pass", maxMessagesPerPass);
    atLeastOne("pastry_socket_pinned_rate_factor", pinnedRateFactor);
    requirePositive("pastry_socket_budget_max_peers", maxPeers);
    requirePositive("pastry_socket_receive_min_share", receiveMinShareBytes);
    requirePositive("pastry_socket_receive_min_rate_bytes_per_second", receiveMinRateBytesPerSecond);
    requirePositive("pastry_socket_receive_min_deadline_millis", receiveMinDeadlineMillis);
  }

  /** @throws IllegalArgumentException if a value is out of range */
  public static PeerBudgetConfig fromParameters(Parameters parameters) {
    return new PeerBudgetConfig(
        parameters.getDouble("pastry_socket_msg_rate_per_peer_per_second"),
        parameters.getDouble("pastry_socket_msg_burst_per_peer"),
        parameters.getDouble("pastry_socket_msg_rate_per_prefix_per_second"),
        parameters.getDouble("pastry_socket_msg_burst_per_prefix"),
        parameters.getInt("pastry_socket_max_messages_per_pass"),
        parameters.getDouble("pastry_socket_pinned_rate_factor"),
        parameters.getInt("pastry_socket_budget_max_peers"),
        parameters.getLong("pastry_socket_receive_min_share"),
        parameters.getLong("pastry_socket_receive_min_rate_bytes_per_second"),
        parameters.getLong("pastry_socket_receive_min_deadline_millis"));
  }

  /** How long a BIG message of this size may take to arrive. */
  long receiveDeadlineMillis(long announcedBytes) {
    return Math.max(receiveMinDeadlineMillis, announcedBytes * 1000 / receiveMinRateBytesPerSecond);
  }

  private static void positive(String key, double value) {
    if (!(value > 0)) throw new IllegalArgumentException(key + " must be positive: " + value);
  }

  private static void atLeastOne(String key, double value) {
    if (!(value >= 1)) throw new IllegalArgumentException(key + " must be at least 1: " + value);
  }
}
