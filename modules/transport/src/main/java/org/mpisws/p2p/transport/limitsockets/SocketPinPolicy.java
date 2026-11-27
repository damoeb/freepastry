package org.mpisws.p2p.transport.limitsockets;

import java.util.function.Predicate;

/**
 * Tells the {@link LimitSocketsTransportLayer} which established sockets it must never close to make room for a
 * newcomer: those of peers the node depends on (its leaf set and routing table). The layer only sees the identifiers
 * of its level (addresses), so the node maps them to its peers.
 */
@FunctionalInterface
public interface SocketPinPolicy<Identifier> {
  /**
   * Called on the selector thread, once per decision; the layer then tests every candidate against the answer, so the
   * answer may be a snapshot that is cheap to test.
   *
   * @return whether a socket with this identifier is pinned now
   */
  Predicate<Identifier> pinnedIdentifiers();
}
