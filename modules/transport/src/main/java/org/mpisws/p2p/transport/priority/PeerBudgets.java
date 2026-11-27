package org.mpisws.p2p.transport.priority;

import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.limitsockets.SocketPinPolicy;
import org.mpisws.p2p.transport.util.PrefixKey;
import org.mpisws.p2p.transport.wire.WireTransportLayer;

/**
 * The budgets the readers of one {@link PriorityTransportLayer} share: messages per peer and pass
 * ({@link PeerReadBudget}), the memory of BIG messages still arriving ({@link ReceiveBudget}) with their deadlines,
 * and the scheduler that resumes paused readers.
 */
final class PeerBudgets {
  /** {@link #receiveDeadlineMillis}: the message has no deadline */
  static final long NO_DEADLINE = -1;

  final ReadScheduler scheduler;
  final PeerReadBudget messages;
  final ReceiveBudget memory;
  private final @Nullable PeerBudgetConfig config;

  private PeerBudgets(ReadScheduler scheduler, PeerReadBudget messages, ReceiveBudget memory,
      @Nullable PeerBudgetConfig config) {
    this.scheduler = scheduler;
    this.messages = messages;
    this.memory = memory;
    this.config = config;
  }

  /** Every message is delivered at once and BIG messages have no deadline; only their total memory is capped. */
  static PeerBudgets unlimited(long receiveBudgetBytes) {
    return new PeerBudgets(ReadScheduler.NONE, PeerReadBudget.unlimited(), ReceiveBudget.withoutShares(receiveBudgetBytes), null);
  }

  /**
   * @param receiveBudgetBytes the memory all BIG messages still arriving may hold together
   * @param maxBigMessageBytes the largest BIG message: any peer may hold that much while the total has room
   */
  static PeerBudgets of(PeerBudgetConfig config, long receiveBudgetBytes, long maxBigMessageBytes,
      ReadScheduler scheduler) {
    return new PeerBudgets(scheduler, PeerReadBudget.of(config, scheduler),
        new ReceiveBudget(receiveBudgetBytes, config.receiveMinShareBytes(), maxBigMessageBytes), config);
  }

  /**
   * @param pinPolicy which peers ({@link #peerKey}) are pinned (leaf set, routing table): they get the pinned factor
   *        and their minimum memory share
   * @param prefixOf the network prefix of a peer ({@link #peerKey}), or null if it has none
   */
  void setPolicy(SocketPinPolicy<Object> pinPolicy, Function<Object, @Nullable PrefixKey> prefixOf) {
    messages.setPolicy(pinPolicy, prefixOf);
    memory.setPinPolicy(pinPolicy);
  }

  /**
   * The peer whose budgets a socket's messages draw on: its identifier. The wire layer also passes up where the
   * socket really connects to ({@link WireTransportLayer#OPTION_REMOTE_ADDRESS}).
   */
  static Object peerKey(P2PSocket<?> socket) {
    return socket.getIdentifier();
  }

  /** Whether a reader may pause and be resumed by the scheduler; an unlimited budget has no scheduler. */
  boolean canWait() {
    return config != null;
  }

  /** How long a BIG message of this size may take to arrive, or {@link #NO_DEADLINE}. */
  long receiveDeadlineMillis(long announcedBytes) {
    PeerBudgetConfig config = this.config;
    return config == null ? NO_DEADLINE : config.receiveDeadlineMillis(announcedBytes);
  }
}
