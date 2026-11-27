package org.mpisws.p2p.transport.priority;

import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.limitsockets.SocketPinPolicy;
import org.mpisws.p2p.transport.util.PrefixKey;

/**
 * The budgets the readers of one {@link PriorityTransportLayer} share: messages per peer and pass
 * ({@link PeerReadBudget}), the memory of BIG messages still arriving ({@link ReceiveBudget}) with their deadlines,
 * and the scheduler that resumes paused readers.
 */
final class PeerBudgets<Identifier> {
  /** {@link #receiveDeadlineMillis}: the message has no deadline */
  static final long NO_DEADLINE = -1;

  final ReadScheduler scheduler;
  final PeerReadBudget<Identifier> messages;
  final ReceiveBudget<Identifier> memory;
  private final @Nullable PeerBudgetConfig config;

  private PeerBudgets(ReadScheduler scheduler, PeerReadBudget<Identifier> messages, ReceiveBudget<Identifier> memory,
      @Nullable PeerBudgetConfig config) {
    this.scheduler = scheduler;
    this.messages = messages;
    this.memory = memory;
    this.config = config;
  }

  /** Every message is delivered at once and BIG messages have no deadline; only their total memory is capped. */
  static <Identifier> PeerBudgets<Identifier> unlimited(long receiveBudgetBytes) {
    return new PeerBudgets<>(ReadScheduler.NONE, PeerReadBudget.unlimited(), ReceiveBudget.withoutShares(receiveBudgetBytes), null);
  }

  /**
   * @param receiveBudgetBytes the memory all BIG messages still arriving may hold together
   * @param maxBigMessageBytes the largest BIG message: any peer may hold that much while the total has room
   */
  static <Identifier> PeerBudgets<Identifier> of(PeerBudgetConfig config, long receiveBudgetBytes, long maxBigMessageBytes,
      ReadScheduler scheduler) {
    // not enforced yet: every message at once, one total for the memory, no deadlines
    return unlimited(receiveBudgetBytes);
  }

  /**
   * @param pinPolicy which peers are pinned (leaf set, routing table): they get the pinned factor and their minimum
   *        memory share
   * @param prefixOf the network prefix of a peer, or null if it has none
   */
  void setPolicy(SocketPinPolicy<Identifier> pinPolicy, Function<Identifier, @Nullable PrefixKey> prefixOf) {
    messages.setPolicy(pinPolicy, prefixOf);
    memory.setPinPolicy(pinPolicy);
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
