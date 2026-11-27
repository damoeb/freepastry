package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.slf4j.LoggerFactory;
import rice.environment.logging.LogRateLimiter;

/**
 * C3 (design 3e): the memory of BIG messages still arriving is shared out per peer, pinned peers keep their minimum
 * share, and a message that does not arrive at the minimum rate is closed at its deadline (R4).
 */
class ReceiveBudgetTest {
  private static final long START_MILLIS = 5_000_000;
  private static final int KB = 1024;

  private final ManualReadScheduler scheduler = new ManualReadScheduler(START_MILLIS);
  private final List<String> delivered = new ArrayList<>();

  private PeerMessageWriterContext<String> context(long totalBytes, long minShareBytes, int maxBigMessageBytes, long minRateBytesPerSecond,
      Set<String> pinned) {
    TransportLayerCallback<String, ByteBuffer> callback = new TransportLayerCallback<>() {
      @Override
      public void messageReceived(String identifier, ByteBuffer msg, Map<String, Object> options) {
        delivered.add(identifier);
      }

      @Override
      public void incomingSocket(P2PSocket<String> socket) {}
    };
    PeerBudgetConfig config = new PeerBudgetConfig(1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 4, 10_000, minShareBytes,
        minRateBytesPerSecond, 10_000);
    PeerBudgets budgets = PeerBudgets.of(config, totalBytes, maxBigMessageBytes, scheduler);
    budgets.setPolicy(() -> pinned::contains, peer -> null);
    return new PeerMessageWriterContext<>(null, null, null, null, LoggerFactory.getLogger(ReceiveBudgetTest.class), 100, 10,
        maxBigMessageBytes, List.of(), new TransportListeners<>(), () -> null, () -> callback, () -> false, id -> null,
        LogRateLimiter.withDefaults(), budgets);
  }

  /** A BIG message socket of the peer that has sent the size header and {@code sent} bytes of the message. */
  private static FakeSocket bigMessage(PeerMessageWriterContext<String> ctx, String peer, int announced, int sent) throws Exception {
    FakeSocket socket = new FakeSocket(peer, ByteBuffer.allocate(Integer.BYTES + sent).putInt(announced).array());
    new BigMessageReader<>(ctx).receiveSelectResult(socket, true, false);
    return socket;
  }

  @Test
  @DisplayName("R4: a peer that trickles a 4 MB BIG message at 1 KB/s is closed at its deadline and its memory is released")
  void aTrickledMessageIsClosedAtItsDeadline() throws Exception {
    PeerMessageWriterContext<String> ctx = context(64L * KB * KB, KB * KB, 4 * KB * KB, 16 * KB, Set.of());
    FakeSocket trickler = bigMessage(ctx, "trickler", 4 * KB * KB, 0);
    // 4 MB at the minimum rate of 16 KB/s: 256 s
    long deadlineSeconds = 4L * KB * KB / (16 * KB);

    for (int second = 1; second < deadlineSeconds && !trickler.closed; second++) {
      trickler.feed(new byte[KB]);
      scheduler.advance(1000);
      trickler.select();
    }
    assertFalse(trickler.closed, "open until its deadline");
    assertTrue(ctx.budgets.memory.reserved() > 0);

    for (int second = 0; second < 600 && !trickler.closed; second++) {
      trickler.feed(new byte[KB]);
      scheduler.advance(1000);
      trickler.select();
    }

    assertTrue(trickler.closed, "closed at the deadline");
    assertEquals(0, ctx.budgets.memory.reserved(), "its memory is released");
  }

  @Test
  @DisplayName("a message that arrives at the minimum rate is delivered, and its deadline is cancelled")
  void aMessageInTimeIsDelivered() throws Exception {
    PeerMessageWriterContext<String> ctx = context(64L * KB * KB, KB * KB, 4 * KB * KB, 16 * KB, Set.of());
    FakeSocket sender = bigMessage(ctx, "sender", 64 * KB, 0);

    for (int second = 0; second < 4; second++) {
      sender.feed(new byte[16 * KB]);
      scheduler.advance(1000);
      sender.select();
    }

    assertEquals(List.of("sender"), delivered);
    assertFalse(sender.closed);
    assertEquals(0, scheduler.pendingTimers(), "no deadline left");
  }

  @Test
  @DisplayName("one peer with many sockets cannot hold the whole budget: another peer still gets memory for its message")
  void onePeerCannotHoldTheWholeBudget() throws Exception {
    PeerMessageWriterContext<String> ctx = context(64 * KB, 8 * KB, 24 * KB, 16 * KB, Set.of());
    List<FakeSocket> hog = new ArrayList<>();
    for (int n = 0; n < 4; n++) {
      hog.add(bigMessage(ctx, "hog", 24 * KB, 20 * KB));
    }

    FakeSocket honest = bigMessage(ctx, "honest", 24 * KB, 24 * KB);

    assertFalse(honest.closed);
    assertEquals(List.of("honest"), delivered);
    assertTrue(ctx.budgets.memory.reserved("hog") <= 32 * KB, "the hog holds at most half the budget: " + ctx.budgets.memory.reserved("hog"));
    hog.forEach(socket -> assertFalse(socket.closed, "a socket over its share waits, it is not closed"));
  }

  @Test
  @DisplayName("a pinned peer gets its minimum share even when the budget is used up")
  void aPinnedPeerKeepsItsMinimumShare() {
    ReceiveBudget budget = new ReceiveBudget(64 * KB, 8 * KB, 24 * KB);
    budget.setPinPolicy(() -> "neighbour"::equals);
    assertTrue(budget.tryReserve("a", 24 * KB));
    assertTrue(budget.tryReserve("b", 24 * KB));
    assertTrue(budget.tryReserve("c", 16 * KB));

    assertFalse(budget.tryReserve("d", KB), "the budget is used up");
    assertTrue(budget.tryReserve("neighbour", 8 * KB), "pinned: its minimum share");
    assertFalse(budget.tryReserve("neighbour", KB), "not more");

    budget.release("a", 24 * KB);
    assertTrue(budget.tryReserve("d", KB));
  }
}
