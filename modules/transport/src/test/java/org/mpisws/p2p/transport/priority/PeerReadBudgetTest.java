package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.PrefixKey;
import org.slf4j.LoggerFactory;
import rice.environment.logging.LogRateLimiter;

/**
 * C2 (design 3d): a peer's messages are delivered within its token bucket, its prefix's bucket and the per-pass cap;
 * beyond them the reader stops reading the peer's socket (TCP backpressure) and resumes later, dropping nothing.
 */
class PeerReadBudgetTest {
  private static final long START_MILLIS = 5_000_000;
  private static final int UNLIMITED = 1_000_000;
  private static final PrefixKey.Lengths PREFIX_LENGTHS = new PrefixKey.Lengths(24, 64);

  private final ManualReadScheduler scheduler = new ManualReadScheduler(START_MILLIS);
  private final Map<String, Integer> delivered = new HashMap<>();

  private PeerMessageWriterContext<String> context(PeerBudgetConfig config, Set<String> pinned, Map<String, String> addresses) {
    TransportLayerCallback<String, ByteBuffer> callback = new TransportLayerCallback<>() {
      @Override
      public void messageReceived(String identifier, ByteBuffer msg, Map<String, Object> options) {
        delivered.merge(identifier, 1, Integer::sum);
      }

      @Override
      public void incomingSocket(P2PSocket<String> socket) {}
    };
    PeerBudgets budgets = PeerBudgets.of(config, 64L * 1024 * 1024, 4 * 1024 * 1024, scheduler);
    budgets.setPolicy(() -> pinned::contains, peer -> {
      String address = addresses.get(peer);
      return address == null ? null : PREFIX_LENGTHS.of(inet(address));
    });
    return new PeerMessageWriterContext<>(null, null, null, null, LoggerFactory.getLogger(PeerReadBudgetTest.class), 100, 10,
        1024, List.of(), new TransportListeners<>(), () -> null, () -> callback, () -> false, id -> null,
        LogRateLimiter.withDefaults(), budgets);
  }

  private static PeerBudgetConfig config(double peerRate, double peerBurst, double prefixRate, double prefixBurst, int perPass,
      double pinnedFactor) {
    return new PeerBudgetConfig(peerRate, peerBurst, prefixRate, prefixBurst, perPass, pinnedFactor, 10_000, 1024 * 1024,
        16 * 1024, 10_000);
  }

  private static InetAddress inet(String address) {
    try {
      return InetAddress.getByName(address);
    } catch (IOException impossible) {
      throw new IllegalArgumentException(address, impossible);
    }
  }

  /** {@code count} framed one-byte messages on a primary socket of the peer, with a reader waiting for the first. */
  private static FakeSocket primarySocket(PeerMessageWriterContext<String> ctx, String peer, int count) {
    ByteBuffer wire = ByteBuffer.allocate(count * (Integer.BYTES + 1));
    for (int n = 0; n < count; n++) {
      wire.putInt(1).put((byte) n);
    }
    FakeSocket socket = new FakeSocket(peer, wire.array());
    new SizeReader<>(new PeerMessageWriter<>(peer, ctx), socket);
    return socket;
  }

  /** Plays selector passes, without moving the clock: each pass first runs what waits for it, then reads the sockets. */
  private void passes(int count, FakeSocket... sockets) throws IOException {
    for (int pass = 0; pass < count; pass++) {
      scheduler.startPass();
      for (FakeSocket socket : sockets) {
        // a socket reads as long as it has data and the reader asks for more, as the selector would over passes
        for (int read = 0; read < 2 && socket.registeredForRead; read++) socket.select();
      }
    }
  }

  private int delivered(String peer) {
    return delivered.getOrDefault(peer, 0);
  }

  @Test
  @DisplayName("a peer that floods gets its burst delivered, then its socket waits for the bucket: nothing is dropped or closed")
  void aFloodingPeerIsPausedNotDropped() throws Exception {
    PeerMessageWriterContext<String> ctx = context(config(10, 20, UNLIMITED, UNLIMITED, UNLIMITED, 4), Set.of(), Map.of());
    FakeSocket flooder = primarySocket(ctx, "flooder", 1000);

    passes(500, flooder);

    assertEquals(20, delivered("flooder"), "the burst, then the reader waits");
    assertFalse(flooder.registeredForRead, "the reader does not read the socket on while it waits");

    scheduler.advance(1000);
    passes(500, flooder);

    assertEquals(30, delivered("flooder"), "one second refills ten tokens");
    for (int second = 0; second < 100; second++) {
      scheduler.advance(1000);
      passes(20, flooder);
    }
    assertEquals(1000, delivered("flooder"), "every message arrives in the end");
    assertFalse(flooder.closed);
  }

  @Test
  @DisplayName("while one peer floods, another peer's messages are delivered at once")
  void anotherPeerIsNotSlowedByTheFlooder() throws Exception {
    PeerMessageWriterContext<String> ctx = context(config(10, 20, UNLIMITED, UNLIMITED, UNLIMITED, 4), Set.of(), Map.of());
    FakeSocket flooder = primarySocket(ctx, "flooder", 1000);
    FakeSocket honest = primarySocket(ctx, "honest", 5);

    passes(50, flooder, honest);

    assertEquals(5, delivered("honest"));
    assertEquals(20, delivered("flooder"));
  }

  @Test
  @DisplayName("a pinned peer (leaf set, routing table) gets the pinned factor times the burst")
  void aPinnedPeerGetsThePinnedFactor() throws Exception {
    PeerMessageWriterContext<String> ctx = context(config(10, 20, UNLIMITED, UNLIMITED, UNLIMITED, 4), Set.of("neighbour"), Map.of());
    FakeSocket neighbour = primarySocket(ctx, "neighbour", 1000);

    passes(500, neighbour);

    assertEquals(80, delivered("neighbour"));
  }

  @Test
  @DisplayName("the peers of one network prefix share one bucket; a pinned peer of that prefix does not wait for it")
  void peersOfOnePrefixShareABucket() throws Exception {
    Map<String, String> addresses = Map.of("a", "192.0.2.1", "b", "192.0.2.2", "neighbour", "192.0.2.3", "other", "198.51.100.1");
    PeerMessageWriterContext<String> ctx = context(config(100, 20, 100, 30, UNLIMITED, 4), Set.of("neighbour"), addresses);
    FakeSocket a = primarySocket(ctx, "a", 100);
    FakeSocket b = primarySocket(ctx, "b", 100);

    passes(500, a, b);
    FakeSocket neighbour = primarySocket(ctx, "neighbour", 10);
    FakeSocket other = primarySocket(ctx, "other", 10);
    passes(50, neighbour, other);

    assertEquals(30, delivered("a") + delivered("b"), "the prefix's burst, shared");
    assertEquals(10, delivered("neighbour"), "pinned: its own bucket only");
    assertEquals(10, delivered("other"), "another prefix has its own bucket");
  }

  @Test
  @DisplayName("one selector pass delivers at most the per-pass cap; the rest are delivered in the next pass")
  void thePassCapDefersTheRestToTheNextPass() throws Exception {
    PeerMessageWriterContext<String> ctx = context(config(UNLIMITED, UNLIMITED, UNLIMITED, UNLIMITED, 5, 4), Set.of(), Map.of());
    FakeSocket first = primarySocket(ctx, "first", 10);
    FakeSocket second = primarySocket(ctx, "second", 10);
    FakeSocket third = primarySocket(ctx, "third", 10);

    // one pass, in which every socket reads all it has
    for (FakeSocket socket : List.of(first, second, third)) {
      for (int read = 0; read < 100 && socket.registeredForRead; read++) socket.select();
    }
    assertEquals(5, delivered("first") + delivered("second") + delivered("third"));

    passes(20, first, second, third);
    assertEquals(30, delivered("first") + delivered("second") + delivered("third"));
  }
}
