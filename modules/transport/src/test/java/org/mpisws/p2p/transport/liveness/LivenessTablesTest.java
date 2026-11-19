package org.mpisws.p2p.transport.liveness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_ALIVE;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_SUSPECTED;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import rice.environment.Environment;

/**
 * The liveness layer keeps one tracker per route. A remote sender chooses the routes its datagrams arrive on, so the
 * table must stay bounded, and a pong this node never asked for must not make a route look alive.
 */
class LivenessTablesTest {
  private static final InetSocketAddress LOCAL = new InetSocketAddress(InetAddress.getLoopbackAddress(), 1);
  private static final InetSocketAddress PEER = new InetSocketAddress(InetAddress.getLoopbackAddress(), 2);
  private static final int MAX_TRACKERS = 100;
  private static final int SPOOFED_ROUTES = 10 * MAX_TRACKERS;
  private static final long TIMEOUT_SECONDS = 10;

  private Environment env;
  private LivenessTransportLayer<InetSocketAddress> target;
  /** the datagrams the layer sent to the layer below */
  private final List<ByteBuffer> sent = new CopyOnWriteArrayList<>();
  /** the liveness notifications the layer gave */
  private final List<Integer> notified = new CopyOnWriteArrayList<>();

  @BeforeEach
  @SuppressWarnings({"unchecked", "ReferenceEquality"})
  void setUp() {
    env = Environment.directEnvironment(1);
    env.getParameters().setInt("pastry_socket_liveness_max_trackers", MAX_TRACKERS);
    TransportLayer<InetSocketAddress, ByteBuffer> lower = (TransportLayer<InetSocketAddress, ByteBuffer>) Proxy
        .newProxyInstance(LivenessTablesTest.class.getClassLoader(), new Class<?>[] {TransportLayer.class},
            (proxy, method, args) -> switch (method.getName()) {
              case "getLocalIdentifier" -> LOCAL;
              case "sendMessage" -> {
                sent.add(((ByteBuffer) args[1]).duplicate());
                yield null;
              }
              case "hashCode" -> System.identityHashCode(proxy);
              case "equals" -> proxy == args[0];
              case "toString" -> "lower";
              default -> null;
            });
    target = new LivenessTransportLayer<>(lower, env, null, 0);
    target.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress address, ByteBuffer msg, Map<String, Object> options) {
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> socket) {
      }
    });
    target.addLivenessListener((i, liveness, options) -> notified.add(liveness));
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private interface SelectorAction {
    void run() throws Exception;
  }

  /** Runs the action on the selector thread, where the layer receives its datagrams. */
  private void onSelector(SelectorAction action) throws Exception {
    CompletableFuture<Void> completed = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> {
      try {
        action.run();
        completed.complete(null);
      } catch (Throwable throwable) {
        completed.completeExceptionally(throwable);
      }
    });
    completed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private static ByteBuffer pong(long sentTime) {
    ByteBuffer pong = ByteBuffer.allocate(1 + Long.BYTES);
    pong.put(LivenessTransportLayer.HDR_PONG).putLong(sentTime).flip();
    return pong;
  }

  private static InetSocketAddress route(int k) {
    return new InetSocketAddress(InetAddress.getLoopbackAddress(), 10_000 + k);
  }

  private int trackers() {
    synchronized (target.managers) {
      return target.managers.size();
    }
  }

  @Test
  @DisplayName("routes marked alive by the identity layer for spoofed datagrams do not grow the table beyond its cap")
  void spoofedRoutesAreBounded() throws Exception {
    onSelector(() -> {
      for (int k = 0; k < SPOOFED_ROUTES; k++) {
        target.setLiveness(route(k), LIVENESS_ALIVE, null);
      }
    });

    assertTrue(trackers() <= MAX_TRACKERS, "trackers: " + trackers());
  }

  @Test
  @DisplayName("pongs from routes this node never pinged do not create trackers")
  void unsolicitedPongsCreateNoTrackers() throws Exception {
    long now = env.getTimeSource().currentTimeMillis();
    onSelector(() -> {
      for (int k = 0; k < SPOOFED_ROUTES; k++) {
        target.messageReceived(route(k), pong(now), null);
      }
    });

    assertEquals(0, trackers());
  }

  @Test
  @DisplayName("a pong for which this node sent no ping does not mark the route alive")
  void unsolicitedPongDoesNotMarkAlive() throws Exception {
    long now = env.getTimeSource().currentTimeMillis();
    onSelector(() -> target.messageReceived(PEER, pong(now), null));

    assertEquals(LIVENESS_SUSPECTED, target.getLiveness(PEER, null));
    assertTrue(notified.isEmpty(), "notified: " + notified);
  }

  @Test
  @DisplayName("a pong that does not echo an outstanding ping's time stamp does not mark the route alive")
  void pongForAnotherPingDoesNotMarkAlive() throws Exception {
    onSelector(() -> target.ping(PEER, null));
    long pingTime = sentTimeOf(sent.getLast());

    onSelector(() -> target.messageReceived(PEER, pong(pingTime - 1), null));

    assertEquals(LIVENESS_SUSPECTED, target.getLiveness(PEER, null));
  }

  @Test
  @DisplayName("the pong to an outstanding ping marks the route alive, once")
  void pongToOutstandingPingMarksAlive() throws Exception {
    onSelector(() -> target.ping(PEER, null));
    long pingTime = sentTimeOf(sent.getLast());

    onSelector(() -> target.messageReceived(PEER, pong(pingTime), null));

    assertEquals(LIVENESS_ALIVE, target.getLiveness(PEER, null));
    assertEquals(List.of(LIVENESS_ALIVE), notified);
  }

  private static long sentTimeOf(ByteBuffer ping) {
    ByteBuffer buffer = ping.duplicate();
    assertEquals(LivenessTransportLayer.HDR_PING, buffer.get());
    return buffer.getLong();
  }
}
