package org.mpisws.p2p.transport.sourceroute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.sourceroute.factory.WireSourceRouteFactory;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/** A spoofed source route with a repeated hop must not make nodes forward in a loop. */
class SourceRouteLoopTest {
  private static final int FORWARD_LIMIT = 50;

  private static Environment env;

  private final WireSourceRouteFactory factory = new WireSourceRouteFactory();
  private final ArrayDeque<Object[]> wire = new ArrayDeque<>();
  private final List<InetSocketAddress> delivered = new ArrayList<>();
  private int forwards;

  @BeforeAll
  static void startEnvironment() {
    env = new Environment();
  }

  @AfterAll
  static void stopEnvironment() {
    env.destroy();
  }

  private static InetSocketAddress addr(int n) throws IOException {
    return new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, (byte) n}), 9000 + n);
  }

  /** Queues every message instead of delivering it, so a loop shows up as a counter, not a stack overflow. */
  private final class QueueLayer implements TransportLayer<InetSocketAddress, ByteBuffer> {
    final InetSocketAddress local;
    TransportLayerCallback<InetSocketAddress, ByteBuffer> callback;

    QueueLayer(InetSocketAddress local) {
      this.local = local;
    }

    @Override
    public MessageRequestHandle<InetSocketAddress, ByteBuffer> sendMessage(InetSocketAddress i, ByteBuffer m,
        @Nullable MessageCallback<InetSocketAddress, ByteBuffer> ack, @Nullable Map<String, Object> options) {
      forwards++;
      byte[] copy = new byte[m.remaining()];
      m.duplicate().get(copy);
      wire.add(new Object[] {local, i, copy});
      return null;
    }

    @Override
    public SocketRequestHandle<InetSocketAddress> openSocket(InetSocketAddress i, SocketCallback<InetSocketAddress> cb,
        @Nullable Map<String, Object> options) {
      return null;
    }

    @Override
    public InetSocketAddress getLocalIdentifier() {
      return local;
    }

    @Override
    public void acceptSockets(boolean b) {}

    @Override
    public void acceptMessages(boolean b) {}

    @Override
    public void setCallback(TransportLayerCallback<InetSocketAddress, ByteBuffer> cb) {
      callback = cb;
    }

    @Override
    public void setErrorHandler(ErrorHandler<InetSocketAddress> handler) {}

    @Override
    public void destroy() {}
  }

  /** Injects a datagram for route[1] from route[0] and pumps the wire until it drains or the limit is hit. */
  private void run(List<InetSocketAddress> route, List<InetSocketAddress> nodes) throws Exception {
    List<QueueLayer> layers = new ArrayList<>();
    for (InetSocketAddress n : nodes) {
      QueueLayer q = new QueueLayer(n);
      layers.add(q);
      SourceRouteTransportLayerImpl<InetSocketAddress> impl =
          new SourceRouteTransportLayerImpl<>(factory, q, null, env, null);
      impl.setCallback(new TransportLayerCallback<>() {
        @Override
        public void messageReceived(SourceRoute<InetSocketAddress> i, ByteBuffer m, @Nullable Map<String, Object> o) {
          delivered.add(n);
        }

        @Override
        public void incomingSocket(P2PSocket<SourceRoute<InetSocketAddress>> s) {}
      });
    }
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    factory.getSourceRoute(route).serialize(out);
    out.write(new byte[] {1, 2, 3}, 0, 3);
    wire.add(new Object[] {route.get(0), route.get(1), out.getBytes()});

    CountDownLatch done = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    env.getSelectorManager().invoke(() -> {
      try {
        int steps = 0;
        while (!wire.isEmpty() && steps++ < 10 * FORWARD_LIMIT && forwards < FORWARD_LIMIT) {
          Object[] p = wire.poll();
          for (int k = 0; k < nodes.size(); k++) {
            if (nodes.get(k).equals(p[1])) {
              layers.get(k).callback.messageReceived((InetSocketAddress) p[0], ByteBuffer.wrap((byte[]) p[2]), null);
            }
          }
        }
      } catch (Throwable t) {
        failure.set(t);
      } finally {
        done.countDown();
      }
    });
    assertTrue(done.await(10, TimeUnit.SECONDS));
    if (failure.get() != null) {
      throw new AssertionError(failure.get());
    }
  }

  @Test
  void routeBouncingBetweenTwoNodesStops() throws Exception {
    InetSocketAddress x = addr(1), a = addr(2), b = addr(3), z = addr(4);

    run(List.of(x, a, b, a, z), List.of(a, b, z));

    assertTrue(forwards < FORWARD_LIMIT, "forwarded " + forwards + " times, expected the route to be dropped");
    assertEquals(0, delivered.size());
  }

  @Test
  void routeForwardingToItselfStops() throws Exception {
    InetSocketAddress x = addr(1), a = addr(2), z = addr(4);

    run(List.of(x, a, a, z), List.of(a, z));

    assertTrue(forwards < FORWARD_LIMIT, "forwarded " + forwards + " times, expected the route to be dropped");
    assertEquals(0, delivered.size());
  }

  @Test
  void relayedRouteIsStillDeliveredToTheLastHop() throws Exception {
    InetSocketAddress x = addr(1), r = addr(2), z = addr(4);

    run(List.of(x, r, z), List.of(r, z));

    assertEquals(1, forwards);
    assertEquals(List.of(z), delivered);
  }
}
