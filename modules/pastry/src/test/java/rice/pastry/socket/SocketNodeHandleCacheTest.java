package rice.pastry.socket;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.identity.IdentityLayerCore;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.PastryNode;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * The node handle factory hands out one canonical instance per handle (id, address and epoch), which carries the
 * handle's liveness observers. Every datagram's identity header names a handle, and any sender can make up as many
 * as it likes: the factory must not keep the ones nothing else refers to.
 */
class SocketNodeHandleCacheTest {

  /** forged handles, one per datagram */
  private static final int FORGED_HANDLES = 50_000;
  /** the identity layer's bindings and the liveness trackers, which may still refer to forged handles */
  private static final int MAX_BINDINGS = 1_000;
  /** forged handles still cached after a garbage collection: the bounded tables above refer to a few each */
  private static final int MAX_CACHED_AFTER_FLOOD = 5 * MAX_BINDINGS;
  private static final long TIMEOUT_SECONDS = 10;

  private Environment env;
  private PastryNode node;
  private SocketNodeHandleFactory handles;
  private IdentityLayerCore<TransportLayerNodeHandle<MultiInetSocketAddress>, MultiInetSocketAddress, ByteBuffer, SourceRoute<MultiInetSocketAddress>> identity;

  @BeforeEach
  void setUp() throws Exception {
    env = new Environment();
    env.getParameters().setInt("pastry_socket_identity_max_bindings", MAX_BINDINGS);
    env.getParameters().setInt("pastry_socket_liveness_max_trackers", MAX_BINDINGS);
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port;
    try (ServerSocket probe = new ServerSocket(0, 1, loopback)) {
      port = probe.getLocalPort();
    }
    SocketPastryNodeFactory factory = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), loopback, port, env) {
      @Override
      protected TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> getLowerIdentityLayer(
          TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> srl, PastryNode pn,
          IdentityLayerCore<TransportLayerNodeHandle<MultiInetSocketAddress>, MultiInetSocketAddress, ByteBuffer, SourceRoute<MultiInetSocketAddress>> core) {
        identity = core;
        return super.getLowerIdentityLayer(srl, pn, core);
      }
    };
    node = factory.newNode();
    handles = (SocketNodeHandleFactory) factory.getNodeHandleFactory(node);
  }

  @AfterEach
  void tearDown() {
    node.destroy();
    env.destroy();
  }

  @Test
  @DisplayName("a handle read twice from the wire is the same instance as long as it is in use")
  void aHandleReadTwiceIsTheSameInstance() throws Exception {
    SocketNodeHandle original = onSelector(() -> handles.getNodeHandle(address(1), 7L, Id.build("0000000000000000000000000000000000001234")));
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    original.serialize(buf);

    SocketNodeHandle first = onSelector(() -> handles.readNodeHandle(new SimpleInputBuffer(buf.getBytes())));
    collectGarbage();
    SocketNodeHandle second = onSelector(() -> handles.readNodeHandle(new SimpleInputBuffer(buf.getBytes())));

    assertSame(original, first);
    assertSame(original, second);
    assertSame(node, second.getLocalNode());
  }

  @Test
  @DisplayName("the local handle stays the canonical instance")
  void theLocalHandleStaysCanonical() throws Exception {
    SocketNodeHandle local = (SocketNodeHandle) node.getLocalHandle();
    collectGarbage();

    SocketNodeHandle actual = onSelector(() -> handles.getNodeHandle(local.getAddress(), local.getEpoch(), local.getNodeId()));

    assertSame(local, actual);
  }

  @Test
  @DisplayName("forged identity headers do not fill the handle cache")
  void forgedIdentityHeadersDoNotFillTheHandleCache() throws Exception {
    @SuppressWarnings("unchecked")
    TransportLayerCallback<SourceRoute<MultiInetSocketAddress>, ByteBuffer> lower =
        (TransportLayerCallback<SourceRoute<MultiInetSocketAddress>, ByteBuffer>) identity.getLowerIdentity();
    MultiAddressSourceRouteFactory routes = new MultiAddressSourceRouteFactory();

    onSelector(() -> {
      for (int k = 0; k < FORGED_HANDLES; k++) {
        try {
          lower.messageReceived(routes.getSourceRoute(address(k)), forgedDatagram(k), null);
        } catch (IOException | RuntimeException e) {
          // the forged payload is not a valid message of the layer above; the header has been read by then
        }
      }
      return null;
    });

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    int cached;
    do {
      collectGarbage();
      cached = onSelector(this::cachedHandles);
    } while (cached > MAX_CACHED_AFTER_FLOOD && System.nanoTime() < deadline);
    assertTrue(cached <= MAX_CACHED_AFTER_FLOOD, "handles cached after " + FORGED_HANDLES + " forged headers: " + cached);
  }

  // helpers

  /** a datagram as the identity layer receives it: no destination, the sender's epoch and id, then one byte */
  private static ByteBuffer forgedDatagram(int k) throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    buf.writeByte(IdentityLayerCore.NO_ID);
    buf.writeLong(k);
    Id.build(String.format("%040X", k + 1)).serialize(buf);
    buf.writeByte(0);
    return ByteBuffer.wrap(buf.getBytes(), 0, buf.getWritten());
  }

  private static MultiInetSocketAddress address(int k) {
    return new MultiInetSocketAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1 + k % 60_000));
  }

  private int cachedHandles() throws Exception {
    Field field = SocketNodeHandleFactory.class.getDeclaredField("handleSet");
    field.setAccessible(true);
    return ((Map<?, ?>) field.get(handles)).size();
  }

  private static void collectGarbage() throws InterruptedException {
    WeakReference<Object> canary = new WeakReference<>(new Object());
    while (canary.get() != null) {
      System.gc();
      Thread.sleep(10);
    }
    System.gc();
  }

  private <T> T onSelector(Callable<T> action) throws Exception {
    CompletableFuture<T> result = new CompletableFuture<>();
    node.getEnvironment().getSelectorManager().invoke(() -> {
      try {
        result.complete(action.call());
      } catch (Throwable throwable) {
        result.completeExceptionally(throwable);
      }
    });
    return result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }
}
