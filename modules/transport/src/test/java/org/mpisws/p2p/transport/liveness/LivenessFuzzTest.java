package org.mpisws.p2p.transport.liveness;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import rice.environment.Environment;

/**
 * Fuzzes the liveness layer's datagram reader: the header byte, and the time stamp of a ping or pong. Every datagram
 * of the node passes through it, unauthenticated. The invariant: nothing but an {@link IOException} escapes.
 */
class LivenessFuzzTest {
  private static final InetSocketAddress PEER = new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 1);
  private static final long TIMEOUT_SECONDS = 10;

  private static Environment env;
  private static LivenessTransportLayer<InetSocketAddress> target;

  @BeforeAll
  @SuppressWarnings({"unchecked", "ReferenceEquality"})
  static void setUp() {
    env = new Environment();
    TransportLayer<InetSocketAddress, ByteBuffer> lower = (TransportLayer<InetSocketAddress, ByteBuffer>) Proxy
        .newProxyInstance(LivenessFuzzTest.class.getClassLoader(), new Class<?>[] {TransportLayer.class},
            (proxy, method, args) -> switch (method.getName()) {
              case "getLocalIdentifier" -> PEER;
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
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  /** Hands the datagram to the layer on the selector thread, where the layer reads it. */
  private static void receive(byte[] datagram) throws Exception {
    CompletableFuture<Void> completed = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> {
      try {
        target.messageReceived(PEER, ByteBuffer.wrap(datagram), null);
        completed.complete(null);
      } catch (Throwable throwable) {
        completed.completeExceptionally(throwable);
      }
    });
    try {
      completed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof Exception cause) {
        throw cause;
      }
      throw e;
    }
  }

  @FuzzTest(maxDuration = "30s")
  void messageReceived(FuzzedDataProvider data) throws Exception {
    try {
      receive(data.consumeRemainingAsBytes());
    } catch (IOException expected) {
      // the accepted way to reject malformed input
    }
  }

  @Test
  @DisplayName("an empty datagram is rejected with an IOException")
  void rejectsAnEmptyDatagram() {
    assertThrows(IOException.class, () -> receive(new byte[0]));
  }

  @Test
  @DisplayName("a ping whose time stamp is cut short is rejected with an IOException")
  void rejectsATruncatedPing() {
    assertThrows(IOException.class, () -> receive(new byte[] {LivenessTransportLayer.HDR_PING, 1, 2, 3}));
  }

  @Test
  @DisplayName("a pong without a time stamp is rejected with an IOException")
  void rejectsAPongWithoutTimeStamp() {
    assertThrows(IOException.class, () -> receive(new byte[] {LivenessTransportLayer.HDR_PONG}));
  }
}
