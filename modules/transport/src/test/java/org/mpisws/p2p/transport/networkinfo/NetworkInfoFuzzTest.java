package org.mpisws.p2p.transport.networkinfo;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.fuzz.FuzzSocket;
import rice.environment.Environment;

/**
 * Fuzzes what the NetworkInfo layer reads from a peer: the header of every datagram and the header and request of every
 * incoming socket. The invariant: nothing but an {@link IOException} escapes.
 */
class NetworkInfoFuzzTest {
  private static final InetSocketAddress PEER = new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 1);

  private static Environment env;
  private static NetworkInfoTransportLayer target;

  @BeforeAll
  @SuppressWarnings({"unchecked", "ReferenceEquality"})
  static void setUp() {
    env = Environment.directEnvironment(1);
    TransportLayer<InetSocketAddress, ByteBuffer> lower = (TransportLayer<InetSocketAddress, ByteBuffer>) Proxy
        .newProxyInstance(NetworkInfoFuzzTest.class.getClassLoader(), new Class<?>[] {TransportLayer.class},
            (proxy, method, args) -> switch (method.getName()) {
              case "getLocalIdentifier" -> PEER;
              case "hashCode" -> System.identityHashCode(proxy);
              case "equals" -> proxy == args[0];
              case "toString" -> "lower";
              default -> null;
            });
    target = new NetworkInfoTransportLayer(lower, env, null);
    target.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, Map<String, Object> options) {
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) {
      }
    });
    target.setProbeStrategy(new ProbeStrategy() {
      @Override
      public java.util.Collection<InetSocketAddress> getExternalAddresses() {
        return List.of(PEER);
      }

      @Override
      public rice.p2p.commonapi.Cancellable requestProbe(org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress addr, long uid,
          rice.Continuation<Boolean, Exception> deliverResultToMe) {
        return null;
      }
    });
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  @FuzzTest(maxDuration = "30s")
  void messageReceived(FuzzedDataProvider data) {
    try {
      target.messageReceived(PEER, ByteBuffer.wrap(data.consumeRemainingAsBytes()), null);
    } catch (IOException expected) {
      // the accepted way to reject malformed input
    }
  }

  @FuzzTest(maxDuration = "30s")
  void incomingSocket(FuzzedDataProvider data) throws IOException {
    FuzzSocket<InetSocketAddress> socket = new FuzzSocket<>(PEER, data.consumeRemainingAsBytes());
    try {
      target.incomingSocket(socket);
    } catch (IOException expected) {
      return;
    }
    socket.drive();
  }
}
