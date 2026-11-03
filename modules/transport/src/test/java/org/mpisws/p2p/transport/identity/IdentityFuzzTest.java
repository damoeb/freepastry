package org.mpisws.p2p.transport.identity;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.simpleidentity.InetSocketAddressSerializer;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * Fuzzes what the identity layer reads from a peer in every datagram: the header and the identifier. (The incoming
 * socket path binds identities on the selector thread, which a fuzz target does not run on.) The invariant: nothing
 * but an {@link IOException} escapes.
 */
class IdentityFuzzTest {
  private static final int LOCAL_ID_BYTES = 8;
  private static final InetSocketAddress PEER = new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 1);

  private static Environment env;
  private static TransportLayerCallback<InetSocketAddress, ByteBuffer> target;

  @SuppressWarnings({"unchecked", "ReferenceEquality"})
  private static <T> T stub(Class<T> type) {
    return (T) Proxy.newProxyInstance(IdentityFuzzTest.class.getClassLoader(), new Class<?>[] {type},
        (proxy, method, args) -> switch (method.getName()) {
          case "hashCode" -> System.identityHashCode(proxy);
          case "equals" -> proxy == args[0];
          case "toString" -> type.getSimpleName();
          default -> null;
        });
  }

  @BeforeAll
  @SuppressWarnings("unchecked")
  static void setUp() {
    env = Environment.directEnvironment(1);
    IdentitySerializer<InetSocketAddress, InetSocketAddress, InetSocketAddress> serializer = new IdentitySerializer<>() {
      @Override
      public void serialize(OutputBuffer buf, InetSocketAddress i) throws IOException {
        InetSocketAddressSerializer.serializeAddress(i, buf);
      }

      @Override
      public InetSocketAddress deserialize(InputBuffer buf, InetSocketAddress l) throws IOException {
        return InetSocketAddressSerializer.deserializeAddress(buf);
      }

      @Override
      public InetSocketAddress translateDown(InetSocketAddress i) {
        return i;
      }

      @Override
      public InetSocketAddress translateUp(InetSocketAddress i) {
        return i;
      }

      @Override
      public void addSerializerListener(SerializerListener<InetSocketAddress> listener) {
      }

      @Override
      public void removeSerializerListener(SerializerListener<InetSocketAddress> listener) {
      }
    };
    IdentityImpl<InetSocketAddress, InetSocketAddress, ByteBuffer, InetSocketAddress> identity = new IdentityImpl<>(
        new byte[LOCAL_ID_BYTES], serializer, (oldDest, newDest) -> true, (upper, middle) -> true, null, env);
    identity.initLowerLayer(stub(TransportLayer.class), null);
    identity.initUpperLayer(PEER, stub(TransportLayer.class), stub(LivenessProvider.class),
        stub(ProximityProvider.class), stub(org.mpisws.p2p.transport.liveness.OverrideLiveness.class));
    target = (TransportLayerCallback<InetSocketAddress, ByteBuffer>) identity.getLowerIdentity();
    ((org.mpisws.p2p.transport.identity.LowerIdentity<InetSocketAddress, ByteBuffer>) identity.getLowerIdentity())
        .setCallback(new TransportLayerCallback<>() {
          @Override
          public void messageReceived(InetSocketAddress i, ByteBuffer m, Map<String, Object> options) {
          }

          @Override
          public void incomingSocket(P2PSocket<InetSocketAddress> s) {
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
    } catch (RuntimeException e) {
      // binding a valid identity needs the selector thread, which a fuzz target is not running on
      if (!"Must be called on selector thread.".equals(e.getMessage())) {
        throw e;
      }
    }
  }
}
