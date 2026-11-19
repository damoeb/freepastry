package org.mpisws.p2p.transport.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;

class IdentityMessageHandleTest {
  private static final InetSocketAddress PEER = new InetSocketAddress(InetAddress.getLoopbackAddress(), 1);

  private final Environment env = Environment.directEnvironment(1);

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @SuppressWarnings("unchecked")
  private IdentityLayerCore<InetSocketAddress, InetSocketAddress, ByteBuffer, InetSocketAddress> identity() {
    IdentitySerializer<InetSocketAddress, InetSocketAddress, InetSocketAddress> serializer =
        (IdentitySerializer<InetSocketAddress, InetSocketAddress, InetSocketAddress>) Proxy.newProxyInstance(
            getClass().getClassLoader(), new Class<?>[] {IdentitySerializer.class}, (proxy, method, args) -> null);
    return new IdentityLayerCore<>(new byte[8], serializer, (oldDest, newDest) -> true, (upper, middle) -> true, null, env);
  }

  private static Map<?, ?> pendingMessages(IdentityLayerCore<?, ?, ?, ?> identity) throws ReflectiveOperationException {
    Field f = IdentityLayerCore.class.getDeclaredField("pendingMessages");
    f.setAccessible(true);
    return (Map<?, ?>) f.get(identity);
  }

  @Test
  @DisplayName("the handle carries the destination, message and options")
  void handleCarriesDestinationMessageAndOptions() {
    ByteBuffer message = ByteBuffer.allocate(3);
    Map<String, Object> options = Map.of("k", "v");

    IdentityLayerCore<InetSocketAddress, InetSocketAddress, ByteBuffer, InetSocketAddress>.IdentityMessageHandle actual =
        identity().new IdentityMessageHandle(PEER, message, options, null);

    assertSame(PEER, actual.getIdentifier());
    assertSame(message, actual.getMessage());
    assertSame(options, actual.getOptions());
  }

  @Test
  @DisplayName("cancel forgets the pending message and cancels the lower send")
  void cancelForgetsThePendingMessageAndCancelsTheLowerSend() throws Exception {
    IdentityLayerCore<InetSocketAddress, InetSocketAddress, ByteBuffer, InetSocketAddress> identity = identity();
    IdentityLayerCore<InetSocketAddress, InetSocketAddress, ByteBuffer, InetSocketAddress>.IdentityMessageHandle testObj =
        identity.new IdentityMessageHandle(PEER, ByteBuffer.allocate(3), null, null);
    identity.addPendingMessage(PEER, testObj);
    AtomicInteger lowerCancels = new AtomicInteger();
    testObj.setSubCancellable(() -> lowerCancels.incrementAndGet() > 0);

    assertTrue(testObj.cancel());
    assertEquals(1, lowerCancels.get());
    assertTrue(pendingMessages(identity).isEmpty());
  }

  @Test
  @DisplayName("cancel without a lower send forgets the pending message and reports false")
  void cancelWithoutLowerSendForgetsThePendingMessage() throws Exception {
    IdentityLayerCore<InetSocketAddress, InetSocketAddress, ByteBuffer, InetSocketAddress> identity = identity();
    IdentityLayerCore<InetSocketAddress, InetSocketAddress, ByteBuffer, InetSocketAddress>.IdentityMessageHandle testObj =
        identity.new IdentityMessageHandle(PEER, ByteBuffer.allocate(3), null, null);
    identity.addPendingMessage(PEER, testObj);

    assertFalse(testObj.cancel());
    assertTrue(pendingMessages(identity).isEmpty());
  }
}
