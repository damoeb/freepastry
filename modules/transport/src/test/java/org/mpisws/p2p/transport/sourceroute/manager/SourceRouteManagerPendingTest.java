package org.mpisws.p2p.transport.sourceroute.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_ALIVE;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.factory.WireSourceRouteFactory;
import rice.environment.Environment;

/** Messages and sockets wait for a route while the destination is dead, and cancel accordingly. */
class SourceRouteManagerPendingTest {
  private static final InetSocketAddress LOCAL = new InetSocketAddress(InetAddress.getLoopbackAddress(), 1);
  private static final InetSocketAddress PEER = new InetSocketAddress(InetAddress.getLoopbackAddress(), 2);

  private final Environment env = new Environment();
  private final WireSourceRouteFactory factory = new WireSourceRouteFactory();
  private final SourceRoute<InetSocketAddress> direct = factory.getSourceRoute(LOCAL, PEER);
  private final AtomicInteger lowerSends = new AtomicInteger();
  private final AtomicInteger lowerCancels = new AtomicInteger();

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @SuppressWarnings("unchecked")
  private <T> T stub(Class<T> type, int liveness) {
    return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getLocalIdentifier":
          return factory.getSourceRoute(LOCAL);
        case "sendMessage":
        case "openSocket":
          lowerSends.incrementAndGet();
          return Proxy.newProxyInstance(getClass().getClassLoader(),
              new Class<?>[] {MessageRequestHandle.class, SocketRequestHandle.class}, (p, m, a) -> {
                if (m.getName().equals("cancel")) {
                  lowerCancels.incrementAndGet();
                  return true;
                }
                return null;
              });
        case "getSourceRoutes":
          return List.of();
        default:
          if (method.getReturnType() == boolean.class) return false;
          if (method.getReturnType() == int.class) return liveness;
          return null;
      }
    });
  }

  @SuppressWarnings("unchecked")
  private SourceRouteSelectingTransportLayer<InetSocketAddress> deadPeer() {
    SourceRouteSelectingTransportLayer<InetSocketAddress> srm = new SourceRouteSelectingTransportLayer<>(factory,
        stub(TransportLayer.class, 0), stub(LivenessProvider.class, LIVENESS_DEAD), stub(ProximityProvider.class, 0),
        env, stub(SourceRouteStrategy.class, 0));
    srm.livenessChanged(direct, LIVENESS_DEAD, null);
    return srm;
  }

  private static final SocketCallback<InetSocketAddress> IGNORE = new SocketCallback<>() {
    @Override
    public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> socket) {
    }

    @Override
    public void receiveException(SocketRequestHandle<InetSocketAddress> request, Exception ex) {
    }
  };

  @Test
  @DisplayName("a message waiting for a route carries its destination, message and options")
  void waitingMessageCarriesDestinationMessageAndOptions() {
    ByteBuffer message = ByteBuffer.allocate(3);
    Map<String, Object> options = Map.of("k", "v");

    MessageRequestHandle<InetSocketAddress, ByteBuffer> actual = deadPeer().sendMessage(PEER, message, null, options);

    assertSame(PEER, actual.getIdentifier());
    assertSame(message, actual.getMessage());
    assertSame(options, actual.getOptions());
    assertEquals(0, lowerSends.get());
  }

  @Test
  @DisplayName("a message waiting for a route leaves the queue on cancel, once")
  void cancelTakesAWaitingMessageOutOfTheQueue() {
    SourceRouteSelectingTransportLayer<InetSocketAddress> srm = deadPeer();
    MessageRequestHandle<InetSocketAddress, ByteBuffer> testObj = srm.sendMessage(PEER, ByteBuffer.allocate(3), null, null);

    assertTrue(testObj.cancel());
    assertFalse(testObj.cancel());
    srm.livenessChanged(direct, LIVENESS_ALIVE, null);
    assertEquals(0, lowerSends.get());
  }

  @Test
  @DisplayName("a message sent once the route is up cancels the lower send")
  void cancelOfASentMessageCancelsTheLowerSend() {
    SourceRouteSelectingTransportLayer<InetSocketAddress> srm = deadPeer();
    MessageRequestHandle<InetSocketAddress, ByteBuffer> testObj = srm.sendMessage(PEER, ByteBuffer.allocate(3), null, null);
    srm.livenessChanged(direct, LIVENESS_ALIVE, null);

    assertTrue(testObj.cancel());
    assertEquals(1, lowerSends.get());
    assertEquals(1, lowerCancels.get());
  }

  @Test
  @DisplayName("a socket waiting for a route carries its destination and options and leaves the queue on cancel")
  void cancelTakesAWaitingSocketOutOfTheQueue() {
    SourceRouteSelectingTransportLayer<InetSocketAddress> srm = deadPeer();
    Map<String, Object> options = Map.of("k", "v");
    SocketRequestHandle<InetSocketAddress> testObj = srm.openSocket(PEER, IGNORE, options);

    assertSame(PEER, testObj.getIdentifier());
    assertSame(options, testObj.getOptions());
    assertTrue(testObj.cancel());
    assertFalse(testObj.cancel());
    srm.livenessChanged(direct, LIVENESS_ALIVE, null);
    assertEquals(0, lowerSends.get());
  }

  @Test
  @DisplayName("a socket opened once the route is up cancels the lower open")
  void cancelOfAnOpenedSocketCancelsTheLowerOpen() {
    SourceRouteSelectingTransportLayer<InetSocketAddress> srm = deadPeer();
    SocketRequestHandle<InetSocketAddress> testObj = srm.openSocket(PEER, IGNORE, null);
    srm.livenessChanged(direct, LIVENESS_ALIVE, null);

    assertTrue(testObj.cancel());
    assertEquals(1, lowerSends.get());
    assertEquals(1, lowerCancels.get());
  }
}
