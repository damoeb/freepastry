package org.mpisws.p2p.transport.wire;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.testing.transportlayer.FreePorts;
import org.mpisws.p2p.transport.MessageRequestHandle;
import rice.environment.Environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class WireTransportLayerImplTest {

  private static final long TIMEOUT_SECONDS = 10;

  private final Environment env = new Environment();

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void acceptSocketsWithoutTcpServerDoesNothing() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    WireTransportLayerImpl wire = new WireTransportLayerImpl(new InetSocketAddress(localhost, FreePorts.freePort(localhost)), env, null, false);
    CompletableFuture<Throwable> thrown = new CompletableFuture<Throwable>();

    // on the selector thread, acceptSockets() runs at once, so a failure surfaces here
    env.getSelectorManager().invoke(() -> {
      try {
        wire.acceptSockets(false);
        wire.acceptSockets(true);
        thrown.complete(null);
      } catch (Throwable t) {
        thrown.complete(t);
      }
    });

    assertNull(thrown.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void cancelTakesAnUnsentDatagramOutOfTheQueueOnce() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    WireTransportLayerImpl wire = new WireTransportLayerImpl(new InetSocketAddress(localhost, FreePorts.freePort(localhost)), env, null, false, true);
    InetSocketAddress destination = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    ByteBuffer message = ByteBuffer.allocate(3);
    Map<String, Object> options = Map.of("k", "v");
    CompletableFuture<List<Object>> actual = new CompletableFuture<List<Object>>();

    // on the selector thread the datagram cannot be written between send and cancel
    env.getSelectorManager().invoke(() -> {
      try {
        MessageRequestHandle<InetSocketAddress, ByteBuffer> handle = wire.sendMessage(destination, message, null, options);
        actual.complete(List.of(handle.cancel(), handle.cancel(), handle.getIdentifier(), handle.getMessage(), handle.getOptions()));
      } catch (Throwable t) {
        actual.completeExceptionally(t);
      }
    });

    List<Object> result = actual.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertEquals(List.of(true, false), result.subList(0, 2));
    assertSame(destination, result.get(2));
    assertSame(message, result.get(3));
    assertSame(options, result.get(4));
  }
}
