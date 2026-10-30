package org.mpisws.p2p.transport.wire;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.testing.transportlayer.FreePorts;
import rice.environment.Environment;

import static org.junit.jupiter.api.Assertions.assertNull;

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
}
