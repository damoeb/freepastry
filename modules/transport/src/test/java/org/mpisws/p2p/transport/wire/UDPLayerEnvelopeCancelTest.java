package org.mpisws.p2p.transport.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.testing.transportlayer.FreePorts;
import org.mpisws.p2p.transport.MessageRequestHandle;
import rice.environment.Environment;

class UDPLayerEnvelopeCancelTest {
  private final Environment env = new Environment();

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("cancel waits for the lock that guards the pending datagrams")
  void cancelTakesThePendingMessagesLock() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    InetSocketAddress bind = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayerImpl wire = new WireTransportLayerImpl(bind, env, null, false, false);
    UDPLayerImpl udp = new UDPLayerImpl(wire);
    try {
      InetSocketAddress destination = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
      AtomicReference<MessageRequestHandle<InetSocketAddress, ByteBuffer>> handle = new AtomicReference<>();
      Thread canceller = new Thread(() -> handle.get().cancel(), "canceller");

      // holding the lock keeps the selector thread from writing the datagram before the cancel
      synchronized (udp.pendingMsgs) {
        handle.set(udp.sendMessage(destination, ByteBuffer.allocate(3), null, null));
        canceller.start();
        // a cancel that honours the lock parks on it; one that does not is done at once
        while (canceller.getState() != Thread.State.BLOCKED && canceller.getState() != Thread.State.TERMINATED) {
          Thread.onSpinWait();
        }
        assertEquals(Thread.State.BLOCKED, canceller.getState(), "cancel() ran without the pendingMsgs lock");
      }

      canceller.join(10_000);
      assertEquals(Thread.State.TERMINATED, canceller.getState());
    } finally {
      udp.destroy();
    }
  }
}
