package org.mpisws.p2p.transport.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.testing.transportlayer.FreePorts;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import rice.environment.Environment;

/**
 * A socket is closed by the selector thread (a failed handshake, a read error) while another thread destroys the
 * transport layer, which closes it too (SSLTest's tearDown once failed with an NPE this way).
 */
class SocketManagerCloseTest {

  private static final long TIMEOUT_SECONDS = 10;
  private static final int ROUNDS = 500;

  private final Environment env = new Environment();

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("two threads closing the same socket at once both return normally")
  void concurrentClosesOfOneSocketDoNotFail() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayerImpl wire = new WireTransportLayerImpl(address, env, null);
    TCPLayer tcp = tcpOf(wire);
    List<Throwable> failures = new CopyOnWriteArrayList<Throwable>();

    for (int round = 0; round < ROUNDS && failures.isEmpty(); round++) {
      SocketManager socket = openSocket(tcp, address);
      CyclicBarrier start = new CyclicBarrier(2);
      Runnable close = () -> {
        try {
          start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
          socket.close();
        } catch (Throwable t) {
          failures.add(t);
        }
      };
      Thread other = new Thread(close, "closer");
      other.start();
      close.run();
      other.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    }

    assertEquals(List.of(), failures);
  }

  /** @return a socket that has finished connecting, so that only the closes race */
  private SocketManager openSocket(TCPLayer tcp, InetSocketAddress to) throws Exception {
    CompletableFuture<SocketManager> connected = new CompletableFuture<SocketManager>();
    env.getSelectorManager().invoke(() -> {
      try {
        new SocketManager(tcp, to, new SocketCallback<InetSocketAddress>() {
          @Override
          public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
            connected.complete((SocketManager) sock);
          }

          @Override
          public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception ex) {
            connected.completeExceptionally(ex);
          }
        }, null);
      } catch (Throwable t) {
        connected.completeExceptionally(t);
      }
    });
    return connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private static TCPLayer tcpOf(WireTransportLayerImpl wire) throws ReflectiveOperationException {
    Field field = WireTransportLayerImpl.class.getDeclaredField("tcp");
    field.setAccessible(true);
    return (TCPLayer) field.get(wire);
  }
}
