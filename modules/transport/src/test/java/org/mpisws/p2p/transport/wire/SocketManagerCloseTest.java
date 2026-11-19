package org.mpisws.p2p.transport.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.testing.transportlayer.FreePorts;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import rice.environment.Environment;
import rice.environment.exception.ExceptionStrategy;
import rice.selector.SelectionKeyHandler;

/**
 * A socket is closed by one thread while another uses it: the selector thread closes it on a failed handshake or a
 * read error, and WireTransportLayer.destroy() closes all sockets on the caller's thread, also those whose key
 * the selector thread is handling at that very moment.
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
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    TCPLayer tcp = tcpOf(wire);
    List<Throwable> failures = new CopyOnWriteArrayList<Throwable>();

    for (int round = 0; round < ROUNDS && failures.isEmpty(); round++) {
      WireSocket socket = openSocket(tcp, address);
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

  /*
   * The two tests below put the selector thread into the state the race leaves it in, instead of hoping for the
   * race: the window between close() cancelling the key and closing the channel is a few instructions wide.
   */

  @Test
  @DisplayName("a socket closed by another thread while the selector thread finishes its connect fails the connect, not the selector")
  void closeWhileConnectingLeavesSelectorAlive() throws Exception {
    List<Throwable> reported = recordReportedExceptions();
    InetAddress localhost = InetAddress.getLoopbackAddress();
    WireTransportLayer wire = new WireTransportLayer(new InetSocketAddress(localhost, FreePorts.freePort(localhost)), env, null);
    TCPLayer tcp = tcpOf(wire);
    CompletableFuture<Exception> connectFailure = new CompletableFuture<Exception>();
    CompletableFuture<Void> handled = new CompletableFuture<Void>();

    try (ServerSocket server = new ServerSocket(0, 1, localhost)) {
      server.setSoTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
      InetSocketAddress serverAddress = (InetSocketAddress) server.getLocalSocketAddress();
      env.getSelectorManager().invoke(() -> {
        try {
          WireSocket socket = new WireSocket(tcp, serverAddress, new SocketCallback<InetSocketAddress>() {
            @Override
            public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
              connectFailure.completeExceptionally(new AssertionError("delivered a closed socket: " + sock));
            }

            @Override
            public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception ex) {
              connectFailure.complete(ex);
            }
          }, null);
          // holding the selector thread, let the connection come up, so that finishConnect() succeeds
          try (Socket accepted = server.accept()) {
            SelectionKey connecting = keyOf(socket);
            // another thread's close() has cancelled the key, but not closed the channel yet...
            connecting.cancel();
            // ...while the selector thread handles the key it found connectable
            ((SelectionKeyHandler) connecting.attachment()).connect(connecting);
          }
        } catch (IOException | ReflectiveOperationException e) {
          handled.completeExceptionally(e);
        } finally {
          handled.complete(null);
        }
      });
      handled.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    awaitSelectorLoop();
    assertEquals(List.of(), reported);
    assertTrue(env.getSelectorManager().isAlive(), "the selector thread still runs");
    assertInstanceOf(IOException.class, connectFailure.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  @DisplayName("a socket closed by another thread while the selector thread handles its key does not fail the selector")
  void closeWhileSelectingLeavesSelectorAlive() throws Exception {
    List<Throwable> reported = recordReportedExceptions();
    InetAddress localhost = InetAddress.getLoopbackAddress();
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    WireSocket socket = openSocket(tcpOf(wire), address);
    CompletableFuture<Void> handled = new CompletableFuture<Void>();

    env.getSelectorManager().invoke(() -> {
      try {
        SelectionKey key = keyOf(socket);
        // another thread's close() has cancelled the key, while the selector thread handles it
        key.cancel();
        socket.read(key);
        socket.write(key);
        socket.modifyKey(key);
      } catch (ReflectiveOperationException e) {
        handled.completeExceptionally(e);
      } finally {
        handled.complete(null);
        socket.close();
      }
    });
    handled.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    awaitSelectorLoop();
    assertEquals(List.of(), reported);
    assertTrue(env.getSelectorManager().isAlive(), "the selector thread still runs");
  }

  /** @return the exceptions that reach the environment's ExceptionStrategy, which still handles them as before */
  private List<Throwable> recordReportedExceptions() {
    List<Throwable> reported = new CopyOnWriteArrayList<Throwable>();
    ExceptionStrategy original = env.getExceptionStrategy();
    env.setExceptionStrategy((source, t) -> {
      reported.add(t);
      original.handleException(source, t);
    });
    return reported;
  }

  /** Lets the selector thread finish its current loop, so that whatever it reports has been reported. */
  private void awaitSelectorLoop() throws Exception {
    CompletableFuture<Void> looped = new CompletableFuture<Void>();
    env.getSelectorManager().invoke(() -> looped.complete(null));
    try {
      looped.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException dropped) {
      // a destroyed selector drops the invocation, the assertions tell why
    }
  }

  /** @return a socket that has finished connecting, so that only the closes race */
  private WireSocket openSocket(TCPLayer tcp, InetSocketAddress to) throws Exception {
    CompletableFuture<WireSocket> connected = new CompletableFuture<WireSocket>();
    env.getSelectorManager().invoke(() -> {
      try {
        new WireSocket(tcp, to, new SocketCallback<InetSocketAddress>() {
          @Override
          public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
            connected.complete((WireSocket) sock);
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

  private static SelectionKey keyOf(WireSocket socket) throws ReflectiveOperationException {
    Field field = WireSocket.class.getDeclaredField("key");
    field.setAccessible(true);
    return (SelectionKey) field.get(socket);
  }

  private static TCPLayer tcpOf(WireTransportLayer wire) throws ReflectiveOperationException {
    Field field = WireTransportLayer.class.getDeclaredField("tcp");
    field.setAccessible(true);
    return (TCPLayer) field.get(wire);
  }
}
