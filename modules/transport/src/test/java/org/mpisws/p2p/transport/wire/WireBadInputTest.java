package org.mpisws.p2p.transport.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.testing.transportlayer.FreePorts;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import rice.environment.Environment;

/**
 * One bad datagram or one bad socket costs that datagram or that socket, never the selector thread: an exception
 * that reaches the selector thread reaches the environment's ExceptionStrategy, which destroys every node of the JVM.
 */
class WireBadInputTest {
  private static final long TIMEOUT_SECONDS = 10;
  private static final int FLOOD_DATAGRAMS = 2000;
  /** fewer datagrams read (the kernel drops some of a flood) would not show that the lines are limited */
  private static final int MIN_FLOOD_DATAGRAMS_READ = 200;
  /** the default limit (logging_rate_limit_max_lines per interval) plus the summary line */
  private static final int MAX_LOG_LINES_PER_FLOOD = 50;

  private final Environment env = new Environment();
  private final List<Throwable> reported = new CopyOnWriteArrayList<>();
  private final List<Throwable> handled = new CopyOnWriteArrayList<>();
  private final ErrorHandler<InetSocketAddress> errorHandler = new ErrorHandler<>() {
    @Override
    public void receivedUnexpectedData(InetSocketAddress i, byte[] bytes, int location, @Nullable Map<String, Object> options) {
    }

    @Override
    public void receivedException(@Nullable InetSocketAddress i, Throwable error) {
      handled.add(error);
    }
  };

  WireBadInputTest() {
    var original = env.getExceptionStrategy();
    env.setExceptionStrategy((source, t) -> {
      reported.add(t);
      original.handleException(source, t);
    });
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("an accept that finds no pending connection leaves the selector alive")
  void acceptWithoutPendingConnection() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    WireTransportLayer wire = new WireTransportLayer(
        new InetSocketAddress(localhost, FreePorts.freePort(localhost)), env, errorHandler);
    TCPLayer tcp = (TCPLayer) field(WireTransportLayer.class, "tcp").get(wire);
    SelectionKey serverKey = (SelectionKey) field(TCPLayer.class, "key").get(tcp);

    // the client reset its connection between select() and accept(): accept() finds nothing and returns null
    env.getSelectorManager().invoke(() -> tcp.accept(serverKey));

    awaitSelectorLoop();
    assertEquals(List.of(), reported);
    assertEquals(List.of(), handled);
    assertTrue(env.getSelectorManager().isAlive(), "the selector thread still runs");
  }

  @Test
  @DisplayName("a datagram whose reader throws is dropped, and the next datagram is still read")
  void datagramWhoseReaderThrows() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, errorHandler);
    CompletableFuture<byte[]> good = new CompletableFuture<>();
    wire.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) {
        byte[] bytes = new byte[m.remaining()];
        m.get(bytes);
        if (bytes[0] == 'x') {
          throw new IllegalStateException("a reader that a crafted datagram makes throw");
        }
        good.complete(bytes);
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) {
      }
    });

    try (DatagramSocket peer = new DatagramSocket(0, localhost)) {
      peer.send(new DatagramPacket(new byte[] {'x'}, 1, address));
      peer.send(new DatagramPacket(new byte[] {'o', 'k'}, 2, address));

      try {
        assertEquals("ok", new String(good.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), java.nio.charset.StandardCharsets.US_ASCII));
      } catch (TimeoutException e) {
        throw new AssertionError("the second datagram never arrived, reported to the environment: " + reported, e);
      }
    }
    awaitSelectorLoop();
    assertEquals(List.of(), reported);
    assertTrue(env.getSelectorManager().isAlive(), "the selector thread still runs");
    assertEquals(1, handled.size(), () -> "handled: " + handled);
    assertInstanceOf(IllegalStateException.class, handled.get(0));
  }

  @Test
  @DisplayName("a socket whose reader throws is closed, and the selector stays alive")
  void socketWhoseReaderThrows() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    WireTransportLayer wire = new WireTransportLayer(
        new InetSocketAddress(localhost, FreePorts.freePort(localhost)), env, errorHandler);
    TCPLayer tcp = (TCPLayer) field(WireTransportLayer.class, "tcp").get(wire);
    CompletableFuture<Exception> toldReader = new CompletableFuture<>();

    try (ServerSocket server = new ServerSocket(0, 1, localhost)) {
      server.setSoTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
      InetSocketAddress serverAddress = (InetSocketAddress) server.getLocalSocketAddress();
      env.getSelectorManager().invoke(() -> {
        try {
          new WireSocket(tcp, serverAddress, new SocketCallback<InetSocketAddress>() {
            @Override
            public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
              sock.register(true, false, new P2PSocketReceiver<InetSocketAddress>() {
                @Override
                public void receiveSelectResult(P2PSocket<InetSocketAddress> socket, boolean canRead, boolean canWrite) throws IOException {
                  // consume the crafted bytes first: closing a socket with unread input makes the kernel send a reset
                  // instead of an orderly close, and the peer then sees "Connection reset" instead of the end of the stream
                  socket.read(ByteBuffer.allocate(16));
                  throw new IllegalStateException("a reader that crafted bytes make throw");
                }

                @Override
                public void receiveException(P2PSocket<InetSocketAddress> socket, Exception ioe) {
                  toldReader.complete(ioe);
                }
              });
            }

            @Override
            public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception ex) {
              toldReader.completeExceptionally(ex);
            }
          }, null);
        } catch (IOException e) {
          toldReader.completeExceptionally(e);
        }
      });
      try (Socket accepted = server.accept()) {
        accepted.setSoTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        accepted.getOutputStream().write(1);
        accepted.getOutputStream().flush();

        assertEquals(-1, accepted.getInputStream().read(), "the node closes the socket");
      }
    }
    assertInstanceOf(IOException.class, toldReader.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    awaitSelectorLoop();
    assertEquals(List.of(), reported);
    assertTrue(env.getSelectorManager().isAlive(), "the selector thread still runs");
    assertEquals(1, handled.size(), () -> "handled: " + handled);
    assertInstanceOf(IllegalStateException.class, handled.get(0));
  }

  @Test
  @DisplayName("a flood of datagrams whose reader throws logs a bounded number of lines")
  void floodOfDatagramsWhoseReaderThrows() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, errorHandler);
    wire.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) {
        throw new IllegalStateException("a reader that a crafted datagram makes throw");
      }
      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) {
      }
    });
    // slf4j-simple writes to whatever System.err is at the time of each line
    PrintStream originalErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
    try (DatagramSocket peer = new DatagramSocket(0, localhost)) {
      for (int i = 0; i < FLOOD_DATAGRAMS; i++) {
        peer.send(new DatagramPacket(new byte[] {'x'}, 1, address));
        if (i % 100 == 99) Thread.sleep(1); // let the receive buffer drain
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
      int seen = -1;
      // wait until the node has read what it is going to read
      while (seen != handled.size() && System.nanoTime() < deadline) {
        seen = handled.size();
        awaitSelectorLoop();
        Thread.sleep(50);
      }
    } finally {
      System.setErr(originalErr);
    }
    awaitSelectorLoop();
    assertTrue(handled.size() >= MIN_FLOOD_DATAGRAMS_READ,
        () -> "the node read only " + handled.size() + " datagrams, too few to show a flood");
    long lines = captured.toString(StandardCharsets.UTF_8).lines()
        .filter(line -> line.contains("Dropping a datagram")).count();
    assertTrue(lines <= MAX_LOG_LINES_PER_FLOOD,
        () -> lines + " log lines for " + handled.size() + " bad datagrams");
    assertEquals(List.of(), reported);
    assertTrue(env.getSelectorManager().isAlive(), "the selector thread still runs");
  }

  /** Lets the selector thread finish its current loop, so that whatever it reports has been reported. */
  private void awaitSelectorLoop() throws Exception {
    CompletableFuture<Void> looped = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> looped.complete(null));
    try {
      looped.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException dropped) {
      // a destroyed selector drops the invocation, the assertions tell why
    }
  }

  private static Field field(Class<?> type, String name) throws ReflectiveOperationException {
    Field field = type.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }
}
