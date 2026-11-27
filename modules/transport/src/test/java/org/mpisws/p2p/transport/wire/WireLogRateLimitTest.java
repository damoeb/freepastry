package org.mpisws.p2p.transport.wire;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.testing.transportlayer.FreePorts;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import rice.environment.Environment;

/** Log lines that remote input can trigger at will are limited per kind: at most the limit, then one summary line. */
class WireLogRateLimitTest {
  private static final long TIMEOUT_SECONDS = 10;
  private static final int TRIGGERS = 200;
  /** the default limit (logging_rate_limit_max_lines per interval) plus the summary line */
  private static final int MAX_LOG_LINES = 11;

  private final Environment env = new Environment();

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("receivers that throw again when told that the socket closed log a bounded number of lines")
  void receiverThatThrowsAgain() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    WireTransportLayer wire = new WireTransportLayer(
        new InetSocketAddress(localhost, FreePorts.freePort(localhost)), env, null);
    TCPLayer tcp = (TCPLayer) WireBadInputTest.field(WireTransportLayer.class, "tcp").get(wire);
    P2PSocketReceiver<InetSocketAddress> receiver = new P2PSocketReceiver<>() {
      @Override
      public void receiveSelectResult(P2PSocket<InetSocketAddress> socket, boolean canRead, boolean canWrite) {
      }

      @Override
      public void receiveException(P2PSocket<InetSocketAddress> socket, Exception ioe) {
        throw new IllegalStateException("a receiver that throws again");
      }
    };
    try (ServerSocket server = new ServerSocket(0, 1, localhost)) {
      InetSocketAddress serverAddress = (InetSocketAddress) server.getLocalSocketAddress();
      CompletableFuture<WireSocket> opened = new CompletableFuture<>();
      env.getSelectorManager().invoke(() -> {
        try {
          opened.complete(new WireSocket(tcp, serverAddress, ignoringCallback(), null));
        } catch (IOException e) {
          opened.completeExceptionally(e);
        }
      });
      WireSocket socket = opened.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      Method receiverFailed = WireSocket.class.getDeclaredMethod("receiverFailed", P2PSocketReceiver.class, RuntimeException.class);
      receiverFailed.setAccessible(true);

      long lines = countLines("threw again", () -> {
        for (int i = 0; i < TRIGGERS; i++) {
          try {
            receiverFailed.invoke(socket, receiver, new IllegalStateException("crafted"));
          } catch (ReflectiveOperationException e) {
            throw new LinkageError(e.getMessage(), e);
          }
        }
      });

      assertTrue(lines <= MAX_LOG_LINES, () -> lines + " log lines for " + TRIGGERS + " triggers");
      assertTrue(lines > 0, "the failure is logged at all");
    }
  }

  @Test
  @DisplayName("sockets that cannot be opened log a bounded number of lines")
  void socketsThatCannotBeOpened() throws Exception {
    InetAddress localhost = InetAddress.getLoopbackAddress();
    WireTransportLayer wire = new WireTransportLayer(
        new InetSocketAddress(localhost, FreePorts.freePort(localhost)), env, null);
    TCPLayer tcp = (TCPLayer) WireBadInputTest.field(WireTransportLayer.class, "tcp").get(wire);
    // connecting to port 0 fails at once with an IOException (BindException on macOS)
    InetSocketAddress unresolved = new InetSocketAddress(localhost, 0);

    long lines = countLines("OPENING PATH", () -> {
      for (int i = 0; i < TRIGGERS; i++) tcp.openSocket(unresolved, ignoringCallback(), null);
    });

    assertTrue(lines <= MAX_LOG_LINES, () -> lines + " log lines for " + TRIGGERS + " triggers");
    assertTrue(lines > 0, "the failure is logged at all");
  }

  private static SocketCallback<InetSocketAddress> ignoringCallback() {
    return new SocketCallback<>() {
      @Override
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
      }

      @Override
      public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception ex) {
      }
    };
  }

  /** slf4j-simple writes to whatever System.err is at the time of each line */
  private static long countLines(String marker, Runnable trigger) {
    PrintStream originalErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
    try {
      trigger.run();
    } finally {
      System.setErr(originalErr);
    }
    return captured.toString(StandardCharsets.UTF_8).lines().filter(line -> line.contains(marker)).count();
  }
}
