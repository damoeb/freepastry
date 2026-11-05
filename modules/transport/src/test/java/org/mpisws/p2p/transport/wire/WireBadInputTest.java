package org.mpisws.p2p.transport.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
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
import rice.environment.Environment;

/**
 * One bad datagram or one bad socket costs that datagram or that socket, never the selector thread: an exception
 * that reaches the selector thread reaches the environment's ExceptionStrategy, which destroys every node of the JVM.
 */
class WireBadInputTest {
  private static final long TIMEOUT_SECONDS = 10;

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
    WireTransportLayerImpl wire = new WireTransportLayerImpl(
        new InetSocketAddress(localhost, FreePorts.freePort(localhost)), env, errorHandler);
    TCPLayer tcp = (TCPLayer) field(WireTransportLayerImpl.class, "tcp").get(wire);
    SelectionKey serverKey = (SelectionKey) field(TCPLayer.class, "key").get(tcp);

    // the client reset its connection between select() and accept(): accept() finds nothing and returns null
    env.getSelectorManager().invoke(() -> tcp.accept(serverKey));

    awaitSelectorLoop();
    assertEquals(List.of(), reported);
    assertEquals(List.of(), handled);
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
