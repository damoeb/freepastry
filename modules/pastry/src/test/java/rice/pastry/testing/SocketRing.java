package rice.pastry.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import rice.environment.Environment;
import rice.environment.exception.ExceptionStrategy;
import rice.pastry.Id;
import rice.pastry.PastryNode;
import rice.pastry.socket.SocketPastryNodeFactory;

/**
 * A ring of socket nodes on loopback in one JVM, booted the way the tutorials boot one: every node joins through the
 * first. The ids are fixed, so the routes, and which node reads a message and sends it on, are the same on every run.
 *
 * <p>Every exception that reaches the environment's {@link ExceptionStrategy} is recorded before the default
 * strategy handles it (which destroys the environment when the selector thread throws), so a test can assert that
 * none did.
 */
public final class SocketRing implements AutoCloseable {
  /** ids spread over the ring, the first node routes some joins on to another one */
  public static final List<String> IDS = List.of("00", "80", "A0", "60", "D0", "30", "18", "F0");

  private static final long READY_SECONDS = 10;
  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

  /** Builds the applications of a node before it boots, as the tutorials do. */
  @FunctionalInterface
  public interface AppFactory<A> {
    A create(PastryNode node) throws IOException;
  }

  public final Environment env;
  public final List<PastryNode> nodes = new ArrayList<>();
  private final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());

  public SocketRing() {
    env = new Environment();
    ExceptionStrategy original = env.getExceptionStrategy();
    env.setExceptionStrategy((source, t) -> {
      exceptions.add(t);
      original.handleException(source, t);
    });
  }

  /**
   * Boots {@code size} nodes, each with its application, and waits until every one is ready.
   *
   * @return the applications, in the order of the nodes
   */
  public <A> List<A> boot(int size, AppFactory<A> apps) throws IOException, InterruptedException {
    List<A> result = new ArrayList<>();
    InetSocketAddress boot = null;
    for (int i = 0; i < size; i++) {
      int port;
      try (ServerSocket probe = new ServerSocket(0, 1, LOOPBACK)) {
        port = probe.getLocalPort();
      }
      String id = IDS.get(i);
      PastryNode node = new SocketPastryNodeFactory(() -> Id.build(id), LOOPBACK, port, env).newNode();
      nodes.add(node);
      result.add(apps.create(node));
      if (boot == null) {
        boot = new InetSocketAddress(LOOPBACK, port);
      }
      node.boot(boot);
      node.awaitReadyOrJoinFailed(READY_SECONDS, TimeUnit.SECONDS);
      assertTrue(node.isReady(), "node " + i + " (" + node.getNodeId() + ") is ready, join failed: "
          + node.joinFailed() + " " + node.joinFailedReason() + ", exceptions: " + exceptions);
    }
    return result;
  }

  /** Waits until the condition holds, failing with the message after the timeout. */
  public void await(String what, long seconds, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
    while (!condition.getAsBoolean()) {
      assertNoExceptions();
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out after " + seconds + " s waiting for: " + what);
      }
      Thread.sleep(50);
    }
  }

  /** Waits for a latch, failing with the message after the timeout. */
  public void await(String what, long seconds, CountDownLatch latch) throws InterruptedException {
    await(what + " (" + latch.getCount() + " left)", seconds, () -> latch.getCount() == 0);
  }

  /** Fails if any exception reached the environment's exception strategy. */
  public void assertNoExceptions() {
    synchronized (exceptions) {
      assertEquals(List.of(), exceptions, "exceptions that reached the environment");
    }
  }

  @Override
  public void close() {
    env.destroy();
  }
}
