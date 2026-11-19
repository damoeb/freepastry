package rice.pastry.socket;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeListener;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.selector.SelectorLoop;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class SocketPastryNodeFactoryTest {

  private static final long READY_TIMEOUT_SECONDS = 10;

  private Environment env;

  @BeforeEach
  void setUp() {
    env = new Environment();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void bootFromAnotherThreadSetsTheNodeReadyOnTheSelectorThread() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port;
    try (ServerSocket probe = new ServerSocket(0, 1, loopback)) {
      port = probe.getLocalPort();
    }
    PastryNode node = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), loopback, port, env).newNode();
    CompletableFuture<Boolean> readyOnSelector = new CompletableFuture<Boolean>();
    node.addPastryNodeListener(new PastryNodeListener() {
      @Override
      public void pastryNodeReady(PastryNode pn, boolean ready) {
        if (ready) readyOnSelector.complete(env.getSelectorManager().isSelectorThread());
      }
    });

    node.boot((Object)null);

    assertTrue(readyOnSelector.get(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void aFailedBindLeavesTheFactorysEnvironmentRunning() throws Exception {
    env.getParameters().setBoolean("pastry_socket_increment_port_after_construction", false);
    InetAddress loopback = InetAddress.getLoopbackAddress();
    try (ServerSocket occupied = new ServerSocket(0, 1, loopback)) {
      SocketPastryNodeFactory factory =
        new SocketPastryNodeFactory(new RandomNodeIdFactory(env), loopback, occupied.getLocalPort(), env);

      assertThrows(RuntimeException.class, factory::newNode);
    }

    CountDownLatch selectorRuns = new CountDownLatch(1);
    env.getSelectorManager().invoke(selectorRuns::countDown);
    assertTrue(selectorRuns.await(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  @DisplayName("a failed bind stops the selector and processor cloned for the node, but not the shared ones")
  void aFailedBindDestroysWhatWasClonedForTheNode() throws Exception {
    env.getParameters().setBoolean("pastry_socket_increment_port_after_construction", false);
    env.getParameters().setBoolean("pastry_factory_multipleNodes", true);
    env.getParameters().setBoolean("pastry_factory_selectorPerNode", true);
    env.getParameters().setBoolean("pastry_factory_processorPerNode", true);
    InetAddress loopback = InetAddress.getLoopbackAddress();
    AtomicReference<Environment> cloned = new AtomicReference<>();
    try (ServerSocket occupied = new ServerSocket(0, 1, loopback)) {
      SocketPastryNodeFactory factory =
        new SocketPastryNodeFactory(new RandomNodeIdFactory(env), loopback, occupied.getLocalPort(), env) {
          @Override
          protected Environment cloneEnvironment(Environment rootEnvironment, Id nodeId) {
            Environment clone = super.cloneEnvironment(rootEnvironment, nodeId);
            cloned.set(clone);
            return clone;
          }
        };

      assertThrows(RuntimeException.class, factory::newNode);
    }

    SelectorLoop clonedSelector = cloned.get().getSelectorManager();
    assertNotSame(env.getSelectorManager(), clonedSelector);
    clonedSelector.join(TimeUnit.SECONDS.toMillis(READY_TIMEOUT_SECONDS));
    assertFalse(clonedSelector.isAlive());
    CountDownLatch selectorRuns = new CountDownLatch(1);
    env.getSelectorManager().invoke(selectorRuns::countDown);
    assertTrue(selectorRuns.await(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void secondNodeBootedToLoopbackReachesFirstNodeBoundToTheLocalAddress() throws Exception {
    InetAddress bind = InetAddress.getLocalHost();
    assumeFalse(bind.isLoopbackAddress(), "no non-loopback local address");
    int firstPort;
    int secondPort;
    try (ServerSocket a = new ServerSocket(0, 1, bind); ServerSocket b = new ServerSocket(0, 1, bind)) {
      firstPort = a.getLocalPort();
      secondPort = b.getLocalPort();
    }
    PastryNode first = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), bind, firstPort, env).newNode();
    PastryNode second = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), bind, secondPort, env).newNode();
    CompletableFuture<Boolean> firstReady = readyFuture(first);
    CompletableFuture<Boolean> secondReady = readyFuture(second);

    first.boot(new InetSocketAddress(InetAddress.getLoopbackAddress(), firstPort));
    assertTrue(firstReady.get(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    second.boot(new InetSocketAddress(InetAddress.getLoopbackAddress(), firstPort));

    assertTrue(secondReady.get(30, TimeUnit.SECONDS));
  }

  private static CompletableFuture<Boolean> readyFuture(PastryNode node) {
    CompletableFuture<Boolean> ready = new CompletableFuture<Boolean>();
    node.addPastryNodeListener(new PastryNodeListener() {
      @Override
      public void pastryNodeReady(PastryNode pn, boolean isReady) {
        if (isReady) ready.complete(true);
      }
    });
    return ready;
  }

  @Test
  void bootToOwnLoopbackAddressOnAnotherInterfaceCreatesANewRing() throws Exception {
    InetAddress bind = InetAddress.getLocalHost();
    int port;
    try (ServerSocket probe = new ServerSocket(0, 1, bind)) {
      port = probe.getLocalPort();
    }
    PastryNode node = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), bind, port, env).newNode();
    CompletableFuture<Boolean> ready = new CompletableFuture<Boolean>();
    node.addPastryNodeListener(new PastryNodeListener() {
      @Override
      public void pastryNodeReady(PastryNode pn, boolean isReady) {
        if (isReady) ready.complete(true);
      }
    });

    node.boot(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));

    assertTrue(ready.get(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }
}
