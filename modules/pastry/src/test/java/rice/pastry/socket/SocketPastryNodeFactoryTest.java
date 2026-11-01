package rice.pastry.socket;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeListener;
import rice.pastry.standard.RandomNodeIdFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
      public void pastryNodeReady(PastryNode n, boolean ready) {
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
      public void pastryNodeReady(PastryNode n, boolean isReady) {
        if (isReady) ready.complete(true);
      }
    });

    node.boot(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));

    assertTrue(ready.get(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }
}
