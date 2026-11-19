package rice.pastry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;

class PastryNodeMaintenanceTasksTest {

  private static final long TIMEOUT_SECONDS = 10;

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
  @DisplayName("a second doneNode keeps the one route set and one leaf set maintenance task")
  void doneNodeSchedulesTheMaintenanceTasksOnlyOnce() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port;
    try (ServerSocket probe = new ServerSocket(0, 1, loopback)) {
      port = probe.getLocalPort();
    }
    PastryNode node = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), loopback, port, env).newNode();
    CompletableFuture<ScheduledMessage[]> tasks = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> {
      try {
        node.doneNode(List.of());
        ScheduledMessage routeSet = node.routeSetMaintenanceTask();
        ScheduledMessage leafSet = node.leafSetMaintenanceTask();
        node.doneNode(List.of());
        tasks.complete(new ScheduledMessage[] {routeSet, leafSet, node.routeSetMaintenanceTask(),
            node.leafSetMaintenanceTask()});
      } catch (Throwable t) {
        tasks.completeExceptionally(t);
      }
    });

    ScheduledMessage[] actual = tasks.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertNotNull(actual[0]);
    assertNotNull(actual[1]);
    assertSame(actual[0], actual[2]);
    assertSame(actual[1], actual[3]);
    assertFalse(actual[2].isCancelled());
    assertNotSame(actual[0], actual[1]);
  }
}
