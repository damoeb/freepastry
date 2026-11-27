package rice.pastry.socket;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.standard.RandomNodeIdFactory;

class SocketPastryNodeFactoryNewNodeTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

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
  @DisplayName("newNode called off the selector thread reports a runtime failure of the node construction instead of waiting forever")
  void newNodeReportsARuntimeFailureOfTheConstruction() throws Exception {
    IllegalStateException failure = new IllegalStateException("construction failed");
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port;
    try (ServerSocket probe = new ServerSocket(0, 1, loopback)) {
      port = probe.getLocalPort();
    }
    SocketPastryNodeFactory target = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), loopback, port, env) {
      @Override
      protected Environment cloneEnvironment(Environment rootEnvironment, Id nodeId) {
        throw failure;
      }
    };

    RuntimeException actual = assertTimeoutPreemptively(TIMEOUT, () -> assertThrows(RuntimeException.class, target::newNode));

    assertSame(failure, actual);
  }
}
