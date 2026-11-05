package rice.pastry.socket;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.PastryNode;

/**
 * A real socket ring on loopback in one JVM, booted the way the tutorials boot it: every node joins through the
 * first one. The ids are fixed so that some joins are routed on from the first node to another one, which is
 * where the join request is read and then sent on over the wire.
 */
class SocketRingJoinTest {

  static final long READY_SECONDS = 10;

  /** the first node, then nodes whose joins the first node routes on to the numerically closer node it knows */
  static final String[] IDS = {"00", "80", "A0", "60", "D0", "30"};

  Environment env;

  @BeforeEach
  void setUp() {
    env = new Environment();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("every node of a six node socket ring booted through the first node becomes ready")
  void everyNodeOfASocketRingBecomesReady() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();
    InetSocketAddress boot = null;
    List<PastryNode> nodes = new ArrayList<>();
    for (String id : IDS) {
      int port = freePort(loopback);
      PastryNode node = new SocketPastryNodeFactory(() -> Id.build(id), loopback, port, env).newNode();
      nodes.add(node);
      if (boot == null) {
        boot = new InetSocketAddress(loopback, port);
      }

      node.boot(boot);

      node.awaitReadyOrJoinFailed(READY_SECONDS, TimeUnit.SECONDS);
      assertTrue(node.isReady(), "node " + nodes.size() + " (" + node.getNodeId() + ") is ready, join failed: "
          + node.joinFailed() + " " + node.joinFailedReason());
    }
  }

  static int freePort(InetAddress address) throws IOException {
    try (ServerSocket probe = new ServerSocket(0, 1, address)) {
      return probe.getLocalPort();
    }
  }
}
