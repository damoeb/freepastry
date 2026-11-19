package rice.pastry.pns;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.FutureContinuation;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.leafset.LeafSet;
import rice.pastry.routing.RouteSet;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.ProximityNeighborSelector;

class PNSApplicationTest {
  private static final long TIMEOUT_SECONDS = 10;
  /** how long a cancelled request is given to be answered anyway */
  private static final long CANCELLED_WAIT_MILLIS = 500;

  private Environment env;
  private final Map<PastryNode, ProximityNeighborSelectionApplication> pnsOf = new ConcurrentHashMap<>();
  private PastryNode first;
  private PastryNode second;

  @BeforeEach
  void setUp() throws Exception {
    env = new Environment();
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int firstPort = freePort(loopback);
    first = newNode("10", loopback, firstPort);
    first.boot(new InetSocketAddress(loopback, firstPort));
    first.awaitReadyOrJoinFailed(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    second = newNode("90", loopback, freePort(loopback));
    second.boot(new InetSocketAddress(loopback, firstPort));
    second.awaitReadyOrJoinFailed(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertTrue(first.isReady() && second.isReady(), "both nodes are ready");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("getLeafSet answers with the leaf set of the node asked")
  void getLeafSetAnswersWithTheRemoteLeafSet() throws Exception {
    NodeHandle target = first.getLocalHandle();
    FutureContinuation<LeafSet, Exception> actual = new FutureContinuation<>();

    onSelector(() -> pnsOf.get(second).getLeafSet(target, actual));

    assertEquals(target, actual.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).get(0));
  }

  @Test
  @DisplayName("getRouteRow answers with the asked row of the node asked")
  void getRouteRowAnswersWithTheRemoteRow() throws Exception {
    short row = 0;
    RouteSet[] expected = first.getRoutingTable().getRow(row);
    FutureContinuation<RouteSet[], Exception> actual = new FutureContinuation<>();

    onSelector(() -> pnsOf.get(second).getRouteRow(first.getLocalHandle(), row, actual));

    assertArrayEquals(handles(expected), handles(actual.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS)));
  }

  @Test
  @DisplayName("a cancelled getLeafSet is not answered")
  void aCancelledGetLeafSetIsNotAnswered() throws Exception {
    FutureContinuation<LeafSet, Exception> actual = new FutureContinuation<>();

    onSelector(() -> pnsOf.get(second).getLeafSet(first.getLocalHandle(), actual).cancel());
    Thread.sleep(CANCELLED_WAIT_MILLIS);

    assertFalse(actual.future().isDone());
  }

  @Test
  @DisplayName("a cancelled getRouteRow is not answered")
  void aCancelledGetRouteRowIsNotAnswered() throws Exception {
    FutureContinuation<RouteSet[], Exception> actual = new FutureContinuation<>();

    onSelector(() -> pnsOf.get(second).getRouteRow(first.getLocalHandle(), (short) 0, actual).cancel());
    Thread.sleep(CANCELLED_WAIT_MILLIS);

    assertFalse(actual.future().isDone());
  }

  private PastryNode newNode(String id, InetAddress address, int port) throws IOException {
    return new SocketPastryNodeFactory(() -> Id.build(id), address, port, env) {
      @Override
      protected ProximityNeighborSelector getProximityNeighborSelector(PastryNode pn) {
        ProximityNeighborSelector result = super.getProximityNeighborSelector(pn);
        pnsOf.put(pn, (ProximityNeighborSelectionApplication) result);
        return result;
      }
    }.newNode();
  }

  private void onSelector(Runnable r) throws InterruptedException {
    CountDownLatch done = new CountDownLatch(1);
    env.getSelectorManager().invoke(() -> {
      r.run();
      done.countDown();
    });
    assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  private static String[] handles(RouteSet[] row) {
    String[] result = new String[row.length];
    for (int i = 0; i < row.length; i++) {
      result[i] = row[i] == null ? null : row[i].closestNode() == null ? null : String.valueOf(row[i].closestNode().getNodeId());
    }
    return result;
  }

  private static int freePort(InetAddress address) throws IOException {
    try (ServerSocket probe = new ServerSocket(0, 1, address)) {
      return probe.getLocalPort();
    }
  }
}
