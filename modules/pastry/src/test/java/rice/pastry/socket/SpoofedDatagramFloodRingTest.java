package rice.pastry.socket;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;

/**
 * C1/C5: the per-prefix datagram budget of the wire layer is keyed on the source address of a datagram, which an
 * outsider can spoof. Flooding with a neighbour's prefix as the source empties that prefix's bucket, so the
 * neighbour's liveness pongs are dropped with the flood and the victim declares an honest neighbour dead.
 *
 * <p>On loopback the flooder shares the neighbour's address 127.0.0.1, which is what a spoofed source amounts to.
 */
class SpoofedDatagramFloodRingTest {
  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
  private static final long READY_SECONDS = 10;
  /** a small datagram bucket, so that a modest flood keeps it empty */
  private static final int DATAGRAM_RATE_PER_SECOND = 20;
  private static final int DATAGRAM_BURST = 20;
  /** quick liveness checks: about 2 s from the first unanswered ping to dead */
  private static final int PING_DELAY_MILLIS = 100;
  private static final long FLOOD_MILLIS = 8000;
  private static final int DATAGRAMS_PER_MILLISECOND = 5;
  private static final long CHECK_INTERVAL_MILLIS = 500;

  private final Environment env = new Environment();
  private final AtomicBoolean isFlooding = new AtomicBoolean(true);

  @AfterEach
  void tearDown() {
    isFlooding.set(false);
    env.destroy();
  }

  @Test
  @DisplayName("a datagram flood from a neighbour's prefix does not make the victim declare the neighbour dead")
  void spoofedFloodDoesNotKillNeighbour() throws Exception {
    env.getParameters().setInt("transport_wire_datagram_rate_per_prefix_per_second", DATAGRAM_RATE_PER_SECOND);
    env.getParameters().setInt("transport_wire_datagram_burst_per_prefix", DATAGRAM_BURST);
    env.getParameters().setInt("pastry_socket_scm_ping_delay", PING_DELAY_MILLIS);
    int victimPort = freePort();
    PastryNode victim = new SocketPastryNodeFactory(() -> Id.build("00"), LOOPBACK, victimPort, env).newNode();
    InetSocketAddress victimAddress = new InetSocketAddress(LOOPBACK, victimPort);
    victim.boot(victimAddress);
    victim.awaitReadyOrJoinFailed(READY_SECONDS, TimeUnit.SECONDS);
    PastryNode neighbour = new SocketPastryNodeFactory(() -> Id.build("80"), LOOPBACK, freePort(), env).newNode();
    neighbour.boot(victimAddress);
    neighbour.awaitReadyOrJoinFailed(READY_SECONDS, TimeUnit.SECONDS);
    assertTrue(victim.isReady() && neighbour.isReady(), "both nodes are ready");
    NodeHandle neighbourAtVictim = awaitNeighbourInLeafSet(victim, neighbour);

    AtomicInteger sent = new AtomicInteger();
    Thread flooder = new Thread(() -> flood(victimAddress, sent), "spoofed datagram flood");
    flooder.start();
    int worstLiveness = NodeHandle.LIVENESS_ALIVE;
    long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(FLOOD_MILLIS);
    while (System.nanoTime() < end) {
      env.getSelectorManager().invoke(neighbourAtVictim::checkLiveness);
      Thread.sleep(CHECK_INTERVAL_MILLIS);
      worstLiveness = Math.max(worstLiveness, neighbourAtVictim.getLiveness());
    }
    isFlooding.set(false);
    flooder.join();

    assertTrue(sent.get() > FLOOD_MILLIS, "the flood ran: " + sent.get() + " datagrams");
    assertTrue(worstLiveness < NodeHandle.LIVENESS_DEAD,
        "the victim declared its honest neighbour dead during a flood of " + sent.get() + " datagrams from its prefix");
  }

  private void flood(InetSocketAddress target, AtomicInteger sent) {
    try (DatagramSocket socket = new DatagramSocket(0, LOOPBACK)) {
      byte[] junk = {0x01, 0x02, 0x03, 0x04};
      while (isFlooding.get()) {
        for (int i = 0; i < DATAGRAMS_PER_MILLISECOND; i++) {
          socket.send(new DatagramPacket(junk, junk.length, target));
          sent.incrementAndGet();
        }
        Thread.sleep(1);
      }
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private static NodeHandle awaitNeighbourInLeafSet(PastryNode victim, PastryNode neighbour) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(READY_SECONDS);
    while (System.nanoTime() < deadline) {
      for (NodeHandle handle : victim.getLeafSet().asList()) {
        if (handle.getId().equals(neighbour.getNodeId())) return handle;
      }
      Thread.sleep(50);
    }
    assertNotNull(null, "the neighbour reached the victim's leaf set");
    throw new AssertionError();
  }

  private static int freePort() throws IOException {
    try (ServerSocket probe = new ServerSocket(0, 1, LOOPBACK)) {
      return probe.getLocalPort();
    }
  }
}
