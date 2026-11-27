package org.mpisws.p2p.testing.transportlayer;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.NO_PASSWORD;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ca;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.freePort;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.store;
import static org.mpisws.p2p.testing.transportlayer.CertifiedDeferredDeliveryTest.onSelector;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;

/**
 * Design 3f against the join-window trust of certified mode: the leaf set a joiner gets in the answer to its own join
 * request comes from an authenticated root, and the joiner checks every handle in it. The per-sender identity probe
 * budget (burst {@code pastry_budget_identity_probe_burst}) must not throttle that answer, or a joiner in a ring larger
 * than the burst would learn its leaf set at one handle per 10 s. Measured 2026-11-27: with burst 4 and with burst 1000
 * every join of a 10-node ring takes the same 10-12 s to ready and a complete leaf set; the members the joiner first
 * admits answer it over their own TLS connections, which authenticates them without a probe. Integration tag: about
 * two minutes.
 */
class CertifiedJoinProbeBudgetTest {
  /** more nodes than the probe burst (4) plus one, all within one leaf set (24) */
  static final int RING_SIZE = 10;
  /** how long a join, and learning the whole leaf set, may take on loopback */
  static final long JOIN_SECONDS = 20;

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();

  @AfterEach
  void tearDown() {
    nodes.forEach(PastryNode::destroy);
    environments.forEach(Environment::destroy);
  }

  @Test
  @Tag("integration")
  @DisplayName("each certified node of a ring larger than the probe burst goes ready and learns its whole leaf set within the join time")
  void joinersOfARingLargerThanTheProbeBurstLearnTheirLeafSetInTime() throws Exception {
    CertificateAuthorityTool certificateAuthorityTool = ca();
    InetSocketAddress bootstrap = null;
    for (int n = 0; n < RING_SIZE; n++) {
      Id id = Id.build(String.format("%040X", 0x1000L * (n * 7919L % 997 + 1)));
      int port = freePort();
      Environment env = new Environment();
      environments.add(env);
      CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, id), NO_PASSWORD, port, env);
      PastryNode node = factory.newNode();
      nodes.add(node);
      if (bootstrap == null) bootstrap = new InetSocketAddress(factory.getBindAddress(), port);
      long startNanos = System.nanoTime();
      node.boot(bootstrap);

      awaitWithin(startNanos, () -> node.isReady(), "node " + n + " is ready");
      List<PastryNode> joined = List.copyOf(nodes);
      awaitWithin(startNanos, () -> leafSetIds(node).containsAll(otherIds(joined, node)),
          "node " + n + " has the other " + n + " nodes in its leaf set");
    }
  }

  private static void awaitWithin(long startNanos, java.util.concurrent.Callable<Boolean> condition, String what) throws Exception {
    long deadline = startNanos + TimeUnit.SECONDS.toNanos(JOIN_SECONDS);
    while (!condition.call() && System.nanoTime() < deadline) {
      Thread.sleep(100);
    }
    assertTrue(condition.call(), what + " within " + JOIN_SECONDS + " s of its boot");
  }

  private static Set<Id> leafSetIds(PastryNode node) throws Exception {
    return onSelector(node, () -> {
      Set<Id> ids = new HashSet<>();
      for (NodeHandle handle : node.getLeafSet().asList()) ids.add(handle.getNodeId());
      return ids;
    });
  }

  private static Set<Id> otherIds(List<PastryNode> joined, PastryNode node) {
    Set<Id> ids = new HashSet<>();
    for (PastryNode other : joined) {
      if (!other.equals(node)) ids.add(other.getNodeId());
    }
    return ids;
  }
}
