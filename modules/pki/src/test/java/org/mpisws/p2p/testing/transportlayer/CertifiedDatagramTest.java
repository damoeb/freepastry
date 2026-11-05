package org.mpisws.p2p.testing.transportlayer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ALICE;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.BOB;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.NO_PASSWORD;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.awaitReady;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ca;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.freePort;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.store;
import static org.mpisws.p2p.testing.transportlayer.CertifiedDeferredDeliveryTest.await;
import static org.mpisws.p2p.testing.transportlayer.CertifiedDeferredDeliveryTest.onSelector;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CAToolImpl;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryEndpoint;
import rice.pastry.commonapi.PastryEndpointMessage;
import rice.pastry.routing.RouteMessage;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * End to end, on loopback with real sockets and TLS: datagrams are outside TLS, so in certified mode nothing that
 * arrives over UDP may pass for an authenticated node. A ring of two certified nodes, A and B (each has
 * authenticated the other), is attacked by a node M that has no certificate. M runs the plain socket stack (the
 * datagram format is the same: TLS only wraps the TCP sockets), claims B's id and sends A a routed application
 * message as a datagram ({@code WireTransportLayer.TRANSPORT_TYPE_DATAGRAM}).
 */
class CertifiedDatagramTest {

  /** how long a forged datagram gets to have an effect (it would take a few milliseconds on loopback) */
  static final long GRACE_MILLIS = 2_000;

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();

  PastryNode a;
  PastryNode b;
  CertifiedDeferredDeliveryTest.Receiver receiverA;
  CertifiedDeferredDeliveryTest.Receiver receiverB;
  InetSocketAddress addressB;
  InetAddress bindAddress;

  @BeforeEach
  void setUp() throws Exception {
    CAToolImpl ca = ca();
    int port = freePort();
    CertifiedSocketPastryNodeFactory factoryA = new CertifiedSocketPastryNodeFactory(store(ca, ALICE), NO_PASSWORD, port, environment());
    a = factoryA.newNode();
    nodes.add(a);
    receiverA = new CertifiedDeferredDeliveryTest.Receiver(a);
    InetSocketAddress addressA = new InetSocketAddress(factoryA.getBindAddress(), port);
    bindAddress = factoryA.getBindAddress();
    a.boot(addressA);
    awaitReady(a);

    int portB = freePort();
    CertifiedSocketPastryNodeFactory factoryB = new CertifiedSocketPastryNodeFactory(store(ca, BOB), NO_PASSWORD, portB, environment());
    b = factoryB.newNode();
    nodes.add(b);
    receiverB = new CertifiedDeferredDeliveryTest.Receiver(b);
    addressB = new InetSocketAddress(factoryB.getBindAddress(), portB);
    b.boot(addressA);
    awaitReady(b);
    await(() -> b.getLeafSet().member(ALICE) && a.getLeafSet().member(BOB), "A and B have each other in their leaf sets");
    assertTrue(factoryA.getCertifiedIdBindStrategy().isAuthenticated(BOB), "A has authenticated B");
  }

  @AfterEach
  void tearDown() {
    nodes.forEach(PastryNode::destroy);
    environments.forEach(Environment::destroy);
  }

  @Test
  @DisplayName("a datagram that claims an authenticated id does not reach the application")
  void aForgedDatagramFromAnAuthenticatedIdIsNotDelivered() throws Exception {
    // M at its own address, claiming B's id
    PastryNode m = attacker(BOB, null);

    sendDatagram(m, 13);

    assertNever(() -> receiverA.received.contains(13), "A's application got the forged message");
    assertEquals(List.of(), receiverA.received, "numbers A's application got");
  }

  @Test
  @DisplayName("a datagram that claims an authenticated node's address with a newer epoch does not kill that node")
  void aForgedDatagramWithANewerEpochDoesNotEvictAnAuthenticatedNode() throws Exception {
    NodeHandle realB = handleOf(a, BOB);
    // M claims B's address (it binds a socket of its own, but the stack's headers say B's address) and, created
    // after B, has the newer epoch; the id is a fresh one, the node change rule only looks at address and epoch
    Thread.sleep(5);
    PastryNode m = attacker(new RandomNodeIdFactory(a.getEnvironment()).generateNodeId(), addressB);
    assertTrue(((SocketNodeHandle) m.getLocalHandle()).getEpoch() > ((SocketNodeHandle) realB).getEpoch());

    sendDatagram(m, 14);

    assertNever(() -> realB.getLiveness() >= NodeHandle.LIVENESS_DEAD, "A marked the real B dead");
    assertTrue(a.getLeafSet().member(BOB), "B is still in A's leaf set");
    assertEquals(realB, handleOf(a, BOB), "B's handle in A's leaf set");
    assertEquals(List.of(), receiverA.received, "numbers A's application got");
  }

  @Test
  @DisplayName("an application message that asks for a datagram still gets through between certified nodes")
  void aDatagramMessageBetweenCertifiedNodesIsDelivered() throws Exception {
    Map<String, Object> options = new HashMap<>();
    options.put(WireTransportLayer.OPTION_TRANSPORT_TYPE, WireTransportLayer.TRANSPORT_TYPE_DATAGRAM);
    onSelector(b, () -> receiverB.endpoint.route(ALICE, new CertifiedDeferredDeliveryTest.Numbered(15), null, null, options));
    await(() -> receiverA.received.contains(15), "A's application got B's message");
  }

  // helpers

  /** sends A a routed message for A's id, carrying this number, from M as a datagram */
  void sendDatagram(PastryNode m, int number) throws Exception {
    CertifiedDeferredDeliveryTest.Receiver receiverM = new CertifiedDeferredDeliveryTest.Receiver(m);
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    a.getLocalHandle().serialize(buf);
    onSelector(m, () -> {
      NodeHandle aAtM = m.readNodeHandle(new SimpleInputBuffer(buf.getBytes()));
      PastryEndpointMessage message = new PastryEndpointMessage(((PastryEndpoint) receiverM.endpoint).getAddress(),
          new CertifiedDeferredDeliveryTest.Numbered(number), m.getLocalHandle());
      RouteMessage route = new RouteMessage(ALICE, message, aAtM, m.getConfig().routeMsgVersion());
      Map<String, Object> options = new HashMap<>();
      options.put(WireTransportLayer.OPTION_TRANSPORT_TYPE, WireTransportLayer.TRANSPORT_TYPE_DATAGRAM);
      options.put(PriorityTransportLayer.OPTION_PRIORITY, message.getPriority());
      return m.send(aAtM, route, null, options);
    });
  }

  /**
   * @param claimedAddress the address M's headers name as its own, or null for its real one
   * @return M, a node of the plain (uncertified) socket stack with this id; it is not booted
   */
  PastryNode attacker(Id id, InetSocketAddress claimedAddress) throws Exception {
    int port = freePort();
    InetSocketAddress real = new InetSocketAddress(bindAddress, port);
    SocketPastryNodeFactory factory = new SocketPastryNodeFactory(null, bindAddress, port, environment()) {
      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> getWireTransportLayer(InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
        return super.getWireTransportLayer(real, pn);
      }
    };
    PastryNode m = factory.newNode(id, new MultiInetSocketAddress(claimedAddress == null ? real : claimedAddress));
    nodes.add(m);
    return m;
  }

  static NodeHandle handleOf(PastryNode node, Id id) {
    return node.getLeafSet().asList().stream().filter(h -> h.getNodeId().equals(id)).findFirst().orElseThrow();
  }

  static void assertNever(BooleanSupplier condition, String what) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(GRACE_MILLIS);
    while (System.nanoTime() < deadline) {
      assertTrue(!condition.getAsBoolean(), what);
      Thread.sleep(20);
    }
    assertTrue(!condition.getAsBoolean(), what);
  }

  Environment environment() {
    Environment env = new Environment();
    environments.add(env);
    return env;
  }
}
