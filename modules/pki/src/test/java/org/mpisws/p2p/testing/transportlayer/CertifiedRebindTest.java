package org.mpisws.p2p.testing.transportlayer;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ALICE;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.BOB;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.NO_PASSWORD;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.awaitReady;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ca;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.freePort;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.store;
import static org.mpisws.p2p.testing.transportlayer.CertifiedDatagramTest.assertNever;
import static org.mpisws.p2p.testing.transportlayer.CertifiedDatagramTest.handleOf;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryEndpoint;
import rice.pastry.commonapi.PastryEndpointMessage;
import rice.pastry.leafset.BroadcastLeafSet;
import rice.pastry.leafset.LeafSet;
import rice.pastry.routing.RouteMessage;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketNodeHandleFactory;
import rice.pastry.socket.SocketPastryNodeFactory;

/**
 * End to end, on loopback with real sockets and TLS: what a datagram's identity header can do to the address
 * bindings of a certified node (roadmap B10). Datagrams carry no certificate, so a node M without one can name any
 * handle in them: the identity layer offers that handle a binding of the address it names, and binding it (or
 * finding it bound already) used to mark the address alive. That skipped the liveness layer's check that a pong
 * answers a ping this node sent. A ring of two certified nodes, A and B; A detects a dead node quickly.
 */
class CertifiedRebindTest {

  /** datagrams M sends, a little apart */
  static final int DATAGRAMS = 5;

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();

  CertificateAuthorityTool certificateAuthorityTool;
  CertifiedSocketPastryNodeFactory factoryA;
  PastryNode a;
  PastryNode b;
  CertifiedDeferredDeliveryTest.Receiver receiverA;
  InetSocketAddress addressB;
  InetAddress bindAddress;

  @BeforeEach
  void setUp() throws Exception {
    certificateAuthorityTool = ca();
    int port = freePort();
    Environment environmentA = environment();
    // a dead node is found dead after two unanswered pings, a few hundred milliseconds
    environmentA.getParameters().setInt("pastry_socket_scm_ping_delay", 100);
    environmentA.getParameters().setInt("pastry_socket_scm_num_ping_tries", 2);
    factoryA = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, ALICE), NO_PASSWORD, port, environmentA);
    a = factoryA.newNode();
    nodes.add(a);
    receiverA = new CertifiedDeferredDeliveryTest.Receiver(a);
    bindAddress = factoryA.getBindAddress();
    InetSocketAddress addressA = new InetSocketAddress(bindAddress, port);
    a.boot(addressA);
    awaitReady(a);

    int portB = freePort();
    CertifiedSocketPastryNodeFactory factoryB = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, BOB), NO_PASSWORD, portB, environment());
    b = factoryB.newNode();
    nodes.add(b);
    addressB = new InetSocketAddress(factoryB.getBindAddress(), portB);
    b.boot(addressA);
    awaitReady(b);
    await(() -> b.getLeafSet().member(ALICE) && a.getLeafSet().member(BOB), "A and B have each other in their leaf sets");
  }

  @AfterEach
  void tearDown() {
    nodes.forEach(PastryNode::destroy);
    environments.forEach(Environment::destroy);
  }

  @Test
  @DisplayName("a datagram that names a node A found dead does not bring that node back to life")
  void aDatagramNamingADeadNodeDoesNotReviveIt() throws Exception {
    NodeHandle realB = handleOf(a, BOB);
    long epochB = ((SocketNodeHandle) realB).getEpoch();
    killAndAwaitDead(realB);

    // M names B exactly: B's id, address and epoch
    PastryNode mallory = outsider(BOB, addressB, epochB);
    sendDatagrams(mallory);

    assertNever(() -> realB.getLiveness() < NodeHandle.LIVENESS_DEAD, "A takes the dead B for alive");
  }

  @Test
  @DisplayName("a datagram that names another id at a dead node's address does not bring that address back to life")
  void aDatagramNamingAnotherIdAtADeadNodesAddressDoesNotReviveIt() throws Exception {
    NodeHandle realB = handleOf(a, BOB);
    killAndAwaitDead(realB);

    PastryNode mallory = outsider(Id.build("00000000000000000000000000000000000000EE"), addressB, Long.MAX_VALUE);
    sendDatagrams(mallory);

    assertNever(() -> realB.getLiveness() < NodeHandle.LIVENESS_DEAD, "A takes the dead B for alive");
  }

  @Test
  @DisplayName("an insider's leaf set that lists a node A found dead does not bring that node back")
  void anInsidersLeafSetListingADeadNodeDoesNotReviveIt() throws Exception {
    NodeHandle realB = handleOf(a, BOB);
    PastryNode mallory = insider();
    NodeHandle aAtMallory = CertifiedForgedSenderTest.copy(a.getLocalHandle(), a, mallory);
    NodeHandle bAtMallory = CertifiedForgedSenderTest.copy(realB, a, mallory);
    killAndAwaitDead(realB);

    CertifiedForgedSenderTest.send(mallory, aAtMallory, () -> new BroadcastLeafSet(mallory.getLocalHandle(),
        new LeafSet(mallory.getLocalHandle(), a.getLeafSet().maxSize(), false, new NodeHandle[] {bAtMallory, aAtMallory}, new NodeHandle[] {aAtMallory, bAtMallory}),
        BroadcastLeafSet.Update, 0));

    // (B stays in A's leaf set until the lease A granted it runs out; the merge skips a dead node)
    assertNever(() -> realB.getLiveness() < NodeHandle.LIVENESS_DEAD, "A takes the dead B for alive");
  }

  @Test
  @DisplayName("an insider that connects in over TLS and names a dead node's address does not bring that address back to life")
  void anInsiderConnectingInAtADeadNodesAddressDoesNotReviveIt() throws Exception {
    NodeHandle realB = handleOf(a, BOB);
    killAndAwaitDead(realB);

    // M's certificate proves M's id; the address M names in the header it writes before TLS is B's
    PastryNode mallory = insiderAt(addressB);
    CertifiedForgedSenderTest.routeTo(a, mallory, 51);
    await(() -> factoryA.getCertifiedIdBindStrategy().isAuthenticated(CertifiedForgedSenderTest.MALLORY), "A saw M's certificate");

    assertNever(() -> realB.getLiveness() < NodeHandle.LIVENESS_DEAD, "A takes the dead B for alive");
  }

  // helpers

  /**
   * @param claimedAddress the address M's headers name as its own
   * @return M, a certified node with its own id and certificate, whose headers name this address; it listens on a
   *     port of its own and is not booted
   */
  PastryNode insiderAt(InetSocketAddress claimedAddress) throws Exception {
    int port = freePort();
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, CertifiedForgedSenderTest.MALLORY), NO_PASSWORD, port, environment()) {
      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> buildWireTransportLayer(InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
        return super.buildWireTransportLayer(new InetSocketAddress(getBindAddress(), port), pn);
      }

      @Override
      public NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory<?> nodeHandleFactory) {
        return ((SocketNodeHandleFactory) nodeHandleFactory).getNodeHandle(new MultiInetSocketAddress(claimedAddress), 1L, pn.getNodeId());
      }
    };
    PastryNode node = factory.newNode(CertifiedForgedSenderTest.MALLORY, new MultiInetSocketAddress(claimedAddress));
    nodes.add(node);
    return node;
  }

  /** @return M, a certified node with its own id, certificate and address; it is not booted */
  PastryNode insider() throws Exception {
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, CertifiedForgedSenderTest.MALLORY), NO_PASSWORD, freePort(), environment());
    PastryNode node = factory.newNode();
    nodes.add(node);
    return node;
  }

  void killAndAwaitDead(NodeHandle realB) throws Exception {
    b.destroy();
    onSelector(a, () -> realB.checkLiveness());
    await(() -> realB.getLiveness() >= NodeHandle.LIVENESS_DEAD, "A found B dead");
  }

  /** sends A a few routed messages for A's id as datagrams, from M */
  void sendDatagrams(PastryNode mallory) throws Exception {
    CertifiedDeferredDeliveryTest.Receiver receiverM = new CertifiedDeferredDeliveryTest.Receiver(mallory);
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    a.getLocalHandle().serialize(buf);
    for (int i = 0; i < DATAGRAMS; i++) {
      int number = 40 + i;
      onSelector(mallory, () -> {
        NodeHandle aAtM = mallory.readNodeHandle(new SimpleInputBuffer(buf.getBytes()));
        PastryEndpointMessage message = new PastryEndpointMessage(((PastryEndpoint) receiverM.endpoint).getAddress(),
            new CertifiedDeferredDeliveryTest.Numbered(number), mallory.getLocalHandle());
        RouteMessage route = new RouteMessage(ALICE, message, aAtM, mallory.getConfig().routeMsgVersion());
        Map<String, Object> options = new HashMap<>();
        options.put(WireTransportLayer.OPTION_TRANSPORT_TYPE, WireTransportLayer.TRANSPORT_TYPE_DATAGRAM);
        options.put(PriorityTransportLayer.OPTION_PRIORITY, message.getPriority());
        return mallory.send(aAtM, route, null, options);
      });
      Thread.sleep(50);
    }
  }

  /**
   * @return M, a node of the plain (uncertified) socket stack, listening on a port of its own, whose headers name
   *     this id, address and epoch; it is not booted
   */
  PastryNode outsider(Id id, InetSocketAddress claimedAddress, long epoch) throws Exception {
    int port = freePort();
    InetSocketAddress real = new InetSocketAddress(bindAddress, port);
    SocketPastryNodeFactory factory = new SocketPastryNodeFactory(null, bindAddress, port, environment()) {
      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> buildWireTransportLayer(InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
        return super.buildWireTransportLayer(real, pn);
      }

      @Override
      public NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory<?> nodeHandleFactory) {
        return ((SocketNodeHandleFactory) nodeHandleFactory).getNodeHandle(new MultiInetSocketAddress(claimedAddress), epoch, pn.getNodeId());
      }
    };
    PastryNode pastryNode = factory.newNode(id, new MultiInetSocketAddress(claimedAddress));
    nodes.add(pastryNode);
    assertTrue(((SocketNodeHandle) pastryNode.getLocalHandle()).getEpoch() == epoch, "M's epoch");
    return pastryNode;
  }

  Environment environment() {
    Environment env = new Environment();
    environments.add(env);
    return env;
  }
}
