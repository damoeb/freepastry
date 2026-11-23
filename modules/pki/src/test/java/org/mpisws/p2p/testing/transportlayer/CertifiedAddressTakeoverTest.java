package org.mpisws.p2p.testing.transportlayer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryEndpoint;
import rice.pastry.commonapi.PastryEndpointMessage;
import rice.pastry.routing.RouteMessage;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketNodeHandleFactory;
import rice.pastry.socket.TransportLayerNodeHandle;

/**
 * End to end, on loopback with real sockets and TLS: a certificate proves the id of the node at the other end of a
 * connection, not the address that node names in the header it sends before TLS. A ring of two certified nodes, A
 * and B, is attacked by an insider M that holds a certificate of the same CA for its own id. M connects to A, names
 * B's address as its own and the highest possible epoch. A must not hand B's address to M, nor mark B dead, nor
 * admit M's handle at B's address.
 */
class CertifiedAddressTakeoverTest {

  static final Id MALLORY = Id.build("00000000000000000000000000000000000000EE");

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();

  CertificateAuthorityTool certificateAuthorityTool;
  CertifiedSocketPastryNodeFactory factoryA;
  PastryNode a;
  PastryNode b;
  CertifiedDeferredDeliveryTest.Receiver receiverA;
  InetSocketAddress addressB;

  @BeforeEach
  void setUp() throws Exception {
    certificateAuthorityTool = ca();
    int port = freePort();
    factoryA = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, ALICE), NO_PASSWORD, port, environment());
    a = factoryA.newNode();
    nodes.add(a);
    receiverA = new CertifiedDeferredDeliveryTest.Receiver(a);
    InetSocketAddress addressA = new InetSocketAddress(factoryA.getBindAddress(), port);
    a.boot(addressA);
    awaitReady(a);

    int portB = freePort();
    CertifiedSocketPastryNodeFactory factoryB = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, BOB), NO_PASSWORD, portB, environment());
    b = factoryB.newNode();
    nodes.add(b);
    new CertifiedDeferredDeliveryTest.Receiver(b);
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
  @DisplayName("an insider that names an honest node's address over TLS does not take that address over")
  void anInsiderClaimingAnHonestNodesAddressDoesNotTakeItOver() throws Exception {
    NodeHandle realB = handleOf(a, BOB);
    PastryNode mallory = insiderAt(addressB);
    SocketNodeHandle malloryHandle = (SocketNodeHandle) mallory.getLocalHandle();
    assertEquals(Long.MAX_VALUE, malloryHandle.getEpoch());

    send(mallory, 21);
    await(() -> factoryA.getCertifiedIdBindStrategy().isAuthenticated(MALLORY), "A saw Mallory's certificate");

    assertNever(() -> realB.getLiveness() >= NodeHandle.LIVENESS_DEAD, "A marked the real B dead");
    assertTrue(a.getLeafSet().member(BOB), "B is still in A's leaf set");
    assertEquals(realB, handleOf(a, BOB), "B's handle in A's leaf set");
    @SuppressWarnings("unchecked")
    TransportLayerNodeHandle<MultiInetSocketAddress> malloryAtA = (TransportLayerNodeHandle<MultiInetSocketAddress>) CertifiedHandleAdmissionTest.handle(
        factoryA, a, malloryHandle.getAddress(), malloryHandle.getEpoch(), MALLORY);
    assertFalse(factoryA.getCertifiedIdBindStrategy().isAuthenticated(malloryAtA), "Mallory's id at B's address is authenticated at A");
    assertFalse(onSelector(a, () -> a.acceptsIdentity(malloryAtA)), "Mallory's id at B's address passes A's admission");
  }

  // helpers

  /** sends A a routed message for A's id, carrying this number, from this node over TLS */
  void send(PastryNode sender, int number) throws Exception {
    CertifiedDeferredDeliveryTest.Receiver receiver = new CertifiedDeferredDeliveryTest.Receiver(sender);
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    a.getLocalHandle().serialize(buf);
    onSelector(sender, () -> {
      NodeHandle aAtSender = sender.readNodeHandle(new SimpleInputBuffer(buf.getBytes()));
      PastryEndpointMessage message = new PastryEndpointMessage(((PastryEndpoint) receiver.endpoint).getAddress(),
          new CertifiedDeferredDeliveryTest.Numbered(number), sender.getLocalHandle());
      RouteMessage route = new RouteMessage(ALICE, message, aAtSender, sender.getConfig().routeMsgVersion());
      Map<String, Object> options = new HashMap<>();
      options.put(PriorityTransportLayer.OPTION_PRIORITY, message.getPriority());
      return sender.send(aAtSender, route, null, options);
    });
  }

  /**
   * @param claimedAddress the address M's headers name as its own
   * @return M, a certified node with its own id and certificate, whose headers name this address and the highest
   *     epoch; it listens on a port of its own and is not booted
   */
  PastryNode insiderAt(InetSocketAddress claimedAddress) throws Exception {
    int port = freePort();
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, MALLORY), NO_PASSWORD, port, environment()) {
      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> buildWireTransportLayer(InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
        return super.buildWireTransportLayer(new InetSocketAddress(getBindAddress(), port), pn);
      }

      @Override
      public NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory<?> nodeHandleFactory) {
        MultiInetSocketAddress address = new MultiInetSocketAddress(claimedAddress);
        return ((SocketNodeHandleFactory) nodeHandleFactory).getNodeHandle(address, Long.MAX_VALUE, pn.getNodeId());
      }
    };
    PastryNode node = factory.newNode(MALLORY, new MultiInetSocketAddress(claimedAddress));
    nodes.add(node);
    return node;
  }

  Environment environment() {
    Environment env = new Environment();
    environments.add(env);
    return env;
  }
}
