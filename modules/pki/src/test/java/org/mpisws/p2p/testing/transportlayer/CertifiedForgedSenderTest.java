package org.mpisws.p2p.testing.transportlayer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ALICE;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.BOB;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.NO_PASSWORD;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.awaitReady;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ca;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.freePort;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.store;
import static org.mpisws.p2p.testing.transportlayer.CertifiedDatagramTest.handleOf;
import static org.mpisws.p2p.testing.transportlayer.CertifiedDeferredDeliveryTest.await;
import static org.mpisws.p2p.testing.transportlayer.CertifiedDeferredDeliveryTest.onSelector;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import rice.FutureContinuation;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryEndpoint;
import rice.pastry.commonapi.PastryEndpointMessage;
import rice.pastry.leafset.BroadcastLeafSet;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.messaging.PastryRawMessage;
import rice.pastry.pns.ProximityNeighborSelectionApplication;
import rice.pastry.pns.messages.LeafSetResponse;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RoutingTable;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;
import rice.pastry.standard.PeriodicLeafSetProtocol;
import rice.pastry.standard.ProximityNeighborSelector;

/**
 * End to end, on loopback with real sockets and TLS: a protocol message that names its sender in its body is acted
 * on only if that sender is the node whose certificate the connection presented. A ring of two certified nodes, A
 * and B, and an insider M with a certificate of the same CA for its own id, which is not in the ring and connects to
 * A as itself.
 */
class CertifiedForgedSenderTest {

  static final Id CAROL = Id.build("00000000000000000000000000000000000000CC");
  static final Id MALLORY = Id.build("00000000000000000000000000000000000000EE");

  /** A's lease period: short, so that a lease B really granted runs out within the test */
  static final int LEASE_PERIOD_MILLIS = 2_000;

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();

  CertificateAuthorityTool certificateAuthorityTool;
  PastryNode a;
  PastryNode b;
  PeriodicLeafSetProtocol leafSetProtocolA;
  ProximityNeighborSelectionApplication proximityA;
  CertifiedSocketPastryNodeFactory factoryA;
  final List<AutoCloseable> silentPorts = new ArrayList<>();
  CertifiedDeferredDeliveryTest.Receiver receiverA;

  @BeforeEach
  void setUp() throws Exception {
    certificateAuthorityTool = ca();
    int port = freePort();
    Environment environmentA = environment();
    environmentA.getParameters().setInt("pastry_protocol_periodicLeafSet_lease_period", LEASE_PERIOD_MILLIS);
    environmentA.getParameters().setInt("pastry_protocol_periodicLeafSet_ping_neighbor_period", LEASE_PERIOD_MILLIS / 4);
    environmentA.getParameters().setInt("pastry_protocol_periodicLeafSet_request_lease_throttle", LEASE_PERIOD_MILLIS / 8);
    factoryA = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, ALICE), NO_PASSWORD, port, environmentA) {
      @Override
      protected LeafSetProtocol getLeafSetProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable) {
        LeafSetProtocol result = super.getLeafSetProtocol(pn, leafSet, routeTable);
        leafSetProtocolA = (PeriodicLeafSetProtocol) result;
        return result;
      }

      @Override
      protected ProximityNeighborSelector getProximityNeighborSelector(PastryNode pn) {
        ProximityNeighborSelector result = super.getProximityNeighborSelector(pn);
        proximityA = (ProximityNeighborSelectionApplication) result;
        return result;
      }
    };
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
    b.boot(addressA);
    awaitReady(b);
    await(() -> b.getLeafSet().member(ALICE) && a.getLeafSet().member(BOB), "A and B have each other in their leaf sets");
  }

  @AfterEach
  void tearDown() throws Exception {
    nodes.forEach(PastryNode::destroy);
    environments.forEach(Environment::destroy);
    for (AutoCloseable port : silentPorts) {
      port.close();
    }
  }

  @Test
  @DisplayName("a leaf set broadcast that names another node as its sender grants no lease from that node")
  void aBroadcastNamingAnotherSenderGrantsNoLease() throws Exception {
    NodeHandle realB = handleOf(a, BOB);
    PastryNode mallory = insider();
    NodeHandle aAtMallory = copy(a.getLocalHandle(), a, mallory);
    NodeHandle bAtMallory = copy(realB, a, mallory);

    // B's leaf set as B would send it to its neighbour A, with a lease that runs for a day
    long aDayFromNow = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
    send(mallory, aAtMallory, () -> new BroadcastLeafSet(bAtMallory,
        new LeafSet(bAtMallory, a.getLeafSet().maxSize(), false, new NodeHandle[] {aAtMallory}, new NodeHandle[] {aAtMallory}),
        BroadcastLeafSet.Update, aDayFromNow));
    // a message M sends after the broadcast, over the same connection, tells that A read the broadcast
    routeToA(mallory, aAtMallory, 31);
    await(() -> receiverA.received.contains(31), "A got M's message after the broadcast");

    b.destroy();

    awaitTrue(() -> !onSelector(a, () -> leafSetProtocolA.hasLease(realB)), 3 * LEASE_PERIOD_MILLIS,
        "A's lease from B ends once B is gone");
  }

  @Test
  @DisplayName("a leaf set that names another node as the one that answers does not answer A's request to that node")
  void aLeafSetResponseNamingAnotherNodeDoesNotAnswerTheRequest() throws Exception {
    // C accepts connections but never answers: A's request to C stays open
    ServerSocket silent = new ServerSocket(0, 50, factoryA.getBindAddress());
    silentPorts.add(silent);
    silentPorts.add(new DatagramSocket(silent.getLocalPort(), factoryA.getBindAddress()));
    NodeHandle carol = CertifiedHandleAdmissionTest.handle(factoryA, a,
        new MultiInetSocketAddress(new InetSocketAddress(factoryA.getBindAddress(), silent.getLocalPort())), 1L, CAROL);
    FutureContinuation<LeafSet, Exception> answer = new FutureContinuation<>();
    onSelector(a, () -> proximityA.getLeafSet(carol, answer));

    PastryNode mallory = insider();
    NodeHandle aAtMallory = copy(a.getLocalHandle(), a, mallory);
    NodeHandle carolAtMallory = copy(carol, a, mallory);
    // C's leaf set as M would like A to see it: M is C's only neighbour
    send(mallory, aAtMallory, () -> new LeafSetResponse(
        new LeafSet(carolAtMallory, a.getLeafSet().maxSize(), false, new NodeHandle[] {mallory.getLocalHandle()}, new NodeHandle[] {mallory.getLocalHandle()}),
        proximityA.getAddress()));
    routeToA(mallory, aAtMallory, 32);
    await(() -> receiverA.received.contains(32), "A got M's message after the leaf set");

    assertFalse(answer.future().isDone() && !answer.future().isCompletedExceptionally(),
        "A's request to C was answered with M's leaf set");
  }

  // helpers

  /** @return the handle as the other node reads it off the wire */
  static NodeHandle copy(NodeHandle handle, PastryNode from, PastryNode to) throws Exception {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    onSelector(from, () -> {
      handle.serialize(buf);
      return null;
    });
    return onSelector(to, () -> to.readNodeHandle(new SimpleInputBuffer(buf.getBytes())));
  }

  /** sends the message, built on the sender's selector, straight to the node (not routed) over TLS */
  static void send(PastryNode sender, NodeHandle to, java.util.concurrent.Callable<PastryRawMessage> message) throws Exception {
    onSelector(sender, () -> {
      PastryRawMessage built = message.call();
      Map<String, Object> options = new HashMap<>();
      options.put(PriorityTransportLayer.OPTION_PRIORITY, built.getPriority());
      return sender.send(to, built, null, options);
    });
  }

  /** routes A's receiver a message with this number from the sender */
  void routeToA(PastryNode sender, NodeHandle aAtSender, int number) throws Exception {
    routeTo(aAtSender, sender, number);
  }

  /** routes the receiver of the node this handle names a message with this number from the sender, over TLS */
  static void routeTo(PastryNode receiverNode, PastryNode sender, int number) throws Exception {
    routeTo(copy(receiverNode.getLocalHandle(), receiverNode, sender), sender, number);
  }

  static void routeTo(NodeHandle aAtSender, PastryNode sender, int number) throws Exception {
    CertifiedDeferredDeliveryTest.Receiver receiver = new CertifiedDeferredDeliveryTest.Receiver(sender);
    onSelector(sender, () -> {
      PastryEndpointMessage message = new PastryEndpointMessage(((PastryEndpoint) receiver.endpoint).getAddress(),
          new CertifiedDeferredDeliveryTest.Numbered(number), sender.getLocalHandle());
      RouteMessage route = new RouteMessage(aAtSender.getNodeId(), message, aAtSender, sender.getConfig().routeMsgVersion());
      Map<String, Object> options = new HashMap<>();
      options.put(PriorityTransportLayer.OPTION_PRIORITY, message.getPriority());
      return sender.send(aAtSender, route, null, options);
    });
  }

  static void awaitTrue(Condition condition, long timeoutMillis, String what) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertTrue(condition.getAsBoolean(), what);
  }

  @FunctionalInterface
  interface Condition {
    boolean getAsBoolean() throws Exception;
  }

  /** @return M, a certified node with its own id, certificate and address; it is not booted */
  PastryNode insider() throws Exception {
    int port = freePort();
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, MALLORY), NO_PASSWORD, port, environment());
    PastryNode node = factory.newNode();
    nodes.add(node);
    return node;
  }

  Environment environment() {
    Environment env = new Environment();
    environments.add(env);
    return env;
  }
}
