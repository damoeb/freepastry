package org.mpisws.p2p.testing.transportlayer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ALICE;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.BOB;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.NO_PASSWORD;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.awaitReady;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.awaitTrue;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ca;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.freePort;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.store;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CAToolImpl;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryEndpoint;
import rice.pastry.commonapi.PastryEndpointMessage;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;

/**
 * End to end, on loopback with real sockets and TLS: a direct application message that reaches a node before
 * the sender's id is verified is queued and delivered, in order, once the verification ends.
 */
class CertifiedDeferredDeliveryTest {

  static final short TYPE = 77;
  static final int MESSAGES = 5;
  static final int DELIVERY_SECONDS = 15;

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();

  @AfterEach
  void tearDown() {
    nodes.forEach(PastryNode::destroy);
    environments.forEach(Environment::destroy);
  }

  @Test
  @DisplayName("direct messages sent the moment a node joins reach the application of a peer that has not verified it yet, in order")
  void messagesFromAnUnverifiedPeerAreDeliveredInOrder() throws Exception {
    CAToolImpl ca = ca();
    int port = freePort();
    CertifiedSocketPastryNodeFactory factoryA = new CertifiedSocketPastryNodeFactory(store(ca, ALICE), NO_PASSWORD, port, environment());
    PastryNode a = factoryA.newNode();
    nodes.add(a);
    Receiver receiverA = new Receiver(a);
    readDirectMessagesWithTheEndpoint(a, receiverA);
    AtomicInteger heldBack = countRefusals(a, factoryA, MESSAGES);
    InetSocketAddress addressA = new InetSocketAddress(factoryA.getBindAddress(), port);
    a.boot(addressA);
    awaitReady(a);

    CertifiedSocketPastryNodeFactory factoryB = new CertifiedSocketPastryNodeFactory(store(ca, BOB), NO_PASSWORD, freePort(), environment());
    PastryNode b = factoryB.newNode();
    nodes.add(b);
    Receiver receiverB = new Receiver(b);
    b.boot(addressA);

    // B has met A over TLS, so A knows B's id. Forget that: A now has B only as an id from the wire, as if it had
    // heard of B from a third node, and has to verify it while B's messages arrive.
    awaitTrue(() -> b.getLeafSet().member(ALICE));
    forget(factoryA, BOB);
    NodeHandle handleOfA = b.getLeafSet().asList().stream().filter(h -> h.getNodeId().equals(ALICE)).findFirst().orElseThrow();
    // a direct message (not a routed one): this is the kind the receiving endpoint checks the sender of
    b.getEnvironment().getSelectorManager().invoke(() -> {
      for (int i = 0; i < MESSAGES; i++) {
        b.send(rice.pastry.NodeHandle.class.cast(handleOfA),
            new PastryEndpointMessage(((PastryEndpoint) receiverB.endpoint).getAddress(), new Numbered(i), b.getLocalHandle()), null, null);
      }
    });

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DELIVERY_SECONDS);
    while (receiverA.received.size() < MESSAGES && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(List.of(0, 1, 2, 3, 4), receiverA.received, "numbers of the messages A's application got, A refused " + heldBack.get() + " times, B authenticated: " + factoryA.getCertifiedIdBindStrategy().isAuthenticated(BOB));
    assertTrue(factoryA.getCertifiedIdBindStrategy().isAuthenticated(BOB));
    assertTrue(heldBack.get() > 0, "the messages arrived while B was unverified");
  }

  /**
   * Counts how often the node's identity policy says "not authenticated yet" and, at the refusal number
   * {@code authenticateAt}, completes the TLS check (A sees a certificate for B whose name matches), as if the
   * handshake of the verification connection ended at that moment. This is the seam that makes the unverified
   * window deterministic: the TLS check of the real node ends only after the messages have been seen.
   */
  static AtomicInteger countRefusals(PastryNode node, CertifiedSocketPastryNodeFactory factory, int authenticateAt) throws ReflectiveOperationException {
    AtomicInteger refusals = new AtomicInteger();
    java.lang.reflect.Field field = PastryNode.class.getDeclaredField("identityPolicy");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.function.Predicate<rice.pastry.NodeHandle> policy = (java.util.function.Predicate<rice.pastry.NodeHandle>) field.get(node);
    node.setIdentityPolicy(handle -> {
      boolean accepted = policy.test(handle);
      if (!accepted && refusals.incrementAndGet() == authenticateAt) {
        factory.getCertifiedIdBindStrategy().accept(SSLTest.handleWithId(BOB), null, Map.of(SSLTransportLayer.OPTION_CERT_SUBJECT, BOB.toStringFull()));
      }
      return accepted;
    });
    return refusals;
  }

  /**
   * {@code PastryEndpoint.register()} hands the transport the application's own deserializer for the endpoint's
   * address, so a frame with a plain endpoint message (not wrapped in a route message) would not come out as a
   * pastry message. Register the endpoint's own deserializer instead, which is what such a frame needs to reach
   * {@code PastryEndpoint.messageForAppl}.
   */
  static void readDirectMessagesWithTheEndpoint(PastryNode node, Receiver receiver) throws ReflectiveOperationException {
    java.lang.reflect.Field own = rice.pastry.client.PastryAppl.class.getDeclaredField("deserializer");
    own.setAccessible(true);
    var endpointDeserializer = (rice.p2p.commonapi.rawserialization.MessageDeserializer) own.get(receiver.endpoint);
    java.lang.reflect.Field transport = rice.pastry.messaging.MessageDispatch.class.getDeclaredField("deserializer");
    transport.setAccessible(true);
    ((rice.pastry.transport.Deserializer) transport.get(node.getMessageDispatch())).setDeserializer(((PastryEndpoint) receiver.endpoint).getAddress(), endpointDeserializer);
  }

  /** puts the id back to "learned over the wire, no certificate seen": A has to verify B again */
  @SuppressWarnings("unchecked")
  static void forget(CertifiedSocketPastryNodeFactory factory, rice.pastry.Id id) throws ReflectiveOperationException {
    var strategy = factory.getCertifiedIdBindStrategy();
    for (String name : new String[] {"authenticated", "pending"}) {
      java.lang.reflect.Field field = strategy.getClass().getDeclaredField(name);
      field.setAccessible(true);
      synchronized (strategy) {
        ((java.util.Map<rice.pastry.Id, Boolean>) field.get(strategy)).remove(id);
      }
    }
  }

  Environment environment() {
    Environment env = new Environment();
    environments.add(env);
    return env;
  }

  /** an application that records the numbers of the messages it gets */
  static class Receiver implements Application {
    final Endpoint endpoint;
    final List<Integer> received = new CopyOnWriteArrayList<>();

    Receiver(PastryNode node) {
      endpoint = node.buildEndpoint(this, "deferred-delivery-test");
      endpoint.setDeserializer((buf, type, priority, sender) -> {
        if (type == TYPE) {
          return new Numbered(buf.readInt());
        }
        throw new IOException("Unknown message type: " + type);
      });
      endpoint.register();
    }

    @Override
    public void deliver(rice.p2p.commonapi.Id id, Message message) {
      received.add(((Numbered) message).number);
    }

    @Override
    public void update(NodeHandle handle, boolean joined) {}

    @Override
    public boolean forward(RouteMessage message) {
      return true;
    }
  }

  static class Numbered implements RawMessage {
    final int number;

    Numbered(int number) {
      this.number = number;
    }

    @Override
    public int getPriority() {
      return Message.LOW_PRIORITY;
    }

    @Override
    public short getType() {
      return TYPE;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(number);
    }
  }
}
