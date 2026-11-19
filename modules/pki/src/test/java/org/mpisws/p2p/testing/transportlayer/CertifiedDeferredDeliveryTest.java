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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.DeferredDeliveries;
import rice.pastry.PastryNode;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;
import rice.pastry.socket.SocketNodeHandle;

/**
 * End to end, on loopback with real sockets and TLS: a routed application message whose previous hop is not
 * authenticated yet is held back from the application. It is delivered, in order, once the TLS check of the hop
 * succeeds, and never if the check fails (a TLS handshake with a peer whose certificate the node does not trust).
 */
class CertifiedDeferredDeliveryTest {

  static final short TYPE = 77;
  static final int MESSAGES = 5;
  static final int WAIT_SECONDS = 5;

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();

  CertifiedSocketPastryNodeFactory factoryA;
  PastryNode a;
  PastryNode b;
  Receiver receiverA;
  Receiver receiverB;
  Predicate<rice.pastry.NodeHandle> realPolicyOfA;

  @BeforeEach
  void setUp() throws Exception {
    CertificateAuthorityTool certificateAuthorityTool = ca();
    int port = freePort();
    factoryA = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, ALICE), NO_PASSWORD, port, environment());
    a = factoryA.newNode();
    nodes.add(a);
    receiverA = new Receiver(a);
    realPolicyOfA = identityPolicy(a);
    InetSocketAddress addressA = new InetSocketAddress(factoryA.getBindAddress(), port);
    a.boot(addressA);
    awaitReady(a);

    CertifiedSocketPastryNodeFactory factoryB = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, BOB), NO_PASSWORD, freePort(), environment());
    b = factoryB.newNode();
    nodes.add(b);
    receiverB = new Receiver(b);
    b.boot(addressA);
    awaitReady(b);
    await(() -> b.getLeafSet().member(ALICE) && a.getLeafSet().member(BOB), "A and B have each other in their leaf sets");

    // B has met A over TLS, so A knows B's id. Forget that: A now has B only as an id from the wire, as if it had
    // heard of B from a third node, and has to verify it again. And hold the check back (A's policy refuses B
    // without starting it) so that B's messages surely arrive while B is unverified.
    CertifiedDeferredDeliveryTest.forget(factoryA, BOB);
    onSelector(a, () -> {
      a.setIdentityPolicy(handle -> !handle.getNodeId().equals(BOB) && realPolicyOfA.test(handle));
      return null;
    });
  }

  @AfterEach
  void tearDown() {
    nodes.forEach(PastryNode::destroy);
    environments.forEach(Environment::destroy);
  }

  @Test
  @DisplayName("routed messages whose previous hop is not verified yet reach the application once the hop's certificate is checked, in order")
  void messagesFromAnUnverifiedHopAreDeliveredInOrderAfterTheCheck() throws Exception {
    routeFromBToA(0, 1, 2, 3, 4);
    await(() -> waiting(a) == MESSAGES || !receiverA.received.isEmpty(), "B's messages wait at A or reached A's application");
    assertEquals(List.of(), receiverA.received, "numbers A's application got before B was verified");

    // let the real check run: the policy starts a TLS connection to B, whose certificate matches its id
    onSelector(a, () -> {
      a.setIdentityPolicy(realPolicyOfA);
      return a.acceptsIdentity(a.getLeafSet().asList().stream().filter(h -> h.getNodeId().equals(BOB)).findFirst().orElseThrow());
    });

    await(() -> receiverA.received.size() >= MESSAGES, "A's application got B's messages");
    assertEquals(List.of(0, 1, 2, 3, 4), receiverA.received, "numbers of the messages A's application got");
    assertTrue(factoryA.getCertifiedIdBindStrategy().isAuthenticated(BOB));
    assertEquals(0, waiting(a));
  }

  @Test
  @DisplayName("routed messages whose previous hop fails the TLS check never reach the application")
  void messagesFromAHopThatFailsTheCheckAreNeverDelivered() throws Exception {
    routeFromBToA(0, 1, 2, 3, 4);
    await(() -> waiting(a) == MESSAGES || !receiverA.received.isEmpty(), "B's messages wait at A or reached A's application");
    assertEquals(List.of(), receiverA.received, "numbers A's application got although B was never verified");

    // B goes away and an impostor takes its address: a TLS server with a certificate for B's id from a CA that A
    // does not trust, so the handshake of A's check fails
    rice.pastry.NodeHandle bobAtA = onSelector(a, () -> a.getLeafSet().asList().stream().filter(h -> h.getNodeId().equals(BOB)).findFirst().orElseThrow());
    InetSocketAddress addressB = ((SocketNodeHandle) b.getLocalHandle()).getInetSocketAddress();
    b.destroy();
    try (Impostor impostor = new Impostor(store(ca(), BOB), addressB)) {
      onSelector(a, () -> {
        a.setIdentityPolicy(realPolicyOfA);
        return a.acceptsIdentity(bobAtA);
      });

      await(() -> waiting(a) == 0, "A dropped B's messages");
      assertTrue(impostor.connections.get() > 0, "A's check reached the impostor");
    }
    assertEquals(List.of(), receiverA.received, "numbers A's application got although B failed the check");
    assertFalse(factoryA.getCertifiedIdBindStrategy().isAuthenticated(BOB), "A authenticated B's id");
  }

  /** a TLS server whose certificate the node does not trust; it counts the connections it gets */
  static final class Impostor implements AutoCloseable {
    final AtomicInteger connections = new AtomicInteger();
    final SSLServerSocket server;

    Impostor(KeyStore store, InetSocketAddress address) throws Exception {
      KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keys.init(store, NO_PASSWORD);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keys.getKeyManagers(), null, null);
      server = (SSLServerSocket) context.getServerSocketFactory().createServerSocket();
      server.setReuseAddress(true);
      server.bind(address);
      Thread.ofVirtual().start(() -> {
        while (!server.isClosed()) {
          try (SSLSocket socket = (SSLSocket) server.accept()) {
            connections.incrementAndGet();
            socket.startHandshake();
          } catch (IOException e) {
            // the node refuses the certificate, or the test closed the server
          }
        }
      });
    }

    @Override
    public void close() throws IOException {
      server.close();
    }
  }

  /** B's application routes messages with these numbers to A's id; A is B's only neighbour, so it is one hop */
  void routeFromBToA(int... numbers) throws Exception {
    onSelector(b, () -> {
      for (int number : numbers) {
        receiverB.endpoint.route(ALICE, new Numbered(number), null);
      }
      return null;
    });
  }

  /** @return the number of messages the node holds back until a previous hop is verified */
  static int waiting(PastryNode node) {
    try {
      java.lang.reflect.Field field = PastryNode.class.getDeclaredField("deferred");
      field.setAccessible(true);
      DeferredDeliveries<?> deferred = (DeferredDeliveries<?>) field.get(node);
      return onSelector(node, deferred::size);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("unchecked")
  static Predicate<rice.pastry.NodeHandle> identityPolicy(PastryNode node) throws ReflectiveOperationException {
    java.lang.reflect.Field field = PastryNode.class.getDeclaredField("identityPolicy");
    field.setAccessible(true);
    return (Predicate<rice.pastry.NodeHandle>) field.get(node);
  }

  /** puts the id back to "learned over the wire, no certificate seen": A has to verify B again */
  @SuppressWarnings("unchecked")
  static void forget(CertifiedSocketPastryNodeFactory factory, rice.pastry.Id id) throws ReflectiveOperationException {
    var strategy = factory.getCertifiedIdBindStrategy();
    // every state the strategy keeps, by id or by handle
    for (java.lang.reflect.Field field : strategy.getClass().getDeclaredFields()) {
      if (java.util.Map.class.isAssignableFrom(field.getType())) {
        field.setAccessible(true);
        synchronized (strategy) {
          ((java.util.Map<Object, ?>) field.get(strategy)).keySet().removeIf(key ->
              id.equals(key) || (key instanceof rice.pastry.NodeHandle handle && id.equals(handle.getNodeId())));
        }
      }
    }
  }

  static <T> T onSelector(PastryNode node, Callable<T> task) throws Exception {
    CompletableFuture<T> result = new CompletableFuture<>();
    node.getEnvironment().getSelectorManager().invoke(() -> {
      try {
        result.complete(task.call());
      } catch (Exception | Error e) {
        result.completeExceptionally(e);
      }
    });
    return result.get(WAIT_SECONDS, TimeUnit.SECONDS);
  }

  static void await(BooleanSupplier condition, String what) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(condition.getAsBoolean(), what);
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
