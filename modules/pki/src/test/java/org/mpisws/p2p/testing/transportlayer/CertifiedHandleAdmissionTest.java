package org.mpisws.p2p.testing.transportlayer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ALICE;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.BOB;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.NO_PASSWORD;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.awaitReady;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.ca;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.freePort;
import static org.mpisws.p2p.testing.transportlayer.CertifiedAdmissionTest.store;
import static org.mpisws.p2p.testing.transportlayer.CertifiedDeferredDeliveryTest.onSelector;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.join.JoinRequest;
import rice.pastry.leafset.LeafSet;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketNodeHandleFactory;

/**
 * End to end, on loopback with real sockets and TLS: a certified node admits a handle, that is an id at an address
 * and epoch, only once that address has presented a certificate for that id. Knowing that the id was authenticated
 * somewhere is not enough. This holds for handles that other nodes advertise (leaf-set and routing-table rows) and
 * for the leaf set of the accepted join request a joining node adopts. The probe that checks an address is not an
 * amplifier: forged ids that all name one address make the node connect to it at most once per retry delay.
 */
class CertifiedHandleAdmissionTest {

  static final Id MALLORY = Id.build("00000000000000000000000000000000000000EE");
  static final Id CAROL = Id.build("00000000000000000000000000000000000000CC");
  static final Id DAVE = Id.build("00000000000000000000000000000000000000DD");
  static final int LEAF_SET_SIZE = 24;
  static final int FORGED_IDS = 10;
  static final long BETWEEN_FORGERIES_MILLIS = 200;

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();
  final List<AutoCloseable> sockets = new ArrayList<>();

  CertificateAuthorityTool certificateAuthorityTool;
  CertifiedSocketPastryNodeFactory factoryA;
  PastryNode a;
  PastryNode b;

  @BeforeEach
  void setUp() throws Exception {
    certificateAuthorityTool = ca();
    int port = freePort();
    factoryA = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, ALICE), NO_PASSWORD, port, environment());
    a = factoryA.newNode();
    nodes.add(a);
    InetSocketAddress addressA = new InetSocketAddress(factoryA.getBindAddress(), port);
    a.boot(addressA);
    awaitReady(a);

    CertifiedSocketPastryNodeFactory factoryB = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, BOB), NO_PASSWORD, freePort(), environment());
    b = factoryB.newNode();
    nodes.add(b);
    b.boot(addressA);
    awaitReady(b);
    CertifiedDeferredDeliveryTest.await(() -> b.getLeafSet().member(ALICE) && a.getLeafSet().member(BOB), "A and B have each other in their leaf sets");
  }

  @AfterEach
  void tearDown() throws Exception {
    nodes.forEach(PastryNode::destroy);
    environments.forEach(Environment::destroy);
    for (AutoCloseable socket : sockets) {
      socket.close();
    }
  }

  @Test
  @DisplayName("a handle with an authenticated id at an address that never presented that id's certificate is not admitted")
  void insiderAdvertisingAnotherIdAtItsOwnAddressIsNotAdmitted() throws Exception {
    // Mallory holds a certificate of the same CA, for her own id
    PastryNode mallory = alone(MALLORY);
    SocketNodeHandle malloryHandle = (SocketNodeHandle) mallory.getLocalHandle();
    assertTrue(factoryA.getCertifiedIdBindStrategy().isAuthenticated(BOB), "A has seen Bob's certificate");

    // Mallory advertises Bob's id at her own address, as a leaf-set or routing-table row would
    NodeHandle forged = handle(factoryA, a, malloryHandle.getAddress(), malloryHandle.getEpoch(), BOB);

    assertFalse(onSelector(a, () -> a.acceptsIdentity(forged)), "Bob's id at Mallory's address passes A's admission");
    assertFalse(probe(a, forged), "a TLS connection to Mallory's address authenticates Bob's id");
    assertFalse(onSelector(a, () -> a.acceptsIdentity(forged)), "Bob's id at Mallory's address passes A's admission after the probe");
    assertFalse(onSelector(a, () -> a.getRoutingTable().put(forged)), "A's routing table takes Bob's id at Mallory's address");
    NodeHandle realBob = onSelector(a, () -> a.getLeafSet().asList().stream().filter(h -> h.getNodeId().equals(BOB)).findFirst().orElseThrow());
    assertTrue(onSelector(a, () -> a.acceptsIdentity(realBob)), "Bob's real handle passes A's admission");
  }

  @Test
  @DisplayName("a joining node ignores an accepted join request whose sender is not authenticated")
  void joinerIgnoresAnAcceptedJoinFromAnUnauthenticatedSender() throws Exception {
    Joiner joiner = joiner();
    NodeHandle bobAtJoiner = joiner.handleOf(b);
    assertTrue(probe(joiner.node, bobAtJoiner), "the joiner authenticates Bob");

    // the stalled bootstrap node answers the join, with a leaf set holding Bob: the content is valid, the sender is not
    LeafSet offered = new LeafSet(joiner.bootstrap, LEAF_SET_SIZE, false);
    offered.put(bobAtJoiner);
    JoinRequest accepted = joiner.acceptedJoin(joiner.bootstrap, offered);

    assertFalse(onSelector(joiner.node, () -> {
      joiner.node.receiveMessage(accepted);
      return joiner.node.getLeafSet().member(BOB);
    }), "the joiner adopted the leaf set of an accepted join request from an unauthenticated sender");
    assertTrue(joiner.isJoining(), "the joiner still waits for an answer to its join request");
  }

  @Test
  @DisplayName("a joining node does not adopt a handle of the accepted leaf set whose address fails the TLS check")
  void joinerDropsAnAdvertisedHandleThatFailsTheCheck() throws Exception {
    Joiner joiner = joiner();
    NodeHandle bobAtJoiner = joiner.handleOf(b);
    NodeHandle aliceAtJoiner = joiner.handleOf(a);
    assertTrue(probe(joiner.node, bobAtJoiner), "the joiner authenticates Bob");
    assertTrue(probe(joiner.node, aliceAtJoiner), "the joiner authenticates Alice");

    // Bob, authenticated, answers the join with a leaf set that puts Alice's id at Bob's own address
    SocketNodeHandle bobHandle = (SocketNodeHandle) bobAtJoiner;
    NodeHandle forged = handle(joiner.factory, joiner.node, bobHandle.getAddress(), bobHandle.getEpoch(), ALICE);
    LeafSet offered = new LeafSet(bobAtJoiner, LEAF_SET_SIZE, false);
    offered.put(forged);
    JoinRequest accepted = joiner.acceptedJoin(bobAtJoiner, offered);

    assertFalse(onSelector(joiner.node, () -> {
      joiner.node.receiveMessage(accepted);
      return joiner.node.getLeafSet().asList().contains(forged);
    }), "the joiner adopted Alice's id at Bob's address");
    assertFalse(probe(joiner.node, forged), "a TLS connection to Bob's address authenticates Alice's id");
    assertFalse(onSelector(joiner.node, () -> joiner.node.getLeafSet().asList().contains(forged)), "the joiner adopted Alice's id at Bob's address after the check");
    CertifiedDeferredDeliveryTest.await(() -> leafSetMember(joiner.node, bobAtJoiner), "the joiner adopts the authenticated acceptor");
  }

  @Test
  @DisplayName("forged ids that name one address make the node connect to that address once, not once per id")
  void forgedIdsAtOneAddressCauseOneProbe() throws Exception {
    // the attacker-chosen address: a TCP port that closes every connection, and a UDP port that swallows datagrams
    InetAddress loopback = factoryA.getBindAddress();
    ServerSocket target = new ServerSocket(0, 50, loopback);
    sockets.add(target);
    sockets.add(new DatagramSocket(target.getLocalPort(), loopback));
    AtomicInteger connections = new AtomicInteger();
    Thread acceptor = Thread.ofVirtual().start(() -> {
      while (!target.isClosed()) {
        try (Socket socket = target.accept()) {
          connections.incrementAndGet();
        } catch (IOException e) {
          // closed by the test
        }
      }
    });
    MultiInetSocketAddress targetAddress = new MultiInetSocketAddress(new InetSocketAddress(loopback, target.getLocalPort()));

    for (int i = 0; i < FORGED_IDS; i++) {
      NodeHandle forged = handle(factoryA, a, targetAddress, i, Id.build(String.format("%040X", 0xF00 + i)));
      assertFalse(onSelector(a, () -> a.acceptsIdentity(forged)), "a forged id is admitted");
      Thread.sleep(BETWEEN_FORGERIES_MILLIS);
    }

    assertEquals(1, connections.get(), "connections A opened to the address the forged ids name");
    target.close();
    acceptor.join(TimeUnit.SECONDS.toMillis(5));
  }

  // helpers

  /** a certified node on its own ring, which is all a TLS peer needs */
  PastryNode alone(Id id) throws Exception {
    int port = freePort();
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, id), NO_PASSWORD, port, environment());
    PastryNode node = factory.newNode();
    nodes.add(node);
    node.boot(new InetSocketAddress(factory.getBindAddress(), port));
    awaitReady(node);
    return node;
  }

  /**
   * A certified node, Carol, that is joining: it sent its join request to a bootstrap node, Dave, whose address
   * accepts TCP connections but never answers, so the join stays open until the test answers it.
   */
  Joiner joiner() throws Exception {
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(store(certificateAuthorityTool, CAROL), NO_PASSWORD, freePort(), environment());
    PastryNode node = factory.newNode();
    nodes.add(node);
    ServerSocket stalled = new ServerSocket(0, 50, factory.getBindAddress());
    sockets.add(stalled);
    sockets.add(new DatagramSocket(stalled.getLocalPort(), factory.getBindAddress()));
    MultiInetSocketAddress stalledAddress = new MultiInetSocketAddress(new InetSocketAddress(factory.getBindAddress(), stalled.getLocalPort()));
    NodeHandle bootstrap = handle(factory, node, stalledAddress, 1, DAVE);
    onSelector(node, () -> {
      node.doneNode(List.of(bootstrap));
      return null;
    });
    Joiner joiner = new Joiner(factory, node, bootstrap);
    CertifiedDeferredDeliveryTest.await(joiner::isJoining, "the joiner sent its join request");
    return joiner;
  }

  record Joiner(CertifiedSocketPastryNodeFactory factory, PastryNode node, NodeHandle bootstrap) {

    /** the handle of another node as this node reads it from the wire */
    NodeHandle handleOf(PastryNode other) throws Exception {
      SocketNodeHandle handle = (SocketNodeHandle) other.getLocalHandle();
      return handle(factory, node, handle.getAddress(), handle.getEpoch(), other.getNodeId());
    }

    /** the joiner's own join request, accepted by {@code acceptor} with that leaf set, as received from {@code acceptor} */
    JoinRequest acceptedJoin(NodeHandle acceptor, LeafSet leafSet) throws Exception {
      return onSelector(node, () -> {
        JoinRequest request = new JoinRequest(node.getLocalHandle(), node.getRoutingTable().baseBitLength(), node.getEnvironment().getTimeSource().currentTimeMillis());
        request.acceptJoin(acceptor, leafSet);
        request.setSender(acceptor);
        return request;
      });
    }

    /** @return true while the join protocol retransmits the join request, that is while it waits for an accepted one */
    boolean isJoining() {
      try {
        java.lang.reflect.Field joinerField = PastryNode.class.getDeclaredField("joiner");
        joinerField.setAccessible(true);
        Object protocol = joinerField.get(node);
        java.lang.reflect.Field field = rice.pastry.standard.ConsistentJoinProtocol.class.getDeclaredField("joinEvent");
        field.setAccessible(true);
        return onSelector(node, () -> field.get(protocol) != null && !node.isReady());
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  static boolean leafSetMember(PastryNode node, NodeHandle handle) {
    try {
      return onSelector(node, () -> node.getLeafSet().asList().contains(handle));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** the handle the node's factory makes for this id, address and epoch, as when it reads one from the wire */
  static NodeHandle handle(CertifiedSocketPastryNodeFactory factory, PastryNode node, MultiInetSocketAddress address, long epoch, Id id) throws Exception {
    SocketNodeHandleFactory handles = (SocketNodeHandleFactory) factory.getNodeHandleFactory(node);
    NodeHandle handle = onSelector(node, () -> handles.getNodeHandle(address, epoch, id));
    assertNotNull(handle);
    return handle;
  }

  /** @return whether a TLS connection to the handle's address authenticated the handle */
  static boolean probe(PastryNode node, NodeHandle handle) throws Exception {
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    onSelector(node, () -> {
      node.probe(handle, result::complete);
      return null;
    });
    return result.get(CertifiedAdmissionTest.TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  Environment environment() {
    Environment env = new Environment();
    environments.add(env);
    return env;
  }
}
