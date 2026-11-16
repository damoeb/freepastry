package org.mpisws.p2p.testing.transportlayer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CATool;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.PastryNode;
import rice.pastry.leafset.LeafSet;
import rice.pastry.socket.CertifiedIdBindStrategy;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;
import rice.pastry.standard.NodeIdCertificate;

class CertifiedAdmissionTest {

  static final Id BOB = Id.build("1234567890ABCDEF1234567890ABCDEF12345678");
  static final Id ALICE = Id.build("00000000000000000000000000000000000000AA");
  static final char[] NO_PASSWORD = new char[0];
  static final int TIMEOUT_SECONDS = 60;

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();

  @AfterEach
  void tearDown() {
    nodes.forEach(PastryNode::destroy);
    environments.forEach(Environment::destroy);
  }

  @Test
  @DisplayName("the certified node factory rejects a certificate CN that is not a node id, without any configuration")
  void factoryRejectsCommonNameThatIsNotANodeId() throws Exception {
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(store(ca(), BOB), NO_PASSWORD, freePort(), environment());

    assertFalse(factory.getSubjectValidator().test("bob"));
    assertFalse(factory.getSubjectValidator().test(BOB.toStringFull() + "00"));
    assertTrue(factory.getSubjectValidator().test(BOB.toStringFull()));
  }

  @Test
  @DisplayName("a leaf set with an admission test does not take an unauthenticated wire-learned id, and takes it after a matching certificate")
  void leafSetAdmitsAnIdOnlyOnceAuthenticated() {
    CertifiedIdBindStrategy strategy = new CertifiedIdBindStrategy();
    LeafSet leafSet = new LeafSet(SSLTest.handleWithId(ALICE), 24, false);
    leafSet.setAdmission(handle -> strategy.isAuthenticated(handle.getNodeId()));
    strategy.accept(SSLTest.handleWithId(BOB), null, null);

    assertFalse(leafSet.put(SSLTest.handleWithId(BOB)), "pending: not admitted");
    assertFalse(leafSet.member(BOB));

    assertTrue(strategy.accept(SSLTest.handleWithId(BOB), null, Map.of(SSLTransportLayer.OPTION_CERT_SUBJECT, BOB.toStringFull())));

    assertTrue(leafSet.put(SSLTest.handleWithId(BOB)), "authenticated: admitted");
    assertTrue(leafSet.member(BOB));
  }

  @Test
  @DisplayName("nodes of a certified ring authenticate each other by TLS and then admit each other into the leaf set")
  void certifiedNodesAdmitEachOtherAfterVerification() throws Exception {
    CATool ca = ca();
    int port = freePort();
    CertifiedSocketPastryNodeFactory factory1 = new CertifiedSocketPastryNodeFactory(store(ca, ALICE), NO_PASSWORD, port, environment());
    PastryNode first = factory1.newNode();
    nodes.add(first);
    InetSocketAddress firstAddress = new InetSocketAddress(factory1.getBindAddress(), port);
    first.boot(firstAddress);
    awaitReady(first);

    CertifiedSocketPastryNodeFactory factory2 = new CertifiedSocketPastryNodeFactory(store(ca, BOB), NO_PASSWORD, freePort(), environment());
    PastryNode second = factory2.newNode();
    nodes.add(second);
    second.boot(firstAddress);
    awaitReady(second);

    awaitTrue(() -> second.getLeafSet().member(ALICE) && first.getLeafSet().member(BOB));
    assertTrue(factory2.getCertifiedIdBindStrategy().isAuthenticated(ALICE));
    assertTrue(factory1.getCertifiedIdBindStrategy().isAuthenticated(BOB));
    assertEquals(BOB, second.getNodeId());
  }

  // helpers

  Environment environment() {
    Environment env = new Environment();
    environments.add(env);
    return env;
  }

  static CATool ca() throws Exception {
    KeyPair pair = CATool.generateKeyPair();
    Instant now = Instant.now();
    X509Certificate cert = CATool.generateNewCA("TestCA", now.minus(Duration.ofMinutes(1)), now.plus(Duration.ofDays(1)), 1, pair, CATool.DEFAULT_SIGNATURE_ALGORITHM);
    return new CATool(cert, pair);
  }

  /** the node's PKCS12 store as written by {@code CATool -store}: key and certificate for the id, and the CA certificate */
  static KeyStore store(CATool ca, Id id) throws Exception {
    KeyPair keys = CATool.generateKeyPair();
    KeyStore store = KeyStore.getInstance(CATool.DEFAULT_KEYSTORE_TYPE);
    store.load(null, null);
    store.setKeyEntry(NodeIdCertificate.NODE_KEY_ALIAS, keys.getPrivate(), NO_PASSWORD, new Certificate[] {ca.signNodeId(id, keys.getPublic())});
    store.setCertificateEntry(NodeIdCertificate.CA_CERT_ALIAS, ca.getCertificate());
    return store;
  }

  static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  static void awaitReady(PastryNode node) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (!node.isReady() && !node.joinFailed() && System.nanoTime() < deadline) {
      node.awaitReadyOrJoinFailed(500, TimeUnit.MILLISECONDS);
    }
    assertTrue(node.isReady(), "node is ready, join failed: " + node.joinFailedReason());
  }

  static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(250);
    }
    assertTrue(condition.getAsBoolean());
  }
}
