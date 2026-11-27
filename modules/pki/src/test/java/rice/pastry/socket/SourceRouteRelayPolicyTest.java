package rice.pastry.socket;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRouteForwardStrategy;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.PastryNode;
import rice.pastry.standard.NodeIdCertificate;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * Whether a node relays source routes for others (dos.md R14): a plain node does, so that NATted nodes can be reached
 * through it; a certified node does not unless {@code pastry_socket_relay_source_routes_certified} says so,
 * because a relay forwards bytes from anyone to any address that passes the address policy, also to hosts that are no
 * Pastry node at all.
 */
class SourceRouteRelayPolicyTest {
  /** an address in TEST-NET-1 (RFC 5737): passes the address policy, and no packet is ever sent to it */
  static final MultiInetSocketAddress NEXT_HOP = new MultiInetSocketAddress(new InetSocketAddress("192.0.2.1", 9000));
  static final Id ID = Id.build("1234567890ABCDEF1234567890ABCDEF12345678");

  final List<Environment> environments = new ArrayList<>();
  final List<PastryNode> nodes = new ArrayList<>();

  @AfterEach
  void tearDown() {
    nodes.forEach(PastryNode::destroy);
    environments.forEach(Environment::destroy);
  }

  @Test
  @DisplayName("a plain node relays a source route to an acceptable next hop")
  void plainNodeRelays() throws Exception {
    Environment env = environment();
    SocketPastryNodeFactory factory = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), freePort(), env);
    assertTrue(relays(factory));
  }

  @Test
  @DisplayName("a certified node does not relay source routes by default")
  void certifiedNodeDoesNotRelayByDefault() throws Exception {
    Environment env = environment();
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(
        store(ID), new char[0], freePort(), env);
    assertFalse(relays(factory));
  }

  @Test
  @DisplayName("a certified node relays source routes when the parameter allows it")
  void certifiedNodeRelaysWhenAllowed() throws Exception {
    Environment env = environment();
    env.getParameters().setBoolean("pastry_socket_relay_source_routes_certified", true);
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(
        store(ID), new char[0], freePort(), env);
    assertTrue(relays(factory));
  }

  /** whether the forward strategy of a node of this factory lets a datagram on to {@link #NEXT_HOP} */
  private boolean relays(SocketPastryNodeFactory factory) throws IOException {
    PastryNode node = factory.newNode();
    nodes.add(node);
    MultiAddressSourceRouteFactory routes = new MultiAddressSourceRouteFactory();
    SourceRouteForwardStrategy<MultiInetSocketAddress> strategy = factory.getSourceRouteForwardStrategy(node, routes);
    MultiInetSocketAddress local = ((SocketNodeHandle) node.getLocalHandle()).getAddress();
    return strategy.forward(NEXT_HOP, routes.getSourceRoute(List.of(local, NEXT_HOP)), false, null);
  }

  Environment environment() {
    Environment env = new Environment();
    environments.add(env);
    return env;
  }

  /** a node store as written by {@code CertificateAuthorityTool -store}, from a new CA */
  static KeyStore store(Id id) throws Exception {
    KeyPair caKeys = CertificateAuthorityTool.generateKeyPair();
    Instant now = Instant.now();
    X509Certificate caCertificate = CertificateAuthorityTool.generateNewCA("TestCA", now.minus(Duration.ofMinutes(1)),
        now.plus(Duration.ofDays(1)), 1, caKeys, CertificateAuthorityTool.DEFAULT_SIGNATURE_ALGORITHM);
    CertificateAuthorityTool ca = new CertificateAuthorityTool(caCertificate, caKeys);
    KeyPair keys = CertificateAuthorityTool.generateKeyPair();
    KeyStore store = KeyStore.getInstance(CertificateAuthorityTool.DEFAULT_KEYSTORE_TYPE);
    store.load(null, null);
    store.setKeyEntry(NodeIdCertificate.NODE_KEY_ALIAS, keys.getPrivate(), new char[0],
        new Certificate[] {ca.signNodeId(id, keys.getPublic())});
    store.setCertificateEntry(NodeIdCertificate.CA_CERT_ALIAS, ca.getCertificate());
    return store;
  }

  static int freePort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      return serverSocket.getLocalPort();
    }
  }
}
