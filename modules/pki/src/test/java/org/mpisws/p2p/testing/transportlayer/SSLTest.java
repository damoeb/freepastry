/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/
package org.mpisws.p2p.testing.transportlayer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CATool;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.identity.BindStrategy;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.dist.DistNodeHandle;
import rice.pastry.socket.CertifiedIdBindStrategy;
import rice.pastry.socket.TransportLayerNodeHandle;
import rice.pastry.standard.NodeIdCertificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two SSL transport layers ("alice" accepts, "bob" connects) over real loopback TCP sockets, with
 * certificates from {@link CATool}'s defaults (2048-bit RSA, SHA256withRSA, PKCS12 stores), which
 * the JDK's TLS disabled-algorithm constraints accept.
 * <p>
 * Replaces the former main() programs SSLTest and SSLTestNoClientAuth (and the empty SSLTest2).
 */
public class SSLTest {
  static final long TIMEOUT_SECONDS = 30;
  static final String PAYLOAD = "foo";
  static final char[] NO_PASSWORD = "".toCharArray();

  static CATool ca;
  static CATool bogusCa;

  Environment rootEnv;
  List<SSLTransportLayer<InetSocketAddress, ByteBuffer>> layers = new ArrayList<SSLTransportLayer<InetSocketAddress, ByteBuffer>>();
  IdentityHashMap<Object, Environment> environments = new IdentityHashMap<Object, Environment>();
  InetAddress addr;
  int alicePort;
  int bobPort;

  /** what the accepting side read, and the certificate subject it saw */
  CompletableFuture<String> aliceRead = new CompletableFuture<String>();
  CompletableFuture<Object> aliceSawSubject = new CompletableFuture<Object>();
  /** exceptions the accepting side reported to its error handler */
  CompletableFuture<Throwable> aliceError = new CompletableFuture<Throwable>();

  static synchronized CATool ca(boolean bogus) throws Exception {
    if (ca == null) {
      ca = newCa("MyCA");
      bogusCa = newCa("Bogus");
    }
    return bogus ? bogusCa : ca;
  }

  static CATool newCa(String cn) throws Exception {
    KeyPair pair = CATool.generateKeyPair();
    Instant now = Instant.now();
    Instant expiry = now.plus(Duration.ofDays(1));
    X509Certificate cert = CATool.generateNewCA(cn, now.minus(Duration.ofMinutes(1)), expiry, 1, pair, CATool.DEFAULT_SIGNATURE_ALGORITHM);
    return new CATool(cert, pair);
  }

  /** a PKCS12 store with a new key signed by signer (if non-null) and trusting trusted (if non-null) */
  static KeyStore store(String cn, CATool signer, CATool trusted) throws Exception {
    return store(cn, signer, trusted, NO_PASSWORD);
  }

  /** like {@link #store(String, CATool, CATool)}, with the given password on the private key entry */
  static KeyStore store(String cn, CATool signer, CATool trusted, char[] keyPassword) throws Exception {
    KeyStore store = KeyStore.getInstance(CATool.DEFAULT_KEYSTORE_TYPE);
    store.load(null, null);
    if (signer != null) {
      KeyPair pair = CATool.generateKeyPair();
      X509Certificate cert = signer.sign(cn, pair.getPublic());
      store.setKeyEntry("private", pair.getPrivate(), keyPassword, new Certificate[] {cert});
    }
    if (trusted != null) store.setCertificateEntry("cert", trusted.getCertificate());
    return store;
  }

  @BeforeEach
  void setUp() throws Exception {
    addr = InetAddress.getLoopbackAddress();
    int[] ports = FreePorts.freePorts(addr, 2);
    alicePort = ports[0];
    bobPort = ports[1];
    rootEnv = new Environment();
  }

  @AfterEach
  void tearDown() {
    try {
      for (SSLTransportLayer<InetSocketAddress, ByteBuffer> tl : layers) tl.destroy();
    } finally {
      // a failing destroy must not leave the selector thread of the environment running
      rootEnv.destroy();
    }
  }

  SSLTransportLayer<InetSocketAddress, ByteBuffer> layer(String name, int port, KeyStore keys, KeyStore trust, int clientAuth) throws IOException {
    return layer(name, port, keys, trust, clientAuth, subject -> true);
  }

  SSLTransportLayer<InetSocketAddress, ByteBuffer> layer(String name, int port, KeyStore keys, KeyStore trust, int clientAuth, Predicate<String> subjectValidator) throws IOException {
    return layer(name, port, keys, NO_PASSWORD, trust, clientAuth, subjectValidator);
  }

  SSLTransportLayer<InetSocketAddress, ByteBuffer> layer(String name, int port, KeyStore keys, char[] keyPassword, KeyStore trust, int clientAuth, Predicate<String> subjectValidator) throws IOException {
    Environment env = rootEnv.cloneEnvironment(name);
    WireTransportLayer wire = new WireTransportLayer(new InetSocketAddress(addr, port), env, null);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> ret = new SSLTransportLayer<InetSocketAddress, ByteBuffer>(wire, keys, keyPassword, trust, clientAuth, subjectValidator, env);
    layers.add(ret);
    environments.put(ret, env);
    return ret;
  }

  /** makes alice read PAYLOAD from incoming sockets into aliceRead */
  void acceptOn(SSLTransportLayer<InetSocketAddress, ByteBuffer> alice) {
    alice.setErrorHandler(new ErrorHandler<InetSocketAddress>() {
      @Override
      public void receivedUnexpectedData(InetSocketAddress i, byte[] bytes, int location, Map<String, Object> options) {
        aliceError.complete(new IOException("unexpected data from " + i));
      }

      @Override
      public void receivedException(InetSocketAddress i, Throwable error) {
        aliceError.complete(error);
      }
    });
    alice.setCallback(new TransportLayerCallback<InetSocketAddress, ByteBuffer>() {
      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) throws IOException {
        Map<String, Object> options = s.getOptions();
        aliceSawSubject.complete(options == null ? "" : String.valueOf(options.get(SSLTransportLayer.OPTION_CERT_SUBJECT)));
        s.register(true, false, new P2PSocketReceiver<InetSocketAddress>() {
          ByteBuffer readMe = ByteBuffer.allocate(PAYLOAD.getBytes(StandardCharsets.UTF_8).length);

          @Override
          public void receiveSelectResult(P2PSocket<InetSocketAddress> socket, boolean canRead, boolean canWrite) throws IOException {
            if (canRead) socket.read(readMe);
            if (readMe.hasRemaining()) {
              socket.register(true, false, this);
            } else {
              aliceRead.complete(new String(readMe.array(), StandardCharsets.UTF_8));
            }
          }

          @Override
          public void receiveException(P2PSocket<InetSocketAddress> socket, Exception ioe) {
            aliceRead.completeExceptionally(ioe);
          }
        });
      }

      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, Map<String, Object> options) throws IOException {
      }
    });
  }

  /**
   * bob opens a socket to alice and writes PAYLOAD
   *
   * @return completes with the opened socket, or exceptionally if opening it failed
   */
  CompletableFuture<P2PSocket<InetSocketAddress>> sendPayload(SSLTransportLayer<InetSocketAddress, ByteBuffer> bob, InetSocketAddress to) {
    final CompletableFuture<P2PSocket<InetSocketAddress>> opened = new CompletableFuture<P2PSocket<InetSocketAddress>>();
    environments.get(bob).getSelectorManager().invoke(new Runnable() {
      @Override
      public void run() {
        bob.openSocket(to, new SocketCallback<InetSocketAddress>() {
          @Override
          public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
            opened.complete(sock);
            sock.register(false, true, new P2PSocketReceiver<InetSocketAddress>() {
              ByteBuffer writeMe = ByteBuffer.wrap(PAYLOAD.getBytes(StandardCharsets.UTF_8));

              @Override
              public void receiveSelectResult(P2PSocket<InetSocketAddress> socket, boolean canRead, boolean canWrite) throws IOException {
                socket.write(writeMe);
                if (writeMe.hasRemaining()) socket.register(false, true, this);
              }

              @Override
              public void receiveException(P2PSocket<InetSocketAddress> socket, Exception ioe) {
              }
            });
          }

          @Override
          public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception ex) {
            opened.completeExceptionally(ex);
          }
        }, null);
      }
    });
    return opened;
  }

  @Test
  @DisplayName("a key entry with a password is used with that password")
  void keyPasswordOfTheKeyStoreIsUsed() throws Exception {
    char[] keyPassword = "x".toCharArray();
    SSLTransportLayer<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(false), ca(false), keyPassword), keyPassword, store("alice", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED, subject -> true);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, store("bob", ca(false), null, keyPassword), keyPassword, store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED, subject -> true);
    acceptOn(alice);

    sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertEquals(PAYLOAD, aliceRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void mutuallyAuthenticatedSocketDeliversDataAndPeerNames() throws Exception {
    SSLTransportLayer<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(false), ca(false)), store("alice", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, store("bob", ca(false), null), store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    acceptOn(alice);

    P2PSocket<InetSocketAddress> bobSocket = sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertEquals(PAYLOAD, aliceRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals("bob", aliceSawSubject.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals("alice", bobSocket.getOptions().get(SSLTransportLayer.OPTION_CERT_SUBJECT));
  }

  @Test
  void serverOnlyAuthenticationDeliversData() throws Exception {
    // alice authenticates to bob, but does not ask for a client certificate (formerly SSLTestNoClientAuth)
    SSLTransportLayer<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(false), null), null, SSLTransportLayer.CLIENT_AUTH_NONE);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, null, store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    acceptOn(alice);

    P2PSocket<InetSocketAddress> bobSocket = sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertEquals(PAYLOAD, aliceRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals("alice", bobSocket.getOptions().get(SSLTransportLayer.OPTION_CERT_SUBJECT));
  }

  @Test
  void clientWithoutClientAuthTrustsServerSignedByItsConfiguredCa() throws Exception {
    // bob's configured trust store holds the test CA, which the JVM default trust store does not know
    SSLTransportLayer<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(false), null), null, SSLTransportLayer.CLIENT_AUTH_NONE);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, null, store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_NONE);
    acceptOn(alice);

    P2PSocket<InetSocketAddress> bobSocket = sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertEquals(PAYLOAD, aliceRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals("alice", bobSocket.getOptions().get(SSLTransportLayer.OPTION_CERT_SUBJECT));
  }

  @Test
  void clientWithoutClientAuthRejectsServerNotSignedByItsConfiguredCa() throws Exception {
    SSLTransportLayer<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(true), null), null, SSLTransportLayer.CLIENT_AUTH_NONE);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, null, store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_NONE);
    acceptOn(alice);

    Exception e = assertThrows(Exception.class, () -> sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

    assertFalse(e instanceof java.util.concurrent.TimeoutException, "handshake should fail, not hang: " + e);
    assertFalse(aliceRead.isDone() && !aliceRead.isCompletedExceptionally(), "alice must not receive data");
  }

  @Test
  void clientRejectsServerCertificateFromUntrustedCa() throws Exception {
    SSLTransportLayer<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(true), ca(false)), store("alice", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, store("bob", ca(false), null), store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    acceptOn(alice);

    Exception e = assertThrows(Exception.class, () -> sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

    assertFalse(e instanceof java.util.concurrent.TimeoutException, "handshake should fail, not hang: " + e);
    assertFalse(aliceRead.isDone() && !aliceRead.isCompletedExceptionally(), "alice must not receive data");
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored") // the handshake fails on alice's side; aliceError is what is checked
  void serverRejectsClientCertificateFromUntrustedCa() throws Exception {
    SSLTransportLayer<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(false), null), store("alice", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, store("bob", ca(true), null), store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    acceptOn(alice);

    sendPayload(bob, new InetSocketAddress(addr, alicePort));

    assertNotNull(aliceError.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertFalse(aliceRead.isDone() && !aliceRead.isCompletedExceptionally(), "alice must not receive data");
  }

  /** a node handle that only has an id; the bind strategy looks at nothing else */
  static TransportLayerNodeHandle<MultiInetSocketAddress> handleWithId(Id id) {
    return new DistNodeHandle<MultiInetSocketAddress>(id) {
      @Override
      public MultiInetSocketAddress getAddress() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long getEpoch() {
        return 0;
      }

      @Override
      public int getLiveness() {
        return LIVENESS_ALIVE;
      }

      @Override
      public boolean ping() {
        return false;
      }

      @Override
      public boolean checkLiveness() {
        return false;
      }

      @Override
      public void serialize(OutputBuffer buf) {
      }

      @Override
      public String toString() {
        return "handle(" + getNodeId() + ")";
      }

      @Override
      public boolean equals(Object obj) {
        return obj instanceof NodeHandle h && h.getNodeId().equals(getNodeId());
      }

      @Override
      public int hashCode() {
        return getNodeId().hashCode();
      }
    };
  }

  /** bob, whose certificate is for bobId, connects to alice; returns the options of the socket alice accepted */
  Map<String, Object> bobConnectsWithCertificateFor(Id bobId) throws Exception {
    return bobConnectsWithCertificateFor(bobId.toStringFull(), subject -> true);
  }

  Map<String, Object> bobConnectsWithCertificateFor(String bobCn, Predicate<String> aliceSubjectValidator) throws Exception {
    SSLTransportLayer<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(false), ca(false)), store("alice", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED, aliceSubjectValidator);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, store(bobCn, ca(false), null), store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    acceptOn(alice);
    sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertEquals(PAYLOAD, aliceRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    return Map.of(SSLTransportLayer.OPTION_CERT_SUBJECT, aliceSawSubject.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void peerClaimingTheIdOfItsCertificateIsAccepted() throws Exception {
    Id bobId = Id.build("1234567890ABCDEF1234567890ABCDEF12345678");

    Map<String, Object> options = bobConnectsWithCertificateFor(bobId);

    assertTrue(new CertifiedIdBindStrategy().accept(handleWithId(bobId), null, options));
  }

  @Test
  void peerClaimingAnotherIdThanItsCertificateIsRefused() throws Exception {
    Id bobId = Id.build("1234567890ABCDEF1234567890ABCDEF12345678");
    Id claimed = Id.build("FEDCBA0987654321FEDCBA0987654321FEDCBA09");

    Map<String, Object> options = bobConnectsWithCertificateFor(bobId);

    assertFalse(new CertifiedIdBindStrategy().accept(handleWithId(claimed), null, options));
  }

  @Test
  void optionsWithoutCertificateAreNotChecked() {
    // UDP datagrams carry no certificate subject
    assertTrue(new CertifiedIdBindStrategy().accept(handleWithId(Id.build("1234567890ABCDEF1234567890ABCDEF12345678")), null, Map.of()));
  }

  static final Id BOB = Id.build("1234567890ABCDEF1234567890ABCDEF12345678");

  @Test
  @DisplayName("only a certificate changes the binding of an authenticated id, and a certificate beats an unverified binding")
  void onlyACertificateReplacesTheBindingOfAnAuthenticatedId() {
    CertifiedIdBindStrategy strategy = new CertifiedIdBindStrategy();
    Id other = Id.build("FEDCBA0987654321FEDCBA0987654321FEDCBA09");
    TransportLayerNodeHandle<MultiInetSocketAddress> bob = handleWithId(BOB);
    TransportLayerNodeHandle<MultiInetSocketAddress> claimant = handleWithId(other);
    Map<String, Object> claimantsCertificate = Map.of(SSLTransportLayer.OPTION_CERT_SUBJECT, other.toStringFull());

    assertEquals(BindStrategy.Replacement.ASK_NODE_CHANGE_STRATEGY, strategy.replace(bob, claimant, null), "neither verified: the epochs decide");
    assertEquals(BindStrategy.Replacement.REPLACE, strategy.replace(bob, claimant, claimantsCertificate), "certified over unverified");

    strategy.accept(bob, null, Map.of(SSLTransportLayer.OPTION_CERT_SUBJECT, BOB.toStringFull()));

    assertEquals(BindStrategy.Replacement.KEEP, strategy.replace(bob, claimant, null), "a datagram or a handle from the wire");
    assertEquals(BindStrategy.Replacement.KEEP, strategy.replace(bob, claimant, Map.of()), "no certificate subject");
    assertEquals(BindStrategy.Replacement.ASK_NODE_CHANGE_STRATEGY, strategy.replace(bob, claimant, claimantsCertificate), "both certified: the epochs decide");
  }

  @Test
  @DisplayName("a received message goes up only if it came with a certificate for its sender")
  void onlyMessagesWithTheSendersCertificateGoUp() {
    CertifiedIdBindStrategy strategy = new CertifiedIdBindStrategy();
    strategy.accept(handleWithId(BOB), null, Map.of(SSLTransportLayer.OPTION_CERT_SUBJECT, BOB.toStringFull()));

    assertTrue(strategy.acceptsMessage(handleWithId(BOB), Map.of(SSLTransportLayer.OPTION_CERT_SUBJECT, BOB.toStringFull())));
    assertFalse(strategy.acceptsMessage(handleWithId(BOB), Map.of()), "a datagram, although the id is authenticated");
    assertFalse(strategy.acceptsMessage(handleWithId(BOB), null));
    assertFalse(strategy.acceptsMessage(handleWithId(BOB), Map.of(SSLTransportLayer.OPTION_CERT_SUBJECT, "FEDCBA0987654321FEDCBA0987654321FEDCBA09")));
  }

  @Test
  @DisplayName("a binding learned over the wire is unverified, not authenticated")
  void wireLearnedBindingIsUnverified() {
    CertifiedIdBindStrategy strategy = new CertifiedIdBindStrategy();

    assertTrue(strategy.accept(handleWithId(BOB), null, null), "usable: routing and connecting towards it is allowed");

    assertTrue(strategy.isPending(BOB));
    assertFalse(strategy.isAuthenticated(BOB));
  }

  @Test
  @DisplayName("a TLS certificate whose CN equals the id promotes the pending binding")
  void matchingCertificatePromotesPendingBinding() throws Exception {
    CertifiedIdBindStrategy strategy = new CertifiedIdBindStrategy();
    strategy.accept(handleWithId(BOB), null, null);

    assertTrue(strategy.accept(handleWithId(BOB), null, bobConnectsWithCertificateFor(BOB)));

    assertTrue(strategy.isAuthenticated(BOB));
    assertFalse(strategy.isPending(BOB));
    strategy.accept(handleWithId(BOB), null, null);
    assertTrue(strategy.isAuthenticated(BOB), "a later certificate-less sighting does not demote it");
    assertFalse(strategy.isPending(BOB));
  }

  @Test
  @DisplayName("a TLS certificate whose CN differs from the claimed id rejects the pending binding")
  void mismatchingCertificateRejectsPendingBinding() throws Exception {
    Id claimed = Id.build("FEDCBA0987654321FEDCBA0987654321FEDCBA09");
    CertifiedIdBindStrategy strategy = new CertifiedIdBindStrategy();
    strategy.accept(handleWithId(claimed), null, null);

    assertFalse(strategy.accept(handleWithId(claimed), null, bobConnectsWithCertificateFor(BOB)));

    assertFalse(strategy.isPending(claimed));
    assertFalse(strategy.isAuthenticated(claimed));
  }

  @Test
  @DisplayName("strict mode aborts the handshake when the CN is not a node id")
  void strictModeRefusesCommonNameThatIsNotANodeId() {
    assertThrows(Exception.class, () -> bobConnectsWithCertificateFor("bob", NodeIdCertificate::isNodeId));
  }

  @Test
  @DisplayName("strict mode lets a CN that is a node id pass")
  void strictModeAcceptsNodeIdCommonName() throws Exception {
    assertEquals(BOB.toStringFull(), bobConnectsWithCertificateFor(BOB.toStringFull(), NodeIdCertificate::isNodeId).get(SSLTransportLayer.OPTION_CERT_SUBJECT));
  }
}
