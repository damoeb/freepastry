package org.mpisws.p2p.testing.transportlayer.peerreview;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.authpush.AuthenticatorPushProtocol;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocol;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AuthPushMessage;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_TRUSTED;

/**
 * Authenticators for subjects without a known certificate wait in the pending store; a peer that pushes
 * authenticators for made-up subjects must not grow it, in memory or on disk, without bound.
 */
class PendingAuthenticatorBoundTest {
  private static final int MAX_PER_SUBJECT = 4;
  private static final int MAX_TOTAL = 256;
  private static final int SUBJECTS_PER_MESSAGE = 1024;
  private static final int MESSAGES = 10;

  @TempDir
  Path dir;

  private Environment environment;

  @BeforeEach
  void setUp() {
    environment = new Environment();
    environment.getParameters().setInt("peerreview_pending_authenticators_max_per_subject", MAX_PER_SUBJECT);
    environment.getParameters().setInt("peerreview_pending_authenticators_max_total", MAX_TOTAL);
  }

  @AfterEach
  void tearDown() {
    environment.destroy();
  }

  @SuppressWarnings("unchecked")
  private static <T> T stub(Class<T> type) {
    return (T) Proxy.newProxyInstance(PendingAuthenticatorBoundTest.class.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> switch (method.getName()) {
      case "hasCertificate" -> false;
      case "getStatus" -> STATUS_TRUSTED;
      default -> null;
    });
  }

  private static Authenticator authenticator(long seq) {
    byte[] hash = new byte[AuthenticatorStoreTest.HASH_LEN];
    byte[] signature = new byte[AuthenticatorStoreTest.SIGN_LEN];
    hash[0] = (byte) seq;
    return new Authenticator(seq, hash, signature);
  }

  @Test
  void pushedAuthenticatorsForUnknownSubjectsStayBounded() throws Exception {
    PeerReview<MyInetSocketAddress, MyInetSocketAddress> peerReview = new TestPeerReview(environment, new AuthenticatorSerializer(AuthenticatorStoreTest.HASH_LEN, AuthenticatorStoreTest.SIGN_LEN));
    AuthenticatorStore<MyInetSocketAddress> inStore = new AuthenticatorStore<>(peerReview);
    AuthenticatorStore<MyInetSocketAddress> outStore = new AuthenticatorStore<>(peerReview);
    AuthenticatorStore<MyInetSocketAddress> pendingStore = new AuthenticatorStore<>(peerReview, true);
    Path pendingFile = dir.resolve("authenticators.pending");
    pendingStore.setFilename(pendingFile);
    AuthenticatorPushProtocol<MyInetSocketAddress, MyInetSocketAddress> testObj = new AuthenticatorPushProtocol<>(
        peerReview, inStore, outStore, pendingStore, stub(IdentityTransport.class), stub(PeerInfoStore.class), stub(EvidenceTransferProtocol.class), environment);
    MyInetSocketAddress attacker = new MyInetSocketAddress(InetAddress.getLoopbackAddress(), 1);

    for (int message = 0; message < MESSAGES; message++) {
      Map<MyInetSocketAddress, List<Authenticator>> authenticators = new LinkedHashMap<>();
      for (int subject = 0; subject < SUBJECTS_PER_MESSAGE; subject++) {
        authenticators.put(new MyInetSocketAddress(InetAddress.getLoopbackAddress(), 10_000 + subject), List.of(authenticator(message)));
      }
      testObj.handleIncomingAuthenticators(attacker, new AuthPushMessage<>(authenticators));
    }

    int total = 0;
    for (MyInetSocketAddress subject : pendingStore.getSubjects()) {
      int count = pendingStore.numAuthenticatorsFor(subject);
      assertTrue(count <= MAX_PER_SUBJECT, "subject holds " + count);
      total += count;
    }
    assertTrue(total <= MAX_TOTAL, "the pending store holds " + total + " authenticators");
    SimpleOutputBuffer record = new SimpleOutputBuffer();
    MyInetSocketAddress.serializer.serialize(attacker, record);
    authenticator(0).serialize(record);
    long maxFileBytes = 2L * MAX_TOTAL * record.getWritten();
    assertTrue(Files.size(pendingFile) <= maxFileBytes, "the pending file holds " + Files.size(pendingFile) + " bytes, more than " + maxFileBytes);
  }
}
