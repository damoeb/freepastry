package org.mpisws.p2p.transport.peerreview.identity;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import org.mpisws.p2p.pki.x509.X509Serializer;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import org.mpisws.p2p.transport.table.TableTransportLayer;
import org.mpisws.p2p.transport.util.Serializer;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.NO_CERTIFICATE;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SIGNATURE_OK;

/**
 * A peer asked for the certificate of another node answers with whatever it likes; the identity layer stores only a
 * certificate the CA issued for the asked principal, and keeps the one it has.
 */
class IdentityCertificateTrustTest {
  private static final String LOCAL = "local";
  private static final String HONEST = "honest";
  private static final String COLLUDER = "colluder";
  private static final byte[] MESSAGE = {1, 2, 3, 4};

  private Environment environment;
  private CertificateAuthorityTool authority;
  private KeyPair authorityKeys;
  private KeyPair nodeKeys;
  private X509Certificate localCert;
  private Serializer<String> serializer;
  /** the sockets the layer opened, in order; each answers once the test delivers its scripted response */
  private final List<ScriptedSocket> sockets = new ArrayList<>();
  private IdentityTransportLayerImpl<String, String> testObj;

  @BeforeEach
  void setUp() throws Exception {
    environment = new Environment();
    authorityKeys = CertificateAuthorityTool.generateKeyPair();
    Instant now = Instant.now();
    authority = new CertificateAuthorityTool(
        CertificateAuthorityTool.generateNewCA("CA", now.minusSeconds(60), now.plusSeconds(3600), 1, authorityKeys, CertificateAuthorityTool.DEFAULT_SIGNATURE_ALGORITHM), authorityKeys);
    nodeKeys = CertificateAuthorityTool.generateKeyPair();
    localCert = authority.sign(LOCAL, nodeKeys.getPublic());
    serializer = new Serializer<>() {
      @Override
      public void serialize(String identifier, OutputBuffer buf) throws IOException {
        buf.writeUTF(identifier);
      }

      @Override
      public String deserialize(InputBuffer buf) throws IOException {
        return buf.readUTF();
      }
    };
    testObj = newIdentityLayer();
  }

  @AfterEach
  void tearDown() {
    environment.destroy();
  }

  private IdentityTransportLayerImpl<String, String> newIdentityLayer() throws Exception {
    return new IdentityTransportLayerImpl<>(serializer, new X509Serializer(), LOCAL, localCert, nodeKeys.getPrivate(), below(), new SHA256HashProvider(), environment);
  }

  /** The transport below: every socket it opens is a {@link ScriptedSocket}. */
  @SuppressWarnings("unchecked")
  private TransportLayer<String, ByteBuffer> below() {
    return (TransportLayer<String, ByteBuffer>) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {TransportLayer.class}, (proxy, method, args) -> {
      if (method.getName().equals("openSocket")) {
        ScriptedSocket socket = new ScriptedSocket((String) args[0]);
        sockets.add(socket);
        ((SocketCallback<String>) args[1]).receiveResult(null, socket);
      }
      return null;
    });
  }

  /** Records what a certificate request ends with. */
  static final class Outcome implements Continuation<X509Certificate, Exception> {
    @Nullable X509Certificate result;
    @Nullable Exception exception;

    @Override
    public void receiveResult(X509Certificate result) {
      this.result = result;
    }

    @Override
    public void receiveException(Exception exception) {
      this.exception = exception;
    }
  }

  private Outcome request(String from, String principal) {
    Outcome outcome = new Outcome();
    testObj.requestCertificate(from, principal, outcome, null);
    return outcome;
  }

  private static ByteBuffer certificateResponse(X509Certificate cert) throws IOException {
    SimpleOutputBuffer body = new SimpleOutputBuffer();
    body.writeByte(TableTransportLayer.RESPONSE_SUCCESS);
    new X509Serializer().serialize(cert, body);
    ByteBuffer framed = ByteBuffer.allocate(Integer.BYTES + body.getWritten());
    framed.putInt(body.getWritten());
    framed.put(body.getBytes(), 0, body.getWritten());
    framed.flip();
    return framed;
  }

  private byte[] signedBy(KeyPair keys) throws Exception {
    Signature signer = Signature.getInstance(IdentityTransportLayerImpl.DEFAULT_SIGNATURE_ALGORITHM);
    signer.initSign(keys.getPrivate());
    signer.update(MESSAGE);
    return signer.sign();
  }

  @Test
  void certificateFromAnotherCaIsNotStored() throws Exception {
    KeyPair rogueAuthorityKeys = CertificateAuthorityTool.generateKeyPair();
    Instant now = Instant.now();
    CertificateAuthorityTool rogue = new CertificateAuthorityTool(
        CertificateAuthorityTool.generateNewCA("CA", now.minusSeconds(60), now.plusSeconds(3600), 1, rogueAuthorityKeys, CertificateAuthorityTool.DEFAULT_SIGNATURE_ALGORITHM), rogueAuthorityKeys);
    KeyPair colluderKeys = CertificateAuthorityTool.generateKeyPair();

    Outcome outcome = request(COLLUDER, HONEST);
    sockets.get(0).deliver(certificateResponse(rogue.sign(HONEST, colluderKeys.getPublic())));

    assertNotNull(outcome.exception, "a forged certificate is refused");
    assertFalse(testObj.hasCertificate(HONEST));
    assertEquals(NO_CERTIFICATE, testObj.verify(HONEST, MESSAGE, signedBy(colluderKeys)));
  }

  @Test
  void certificateForAnotherSubjectIsNotStored() throws Exception {
    KeyPair colluderKeys = CertificateAuthorityTool.generateKeyPair();
    X509Certificate colluderCert = authority.sign(COLLUDER, colluderKeys.getPublic());

    Outcome outcome = request(COLLUDER, HONEST);
    sockets.get(0).deliver(certificateResponse(colluderCert));

    assertNotNull(outcome.exception, "the colluder's own certificate is no certificate for the honest node");
    assertFalse(testObj.hasCertificate(HONEST));
    assertEquals(NO_CERTIFICATE, testObj.verify(HONEST, MESSAGE, signedBy(colluderKeys)));
  }

  @Test
  void expiredCertificateIsNotStored() throws Exception {
    KeyPair honestKeys = CertificateAuthorityTool.generateKeyPair();
    Instant now = Instant.now();
    X509Certificate expired = CertificateAuthorityTool.sign(HONEST, honestKeys.getPublic(), now.minusSeconds(120), now.minusSeconds(60), 7, authority.getCertificate(), authorityKeys.getPrivate());

    Outcome outcome = request(COLLUDER, HONEST);
    sockets.get(0).deliver(certificateResponse(expired));

    assertNotNull(outcome.exception);
    assertFalse(testObj.hasCertificate(HONEST));
  }

  @Test
  void storedCertificateIsNotReplacedByAnotherValidOne() throws Exception {
    KeyPair honestKeys = CertificateAuthorityTool.generateKeyPair();
    KeyPair colluderKeys = CertificateAuthorityTool.generateKeyPair();
    // two requests in flight; the colluder's answer, a second certificate the CA issued for the same id, comes last
    Outcome fromHonest = request(HONEST, HONEST);
    Outcome fromColluder = request(COLLUDER, HONEST);
    sockets.get(0).deliver(certificateResponse(authority.sign(HONEST, honestKeys.getPublic())));
    sockets.get(1).deliver(certificateResponse(authority.sign(HONEST, colluderKeys.getPublic())));

    assertNotNull(fromHonest.result);
    assertTrue(fromColluder.exception != null || fromHonest.result.equals(fromColluder.result), "the second certificate is not taken");
    assertEquals(SIGNATURE_OK, testObj.verify(HONEST, MESSAGE, signedBy(honestKeys)));
    assertTrue(testObj.verify(HONEST, MESSAGE, signedBy(colluderKeys)) != SIGNATURE_OK);
  }

  @Test
  void genuineCertificateIsStored() throws Exception {
    KeyPair honestKeys = CertificateAuthorityTool.generateKeyPair();

    Outcome outcome = request(COLLUDER, HONEST);
    sockets.get(0).deliver(certificateResponse(authority.sign(HONEST, honestKeys.getPublic())));

    assertNotNull(outcome.result);
    assertEquals(SIGNATURE_OK, testObj.verify(HONEST, MESSAGE, signedBy(honestKeys)));
  }

  @Test
  void certificateStoreIsBoundedAndKeepsTheLocalCertificate() throws Exception {
    environment.getParameters().setInt("peerreview_identity_max_certificates", 16);
    testObj = newIdentityLayer();
    int count = 40;
    for (int i = 0; i < count; i++) {
      request(COLLUDER, "node" + i);
      sockets.get(sockets.size() - 1).deliver(certificateResponse(authority.sign("node" + i, nodeKeys.getPublic())));
    }

    assertTrue(testObj.hasCertificate(LOCAL), "the local certificate is never evicted");
    assertTrue(testObj.hasCertificate("node" + (count - 1)));
    assertFalse(testObj.hasCertificate("node0"), "the oldest certificate was evicted");
  }

  /**
   * A socket to the peer: it swallows the request, and answers with the response once the test delivers it.
   */
  static final class ScriptedSocket implements P2PSocket<String> {
    private final String peer;
    private ByteBuffer response = ByteBuffer.allocate(0);
    private @Nullable P2PSocketReceiver<String> reader;

    ScriptedSocket(String peer) {
      this.peer = peer;
    }

    void deliver(ByteBuffer scripted) throws IOException {
      response = scripted;
      P2PSocketReceiver<String> waiting = reader;
      reader = null;
      if (waiting != null) waiting.receiveSelectResult(this, true, false);
    }

    @Override
    public @Nullable Map<String, Object> getOptions() {
      return null;
    }

    @Override
    public String getIdentifier() {
      return peer;
    }

    @Override
    public long read(ByteBuffer dsts) {
      int count = Math.min(dsts.remaining(), response.remaining());
      for (int i = 0; i < count; i++) dsts.put(response.get());
      return count;
    }

    @Override
    public long write(ByteBuffer srcs) {
      int count = srcs.remaining();
      srcs.position(srcs.limit());
      return count;
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<String> receiver) {
      if (wantToRead) reader = receiver;
    }

    @Override
    public void shutdownOutput() {
    }

    @Override
    public void close() {
    }
  }
}
