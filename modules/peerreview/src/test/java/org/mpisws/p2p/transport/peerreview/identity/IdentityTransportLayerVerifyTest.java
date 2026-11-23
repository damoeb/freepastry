package org.mpisws.p2p.transport.peerreview.identity;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import org.mpisws.p2p.pki.x509.X509Serializer;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import org.mpisws.p2p.transport.util.Serializer;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SIGNATURE_BAD;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SIGNATURE_OK;

/** A signature from a peer that is garbage is a failed verification, not an exception on the selector thread. */
class IdentityTransportLayerVerifyTest {
  private static final String LOCAL = "local";
  private static final byte[] MESSAGE = {1, 2, 3, 4};

  private Environment environment;
  private IdentityTransportLayerImpl<String, String> testObj;
  private KeyPair nodeKeys;
  private X509Certificate nodeCert;
  private TransportLayer<String, ByteBuffer> below;
  private CertificateValidator<String> validator;
  private Serializer<String> serializer;

  @BeforeEach
  void setUp() throws Exception {
    environment = new Environment();
    KeyPair authorityKeys = CertificateAuthorityTool.generateKeyPair();
    Instant now = Instant.now();
    CertificateAuthorityTool authority = new CertificateAuthorityTool(
        CertificateAuthorityTool.generateNewCA("CA", now.minusSeconds(60), now.plusSeconds(3600), 1, authorityKeys, CertificateAuthorityTool.DEFAULT_SIGNATURE_ALGORITHM), authorityKeys);
    nodeKeys = CertificateAuthorityTool.generateKeyPair();
    nodeCert = authority.sign(LOCAL, nodeKeys.getPublic());
    validator = new CaCertificateValidator<>(authority.getCertificate(), principal -> principal, Instant::now);
    @SuppressWarnings("unchecked")
    TransportLayer<String, ByteBuffer> stubBelow = (TransportLayer<String, ByteBuffer>) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {TransportLayer.class}, (proxy, method, args) -> null);
    below = stubBelow;
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
    testObj = new IdentityTransportLayerImpl<>(serializer, new X509Serializer(), LOCAL, nodeCert, nodeKeys.getPrivate(), below, new SHA256HashProvider(), validator, environment);
  }

  @AfterEach
  void tearDown() {
    environment.destroy();
  }

  @Test
  void validSignatureIsAccepted() throws Exception {
    Signature signer = Signature.getInstance(IdentityTransportLayerImpl.DEFAULT_SIGNATURE_ALGORITHM);
    signer.initSign(nodeKeys.getPrivate());
    signer.update(MESSAGE);

    assertEquals(SIGNATURE_OK, testObj.verify(LOCAL, MESSAGE, signer.sign()));
  }

  @Test
  void malformedSignatureIsReportedAsBadNotThrown() {
    assertEquals(SIGNATURE_BAD, testObj.verify(LOCAL, MESSAGE, new byte[] {1, 2, 3}));
  }

  @Test
  void emptySignatureIsReportedAsBad() {
    assertEquals(SIGNATURE_BAD, testObj.verify(LOCAL, MESSAGE, new byte[0]));
  }

  /** Lets the test fill the certificate store without the certificate exchange. */
  static class CertificateHoldingTransport extends IdentityTransportLayerImpl<String, String> {
    CertificateHoldingTransport(Serializer<String> serializer, X509Certificate localCert, KeyPair localKeys, TransportLayer<String, ByteBuffer> below, CertificateValidator<String> validator, Environment env) throws Exception {
      super(serializer, new X509Serializer(), LOCAL, localCert, localKeys.getPrivate(), below, new SHA256HashProvider(), validator, env);
    }

    void addCertificate(String identifier, X509Certificate cert) {
      knownValues.put(identifier, cert);
    }
  }

  @Test
  void verifierCacheStaysBoundedAndEvictedVerifiersAreRebuilt() throws Exception {
    CertificateHoldingTransport transport = new CertificateHoldingTransport(serializer, nodeCert, nodeKeys, below, validator, environment);
    int identityCount = 2_000;
    for (int i = 0; i < identityCount; i++) {
      transport.addCertificate("node" + i, nodeCert);
    }
    Signature signer = Signature.getInstance(IdentityTransportLayerImpl.DEFAULT_SIGNATURE_ALGORITHM);
    signer.initSign(nodeKeys.getPrivate());
    signer.update(MESSAGE);
    byte[] signature = signer.sign();

    for (int i = 0; i < identityCount; i++) {
      assertEquals(SIGNATURE_OK, transport.verify("node" + i, MESSAGE, signature));
    }

    assertTrue(transport.verifiers.size() <= IdentityTransportLayerImpl.MAX_CACHED_VERIFIERS, "the verifier cache holds " + transport.verifiers.size() + " entries");
    // the verifier of the first identity was evicted and is built again from the certificate
    assertEquals(SIGNATURE_OK, transport.verify("node0", MESSAGE, signature));
  }
}
