package org.mpisws.p2p.testing.pki.x509;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.standard.CertifiedNodeIdFactory;
import rice.pastry.standard.NodeIdCertificate;
import rice.pastry.standard.RandomNodeIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Node ids certified by a CA: {@link NodeIdCertificate#verify} and {@link CertifiedNodeIdFactory}.
 */
public class CertifiedNodeIdTest {

  static final char[] NO_PASSWORD = "".toCharArray();

  Environment env;
  CertificateAuthorityTool ca;
  KeyPair nodeKeys;
  Id id;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    ca = newCa("MyCA");
    nodeKeys = CertificateAuthorityTool.generateKeyPair();
    id = new RandomNodeIdFactory(env).generateNodeId();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  static CertificateAuthorityTool newCa(String cn) throws Exception {
    KeyPair pair = CertificateAuthorityTool.generateKeyPair();
    Instant now = Instant.now();
    X509Certificate cert = CertificateAuthorityTool.generateNewCA(cn, now.minus(Duration.ofMinutes(1)), now.plus(Duration.ofDays(1)), 1, pair, CertificateAuthorityTool.DEFAULT_SIGNATURE_ALGORITHM);
    return new CertificateAuthorityTool(cert, pair);
  }

  CertifiedNodeIdFactory factory(X509Certificate nodeCert, X509Certificate trusted) {
    return new CertifiedNodeIdFactory(nodeCert, nodeKeys.getPrivate(), trusted, env);
  }

  static X509Certificate parse(byte[] der) throws CertificateException {
    return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(der));
  }

  @Test
  void validCertificateYieldsItsNodeId() throws Exception {
    X509Certificate cert = ca.signNodeId(id, nodeKeys.getPublic());

    assertEquals(id, NodeIdCertificate.verify(cert, ca.getCertificate(), Instant.now()));
    assertEquals(id, factory(cert, ca.getCertificate()).generateNodeId());
  }

  @Test
  void factoryLoadsTheNodeStoreWrittenByTheCaTool() throws Exception {
    X509Certificate cert = ca.signNodeId(id, nodeKeys.getPublic());
    KeyStore store = KeyStore.getInstance(CertificateAuthorityTool.DEFAULT_KEYSTORE_TYPE);
    store.load(null, null);
    store.setKeyEntry(NodeIdCertificate.NODE_KEY_ALIAS, nodeKeys.getPrivate(), NO_PASSWORD, new Certificate[] {cert});
    store.setCertificateEntry(NodeIdCertificate.CA_CERT_ALIAS, ca.getCertificate());

    assertEquals(id, CertifiedNodeIdFactory.fromKeyStore(store, NO_PASSWORD, env).generateNodeId());
  }

  @Test
  void certificateSignedByAnotherCaWithTheSameNameIsRejected() throws Exception {
    X509Certificate cert = newCa("MyCA").signNodeId(id, nodeKeys.getPublic());

    assertThrows(CertificateException.class, () -> NodeIdCertificate.verify(cert, ca.getCertificate(), Instant.now()));
    assertThrows(SecurityException.class, () -> factory(cert, ca.getCertificate()).generateNodeId());
  }

  @Test
  void certificateIssuedByAnotherCaIsRejected() throws Exception {
    X509Certificate cert = newCa("Bogus").signNodeId(id, nodeKeys.getPublic());

    assertThrows(CertificateException.class, () -> NodeIdCertificate.verify(cert, ca.getCertificate(), Instant.now()));
  }

  @Test
  void certificateForAnotherNodeIdDoesNotGrantTheClaimedId() throws Exception {
    Id other = new RandomNodeIdFactory(env).generateNodeId();
    X509Certificate cert = ca.signNodeId(other, nodeKeys.getPublic());

    Id actual = NodeIdCertificate.verify(cert, ca.getCertificate(), Instant.now());

    assertEquals(other, actual);
    assertFalse(actual.equals(id), "the certificate must not grant the id of another node");
    assertTrue(NodeIdCertificate.matches(other, other.toStringFull()));
    assertFalse(NodeIdCertificate.matches(id, other.toStringFull()));
    assertFalse(NodeIdCertificate.matches(id, null));
  }

  @Test
  void tamperedSignatureIsRejected() throws Exception {
    byte[] der = ca.signNodeId(id, nodeKeys.getPublic()).getEncoded();
    der[der.length - 1] ^= 1; // the last byte belongs to the signature
    X509Certificate tampered = parse(der);

    assertThrows(CertificateException.class, () -> NodeIdCertificate.verify(tampered, ca.getCertificate(), Instant.now()));
    assertThrows(SecurityException.class, () -> factory(tampered, ca.getCertificate()).generateNodeId());
  }

  @Test
  void tamperedNodeIdIsRejected() throws Exception {
    Id other = new RandomNodeIdFactory(env).generateNodeId();
    byte[] der = ca.signNodeId(id, nodeKeys.getPublic()).getEncoded();
    byte[] from = id.toStringFull().getBytes(StandardCharsets.US_ASCII);
    byte[] bytes = other.toStringFull().getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(bytes, 0, der, indexOf(der, from), bytes.length); // the id is in the subject, which the signature covers
    X509Certificate tampered = parse(der);

    assertThrows(CertificateException.class, () -> NodeIdCertificate.verify(tampered, ca.getCertificate(), Instant.now()));
  }

  @Test
  void expiredCertificateIsRejected() throws Exception {
    Instant now = Instant.now();
    X509Certificate cert = ca.signNodeId(id, nodeKeys.getPublic(), now.minus(Duration.ofHours(2)), now.minus(Duration.ofHours(1)));

    assertThrows(CertificateException.class, () -> NodeIdCertificate.verify(cert, ca.getCertificate(), now));
    assertThrows(SecurityException.class, () -> factory(cert, ca.getCertificate()).generateNodeId());
  }

  @Test
  void certificateThatIsNotYetValidIsRejected() throws Exception {
    Instant now = Instant.now();
    X509Certificate cert = ca.signNodeId(id, nodeKeys.getPublic(), now.plus(Duration.ofHours(1)), now.plus(Duration.ofHours(2)));

    assertThrows(CertificateException.class, () -> NodeIdCertificate.verify(cert, ca.getCertificate(), now));
  }

  @Test
  void subjectThatIsNotANodeIdIsRejected() throws Exception {
    X509Certificate cert = ca.sign("alice", nodeKeys.getPublic());

    assertThrows(CertificateException.class, () -> NodeIdCertificate.verify(cert, ca.getCertificate(), Instant.now()));
  }

  @Test
  void nodeThatDoesNotHoldTheCertificateKeyCannotUseTheId() throws Exception {
    X509Certificate cert = ca.signNodeId(id, nodeKeys.getPublic());
    KeyPair keyPair = CertificateAuthorityTool.generateKeyPair();

    CertifiedNodeIdFactory factory = new CertifiedNodeIdFactory(cert, keyPair.getPrivate(), ca.getCertificate(), env);

    assertThrows(SecurityException.class, factory::generateNodeId);
  }

  static int indexOf(byte[] haystack, byte[] needle) {
    for (int i = 0; i + needle.length <= haystack.length; i++) {
      int j = 0;
      while (j < needle.length && haystack[i + j] == needle[j]) j++;
      if (j == needle.length) return i;
    }
    throw new IllegalArgumentException("not found");
  }
}
