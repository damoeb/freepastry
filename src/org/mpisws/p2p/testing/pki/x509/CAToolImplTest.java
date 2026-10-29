package org.mpisws.p2p.testing.pki.x509;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.interfaces.RSAPublicKey;
import java.security.cert.X509Certificate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.pki.x509.CAToolImpl;

/**
 * CAToolImpl's defaults are accepted by JDK 25's TLS constraints, and its CA store round-trips as PKCS12.
 */
public class CAToolImplTest {

  @TempDir
  Path dir;

  @Test
  void generatedKeysAndCertificatesUseModernDefaults() throws Exception {
    CAToolImpl ca = CAToolImpl.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray());
    KeyPair node = CAToolImpl.generateKeyPair();

    X509Certificate cert = ca.sign("node", node.getPublic());

    assertEquals(CAToolImpl.DEFAULT_KEY_SIZE, ((RSAPublicKey) node.getPublic()).getModulus().bitLength());
    assertEquals(CAToolImpl.DEFAULT_KEY_SIZE, ((RSAPublicKey) ca.getCertificate().getPublicKey()).getModulus().bitLength());
    assertEquals("SHA256WITHRSA", cert.getSigAlgName().toUpperCase());
    assertEquals("SHA256WITHRSA", ca.getCertificate().getSigAlgName().toUpperCase());
    cert.verify(ca.getCertificate().getPublicKey());
  }

  @Test
  void caStoreIsReloadedFromPkcs12File() throws Exception {
    String cn = new File(dir.toFile(), "TestCA").getPath();
    CAToolImpl first = CAToolImpl.getCATool(cn, "pw".toCharArray());

    CAToolImpl second = CAToolImpl.getCATool(cn, "pw".toCharArray());
    X509Certificate cert = second.sign("node", CAToolImpl.generateKeyPair().getPublic());

    assertEquals(first.getCertificate(), second.getCertificate());
    cert.verify(first.getCertificate().getPublicKey()); // the reloaded private key is the CA's
    assertTrue(new File(cn + "-store").isFile());
  }

  @Test
  void wrongPasswordIsReported() throws Exception {
    String cn = new File(dir.toFile(), "TestCA").getPath();
    CAToolImpl.getCATool(cn, "pw".toCharArray());

    IOException e = assertThrows(IOException.class, () -> CAToolImpl.getCATool(cn, "wrong".toCharArray()));
    assertTrue(e.getMessage().contains("PKCS12"), e.getMessage());
  }
}
