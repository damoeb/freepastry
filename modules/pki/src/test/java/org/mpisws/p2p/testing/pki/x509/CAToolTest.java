package org.mpisws.p2p.testing.pki.x509;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.pki.x509.CATool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * CATool's defaults are accepted by JDK 25's TLS constraints, and its CA store round-trips as PKCS12.
 */
public class CAToolTest {

  @TempDir
  Path dir;

  @Test
  void generatedKeysAndCertificatesUseModernDefaults() throws Exception {
    CATool ca = CATool.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray());
    KeyPair node = CATool.generateKeyPair();

    X509Certificate cert = ca.sign("node", node.getPublic());

    assertEquals(CATool.DEFAULT_KEY_SIZE, ((RSAPublicKey) node.getPublic()).getModulus().bitLength());
    assertEquals(CATool.DEFAULT_KEY_SIZE, ((RSAPublicKey) ca.getCertificate().getPublicKey()).getModulus().bitLength());
    assertEquals("SHA256WITHRSA", cert.getSigAlgName().toUpperCase(Locale.ROOT));
    assertEquals("SHA256WITHRSA", ca.getCertificate().getSigAlgName().toUpperCase(Locale.ROOT));
    cert.verify(ca.getCertificate().getPublicKey());
  }

  @Test
  void caStoreIsReloadedFromPkcs12File() throws Exception {
    String cn = new File(dir.toFile(), "TestCA").getPath();
    CATool first = CATool.getCATool(cn, "pw".toCharArray());

    CATool second = CATool.getCATool(cn, "pw".toCharArray());
    X509Certificate cert = second.sign("node", CATool.generateKeyPair().getPublic());

    assertEquals(first.getCertificate(), second.getCertificate());
    cert.verify(first.getCertificate().getPublicKey()); // the reloaded private key is the CA's
    assertTrue(new File(cn + "-store").isFile());
  }

  @Test
  void wrongPasswordIsReported() throws Exception {
    String cn = new File(dir.toFile(), "TestCA").getPath();
    CATool.getCATool(cn, "pw".toCharArray());

    IOException e = assertThrows(IOException.class, () -> CATool.getCATool(cn, "wrong".toCharArray()));
    assertTrue(e.getMessage().contains("PKCS12"), e.getMessage());
  }

  @Test
  void caStoreIsOwnerOnly() throws Exception {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    String cn = new File(dir.toFile(), "TestCA").getPath();

    CATool.getCATool(cn, "pw".toCharArray());

    assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(Path.of(cn + "-store")));
  }

  @Test
  void emptyCaPasswordIsRefused() {
    String cn = new File(dir.toFile(), "TestCA").getPath();

    assertThrows(IllegalArgumentException.class, () -> CATool.getCATool(cn, new char[0]));
    assertFalse(new File(cn + "-store").exists());
  }
}
