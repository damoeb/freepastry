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
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * CertificateAuthorityTool's defaults are accepted by JDK 25's TLS constraints, and its CA store round-trips as PKCS12.
 */
public class CAToolTest {

  @TempDir
  Path dir;

  @Test
  void generatedKeysAndCertificatesUseModernDefaults() throws Exception {
    CertificateAuthorityTool certificateAuthorityTool = CertificateAuthorityTool.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray());
    KeyPair node = CertificateAuthorityTool.generateKeyPair();

    X509Certificate cert = certificateAuthorityTool.sign("node", node.getPublic());

    assertEquals(CertificateAuthorityTool.DEFAULT_KEY_SIZE, ((RSAPublicKey) node.getPublic()).getModulus().bitLength());
    assertEquals(CertificateAuthorityTool.DEFAULT_KEY_SIZE, ((RSAPublicKey) certificateAuthorityTool.getCertificate().getPublicKey()).getModulus().bitLength());
    assertEquals("SHA256WITHRSA", cert.getSigAlgName().toUpperCase(Locale.ROOT));
    assertEquals("SHA256WITHRSA", certificateAuthorityTool.getCertificate().getSigAlgName().toUpperCase(Locale.ROOT));
    cert.verify(certificateAuthorityTool.getCertificate().getPublicKey());
  }

  @Test
  void caStoreIsReloadedFromPkcs12File() throws Exception {
    String cn = new File(dir.toFile(), "TestCA").getPath();
    CertificateAuthorityTool first = CertificateAuthorityTool.getCATool(cn, "pw".toCharArray());

    CertificateAuthorityTool second = CertificateAuthorityTool.getCATool(cn, "pw".toCharArray());
    X509Certificate cert = second.sign("node", CertificateAuthorityTool.generateKeyPair().getPublic());

    assertEquals(first.getCertificate(), second.getCertificate());
    cert.verify(first.getCertificate().getPublicKey()); // the reloaded private key is the CA's
    assertTrue(new File(cn + "-store").isFile());
  }

  @Test
  void wrongPasswordIsReported() throws Exception {
    String cn = new File(dir.toFile(), "TestCA").getPath();
    CertificateAuthorityTool.getCATool(cn, "pw".toCharArray());

    IOException e = assertThrows(IOException.class, () -> CertificateAuthorityTool.getCATool(cn, "wrong".toCharArray()));
    assertTrue(e.getMessage().contains("PKCS12"), e.getMessage());
  }

  @Test
  void caStoreIsOwnerOnly() throws Exception {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    String cn = new File(dir.toFile(), "TestCA").getPath();

    CertificateAuthorityTool.getCATool(cn, "pw".toCharArray());

    assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(Path.of(cn + "-store")));
  }

  @Test
  void emptyCaPasswordIsRefused() {
    String cn = new File(dir.toFile(), "TestCA").getPath();

    assertThrows(IllegalArgumentException.class, () -> CertificateAuthorityTool.getCATool(cn, new char[0]));
    assertFalse(new File(cn + "-store").exists());
  }

  @Test
  void existingStoreFileIsTightenedToOwnerOnly() throws Exception {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    Path file = dir.resolve("loose");
    Files.createFile(file, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--")));

    CertificateAuthorityTool.newPrivateOutputStream(file).close();

    assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(file));
  }

  @Test
  void passwordComesFromTheEnvironment() {
    char[] chars = CertificateAuthorityTool.readPassword("PW", "prompt", java.util.Map.of("PW", "secret"), null);

    assertEquals("secret", new String(chars));
  }

  @Test
  void missingOrEmptyPasswordIsRefused() {
    assertThrows(IllegalStateException.class, () -> CertificateAuthorityTool.readPassword("PW", "prompt", java.util.Map.of(), null));
    assertThrows(IllegalStateException.class, () -> CertificateAuthorityTool.readPassword("PW", "prompt", java.util.Map.of("PW", ""), null));
  }

  @Test
  void passwordOptionOnTheCommandLineIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> CertificateAuthorityTool.main(new String[] {"-p", "secret"}));
  }

  @Test
  void nodeCertificateIsALeafForTlsOnly() throws Exception {
    CertificateAuthorityTool certificateAuthorityTool = CertificateAuthorityTool.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray());

    X509Certificate cert = certificateAuthorityTool.sign("node", CertificateAuthorityTool.generateKeyPair().getPublic());

    assertEquals(-1, cert.getBasicConstraints(), "basicConstraints CA:false");
    assertTrue(cert.getCriticalExtensionOIDs().contains("2.5.29.19"), "basicConstraints is critical");
    boolean[] usage = cert.getKeyUsage();
    assertTrue(usage != null && usage[0] && usage[2], "digitalSignature and keyEncipherment");
    for (int bit = 0; bit < usage.length; bit++) {
      if (bit != 0 && bit != 2) assertFalse(usage[bit], "no other key usage, bit " + bit);
    }
    assertEquals(java.util.List.of("1.3.6.1.5.5.7.3.1", "1.3.6.1.5.5.7.3.2"), cert.getExtendedKeyUsage().stream().sorted().toList());
  }

  @Test
  void nodeCertificateSerialsAreRandom128BitPositive() throws Exception {
    CertificateAuthorityTool certificateAuthorityTool = CertificateAuthorityTool.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray());
    java.security.PublicKey key = CertificateAuthorityTool.generateKeyPair().getPublic();

    java.math.BigInteger first = certificateAuthorityTool.sign("node", key).getSerialNumber();
    java.math.BigInteger second = certificateAuthorityTool.sign("node", key).getSerialNumber();

    assertTrue(first.signum() > 0 && second.signum() > 0);
    assertTrue(first.bitLength() > 64 && second.bitLength() > 64, "not a millisecond clock: " + first);
    assertFalse(first.equals(second));
  }

  @Test
  void caCertificateIsAVersion3CaForCertificateSigning() throws Exception {
    X509Certificate ca = CertificateAuthorityTool.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray()).getCertificate();

    assertEquals(3, ca.getVersion());
    assertTrue(ca.getBasicConstraints() >= 0, "basicConstraints CA:true");
    assertTrue(ca.getCriticalExtensionOIDs().contains("2.5.29.19"), "basicConstraints is critical");
    boolean[] usage = ca.getKeyUsage();
    assertTrue(usage != null && usage[5], "keyCertSign");
    assertTrue(ca.getCriticalExtensionOIDs().contains("2.5.29.15"), "keyUsage is critical");
  }

  @Test
  void requesterChosenIdIsRefusedWithoutOperatorFlag() {
    String ca = new File(dir.toFile(), "TestCA").getPath();

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CertificateAuthorityTool.main(new String[] {"-ca", ca, "-cn", "0000000000000000000000000000000000000001"}));
    assertTrue(e.getMessage().contains("-cn"), e.getMessage());
    assertFalse(new File(ca + "-store").exists(), "refused before the CA is touched");
  }

  @Test
  void everyIssuanceIsAppendedToTheIssuanceLog() throws Exception {
    String cn = new File(dir.toFile(), "TestCA").getPath();
    CertificateAuthorityTool certificateAuthorityTool = CertificateAuthorityTool.getCATool(cn, "pw".toCharArray());
    java.security.PublicKey key = CertificateAuthorityTool.generateKeyPair().getPublic();
    rice.pastry.Id id = rice.pastry.Id.build("00000000000000000000000000000000000000ab");

    X509Certificate first = certificateAuthorityTool.signNodeId(id, key);
    X509Certificate second = CertificateAuthorityTool.getCATool(cn, "pw".toCharArray()).signNodeId(id, key);

    java.util.List<String> lines = Files.readAllLines(Path.of(cn + "-issued.log"));
    assertEquals(2, lines.size(), lines.toString());
    assertTrue(lines.get(0).contains(first.getSerialNumber().toString(16)) && lines.get(0).contains(id.toStringFull()), lines.get(0));
    assertTrue(lines.get(1).contains(second.getSerialNumber().toString(16)), lines.get(1));
  }
}
