package rice.p2p.multiring;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.interfaces.RSAPrivateKey;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.util.SecurityUtils;
import rice.pastry.commonapi.PastryIdFactory;

/**
 * Round trips of the ring certificate file format and of the signature over it.
 */
class RingCertificateTest {

  private Environment env;
  private Id ringId;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    ringId = new PastryIdFactory(env).buildId("Rice");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private static RingCertificate reread(RingCertificate cert) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    cert.writeToStream(bytes);
    return RingCertificate.readFromStream(new ByteArrayInputStream(bytes.toByteArray()));
  }

  @Test
  void writesAndReadsASignedCertificate() throws IOException {
    KeyPair ca = SecurityUtils.generateKeyAsymmetric();
    KeyPair ring = SecurityUtils.generateKeyAsymmetric();
    InetSocketAddress[] bootstraps = {InetSocketAddress.createUnresolved("10.0.0.1", 9001), InetSocketAddress.createUnresolved("10.0.0.2", 9002)};
    RingCertificate expected = new RingCertificate("rice", ringId, 4, bootstraps, 9000, ring.getPublic(), InetSocketAddress.createUnresolved("10.0.0.3", 8000));
    expected.sign(ca.getPrivate());

    RingCertificate actual = reread(expected);

    assertTrue(actual.verify(ca.getPublic()));
    assertFalse(actual.verify(ring.getPublic()));
    assertEquals("rice", actual.getName());
    assertEquals(ringId, actual.getId());
    assertEquals(4, actual.getProtocol());
    assertEquals(9000, actual.getPort());
    assertEquals(expected.getVersion(), actual.getVersion());
    assertEquals(2, actual.getBootstraps().length);
    assertEquals("10.0.0.2", actual.getBootstraps()[1].getHostString());
    assertEquals(9002, actual.getBootstraps()[1].getPort());
    assertEquals(8000, actual.getLogServer().getPort());
    assertArrayEquals(ring.getPublic().getEncoded(), actual.getKey().getEncoded());
  }

  @Test
  void keepsAMissingLogServer() throws IOException {
    KeyPair ca = SecurityUtils.generateKeyAsymmetric();
    RingCertificate expected = new RingCertificate("rice", ringId, 4, new InetSocketAddress[0], 9000, ca.getPublic(), null);
    expected.sign(ca.getPrivate());

    RingCertificate actual = reread(expected);

    assertNull(actual.getLogServer());
    assertTrue(actual.verify(ca.getPublic()));
  }

  @Test
  void refusesAnotherFormatVersion() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(new GZIPOutputStream(bytes))) {
      out.writeInt(99);
    }

    assertThrows(IOException.class, () -> RingCertificate.readFromStream(new ByteArrayInputStream(bytes.toByteArray())));
  }

  @Test
  void encodesAndDecodesAKeyPair() {
    KeyPair expected = SecurityUtils.generateKeyAsymmetric();

    KeyPair actual = SecurityUtils.decodeKeyPair(SecurityUtils.encodeKeyPair(expected));

    assertArrayEquals(expected.getPublic().getEncoded(), actual.getPublic().getEncoded());
    assertEquals(((RSAPrivateKey) expected.getPrivate()).getModulus(), ((RSAPrivateKey) actual.getPrivate()).getModulus());
    assertEquals(((RSAPrivateKey) expected.getPrivate()).getPrivateExponent(), ((RSAPrivateKey) actual.getPrivate()).getPrivateExponent());
  }
}
