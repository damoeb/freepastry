package rice.p2p.multiring;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.interfaces.RSAPrivateKey;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.p2p.util.SecurityUtils;

/**
 * The password-encrypted key pair files: the ring key pair that {@link RingCertificate#readKeyPair(String, String)}
 * reads, and the CA key pair ({@code ca.keypair.enc}) that {@code CertifiedNodeIdFactory} and
 * {@code RingCertificate.main} load. Both files hold {@link SecurityUtils#encodeKeyPair(KeyPair)} encrypted with
 * {@link SecurityUtils#encryptWithPassword(byte[], char[])}, preceded by its length and gzipped.
 */
class PasswordEncryptedKeyPairFileTest {

  private static final String PASSWORD = "correct horse battery staple";
  private static final String WRONG_PASSWORD = "Tr0ub4dor&3";

  private static KeyPair pair;

  @TempDir
  Path dir;

  @BeforeAll
  static void generateKeyPair() {
    pair = SecurityUtils.generateKeyAsymmetric();
  }

  /** Writes a key pair file the way the tools do: encrypted, length-prefixed, gzipped. */
  private static void writeKeyPairFile(Path file, KeyPair pair, String password) throws IOException {
    writeGzippedBytes(file, SecurityUtils.encryptWithPassword(SecurityUtils.encodeKeyPair(pair), password.toCharArray()));
  }

  private static void writeGzippedBytes(Path file, byte[] bytes) throws IOException {
    try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(Files.newOutputStream(file))))) {
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  private static byte[] readGzippedBytes(Path file) throws IOException {
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new GZIPInputStream(Files.newInputStream(file))))) {
      byte[] bytes = new byte[in.readInt()];
      in.readFully(bytes);
      return bytes;
    }
  }

  /** Flips one bit of the ciphertext inside the file, keeping the gzip and length framing intact. */
  private static void tamper(Path file) throws IOException {
    byte[] cipher = readGzippedBytes(file);
    cipher[cipher.length / 2] ^= 0x01;
    writeGzippedBytes(file, cipher);
  }

  private static void assertSameKeyPair(KeyPair expected, KeyPair actual) {
    assertArrayEquals(expected.getPublic().getEncoded(), actual.getPublic().getEncoded());
    // the file keeps only the modulus and private exponent, not the CRT parameters, so compare those
    RSAPrivateKey expectedPrivate = (RSAPrivateKey) expected.getPrivate();
    RSAPrivateKey actualPrivate = (RSAPrivateKey) actual.getPrivate();
    assertEquals(expectedPrivate.getModulus(), actualPrivate.getModulus());
    assertEquals(expectedPrivate.getPrivateExponent(), actualPrivate.getPrivateExponent());
  }

  @Nested
  @DisplayName("ring key pair file read by RingCertificate.readKeyPair")
  class RingKeyPair {

    private String ring;
    private Path file;

    @BeforeEach
    void locateFile() {
      // readKeyPair lower-cases the ring name, which here is a path prefix inside the temp directory
      ring = dir.resolve("testring").toString();
      file = Path.of(ring.toLowerCase() + ".ringkeypair.enc");
    }

    @Test
    void readsBackTheKeyPairWrittenWithThePassword() throws IOException {
      writeKeyPairFile(file, pair, PASSWORD);

      KeyPair actual = RingCertificate.readKeyPair(ring, PASSWORD);

      assertSameKeyPair(pair, actual);
    }

    @Test
    void rejectsTheWrongPassword() throws IOException {
      writeKeyPairFile(file, pair, PASSWORD);

      assertThrows(SecurityException.class, () -> RingCertificate.readKeyPair(ring, WRONG_PASSWORD));
    }

    @Test
    void rejectsATamperedFile() throws IOException {
      writeKeyPairFile(file, pair, PASSWORD);
      tamper(file);

      assertThrows(SecurityException.class, () -> RingCertificate.readKeyPair(ring, PASSWORD));
    }
  }

  @Nested
  @DisplayName("CA key pair file ca.keypair.enc")
  class CaKeyPair {

    private Path file;

    @BeforeEach
    void locateFile() {
      file = dir.resolve("ca.keypair.enc");
    }

    /** Loads the file the way CertifiedNodeIdFactory.main does. */
    private KeyPair load(String password) throws IOException {
      return SecurityUtils.decodeKeyPair(SecurityUtils.decryptWithPassword(readGzippedBytes(file), password.toCharArray()));
    }

    @Test
    void readsBackTheKeyPairWrittenWithThePassword() throws IOException {
      writeKeyPairFile(file, pair, PASSWORD);

      KeyPair actual = load(PASSWORD);

      assertSameKeyPair(pair, actual);
    }

    @Test
    void rejectsTheWrongPassword() throws IOException {
      writeKeyPairFile(file, pair, PASSWORD);

      assertThrows(SecurityException.class, () -> load(WRONG_PASSWORD));
    }

    @Test
    void rejectsATamperedFile() throws IOException {
      writeKeyPairFile(file, pair, PASSWORD);
      tamper(file);

      assertThrows(SecurityException.class, () -> load(PASSWORD));
    }
  }
}
