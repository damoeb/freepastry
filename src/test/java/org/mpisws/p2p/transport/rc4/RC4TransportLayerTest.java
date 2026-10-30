package org.mpisws.p2p.transport.rc4;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import org.junit.jupiter.api.Test;
import rice.p2p.util.SecurityUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Pins the socket encryption of {@link RC4TransportLayer}: AES-256-CTR under HMAC-SHA256(password, seed).
 */
public class RC4TransportLayerTest {

  static final byte[] PASSWORD = "badpassword".getBytes(StandardCharsets.UTF_8);
  static final byte[] SEED = new byte[RC4TransportLayer.KEY_LENGTH];

  static Cipher cipher(int mode, byte[] seed) throws Exception {
    Cipher cipher = Cipher.getInstance(RC4TransportLayer.ALGORITHM, RC4TransportLayer.PROVIDER);
    cipher.init(mode, RC4TransportLayer.sessionKey(PASSWORD, seed), new IvParameterSpec(new byte[RC4TransportLayer.IV_LENGTH]));
    return cipher;
  }

  @Test
  public void sessionKeyIsTheHmacSha256OfTheSeedUnderThePassword() {
    SecretKey key = RC4TransportLayer.sessionKey(PASSWORD, SEED);
    assertEquals("AES", key.getAlgorithm());
    assertArrayEquals(SecurityUtils.hmac(PASSWORD, SEED), key.getEncoded());
    assertEquals(32, key.getEncoded().length);
  }

  @Test
  public void streamsDecryptPieceByPieceAcrossBlockBoundaries() throws Exception {
    byte[] plain = "an odd-length message that spans several AES blocks".getBytes(StandardCharsets.UTF_8);
    Cipher encrypt = cipher(Cipher.ENCRYPT_MODE, SEED);
    Cipher decrypt = cipher(Cipher.DECRYPT_MODE, SEED);

    // like EncryptedSocket: update() only, in pieces that are not block-aligned, and each piece comes out whole
    byte[] first = encrypt.update(plain, 0, 5);
    byte[] rest = encrypt.update(plain, 5, plain.length - 5);
    assertEquals(5, first.length);
    assertEquals(plain.length - 5, rest.length);
    byte[] decrypted = new byte[plain.length];
    System.arraycopy(decrypt.update(first), 0, decrypted, 0, first.length);
    System.arraycopy(decrypt.update(rest), 0, decrypted, first.length, rest.length);
    assertArrayEquals(plain, decrypted);
  }

  @Test
  public void eachSeedGivesADifferentKeystream() throws Exception {
    byte[] otherSeed = SEED.clone();
    otherSeed[0] = 1;
    byte[] zeros = new byte[32];
    assertFalse(Arrays.equals(cipher(Cipher.ENCRYPT_MODE, SEED).update(zeros), cipher(Cipher.ENCRYPT_MODE, otherSeed).update(zeros)));
  }
}
