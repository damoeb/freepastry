/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/
package rice.p2p.util;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SecurityUtilsTest {

  static final int IV_BYTES = SecurityUtils.SYMMETRIC_IV_LENGTH / 8;
  static final int TAG_BYTES = SecurityUtils.SYMMETRIC_TAG_LENGTH / 8;
  static final byte[] PLAIN = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);

  // ----- symmetric: AES-256-GCM -----

  @Test
  public void symmetricRoundTrip() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    assertArrayEquals(PLAIN, SecurityUtils.decryptSymmetric(SecurityUtils.encryptSymmetric(PLAIN, key), key));
    assertArrayEquals(new byte[0], SecurityUtils.decryptSymmetric(SecurityUtils.encryptSymmetric(new byte[0], key), key));
  }

  @Test
  public void symmetricRoundTripOfARange() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] cipher = SecurityUtils.encryptSymmetric(PLAIN, key, 4, 11);
    assertArrayEquals(Arrays.copyOfRange(PLAIN, 4, 15), SecurityUtils.decryptSymmetric(cipher, key));
  }

  @Test
  public void generatesAes256Keys() {
    assertEquals(32, SecurityUtils.generateKeySymmetric().length);
  }

  @Test
  public void ciphertextIsTheIVThenTheCiphertextThenTheTag() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    assertEquals(12, IV_BYTES);
    assertEquals(16, TAG_BYTES);
    assertEquals(IV_BYTES + PLAIN.length + TAG_BYTES, SecurityUtils.encryptSymmetric(PLAIN, key).length);
  }

  @Test
  public void ciphertextStartsWithAFreshRandomIV() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] first = SecurityUtils.encryptSymmetric(PLAIN, key);
    byte[] second = SecurityUtils.encryptSymmetric(PLAIN, key);
    byte[] firstIV = Arrays.copyOf(first, IV_BYTES);
    assertFalse(Arrays.equals(new byte[IV_BYTES], firstIV), "the IV must not be all zeros");
    assertFalse(Arrays.equals(firstIV, Arrays.copyOf(second, IV_BYTES)), "each encryption must use a new IV");
    assertFalse(Arrays.equals(Arrays.copyOfRange(first, IV_BYTES, first.length),
        Arrays.copyOfRange(second, IV_BYTES, second.length)), "the cipher must use the IV");
  }

  @Test
  public void decryptingATamperedCiphertextThrows() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] cipher = SecurityUtils.encryptSymmetric(PLAIN, key);
    // flip one bit each in the IV, the ciphertext body and the tag
    for (int position : new int[] {0, IV_BYTES + 3, cipher.length - 1}) {
      byte[] tampered = cipher.clone();
      tampered[position] ^= 0x01;
      assertThrows(SecurityException.class, () -> SecurityUtils.decryptSymmetric(tampered, key), "bit flipped at " + position);
    }
    byte[] truncated = Arrays.copyOf(cipher, cipher.length - 1);
    assertThrows(SecurityException.class, () -> SecurityUtils.decryptSymmetric(truncated, key));
  }

  @Test
  public void decryptingWithTheWrongKeyThrows() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] wrongKey = key.clone();
    wrongKey[0] ^= 0x01;
    byte[] cipher = SecurityUtils.encryptSymmetric(PLAIN, key);
    assertThrows(SecurityException.class, () -> SecurityUtils.decryptSymmetric(cipher, wrongKey));
  }

  @Test
  public void associatedDataIsAuthenticated() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] aad = {1, 2, 3};
    byte[] cipher = SecurityUtils.encryptSymmetric(PLAIN, key, 0, PLAIN.length, aad);
    assertArrayEquals(PLAIN, SecurityUtils.decryptSymmetric(cipher, key, aad));
    assertThrows(SecurityException.class, () -> SecurityUtils.decryptSymmetric(cipher, key, new byte[] {1, 2, 4}));
    assertThrows(SecurityException.class, () -> SecurityUtils.decryptSymmetric(cipher, key));
  }

  @Test
  public void rejectsKeysThatAreNot256Bits() {
    byte[] desSizedKey = new byte[8];
    byte[] aes128Key = new byte[16];
    assertThrows(SecurityException.class, () -> SecurityUtils.encryptSymmetric(PLAIN, desSizedKey));
    assertThrows(SecurityException.class, () -> SecurityUtils.encryptSymmetric(PLAIN, aes128Key));
  }

  @Test
  public void rejectsCiphertextShorterThanTheIVAndTag() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    assertThrows(SecurityException.class, () -> SecurityUtils.decryptSymmetric(new byte[IV_BYTES + TAG_BYTES - 1], key));
  }

  @Test
  public void generatesIVsOfTheGcmNonceSize() {
    byte[] iv = SecurityUtils.generateIVSymmetric();
    assertEquals(IV_BYTES, iv.length);
    assertFalse(Arrays.equals(iv, SecurityUtils.generateIVSymmetric()));
  }

  // ----- password-based: PBKDF2-HMAC-SHA256 + AES-256-GCM -----

  @Test
  public void passwordEncryptionRoundTripsAndRejectsTheWrongPassword() {
    byte[] cipher = SecurityUtils.encryptWithPassword(PLAIN, "correct horse".toCharArray());
    assertEquals(SecurityUtils.PASSWORD_SALT_LENGTH + IV_BYTES + PLAIN.length + TAG_BYTES, cipher.length);
    assertArrayEquals(PLAIN, SecurityUtils.decryptWithPassword(cipher, "correct horse".toCharArray()));
    assertThrows(SecurityException.class, () -> SecurityUtils.decryptWithPassword(cipher, "battery staple".toCharArray()));
  }

  @Test
  public void passwordKeyDerivationIsSaltedAndDeterministic() {
    char[] password = "pw".toCharArray();
    byte[] salt = new byte[SecurityUtils.PASSWORD_SALT_LENGTH];
    byte[] otherSalt = salt.clone();
    otherSalt[0] = 1;
    byte[] key = SecurityUtils.deriveKeyFromPassword(password, salt);
    assertEquals(32, key.length);
    assertArrayEquals(key, SecurityUtils.deriveKeyFromPassword(password, salt));
    assertFalse(Arrays.equals(key, SecurityUtils.deriveKeyFromPassword(password, otherSalt)));
  }

  // ----- MAC: HMAC-SHA256 -----

  @Test
  public void hmacIsHmacSha256() {
    // RFC 4231, test case 2
    byte[] expected = HexFormat.of().parseHex("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    assertArrayEquals(expected, SecurityUtils.hmac("Jefe".getBytes(StandardCharsets.US_ASCII),
        "what do ya want for nothing?".getBytes(StandardCharsets.US_ASCII)));
  }

  @Test
  public void hmacDependsOnTheWholeKey() {
    // the old MD5 construction silently cut keys to 64 bytes
    byte[] longKey = new byte[100];
    byte[] otherLongKey = longKey.clone();
    otherLongKey[99] = 1;
    assertFalse(Arrays.equals(SecurityUtils.hmac(longKey, PLAIN), SecurityUtils.hmac(otherLongKey, PLAIN)));
  }

  // ----- asymmetric -----

  static BigInteger modulus(KeyPair pair) {
    return ((RSAPublicKey) pair.getPublic()).getModulus();
  }

  /** Returns the value as an unsigned big-endian array of exactly the given length. */
  static byte[] unsigned(BigInteger value, int length) {
    byte[] signed = value.toByteArray();
    byte[] result = new byte[length];
    int copied = Math.min(signed.length, length);
    System.arraycopy(signed, signed.length - copied, result, length - copied, copied);
    return result;
  }

  @Test
  public void decryptingWithTheWrongPrivateKeyThrowsSecurityException() {
    KeyPair a = SecurityUtils.generateKeyAsymmetric();
    KeyPair b = SecurityUtils.generateKeyAsymmetric();
    // a ciphertext for the key with the larger modulus, decrypted with the other one
    boolean aIsLarger = modulus(a).compareTo(modulus(b)) > 0;
    KeyPair right = aIsLarger ? a : b;
    KeyPair wrong = aIsLarger ? b : a;
    BigInteger wrongModulus = modulus(wrong);
    // a ciphertext larger than the wrong key's modulus is the case that escaped as an unchecked DataLengthException;
    // the wrong modulus itself is a valid ciphertext for the right key and never fits below the wrong one
    byte[] cipher = unsigned(wrongModulus, SecurityUtils.encryptAsymmetric(new byte[0], right.getPublic()).length);
    assertTrue(new BigInteger(1, cipher).compareTo(modulus(right)) < 0);
    assertThrows(SecurityException.class, () -> SecurityUtils.decryptAsymmetric(cipher, wrong.getPrivate()));
  }
}
