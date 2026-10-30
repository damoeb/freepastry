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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class SecurityUtilsTest {

  static final int IV_BYTES = SecurityUtils.SYMMETRIC_IV_LENGTH / 8;
  static final byte[] PLAIN = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);

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
  public void ciphertextStartsWithAFreshRandomIV() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] first = SecurityUtils.encryptSymmetric(PLAIN, key);
    byte[] second = SecurityUtils.encryptSymmetric(PLAIN, key);
    byte[] firstIV = Arrays.copyOf(first, IV_BYTES);
    assertFalse(Arrays.equals(new byte[IV_BYTES], firstIV), "the IV must not be all zeros");
    assertFalse(Arrays.equals(firstIV, Arrays.copyOf(second, IV_BYTES)), "each encryption must use a new IV");
  }

  @Test
  public void equalPlaintextsEncryptToDifferentCiphertexts() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] first = SecurityUtils.encryptSymmetric(PLAIN, key);
    byte[] second = SecurityUtils.encryptSymmetric(PLAIN, key);
    assertFalse(Arrays.equals(Arrays.copyOfRange(first, IV_BYTES, first.length),
        Arrays.copyOfRange(second, IV_BYTES, second.length)), "the cipher must use the IV");
  }

  @Test
  public void equalBlocksEncryptToDifferentCiphertext() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] twoEqualBlocks = new byte[2 * IV_BYTES];
    byte[] cipher = SecurityUtils.encryptSymmetric(twoEqualBlocks, key, new byte[IV_BYTES]);
    assertFalse(Arrays.equals(Arrays.copyOfRange(cipher, 0, IV_BYTES), Arrays.copyOfRange(cipher, IV_BYTES, 2 * IV_BYTES)),
        "equal plaintext blocks must not give equal ciphertext blocks");
  }

  @Test
  public void generatesIVsOfTheCipherBlockSize() {
    byte[] iv = SecurityUtils.generateIVSymmetric();
    assertEquals(IV_BYTES, iv.length);
    assertFalse(Arrays.equals(iv, SecurityUtils.generateIVSymmetric()));
  }

  static BigInteger modulus(KeyPair pair) {
    return ((RSAPublicKey) pair.getPublic()).getModulus();
  }

  @Test
  public void decryptingWithTheWrongPrivateKeyThrowsSecurityException() {
    byte[] secret = SecurityUtils.generateKeySymmetric();
    KeyPair a = SecurityUtils.generateKeyAsymmetric();
    KeyPair b = SecurityUtils.generateKeyAsymmetric();
    // encrypt for the key with the larger modulus, decrypt with the other one
    boolean aIsLarger = modulus(a).compareTo(modulus(b)) > 0;
    KeyPair right = aIsLarger ? a : b;
    KeyPair wrong = aIsLarger ? b : a;
    BigInteger wrongModulus = modulus(wrong);
    // a ciphertext larger than the wrong key's modulus is the case that escaped as an unchecked DataLengthException
    for (int i = 0; i < 1000; i++) {
      byte[] cipher = SecurityUtils.encryptAsymmetric(secret, right.getPublic());
      if (new BigInteger(1, cipher).compareTo(wrongModulus) >= 0) {
        assertThrows(SecurityException.class, () -> SecurityUtils.decryptAsymmetric(cipher, wrong.getPrivate()));
        return;
      }
    }
    fail("found no ciphertext larger than the other modulus");
  }

  @Test
  public void rejectsCiphertextShorterThanTheIV() {
    byte[] key = SecurityUtils.generateKeySymmetric();
    assertThrows(SecurityException.class, () -> SecurityUtils.decryptSymmetric(new byte[IV_BYTES - 1], key));
  }
}
