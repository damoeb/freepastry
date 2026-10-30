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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPrivateKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.crypto.AEADBadTagException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.crypto.DataLengthException;

/**
 * This class contains a large number of static methods for performing
 * security-related primitives, such as encrypt, decrypt, etc...
 *
 * @version $Id: SecurityUtils.java 4167 2008-03-28 15:26:37Z jstewart $
 * @author amislove
 */
public class SecurityUtils {

  // ----- STATIC CONFIGURATION FIELDS -----

  /**
   * The name of the asymmetric cipher to use.
   */
  public final static String ASYMMETRIC_ALGORITHM = "RSA/ECB/OAEPPadding";
  public final static String DEPRECATED_ASYMMETRIC_ALGORITHM = "RSA";

  /**
   * The name of the symmetric cipher to use.
   */
  public final static String SYMMETRIC_ALGORITHM = "AES";

  /**
   * The mode and padding of the symmetric cipher.  GCM is authenticated: decryption fails on any change to the
   * initialization vector, the ciphertext or the tag, and on the wrong key.
   */
  public final static String SYMMETRIC_TRANSFORMATION = SYMMETRIC_ALGORITHM + "/GCM/NoPadding";

  /**
   * The name of the asymmetric generator to use.
   */
  public final static String ASYMMETRIC_GENERATOR = "RSA";

  /**
   * The name of the symmetric key generator to use.
   */
  public final static String SYMMETRIC_GENERATOR = "AES";

  /**
   * The name of the signature algorithm to use.
   */
  public final static String SIGNATURE_ALGORITHM = "SHA1withRSA";

  /**
   * The length of the symmetric keys, in bits.  Only keys of exactly this length are accepted.
   */
  public final static int SYMMETRIC_KEY_LENGTH = 256;

  /**
   * The length of the symmetric initialization vectors, in bits (the 96-bit nonce recommended for GCM)
   */
  public final static int SYMMETRIC_IV_LENGTH = 96;

  /**
   * The length of the GCM authentication tag appended to each ciphertext, in bits
   */
  public final static int SYMMETRIC_TAG_LENGTH = 128;

  /**
   * The key derivation function turning a password into a symmetric key
   */
  public final static String PASSWORD_KDF_ALGORITHM = "PBKDF2WithHmacSHA256";

  /**
   * The number of PBKDF2 iterations (the OWASP recommendation for PBKDF2-HMAC-SHA256)
   */
  public final static int PASSWORD_KDF_ITERATIONS = 600_000;

  /**
   * The length of the random salt prepended to password-encrypted data, in bytes
   */
  public final static int PASSWORD_SALT_LENGTH = 16;

  /**
   * The name of the hash function.
   */
  public final static String HASH_ALGORITHM = HashUtils.ALGORITHM;

  /**
   * The name of the hmac function.
   */
  public final static String HMAC_ALGORITHM = "MD5";

  /**
   * The name of the apop function.
   */
  public final static String APOP_ALGORITHM = "MD5";

  /**
   * The length of hmac keys
   */
  public final static int HMAC_KEY_LENGTH = 64;

  /**
   * The ipad of hmac keys, as defined in RFC 2195
   */
  public final static byte HMAC_IPAD_BYTE = 0x36;

  /**
   * The opad of hmac keys, as defined in RFC 2195
   */
  public final static byte HMAC_OPAD_BYTE = 0x5C;

  /**
   * The ipad byte array for use in hmac
   */
  public final static byte[] HMAC_IPAD = new byte[HMAC_KEY_LENGTH];

  /**
   * The opad byte array for use in hmac
   */
  public final static byte[] HMAC_OPAD = new byte[HMAC_KEY_LENGTH];

  /**
   * Initialize the ipad/opad buffers
   */
  static {
    Arrays.fill(HMAC_IPAD, HMAC_IPAD_BYTE);
    Arrays.fill(HMAC_OPAD, HMAC_OPAD_BYTE);
  }

  // ----- STATIC CIPHER OBJECTS -----

  /**
   * The message digest used for doing apop
   */
  private static final MessageDigest apop;

  /**
   * The message digest used for doing hmacing
   */
  private static final MessageDigest hmac1;

  /**
   * The message digest used for doing hmacing
   */
  private static final MessageDigest hmac2;

  /**
   * The cipher used to encrypt/decrypt data using RSA
   */
  private static final Cipher cipherAsymmetric;
  private static final Cipher deprecatedCipherAsymmetric;

  /**
   * The generator used to generate AES keys
   */
  private static final KeyGenerator generatorSymmetric;

  /**
   * The generator used to generate RSA keys
   */
  private static final KeyPairGenerator generatorAsymmetric;

  /**
   * The generator used to decode RSA keys
   */
  private static final KeyFactory factoryAsymmetric;

  /**
   * The signature used for verification and signing data.
   */
  private static final Signature signature;

  /**
   * The RNG for generating IVs, salts and keys
   */
  private static final SecureRandom random = new SecureRandom();

  // ----- STATIC BLOCK TO INITIALIZE THE KEY GENERATORS -----

  static {
    // Add a provider for RSA encryption
    Security.insertProviderAt(new org.bouncycastle.jce.provider.BouncyCastleProvider(), 2);

    try {
      cipherAsymmetric = Cipher.getInstance(ASYMMETRIC_ALGORITHM, "BC");
      deprecatedCipherAsymmetric = Cipher.getInstance(DEPRECATED_ASYMMETRIC_ALGORITHM);
      generatorSymmetric = KeyGenerator.getInstance(SYMMETRIC_GENERATOR);
      generatorAsymmetric = KeyPairGenerator.getInstance(ASYMMETRIC_GENERATOR);
      factoryAsymmetric = KeyFactory.getInstance(ASYMMETRIC_GENERATOR);
      signature = Signature.getInstance(SIGNATURE_ALGORITHM);
      apop = MessageDigest.getInstance(APOP_ALGORITHM);
      hmac1 = MessageDigest.getInstance(HMAC_ALGORITHM);
      hmac2 = MessageDigest.getInstance(HMAC_ALGORITHM);

      generatorSymmetric.init(SYMMETRIC_KEY_LENGTH, random);
    } catch (NoSuchAlgorithmException e) {
      throw new SecurityException("NoSuchAlgorithmException on construction: " + e);
    } catch (NoSuchPaddingException e) {
      throw new SecurityException("NoSuchPaddingException on construction: " + e);
    } catch (NoSuchProviderException e) {
      throw new SecurityException("NoSuchProviderException on construction: " + e);
    }
  }

  /**
   * Make the constructor private so no SecurityUtils is ever created.
   */
  private SecurityUtils() {
  }

  /**
   * Utility method for serializing an object to a byte[].
   *
   * @param o The object to serialize
   * @return The byte[] of the object
   * @exception IOException If serialization does not happen properly
   */
  public static byte[] serialize(Object o) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(baos)));

    oos.writeObject(o);
    oos.flush();
    oos.close();

    return baos.toByteArray();
  }

  /**
   * Utility method for deserializing an object from a byte[]
   *
   * @param data The data to deserialize
   * @return The object
   * @exception IOException If deserialization does not happen properly
   * @exception ClassNotFoundException If the deserialized class is not found
   */
  public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(bais)));

    return ois.readObject();
  }

  /**
   * Utility method for determining the content hash of a byte[], see {@link HashUtils#hash(byte[]...)}.
   *
   * @param input The input
   * @return The 32-byte SHA-256 hash value
   */
  public static byte[] hash(byte[] input) {
    return HashUtils.hash(input);
  }

  /**
   * Utility method for determining the apop of a challenge and password using a secure hashing
   * algorithm.
   *
   * @param password The password
   * @param challenge The challengr
   * @return The hash value
   * @exception SecurityException If the hashing does not happen properly
   */
  public static byte[] apop(byte[] challenge, byte[] password) throws SecurityException {
    synchronized (apop) {
      apop.update(challenge);
      apop.update(password);
      return apop.digest();
    }
  }

  /**
   * Utility method for determining the hmac of a byte[] and key using a secure hashing
   * algorithm.
   *
   * @param text The text
   * @param key The key
   * @return The hmac value
   * @exception SecurityException If the hmacing does not happen properly
   */
  public static byte[] hmac(byte[] key, byte[] text) throws SecurityException {
    synchronized (hmac1) {
      byte[] realKey = new byte[HMAC_KEY_LENGTH];
      System.arraycopy(key, 0, realKey, 0, (key.length < realKey.length ? key.length : realKey.length));

      hmac1.update(MathUtils.xor(realKey, HMAC_IPAD));
      hmac1.update(text);
      hmac2.update(MathUtils.xor(realKey, HMAC_OPAD));
      hmac2.update(hmac1.digest());
      return hmac2.digest();
    }
  }

  /**
   * Utility method for encrypting a block of data with AES-256-GCM, under a fresh random initialization vector.
   * {@link #decryptSymmetric(byte[], byte[])} reverses it.
   *
   * @param data The data
   * @param key The 256-bit key
   * @return The 12-byte initialization vector, followed by the ciphertext and the 16-byte tag
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptSymmetric(byte[] data, byte[] key) throws SecurityException {
    return encryptSymmetric(data, key, 0, data.length);
  }

  /**
   * Utility method for encrypting a range of data with AES-256-GCM, under a fresh random initialization vector.
   * {@link #decryptSymmetric(byte[], byte[])} reverses it.
   *
   * @param data The data
   * @param key The 256-bit key
   * @param offset The offset into the data
   * @param length The length of data to write
   * @return The 12-byte initialization vector, followed by the ciphertext and the 16-byte tag
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptSymmetric(byte[] data, byte[] key, int offset, int length) throws SecurityException {
    return encryptSymmetric(data, key, offset, length, null);
  }

  /**
   * Utility method for encrypting a range of data with AES-256-GCM, under a fresh random initialization vector,
   * authenticating additional data along with it.  {@link #decryptSymmetric(byte[], byte[], byte[])} with the
   * same additional data reverses it.
   *
   * @param data The data
   * @param key The 256-bit key
   * @param offset The offset into the data
   * @param length The length of data to write
   * @param associatedData Data that is authenticated but neither encrypted nor included in the output, or null
   * @return The 12-byte initialization vector, followed by the ciphertext and the 16-byte tag
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptSymmetric(byte[] data, byte[] key, int offset, int length, byte[] associatedData) throws SecurityException {
    byte[] iv = generateIVSymmetric();
    try {
      Cipher cipher = Cipher.getInstance(SYMMETRIC_TRANSFORMATION);
      cipher.init(Cipher.ENCRYPT_MODE, symmetricKey(key), new GCMParameterSpec(SYMMETRIC_TAG_LENGTH, iv));
      if (associatedData != null) {
        cipher.updateAAD(associatedData);
      }

      byte[] result = new byte[iv.length + cipher.getOutputSize(length)];
      System.arraycopy(iv, 0, result, 0, iv.length);
      cipher.doFinal(data, offset, length, result, iv.length);
      return result;
    } catch (GeneralSecurityException e) {
      throw new SecurityException("Could not encrypt " + length + " bytes: " + e, e);
    }
  }

  /**
   * Utility method for decrypting and authenticating some data encrypted with AES-256-GCM.
   *
   * @param data The initialization vector, followed by the ciphertext and the tag, as written by
   *          {@link #encryptSymmetric(byte[], byte[])}
   * @param key The 256-bit key
   * @return The decrypted data
   * @exception SecurityException If the data was not encrypted under this key, or was changed since
   */
  public static byte[] decryptSymmetric(byte[] data, byte[] key) throws SecurityException {
    return decryptSymmetric(data, key, null);
  }

  /**
   * Utility method for decrypting and authenticating some data encrypted with AES-256-GCM.
   *
   * @param data The initialization vector, followed by the ciphertext and the tag, as written by
   *          {@link #encryptSymmetric(byte[], byte[], int, int, byte[])}
   * @param key The 256-bit key
   * @param associatedData The additional data the encryption authenticated, or null
   * @return The decrypted data
   * @exception SecurityException If the data was not encrypted under this key and additional data, or was
   *          changed since
   */
  public static byte[] decryptSymmetric(byte[] data, byte[] key, byte[] associatedData) throws SecurityException {
    int ivLength = SYMMETRIC_IV_LENGTH / 8;
    if (data.length < ivLength + SYMMETRIC_TAG_LENGTH / 8) {
      throw new SecurityException("Ciphertext of " + data.length + " bytes is shorter than the initialization vector and tag");
    }
    try {
      Cipher cipher = Cipher.getInstance(SYMMETRIC_TRANSFORMATION);
      cipher.init(Cipher.DECRYPT_MODE, symmetricKey(key), new GCMParameterSpec(SYMMETRIC_TAG_LENGTH, data, 0, ivLength));
      if (associatedData != null) {
        cipher.updateAAD(associatedData);
      }
      return cipher.doFinal(data, ivLength, data.length - ivLength);
    } catch (AEADBadTagException e) {
      throw new SecurityException("Ciphertext failed authentication (wrong key, or tampered with): " + e, e);
    } catch (GeneralSecurityException e) {
      throw new SecurityException("Could not decrypt " + data.length + " bytes: " + e, e);
    }
  }

  /**
   * Encrypts data under a password: a fresh random salt, followed by the output of
   * {@link #encryptSymmetric(byte[], byte[])} under the key PBKDF2-HMAC-SHA256 derives from the password and salt.
   *
   * @param data The data
   * @param password The password
   * @return The 16-byte salt, followed by the IV, the ciphertext and the tag
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptWithPassword(byte[] data, char[] password) throws SecurityException {
    byte[] salt = new byte[PASSWORD_SALT_LENGTH];
    random.nextBytes(salt);

    byte[] cipher = encryptSymmetric(data, deriveKeyFromPassword(password, salt));
    byte[] result = new byte[salt.length + cipher.length];
    System.arraycopy(salt, 0, result, 0, salt.length);
    System.arraycopy(cipher, 0, result, salt.length, cipher.length);
    return result;
  }

  /**
   * Decrypts data written by {@link #encryptWithPassword(byte[], char[])}.
   *
   * @param data The salt, followed by the IV, the ciphertext and the tag
   * @param password The password
   * @return The decrypted data
   * @exception SecurityException If the password is wrong, or the data was changed
   */
  public static byte[] decryptWithPassword(byte[] data, char[] password) throws SecurityException {
    if (data.length < PASSWORD_SALT_LENGTH) {
      throw new SecurityException("Ciphertext of " + data.length + " bytes is shorter than the salt");
    }
    byte[] salt = Arrays.copyOf(data, PASSWORD_SALT_LENGTH);
    return decryptSymmetric(Arrays.copyOfRange(data, PASSWORD_SALT_LENGTH, data.length), deriveKeyFromPassword(password, salt));
  }

  /**
   * Derives a symmetric key from a password with PBKDF2-HMAC-SHA256.
   *
   * @param password The password
   * @param salt The salt
   * @return A key of {@link #SYMMETRIC_KEY_LENGTH} bits
   */
  static byte[] deriveKeyFromPassword(char[] password, byte[] salt) throws SecurityException {
    PBEKeySpec spec = new PBEKeySpec(password, salt, PASSWORD_KDF_ITERATIONS, SYMMETRIC_KEY_LENGTH);
    try {
      return SecretKeyFactory.getInstance(PASSWORD_KDF_ALGORITHM).generateSecret(spec).getEncoded();
    } catch (GeneralSecurityException e) {
      throw new SecurityException("Could not derive a key from the password: " + e, e);
    } finally {
      spec.clearPassword();
    }
  }

  private static SecretKeySpec symmetricKey(byte[] key) throws SecurityException {
    if (key.length != SYMMETRIC_KEY_LENGTH / 8) {
      throw new SecurityException("Symmetric key of " + key.length + " bytes, expected " + SYMMETRIC_KEY_LENGTH / 8);
    }
    return new SecretKeySpec(key, SYMMETRIC_ALGORITHM);
  }

  /**
   * Utility method for signing a block of data with the a private key
   *
   * @param data The data
   * @param key The key to use to sign
   * @return The signature
   * @exception SecurityException If the signing does not happen properly
   */
  public static byte[] sign(byte[] data, PrivateKey key) throws SecurityException {
    try {
      synchronized (signature) {
        signature.initSign(key);
        signature.update(hash(data));

        return signature.sign();
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException signing object: " + e);
    } catch (SignatureException e) {
      throw new SecurityException("SignatureException signing object: " + e);
    }
  }

  /**
   * Utility method for verifying a signature
   *
   * @param data The data to verify
   * @param sig The proposed signature
   * @param key The key to verify against
   * @return Whether or not the sig matches.
   * @exception SecurityException If the verification does not happen properly
   */
  public static boolean verify(byte[] data, byte[] sig, PublicKey key) throws SecurityException {
    try {
      synchronized (signature) {
        signature.initVerify(key);
        signature.update(hash(data));

        return signature.verify(sig);
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException verifying object: " + e);
    } catch (SignatureException e) {
      throw new SecurityException("SignatureException verifying object: " + e);
    }
  }

  /**
   * Encrypts the given byte[] using the provided public key. TO DO: Check
   * length of input
   *
   * @param data The data to encrypt
   * @param key The key to encrypt with
   * @return The encrypted data
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptAsymmetric(byte[] data, PublicKey key) throws SecurityException {
    try {
      synchronized (cipherAsymmetric) {
        cipherAsymmetric.init(Cipher.ENCRYPT_MODE, key);

        return cipherAsymmetric.doFinal(data);
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException encrypting object: " + e);
    } catch (IllegalBlockSizeException e) {
      throw new SecurityException("IllegalBlockSizeException encrypting object: " + e);
    } catch (BadPaddingException e) {
      throw new SecurityException("BadPaddingException encrypting object: " + e);
    }
  }

  /**
   * Decrypts the given byte[] using the provided private key. TO DO: Check
   * length of input
   *
   * @param data The data to decrypt
   * @param key The private key to use
   * @return The decrypted data
   * @exception SecurityException If the decryption does not happen properly
   */
  public static byte[] decryptAsymmetric(byte[] data, PrivateKey key) throws SecurityException {
    try {
      try {
        // First try normal way of decrypting
        synchronized (cipherAsymmetric) {
          cipherAsymmetric.init(Cipher.DECRYPT_MODE, key);

          return cipherAsymmetric.doFinal(data);
        }
      } catch (BadPaddingException e) {
        // Trying deprecated way of decrypting
        synchronized (deprecatedCipherAsymmetric) {
          deprecatedCipherAsymmetric.init(Cipher.DECRYPT_MODE, key);

          return deprecatedCipherAsymmetric.doFinal(data);
        }
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException decrypting object: " + e);
    } catch (IllegalBlockSizeException e) {
      throw new SecurityException("IllegalBlockSizeException decrypting object: " + e);
    } catch (BadPaddingException e) {
      throw new SecurityException("BadPaddingException decrypting object: " + e);
    } catch (DataLengthException e) {
      // the ciphertext is larger than the modulus of this key, so it was encrypted with another one
      throw new SecurityException("DataLengthException decrypting object: " + e);
    }
  }

  /**
   * Utility method which will generate a random AES key for applications to use.
   *
   * @return A new, random key of {@link #SYMMETRIC_KEY_LENGTH} bits
   */
  public static byte[] generateKeySymmetric() {
    synchronized (generatorSymmetric) {
      return generatorSymmetric.generateKey().getEncoded();
    }
  }

  /**
   * Utility method which will encode a public key
   *
   * @param key The key to encode
   * @return An encoded public key
   */
  public static byte[] encodePublicKey(PublicKey key) {
    RSAPublicKey rkey = (RSAPublicKey) key;

    byte[] modulus = rkey.getModulus().toByteArray();
    byte[] exponent = rkey.getPublicExponent().toByteArray();
    byte[] length = MathUtils.intToByteArray(modulus.length);

    byte[] result = new byte[length.length + modulus.length + exponent.length];
    System.arraycopy(length, 0, result, 0, length.length);
    System.arraycopy(modulus, 0, result, length.length, modulus.length);
    System.arraycopy(exponent, 0, result, length.length + modulus.length, exponent.length);

    return result;
  }

  /**
   * Utility method which will decode a previously encoded public key
   *
   * @param data The previously encoded key
   * @return The key
   */
  public static PublicKey decodePublicKey(byte[] data) throws SecurityException {
    byte[] len = new byte[4];
    System.arraycopy(data, 0, len, 0, len.length);
    int length = MathUtils.byteArrayToInt(len);

    byte[] modulus = new byte[length];
    System.arraycopy(data, len.length, modulus, 0, length);
    byte[] exponent = new byte[data.length - length - 4];
    System.arraycopy(data, len.length + length, exponent, 0, exponent.length);

    RSAPublicKeySpec rpks = new RSAPublicKeySpec(new BigInteger(modulus), new BigInteger(exponent));

    try {
      synchronized (factoryAsymmetric) {
        return factoryAsymmetric.generatePublic(rpks);
      }
    } catch (InvalidKeySpecException e) {
      throw new SecurityException("InvalidKeySpecException while decoding key: " + e);
    }
  }

  /**
   * Utility method which will encode a private key
   *
   * @param key The key to encode
   * @return An encoded public key
   */
  public static byte[] encodePrivateKey(PrivateKey key) {
    RSAPrivateKey rkey = (RSAPrivateKey) key;

    byte[] modulus = rkey.getModulus().toByteArray();
    byte[] exponent = rkey.getPrivateExponent().toByteArray();
    byte[] length = MathUtils.intToByteArray(modulus.length);

    byte[] result = new byte[length.length + modulus.length + exponent.length];
    System.arraycopy(length, 0, result, 0, length.length);
    System.arraycopy(modulus, 0, result, length.length, modulus.length);
    System.arraycopy(exponent, 0, result, length.length + modulus.length, exponent.length);

    return result;
  }

  /**
   * Utility method which will decode a previously encoded private key
   *
   * @param data The previously encoded key
   * @return The key
   */
  public static PrivateKey decodePrivateKey(byte[] data) throws SecurityException {
    byte[] len = new byte[4];
    System.arraycopy(data, 0, len, 0, len.length);
    int length = MathUtils.byteArrayToInt(len);

    byte[] modulus = new byte[length];
    System.arraycopy(data, len.length, modulus, 0, length);
    byte[] exponent = new byte[data.length - length - 4];
    System.arraycopy(data, len.length + length, exponent, 0, exponent.length);

    RSAPrivateKeySpec rpks = new RSAPrivateKeySpec(new BigInteger(modulus), new BigInteger(exponent));

    try {
      synchronized (factoryAsymmetric) {
        return factoryAsymmetric.generatePrivate(rpks);
      }
    } catch (InvalidKeySpecException e) {
      throw new SecurityException("InvalidKeySpecException while decoding key: " + e);
    }
  }

  /**
   * Generates a random GCM initialization vector.  Never reuse one under the same key.
   *
   * @return A new, random initialization vector of {@link #SYMMETRIC_IV_LENGTH} bits
   */
  static byte[] generateIVSymmetric() {
    byte[] result = new byte[SYMMETRIC_IV_LENGTH/8];
    random.nextBytes(result);

    return result;
  }

  /**
   * Utility method which will generate an RSA key pair for applications to use.
   *
   * @return A new, random RSA key pair
   */
  public static KeyPair generateKeyAsymmetric() {
    synchronized (generatorAsymmetric) {
      return generatorAsymmetric.generateKeyPair();
    }
  }
}
