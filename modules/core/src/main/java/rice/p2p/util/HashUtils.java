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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * The one place that decides how FreePastry hashes: "the content hash" of some bytes, and "hash to Id", the
 * material a 160-bit {@code rice.pastry.Id} is built from.
 *
 * <p>Both use SHA-256.  A content hash is the full 32-byte digest.  Id material is the digest truncated to its
 * first {@link #ID_LENGTH} bytes, because an Id has only 160 bits; truncating SHA-256 keeps its collision
 * resistance at the 80 bits a 160-bit value can offer at best.
 */
public final class HashUtils {

  /**
   * The hash function.
   */
  public static final String ALGORITHM = "SHA-256";

  /**
   * The length of a content hash, in bytes.
   */
  public static final int LENGTH = 32;

  /**
   * The length of Id material, in bytes: that of a {@code rice.pastry.Id} ({@code Id.IdBitLength / 8}).
   */
  public static final int ID_LENGTH = 20;

  private HashUtils() {
  }

  /**
   * @return A new SHA-256 digest, for callers that feed it incrementally
   */
  public static MessageDigest newDigest() {
    try {
      return MessageDigest.getInstance(ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      // every JDK must support SHA-256
      throw new IllegalStateException(ALGORITHM + " is not supported", e);
    }
  }

  /**
   * Returns the content hash of the concatenation of the given byte arrays.
   *
   * @param parts The input
   * @return The 32-byte SHA-256 digest
   */
  public static byte[] hash(byte[]... parts) {
    MessageDigest md = newDigest();
    for (byte[] part : parts) {
      md.update(part);
    }
    return md.digest();
  }

  /**
   * Returns the content hash of a range of a byte array.
   *
   * @param data The input
   * @param offset The start of the range
   * @param length The length of the range
   * @return The 32-byte SHA-256 digest
   */
  public static byte[] hash(byte[] data, int offset, int length) {
    MessageDigest md = newDigest();
    md.update(data, offset, length);
    return md.digest();
  }

  /**
   * Returns the material for an Id derived from the concatenation of the given byte arrays: their SHA-256 digest,
   * truncated to 160 bits.
   *
   * @param parts The input
   * @return The first {@link #ID_LENGTH} bytes of the SHA-256 digest
   */
  public static byte[] hashToIdMaterial(byte[]... parts) {
    return Arrays.copyOf(hash(parts), ID_LENGTH);
  }
}
