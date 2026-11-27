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


package rice.pastry;

/**
 * A class for representing and manipulating the distance between two Ids on the circle.
 *
 * @version $Id: Id.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author amislove
 */
public class IdDistance implements rice.p2p.commonapi.Id.Distance {

  int difference[];

  /**
   * Constructor.
   */
  public IdDistance() {
    difference = new int[Id.nlen];
  }

  /**
   * Constructor.
   *
   * @param diff The different, as a byte array
   */
  public IdDistance(byte[] diff) {
    difference = new int[Id.nlen];

    for (int j = 0; (j < Id.IdBitLength / 8) && (j < diff.length); j++) {
      int k = diff[j] & 0xff;
      difference[j / 4] |= k << ((j % 4) * 8);
    }
  }

  /**
   * Constructor.
   *
   * @param diff The difference, as an int array
   */
  public IdDistance(int[] diff) {
    difference = diff;
  }

  /**
   * Blits the distance into a target array.
   *
   * @param target an array of length at least Id.IdBitLength/8 for the distance to be stored in.
   */
  public void blit(byte target[]) {
    blit(target, 0);
  }

  /**
   * Blits the distance into a target array, starting at the given offset.
   *
   * @param offset The offset to start at
   * @param target an array of length at least Id.IdBitLength/8 for the distance to be stored in.
   */
  public void blit(byte target[], int offset) {
    for (int j = 0; j < Id.IdBitLength / 8; j++) {
      int k = difference[j / 4] >> ((j % 4) * 8);
      target[offset+j] = (byte) (k & 0xff);
    }
  }

  /**
   * Copy the distance into a freshly generated array.
   *
   * @return a fresh copy of the distance material
   */
  public byte[] copy() {
    byte target[] = new byte[Id.IdBitLength / 8];
    blit(target);
    return target;
  }

  /**
   * Comparison operator. The comparison that occurs is an absolute magnitude comparison.
   *
   * @param obj the IdDistance to compare with.
   * @return negative if this &lt; obj, 0 if they are equal and positive if this &gt; obj.
   */
  @Override
  public int compareTo(rice.p2p.commonapi.Id.Distance obj) {
    IdDistance oth = (IdDistance) obj;

    for (int i = Id.nlen - 1; i >= 0; i--) {
      if (difference[i] != oth.difference[i]) {
        long ownWord = difference[i] & 0x0ffffffffL;
        long peerWord = oth.difference[i] & 0x0ffffffffL;
        if (ownWord < peerWord) {
          return -1;
        } else {
          return 1;
        }
      }
    }

    return 0;
  }

  /**
   * Equality operator.
   *
   * @param obj another Distance.
   * @return true if they are the same, false otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (compareTo((IdDistance)obj) == 0) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Shift operator. shift(-1,0) multiplies value of this by two, shift(1,0) divides by 2
   *
   * @param count the number of bits to shift, negative shifts left, positive shifts right
   * @param fill value of bit shifted in (0 if fill == 0, 1 otherwise)
   * @return this
   */
  public IdDistance shift(int count, int fill) {
    return shift(count, fill, false);
  }


  /**
   * Shift operator. shift(-1,0) multiplies value of this by two, shift(1,0) divides by 2
   *
   * @param count the number of bits to shift, negative shifts left, positive shifts right
   * @param fill value of bit shifted in (0 if fill == 0, 1 otherwise)
   * @param roundUp if true, round up the results after right shifting
   * @return this
   */
  public IdDistance shift(int count, int fill, boolean roundUp) {
    int carry;
    int bit = 0;
    int lsb = 0;

    if (count > 0) {
      for (int j = 0; j < count; j++) {
        // shift right one bit
        carry = (fill == 0) ? 0 : 0x80000000;
        for (int i = Id.nlen - 1; i >= 0; i--) {
          bit = difference[i] & 1;
          difference[i] = (difference[i] >>> 1) | carry;
          carry = (bit == 0) ? 0 : 0x80000000;
        }

        if (j == 0) {
          lsb = bit;
        }
      }

      if (roundUp && lsb > 0) {
        inc();
      }
    } else {
      for (int j = 0; j < -count; j++) {
        // shift left one bit
        carry = (fill == 0) ? 0 : 1;
        for (int i = 0; i < Id.nlen; i++) {
          bit = difference[i] & 0x80000000;
          difference[i] = (difference[i] << 1) | carry;
          carry = (bit == 0) ? 0 : 1;
        }
      }
    }

    return this;
  }

  /**
   * Hash codes.
   *
   * @return a hash code.
   */
  @Override
  public int hashCode() {
    int h = 0;

    // Hash function is computed by XORing the bits of the Id.
    for (int i = 0; i < Id.nlen; i++) {
      h ^= difference[i];
    }

    return h;
  }

  /**
   * Returns a string representation of the distance The string is a byte string from most to
   * least significant.
   *
   * @return The string representation of this distance
   */
  @Override
  public String toString() {
    String description = "0x";

    String tran[] = {"0", "1", "2", "3", "4", "5", "6", "7",
      "8", "9", "A", "B", "C", "D", "E", "F"};

    for (int j = Id.IdBitLength / 8 - 1; j >= 0; j--) {
      int k = difference[j / 4] >> ((j % 4) * 8);
      description = description + tran[(k >> 4) & 0x0f] + tran[k & 0x0f];
    }

    return "< Id.distance " + description + " >";
  }

  // common API Support

  /**
   * Shift operator. shift(-1,0) multiplies value of this by two, shift(1,0) divides by 2
   *
   * @param count the number of bits to shift, negative shifts left, positive shifts right
   * @param fill value of bit shifted in (0 if fill == 0, 1 otherwise)
   * @return this
   */
  @Override
  public rice.p2p.commonapi.Id.Distance shiftDistance(int count, int fill) {
    return shift(count, fill);
  }

  /**
   * increment this distance
   */
  private void inc() {
    long x;
    long sum;
    int carry = 1;

    // add one
    for (int i = 0; i < Id.nlen; i++) {
      x = difference[i] & 0x0ffffffffL;
      sum = x + carry;
      if (sum >= 0x100000000L) {
        carry = 1;
      } else {
        carry = 0;
      }

      difference[i] = (int) sum;
    }
  }
}
