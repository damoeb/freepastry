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
package rice.p2p.glacier;

import java.io.IOException;

import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdType;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.MathUtils;

/**
 * The key of a Glacier fragment: the key of an object version plus the fragment number.
 *
 * @version $Id: FragmentKey.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author ahae
 */
public record FragmentKey(VersionKey versionKey, int fragmentId) implements Id {
  /**
   * Not supported.
   *
   * @param ccw the counterclockwise end
   * @param cw the clockwise end
   * @return never returns
   * @throws RuntimeException always
   */
  @Override
  public boolean isBetween(Id ccw, Id cw) {
    throw new RuntimeException("FragmentKey.isBetween() is not supported!");
  }

  /**
   * Gets the PeerKey attribute of the FragmentKey object
   *
   * @param otherId the fragment number of the peer
   * @return The PeerKey value
   */
  public FragmentKey getPeerKey(int otherId) {
    return new FragmentKey(versionKey, otherId);
  }

  /**
   * Determines equality
   *
   * @param peer the object to compare to
   * @return whether the given object is an equal key
   */
  @Override
  public boolean equals(Object peer) {
    if (!(peer instanceof FragmentKey sk)) {
      return false;
    }

    return (sk.versionKey.equals(this.versionKey) && (sk.fragmentId == this.fragmentId));
  }

  /**
   * Returns the byte[] value of this key
   *
   * @return a byte[] representing this key
   */
  @Override
  public byte[] toByteArray() {
    byte[] result = new byte[getByteArrayLength()];

    toByteArray(result, 0);

    return result;
  }

  /**
   * Stores the byte[] value of this Id in the provided byte array
   *
   * @param result the array to write to
   * @param offset the offset in the array to start writing at
   */
  @Override
  public void toByteArray(byte[] result, int offset) {
    versionKey.toByteArray(result, offset);
    MathUtils.intToByteArray(fragmentId, result, offset+versionKey.getByteArrayLength());
  }

  /**
    * Returns the length of the byte[] representing this Id
   *
   * @return The length of the byte[] representing this Id
   */
  @Override
  public int getByteArrayLength() {
    return versionKey.getByteArrayLength() + 4;
  }

  /**
   * Returns the full string form of this key
   *
   * @return the full string form
   */
  @Override
  public String toStringFull() {
    return versionKey.toStringFull() + "#" + fragmentId;
  }

  /**
   * Returns a short string form of this key
   *
   * @return the short string form
   */
  @Override
  public String toString() {
    return versionKey.toString() + "#" + fragmentId;
  }

  /**
   * Not supported.
   *
   * @param nid the other id
   * @return never returns
   * @throws RuntimeException always
   */
  @Override
  public Distance longDistanceFromId(Id nid) {
    throw new RuntimeException("FragmentKey.longDistanceFromId() is not supported!");
  }

  /**
   * Not supported.
   *
   * @param nid the other id
   * @return never returns
   * @throws RuntimeException always
   */
  @Override
  public Distance distanceFromId(Id nid) {
    throw new RuntimeException("FragmentKey.distanceFromId() is not supported!");
  }

  /**
   * Not supported.
   *
   * @param offset The feature to be added to the ToId attribute
   * @return never returns
   * @throws RuntimeException always
   */
  @Override
  public Id addToId(Distance offset) {
    throw new RuntimeException("FragmentKey.addToId() is not supported!");
  }

  /**
   * Not supported.
   *
   * @param nid the other id
   * @return never returns
   * @throws RuntimeException always
   */
  @Override
  public boolean clockwise(Id nid) {
    throw new RuntimeException("FragmentKey.clockwise() is not supported!");
  }

  /**
   * Compares this key to another key of the same type
   *
   * @param o the key to compare to
   * @return a negative number, zero or a positive number as this key is less than, equal to or greater than o
   */
  @Override
  public int compareTo(Id o) {
    int keyResult = versionKey.compareTo(((FragmentKey) o).versionKey);
    if (keyResult != 0) {
      return keyResult;
    }

    if (this.fragmentId < ((FragmentKey) o).fragmentId) {
      return -1;
    }
    if (this.fragmentId > ((FragmentKey) o).fragmentId) {
      return 1;
    }

    return 0;
  }

  /**
   * Returns a hash code for this key
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return (versionKey.hashCode() + fragmentId);
  }

  public FragmentKey(InputBuffer buf, Endpoint endpoint) throws IOException {
    int fragmentId = buf.readInt();
    VersionKey versionKey = new VersionKey(buf, endpoint);
    this(versionKey, fragmentId);
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeInt(fragmentId);
    versionKey.serialize(buf);
  }

  @Override
  public short getType() {
    return IdType.FRAGMENT_KEY.id();
  }
}
