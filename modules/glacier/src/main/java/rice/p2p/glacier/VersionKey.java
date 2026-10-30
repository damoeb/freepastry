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
import rice.p2p.multiring.RingId;
import rice.p2p.util.MathUtils;

/**
 * The key of one version of an object: the object's id plus a version number.
 *
 * @version $Id: VersionKey.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author ahae
 */
public record VersionKey(Id id, long version) implements Id {
  /**
   * Determines equality
   *
   * @param peer the object to compare to
   * @return whether the given object is an equal key
   */
  @Override
  public boolean equals(Object peer) {
    if (!(peer instanceof VersionKey fk)) {
      return false;
    }

    return ((fk.version == this.version) && fk.id.equals(this.id));
  }

  /**
   * Returns a short string form of this key
   *
   * @return the short string form
   */
  @Override
  public String toString() {
    return id.toString() + "v" + version;
  }

  /**
   * Returns the full string form of this key
   *
   * @return the full string form
   */
  @Override
  public String toStringFull() {
    return id.toStringFull() + "v" + version;
  }

  /**
   * Compares this key to another key of the same type
   *
   * @param o the key to compare to
   * @return a negative number, zero or a positive number as this key is less than, equal to or greater than o
   */
  @Override
  public int compareTo(Id o) {
    int idResult = id.compareTo(((VersionKey) o).id);
    if (idResult != 0) {
      return idResult;
    }

    if ((version - ((VersionKey) o).version)<0)
      return -1;

    if ((version - ((VersionKey) o).version)>0)
      return 1;

    return 0;
  }

  /**
   * Returns a hash code for this key
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return (id.hashCode() + Long.hashCode(version));
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
    id.toByteArray(result, offset);
    MathUtils.longToByteArray(version, result, offset + id.getByteArrayLength());
  }

  /**
   * Returns the length of the byte[] representing this Id
   *
   * @return The length of the byte[] representing this Id
   */
  @Override
  public int getByteArrayLength() {
    return id.getByteArrayLength() + 8;
  }

  @Override
  public boolean isBetween(Id ccw, Id cw) {
    throw new RuntimeException("VersionKey.isBetween() is not supported!");
  }

  @Override
  public Distance longDistanceFromId(Id nid) {
    throw new RuntimeException("VersionKey.longDistanceFromId() is not supported!");
  }

  @Override
  public Distance distanceFromId(Id nid) {
    throw new RuntimeException("VersionKey.distanceFromId() is not supported!");
  }

  @Override
  public Id addToId(Distance offset) {
    throw new RuntimeException("VersionKey.addToId() is not supported!");
  }

  @Override
  public boolean clockwise(Id nid) {
    throw new RuntimeException("VersionKey.clockwise() is not supported!");
  }

  public static VersionKey build(String s) {
    @SuppressWarnings("StringSplitter") // the fixed format of toString(), no trailing empty fields
    String[] sArray = s.split("v");
    return new VersionKey(RingId.build(sArray[0]), Long.parseLong(sArray[1]));
  }

  public VersionKey(InputBuffer buf, Endpoint endpoint) throws IOException {
    long version = buf.readLong();
    Id id = endpoint.readId(buf, buf.readShort());
    this(id, version);
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeLong(version);
    buf.writeShort(id.getType());
    id.serialize(buf);
  }

  @Override
  public short getType() {
    return IdType.VERSION_KEY.id();
  }
}
