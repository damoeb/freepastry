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
package rice.p2p.multiring;

import java.io.IOException;

import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * Represents a contiguous range of Ids in a multiring heirarchy.
 *
 * @version $Id: MultiringIdRange.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public record MultiringIdRange(Id ringId, IdRange range) implements IdRange {

  /**
   * Constructor
   */
  public MultiringIdRange {

    if ((ringId instanceof RingId) || (range instanceof MultiringIdRange))
      throw new IllegalArgumentException("Illegal creation of MRIdRange: " + ringId.getClass() + ", " + range.getClass());
  }

  /**
   * test if a given key lies within this range
   *
   * @param key the key
   * @return true if the key lies within this range, false otherwise
   */
  @Override
  public boolean containsId(Id key) {
    if (key instanceof RingId rkey) {
      if (!rkey.getRingId().equals(this.ringId)) {
        return false;
      }

      return range.containsId(rkey.getId());
    } else throw new IllegalArgumentException("Cannot test membership for keys other than RingId");
  }

  /**
  * get counterclockwise edge of range
   *
   * @return the id at the counterclockwise edge of the range (inclusive)
   */
  @Override
  public Id getCounterClockwiseBound() {
    return RingId.build(ringId, range.getCounterClockwiseBound());
  }

  /**
    * get clockwise edge of range
   *
   * @return the id at the clockwise edge of the range (exclusive)
   */
  @Override
  public Id getClockwiseBound() {
    return RingId.build(ringId, range.getClockwiseBound());
  }

  /**
    * get the complement of this range
   *
   * @return This range's complement
   */
  @Override
  public IdRange getComplementRange() {
    return new MultiringIdRange(ringId, range.getComplementRange());
  }

  /**
    * merges the given range with this range
   *
   * @return The merge
   */
  @Override
  public IdRange mergeRange(IdRange merge) {
    return new MultiringIdRange(ringId, range.mergeRange(((MultiringIdRange) merge).range()));
  }

  /**
    * diffs the given range with this range
   *
   * @return The merge
   */
  @Override
  public IdRange diffRange(IdRange diff) {
    return new MultiringIdRange(ringId, range.diffRange(((MultiringIdRange) diff).range()));
  }

  /**
    * intersects the given range with this range
   *
   * @return The merge
   */
  @Override
  public IdRange intersectRange(IdRange intersect) {
    return new MultiringIdRange(ringId, range.intersectRange(((MultiringIdRange) intersect).range()));
  }

  /**
    * returns whether or not this range is empty
   *
   * @return Whether or not this range is empty
   */
  @Override
  public boolean isEmpty() {
    return range.isEmpty();
  }

  /**
   * Determines equality
   *
   * @param o To compare to
   * @return Equals
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MultiringIdRange other))
      return false;
    return (other.range().equals(range) && other.ringId.equals(ringId));
  }

  /**
   * Returns the hashCode
   *
   * @return hashCode
   */
  @Override
  public int hashCode() {
    return (range.hashCode() + ringId.hashCode());
  }

  /**
   * Prints out the string
   *
   * @return A string
   */
  @Override
  public String toString() {
    return "{RingId " + ringId + " " + range.toString() + "}";
  }

  public MultiringIdRange(InputBuffer buf, Endpoint endpoint) throws IOException {
    Id ringId = endpoint.readId(buf, buf.readShort());
    IdRange range = endpoint.readIdRange(buf);
    this(ringId, range);
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeShort(ringId.getType());
    ringId.serialize(buf);
    range.serialize(buf);
  }
}
