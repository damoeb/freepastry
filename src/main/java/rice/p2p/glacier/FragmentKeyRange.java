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
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * A range of FragmentKeys, backed by a range of the underlying ids.
 *
 * @version $Id: FragmentKeyRange.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author ahae
 */
public class FragmentKeyRange implements IdRange {

  /**
   * The actual IdRange
   */
  private IdRange range;

  /**
   * Constructor
   *
   * @param range the underlying range of ids
   */
  protected FragmentKeyRange(IdRange range) {
    this.range = range;
  }

  /**
   * get counterclockwise edge of range
   *
   * @return the id at the counterclockwise edge of the range (inclusive)
   */
  public Id getCCWId() {
    return new FragmentKey(new VersionKey(range.getCCWId(), 0L), 0);
  }

  /**
   * get clockwise edge of range
   *
   * @return the id at the clockwise edge of the range (exclusive)
   */
  public Id getCWId() {
    return new FragmentKey(new VersionKey(range.getCWId(), 0L), 0);
  }

  /**
   * Not supported.
   *
   * @return never returns
   * @throws RuntimeException always
   */
  public IdRange getComplementRange() {
    throw new RuntimeException("FragmentKeyRange.getComplementRange() is not supported!");
  }

  /**
   * returns whether or not this range is empty
   *
   * @return Whether or not this range is empty
   */
  public boolean isEmpty() {
    return range.isEmpty();
  }

  /**
   * test if a given key lies within this range
   *
   * @param key the key
   * @return true if the key lies within this range, false otherwise
   */
  public boolean containsId(Id key) {
    return range.containsId(((FragmentKey)key).getVersionKey().getId());
  }

  /**
   * Not supported.
   *
   * @param merge the range to merge with
   * @return never returns
   * @throws RuntimeException always
   */
  public IdRange mergeRange(IdRange merge) {
    throw new RuntimeException("FragmentKeyRange.mergeRange() is not supported!");
  }

  /**
   * Not supported.
   *
   * @param diff the range to subtract
   * @return never returns
   * @throws RuntimeException always
   */
  public IdRange diffRange(IdRange diff) {
    throw new RuntimeException("FragmentKeyRange.diffRange() is not supported!");
  }

  /**
   * Not supported.
   *
   * @param intersect the range to intersect with
   * @return never returns
   * @throws RuntimeException always
   */
  public IdRange intersectRange(IdRange intersect) {
    throw new RuntimeException("FragmentKeyRange.intersectRange() is not supported!");
  }

  /**
   * Two FragmentKeyRanges are equal if their underlying ranges are.
   *
   * @param o the object to compare to
   * @return whether o is a FragmentKeyRange over an equal range
   */
  public boolean equals(Object o) {
    return (o instanceof FragmentKeyRange other) && range.equals(other.range);
  }

  /**
   * Returns the hash code of the underlying range.
   *
   * @return the hash code
   */
  public int hashCode() {
    return range.hashCode();
  }

  /**
   * Returns a string representation of this range.
   *
   * @return the underlying range, marked as a FragmentKeyRange
   */
  public String toString() {
    return "FragmentKeyRange[" + range + "]";
  }

  public void FragmentKeyRange(InputBuffer buf, Endpoint endpoint) throws IOException {
    range = endpoint.readIdRange(buf);
  }

  public void serialize(OutputBuffer buf) throws IOException {
    range.serialize(buf);
  }
}
