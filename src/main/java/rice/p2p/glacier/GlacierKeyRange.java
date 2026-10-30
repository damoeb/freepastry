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

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * A range of Glacier keys, backed by a range of the ids the keys are made of. It supports only membership tests and
 * its edges; the range arithmetic is not supported.
 *
 * @author ahae
 */
public abstract class GlacierKeyRange implements IdRange {

  /**
   * The underlying range of ids
   */
  protected final IdRange range;

  /**
   * Constructor
   *
   * @param range the underlying range of ids
   */
  protected GlacierKeyRange(IdRange range) {
    this.range = range;
  }

  /**
   * Returns the smallest key made of the given id.
   *
   * @param id an id of the underlying range
   * @return the key
   */
  protected abstract Id toKey(Id id);

  /**
   * Returns the id the given key is made of.
   *
   * @param key a key of this range's type
   * @return the underlying id
   */
  protected abstract Id toId(Id key);

  /**
   * get counterclockwise edge of range
   *
   * @return the id at the counterclockwise edge of the range (inclusive)
   */
  public Id getCCWId() {
    return toKey(range.getCCWId());
  }

  /**
   * get clockwise edge of range
   *
   * @return the id at the clockwise edge of the range (exclusive)
   */
  public Id getCWId() {
    return toKey(range.getCWId());
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
    return range.containsId(toId(key));
  }

  private RuntimeException unsupported(String method) {
    return new RuntimeException(getClass().getSimpleName() + "." + method + "() is not supported!");
  }

  /**
   * Not supported.
   *
   * @return never returns
   * @throws RuntimeException always
   */
  public IdRange getComplementRange() {
    throw unsupported("getComplementRange");
  }

  /**
   * Not supported.
   *
   * @param merge the range to merge with
   * @return never returns
   * @throws RuntimeException always
   */
  public IdRange mergeRange(IdRange merge) {
    throw unsupported("mergeRange");
  }

  /**
   * Not supported.
   *
   * @param diff the range to subtract
   * @return never returns
   * @throws RuntimeException always
   */
  public IdRange diffRange(IdRange diff) {
    throw unsupported("diffRange");
  }

  /**
   * Not supported.
   *
   * @param intersect the range to intersect with
   * @return never returns
   * @throws RuntimeException always
   */
  public IdRange intersectRange(IdRange intersect) {
    throw unsupported("intersectRange");
  }

  /**
   * Returns the hash code of the underlying range.
   *
   * @return the hash code
   */
  public int hashCode() {
    return range.hashCode();
  }

  public void serialize(OutputBuffer buf) throws IOException {
    range.serialize(buf);
  }
}
