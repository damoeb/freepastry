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

import java.io.IOException;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * Represents a contiguous range of Pastry ids. *
 *
 * @version $Id: IdRange.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Peter Druschel
 */

public class IdRange implements rice.p2p.commonapi.IdRange {
  /**
   *
   */

  private boolean empty;

  private Id counterClockwiseId;

  private Id clockwiseId;

  /**
   * Constructor.
   *
   * @param counterClockwiseId the id at the counterclockwise edge of the range (inclusive)
   * @param clockwiseId the id at the clockwise edge of the range (exclusive)
   */
  public IdRange(Id counterClockwiseId, Id clockwiseId) {
    empty = false;
    this.counterClockwiseId = counterClockwiseId;
    this.clockwiseId = clockwiseId;
  }

  /**
   * Constructor, constructs an empty IdRange
   *
   */
  public IdRange() {
    empty = true;
    counterClockwiseId = Id.build();
    clockwiseId = counterClockwiseId;
  }

  /**
   * Constructs an empty/full range
   *
   * @param type - if type is true then constructs an empty range, else
   * constructs a full range
   */
  public IdRange(boolean type) {
    empty = type;
    counterClockwiseId = Id.build();
    clockwiseId = counterClockwiseId;

  }

  /**
   * Copy constructor.
   */
  public IdRange(IdRange range) {
    this.empty = range.empty;
    this.counterClockwiseId = range.counterClockwiseId;
    this.clockwiseId = range.clockwiseId;
  }

  @Override
  public int hashCode() {
    if (empty) return 0;
    if (isFull()) return 1;
    return counterClockwiseId.hashCode()^clockwiseId.hashCode();
  }

  /**
   * equality operator
   *
   * @param obj the other IdRange
   * @return true if the IdRanges are equal
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (!(obj instanceof IdRange range)) return false;

    if (empty && range.empty)
      return true;
    if (isFull() && range.isFull())
      return true;
    if (empty == range.empty && counterClockwiseId.equals(range.counterClockwiseId) && clockwiseId.equals(range.clockwiseId))
      return true;
    else
      return false;
  }

  /**
   * return the size of the range
   *
   * @return the numerical distance of the range
   */
  private Id.Distance size() {
    if (counterClockwiseId.clockwise(clockwiseId))
      return counterClockwiseId.distance(clockwiseId);
    else
      return counterClockwiseId.longDistance(clockwiseId);
  }

  /**
   * test if the range is empty
   *
   * @return true if the range is empty
   */
  @Override
  public boolean isEmpty() {
    return empty;
  }

  /**
   * test if the range is the full circle
   *
   * @return true if the range is full circle
   */
  public boolean isFull() {
    return counterClockwiseId.equals(clockwiseId) && !empty;
  }

  /**
   * test if this range is adjacent to another range
   *
   * @param range another range
   * @return true if the range is adjacent to o
   */
  public boolean isAdjacent(IdRange range) {
    return (counterClockwiseId.equals(range.clockwiseId) || range.counterClockwiseId.equals(clockwiseId)) && !empty && !range.empty
        && !isFull() && !range.isFull();
  }

  /**
   * test if a given key lies within this range
   *
   * @param key the key
   * @return true if the key lies within this range, false otherwise
   */
  public boolean contains(Id key) {
    if (counterClockwiseId.equals(clockwiseId) && !empty)
      return true;
    else
      return key.isBetween(counterClockwiseId, clockwiseId);
  }

  /**
   * get counterclockwise edge of range
   *
   * @return the id at the counterclockwise edge of the range (inclusive)
   */
  @Override
  public Id getCounterClockwiseBound() {
    return counterClockwiseId;
  }

  /**
   * get clockwise edge of range
   *
   * @return the id at the clockwise edge of the range (exclusive)
   */
  @Override
  public Id getClockwiseBound() {
    return clockwiseId;
  }

  /**
   * merge two ranges if this and other don't overlap, are not adjacent, and
   * this is not empty, then the result is this
   *
   * @param range the other range
   * @return the resulting range
   */
  public IdRange merge(IdRange range) {

    if (range.empty || (counterClockwiseId.equals(clockwiseId) && !empty))
      return this;
    if (empty || (range.counterClockwiseId.equals(range.clockwiseId) && !range.empty))
      return range;

    boolean ccwIn = counterClockwiseId.isBetween(range.counterClockwiseId, range.clockwiseId) || counterClockwiseId.equals(range.clockwiseId);
    boolean cwIn = clockwiseId.isBetween(range.counterClockwiseId, range.clockwiseId);
    boolean occwIn = range.counterClockwiseId.isBetween(counterClockwiseId, clockwiseId) || range.counterClockwiseId.equals(clockwiseId);
    boolean ocwIn = range.clockwiseId.isBetween(counterClockwiseId, clockwiseId);

    if (ccwIn && cwIn && occwIn && ocwIn) {
      // ranges cover entire ring
      return new IdRange(counterClockwiseId, counterClockwiseId);
    }

    if (ccwIn) {
      if (cwIn)
        return range;
      else
        return new IdRange(range.counterClockwiseId, clockwiseId);
    }

    if (cwIn) {
      return new IdRange(counterClockwiseId, range.clockwiseId);
    }

    if (occwIn) {
      return this;
    }

    // no intersection
    return this;

  }

  /**
   * get the complement of this range on the ring
   *
   * @return the complement range
   */
  public IdRange complement() {
    if (counterClockwiseId.equals(clockwiseId) && !empty)
      return new IdRange();
    else
      return new IdRange(clockwiseId, counterClockwiseId);
  }

  /**
   * intersect two ranges returns an empty range if the ranges don't intersect
   *
   * two ranges may intersect in two ranges on the circle; this method produces
   * one such range of intersection if one exists the other range of
   * intersection can be computed by invoking o.intersect(this)
   *
   * @param range the other range
   * @return the result range
   */
  public IdRange intersect(IdRange range) {

    if (empty || range.empty)
      return new IdRange();
    if (counterClockwiseId.equals(clockwiseId))
      return range;
    if (range.counterClockwiseId.equals(range.clockwiseId))
      return this;

    boolean ccwIn = counterClockwiseId.isBetween(range.counterClockwiseId, range.clockwiseId);
    boolean cwIn = clockwiseId.isBetween(range.counterClockwiseId, range.clockwiseId) && !clockwiseId.equals(range.counterClockwiseId);
    boolean occwIn = range.counterClockwiseId.isBetween(counterClockwiseId, clockwiseId);
    boolean ocwIn = range.clockwiseId.isBetween(counterClockwiseId, clockwiseId) && !range.clockwiseId.equals(counterClockwiseId);

    if (ccwIn && cwIn && occwIn && ocwIn) {
      // ranges intersect in two ranges, return ccw range
      return new IdRange(counterClockwiseId, range.clockwiseId);
    }

    if (ccwIn) {
      if (cwIn)
        return this;
      else
        return new IdRange(counterClockwiseId, range.clockwiseId);
    }

    if (cwIn) {
      return new IdRange(range.counterClockwiseId, clockwiseId);
    }

    if (occwIn) {
      return range;
    }

    // no intersection
    return new IdRange();

  }

  /**
   * compute the difference between two ranges (exclusive or of keys in the two
   * ranges)
   *
   * two ranges may differ in two ranges on the circle; this method produces one
   * such range of difference if one exists the other range of difference can be
   * computed by invoking o.diff(this)
   *
   * @param range the other range
   * @return the result range
   */
  public IdRange diff(IdRange range) {
    IdRange result = intersect(range.complement());
    if (result.isEmpty())
      result = complement().intersect(range);
    return result;
  }

  /**
   * subtract the other range from this computes the ranges of keys that are in
   * this but not in o
   *
   * subtracting a range may produce two ranges on the circle; this method
   * produces one such ranges under control of the cwPart parameter
   *
   * @param range the other range
   * @param cwPart if true, returns the clockwise part of the range subtraction,
   *          else the counterclockwise part
   * @return the result range
   */
  public IdRange subtract(IdRange range, boolean cwPart) {
    if (!cwPart)
      return intersect(range.complement());
    else
      return range.complement().intersect(this);
  }

  /**
   * get counterclockwise half of the range
   *
   * @return the range corresponding to the ccw half of this range
   */
  public IdRange counterClockwiseHalf() {
    if (empty)
      return new IdRange();
    if (isFull())
      return new IdRange(Id.build(Id.Null), Id.build(Id.Half));

    Id newCW = counterClockwiseId.add(size().shift(1, 0, true));
    return new IdRange(counterClockwiseId, newCW);
  }

  /**
   * get clockwise half of the range
   *
   * @return the range corresponding to the cw half of this range
   */
  public IdRange clockwiseHalf() {
    if (empty)
      return new IdRange();
    if (isFull())
      return new IdRange(Id.build(Id.Half), Id.build(Id.Null));

    Id newCCW = counterClockwiseId.add(size().shift(1, 0, true));
    return new IdRange(newCCW, clockwiseId);
  }

  /**
   * Returns a string representation of the range.
   */

  @Override
  public String toString() {
    if (empty)
      return "IdRange: empty";
    else
      return "IdRange: from:" + counterClockwiseId + " to:" + clockwiseId;
  }

  // Common API Support

  /**
   * test if a given key lies within this range
   *
   * @param key the key
   * @return true if the key lies within this range, false otherwise
   */
  @Override
  public boolean containsId(rice.p2p.commonapi.Id key) {
    return contains((Id) key);
  }

  /**
   * get the complement of this range
   *
   * @return This range's complement
   */
  @Override
  public rice.p2p.commonapi.IdRange getComplementRange() {
    return complement();
  }

  /**
   * merges the given range with this range
   *
   * @return The merge
   */
  @Override
  public rice.p2p.commonapi.IdRange mergeRange(rice.p2p.commonapi.IdRange range) {
    return merge((IdRange) range);
  }

  /**
   * diffs the given range with this range
   *
   * @return The merge
   */
  @Override
  public rice.p2p.commonapi.IdRange diffRange(rice.p2p.commonapi.IdRange range) {
    return diff((IdRange) range);
  }

  /**
   * intersects the given range with this range
   *
   * @return The merge
   */
  @Override
  public rice.p2p.commonapi.IdRange intersectRange(
      rice.p2p.commonapi.IdRange range) {
    return intersect((IdRange) range);
  }

  public IdRange(InputBuffer buf) throws IOException {
    clockwiseId = Id.build(buf);
    counterClockwiseId = Id.build(buf);
    empty = buf.readBoolean();
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    clockwiseId.serialize(buf);
    counterClockwiseId.serialize(buf);
    buf.writeBoolean(empty);
  }
}
