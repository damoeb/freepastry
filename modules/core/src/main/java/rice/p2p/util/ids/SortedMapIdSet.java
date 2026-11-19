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
package rice.p2p.util.ids;

import java.util.Iterator;
import java.util.SortedMap;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.util.ConcurrentRingMap;

/**
 * An IdSet whose members are the keys of a sorted map, iterated in the map's order. A subset is a live view of the
 * same map. Subclasses name the member type and decide whether the set has a content hash.
 *
 * @param <S> the concrete set type, returned by subSet(), copy() and build()
 */
public abstract class SortedMapIdSet<S extends SortedMapIdSet<S>> implements IdSet {

  /** the members, as keys; the values are unused */
  private final SortedMap<Id, ?> members;

  /** Creates an empty set. */
  protected SortedMapIdSet() {
    this(new ConcurrentRingMap<Id, Object>());
  }

  /**
   * Creates a set whose members are the keys of the given map. The set is a view of the map, not a copy.
   *
   * @param members the map whose keys are the members
   */
  protected SortedMapIdSet(SortedMap<Id, ?> members) {
    this.members = members;
  }

  /**
   * Creates a set of this type whose members are the keys of the given map.
   *
   * @param members the map whose keys are the members
   * @return the new set
   */
  protected abstract S newSet(SortedMap<Id, ?> members);

  /** Called after a member was added or removed, for example to drop a cached hash. */
  protected void membersChanged() {}

  @Override
  public int numElements() {
    return members.size();
  }

  @Override
  public void addId(Id id) {
    members.put(id, null);
    membersChanged();
  }

  @Override
  public void removeId(Id id) {
    members.remove(id);
    membersChanged();
  }

  @Override
  public boolean isMemberId(Id id) {
    return members.containsKey(id);
  }

  @Override
  public Iterator<Id> getIterator() {
    return members.keySet().iterator();
  }

  @Override
  public Id[] asArray() {
    return members.keySet().toArray(new Id[0]);
  }

  /**
   * Returns a view of the members from from (inclusive) to to (exclusive), wrapping around the ring if from is above to.
   *
   * @param from the counterclockwise end of the range (inclusive)
   * @param to the clockwise end of the range (exclusive)
   * @return the subset
   */
  public S subSet(Id from, Id to) {
    return newSet(members.subMap(from, to));
  }

  /**
   * Returns the members in the given range: an empty set for an empty range, this set for the full ring, and a view of
   * this set otherwise.
   *
   * @param range the range
   * @return the subset
   */
  @Override
  @SuppressWarnings("unchecked")
  public S subSet(IdRange range) {
    if (range.isEmpty()) {
      return build();
    }
    Id ccw = range.getCounterClockwiseBound();
    Id cw = range.getClockwiseBound();
    if (ccw.equals(cw)) {
      return (S) this;
    }
    return subSet(ccw, cw);
  }

  /**
   * Returns an independent copy of this set, with the same members in the same order. A copy of a ConcurrentRingMap
   * keeps its ring range; any other map is copied in natural order.
   *
   * @return the copy
   */
  @Override
  public S copy() {
    if (members instanceof ConcurrentRingMap<Id, ?> ring) {
      return newSet(ring.copy());
    }
    return newSet(new ConcurrentRingMap<Id, Object>(members));
  }

  @Override
  public S build() {
    return newSet(new ConcurrentRingMap<Id, Object>());
  }

  /**
   * Returns the members, in order.
   *
   * @return "[ IdSet: " followed by each member and a comma, then " ]"
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder("[ IdSet: ");
    Iterator<Id> it = getIterator();
    while (it.hasNext()) {
      result.append(it.next()).append(',');
    }
    return result.append(" ]").toString();
  }
}
