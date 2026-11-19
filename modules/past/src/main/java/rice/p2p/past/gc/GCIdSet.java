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
package rice.p2p.past.gc;

import java.util.Iterator;
import java.util.SortedMap;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.util.ConcurrentRingMap;

/**
 * Internal representation of a set of GCIds
 *
 * @version $Id: GCIdSet.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class GCIdSet implements IdSet {

  // internal representation of the ids
  private final IdSet ids;

  // interal list of the timeouts
  private final SortedMap<Id, Object> timeouts;

  /**
   * Constructor
   */
  protected GCIdSet(IdFactory factory) {
    this.ids = factory.buildIdSet();
    this.timeouts = new ConcurrentRingMap<Id, Object>();
  }

  /**
   * Constructor
   */
  protected GCIdSet(IdSet idSet, SortedMap<Id, Object> timeouts) {
    this.ids = idSet;
    this.timeouts = timeouts;
  }

  /**
   * return the number of elements
   */
  @Override
  public int numElements() {
    return ids.numElements();
  }

  /**
   * add a member
   * @param id the id to add
   */
  @Override
  public void addId(Id id) {
    ExpiringId expiringId = (ExpiringId) id;

    ids.addId(expiringId.id());
    timeouts.put(expiringId.id(), new ExpiringPastMetadata(expiringId.expiration()));
  }

  /**
   * remove a member
   * @param id the id to remove
   */
  @Override
  public void removeId(Id id) {
    ExpiringId expiringId = (ExpiringId) id;
    ids.removeId(expiringId.id());
    timeouts.remove(expiringId.id());
  }

  /**
   * test membership
   * @param id the id to test
   * @return true of id is a member, false otherwise
   */
  @Override
  public boolean isMemberId(Id id) {
    ExpiringId expiringId = (ExpiringId) id;
    return ids.isMemberId(expiringId.id());
  }

  /**
   * return a subset of this set, consisting of the member ids in a given range
   * @param range the range (a GCIdRange)
   * @return the subset
   */
  @Override
  public IdSet subSet(IdRange range) {
    return new GCIdSet(ids.subSet(((GCIdRange) range).range()), timeouts);
  }

  /**
   * return an iterator over the elements of this set
   * @return the interator
   */
  @Override
  public Iterator<Id> getIterator() {
    return new Iterator<Id>() {
      Iterator<Id> i = ids.getIterator();

      @Override
      public boolean hasNext() { return i.hasNext(); }
      @Override
      public Id next() { return getGCId((Id) i.next()); }
      @Override
      public void remove() { throw new UnsupportedOperationException("Remove on GCIdSet()!"); }
    };
  }

  protected ExpiringId getGCId(Id id) {
    ExpiringPastMetadata metadata = (ExpiringPastMetadata) timeouts.get(id);

    if (metadata != null)
      return new ExpiringId(id, metadata.expiration());
    else
      return new ExpiringId(id, ExpiringPastImpl.DEFAULT_EXPIRATION);
  }

  /**
   * return this set as an array
   * @return the array
   */
  @Override
  public Id[] asArray() {
    Id[] array = ids.asArray();

    for (int i=0; i<array.length; i++)
      array[i] = getGCId(array[i]);

    return array;
  }

  /**
   * Not supported.
   *
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public byte[] hash() {
    throw new UnsupportedOperationException("hash on GCIdSet()!");
  }

  /**
   * Determines equality
   *
   * @param object To compare to
   * @return Equals
   */
  @Override
  public boolean equals(Object object) {
    if (!(object instanceof GCIdSet other))
      return false;

    if (numElements() != other.numElements())
      return false;

    Iterator<Id> i = ids.getIterator();
    while (i.hasNext())
      if (! other.ids.isMemberId(i.next()))
        return false;

    return true;
  }

  /**
   * Returns the hashCode
   *
   * @return hashCode
   */
  @Override
  public int hashCode() {
    return ids.hashCode();
  }

  /**
   * Prints out the string
   *
   * @return A string
   */
  @Override
  public String toString() {
    return "{GCIdSet of size " + numElements() + "}";
  }

  /**
   * Returns an independent copy of this set, with the same members and expiration times.
   *
   * @return the copy
   */
  @Override
  public GCIdSet copy() {
    return new GCIdSet(ids.copy(), new ConcurrentRingMap<Id, Object>(timeouts));
  }

  /**
    * Returns a new, empty IdSet of this type
   *
   * @return A new IdSet
   */
  @Override
  public IdSet build() {
    return new GCIdSet(ids.build(), new ConcurrentRingMap<Id, Object>());
  }
}
