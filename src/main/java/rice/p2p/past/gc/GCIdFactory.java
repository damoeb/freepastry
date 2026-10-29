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

import java.util.*;

import rice.environment.random.RandomSource;
import rice.p2p.commonapi.*;

/**
 * This class provides the ability to build Ids which can support a multi-ring
 * hierarchy.
 *
 * @version $Id: GCIdFactory.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class GCIdFactory implements IdFactory {
  
  /**
   * The underlying IdFactory
   */
  protected IdFactory factory;
  
  /**
   * Constructor
   *
   * @param factory the underlying factory to use
   */
  public GCIdFactory(IdFactory factory) {
    this.factory = factory;
  }
  
  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public Id buildNormalId(byte[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public Id buildId(byte[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public Id buildId(int[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Not supported.
   *
   * @param string The string to use as source data
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public Id buildId(String string) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Not supported.
   *
   * @param rng A random number generator
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public Id buildRandomId(Random rng) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  public Id buildRandomId(RandomSource rng) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @param string The toString() representation of an Id
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public Id buildIdFromToString(String string) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Not supported.
   *
   * @param chars The character array
   * @param offset The offset to start reading at
   * @param length The length to read
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public Id buildIdFromToString(char[] chars, int offset, int length) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");  
  }
  
  /**
   * Not supported.
   *
   * @param string The toString() representation of an Id
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public IdRange buildIdRangeFromPrefix(String string) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");  
  }
  
  /**
   * Not supported.
   *
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public int getIdToStringLength() {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public Id.Distance buildIdDistance(byte[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build IdDistances!");  }
  
  /**
   * Creates an IdRange given the CW and CCW ids.
   *
   * @param cw The clockwise Id
   * @param ccw The counterclockwise Id
   * @return An IdRange with the appropriate delimiters.
   */
  public IdRange buildIdRange(Id cw, Id ccw) {
    return new GCIdRange(factory.buildIdRange(cw, ccw));
  }
  
  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  public IdSet buildIdSet() {
    return new GCIdSet(factory);
  }
  
  /**
   * Not supported.
   *
   * @param map ignored
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public IdSet buildIdSet(SortedMap<Id, ?> map) {
    throw new UnsupportedOperationException("GCIdFactory.buildIDSet()");
  }
  
  /**
   * Not supported.
   *
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public NodeHandleSet buildNodeHandleSet() {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build NodeHandleSets!");
  }
  
  protected class GCSortedMap<V> implements SortedMap<Id, V> {
    protected SortedMap<Id, V> map;
    
    public GCSortedMap(SortedMap<Id, V> map) {
      this.map = map;
    }
    
    public Comparator<? super Id> comparator() { return null; }
    public Id firstKey() { throw new UnsupportedOperationException("firstKey not supported!"); }
    public SortedMap<Id, V> headMap(Id toKey) { throw new UnsupportedOperationException("headMap not supported!"); }
    public Id lastKey() { throw new UnsupportedOperationException("lastKey not supported!"); }
    public SortedMap<Id, V> subMap(Id fromKey, Id toKey) { throw new UnsupportedOperationException("subMap not supported!"); }
    public SortedMap<Id, V> tailMap(Id fromKey) { throw new UnsupportedOperationException("tailMap not supported!");}
    public void clear() { throw new UnsupportedOperationException("clear not supported!"); }
    public boolean containsKey(Object key) { throw new UnsupportedOperationException("containsKey not supported!"); }
    public boolean containsValue(Object value) { throw new UnsupportedOperationException("containsValue not supported!"); }
    public Set<Map.Entry<Id, V>> entrySet() { return new GCEntrySet<V>(map.entrySet()); }
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public V get(Object key) { throw new UnsupportedOperationException("get not supported!"); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!");  }
    public boolean isEmpty() { throw new UnsupportedOperationException("isEmpty not supported!");  }
    public Set<Id> keySet() { throw new UnsupportedOperationException("keyset not supported!"); }
    public V put(Id key, V value) { throw new UnsupportedOperationException("put not supported!"); }
    public void putAll(Map<? extends Id, ? extends V> t) { throw new UnsupportedOperationException("putAll not supported!"); }
    public V remove(Object key) { throw new UnsupportedOperationException("remove not supported!"); }
    public int size() { return map.size(); }
    public Collection<V> values() { throw new UnsupportedOperationException("values not supported!"); }
  }
  
  protected class GCEntrySet<V> implements Set<Map.Entry<Id, V>> {
    protected Set<Map.Entry<Id, V>> set;
    
    public GCEntrySet(Set<Map.Entry<Id, V>> set) {
      this.set = set;
    }
    
    public boolean add(Map.Entry<Id, V> o) { throw new UnsupportedOperationException("add not supported!"); }
    public boolean addAll(Collection<? extends Map.Entry<Id, V>> c) { throw new UnsupportedOperationException("addAll not supported!"); }
    public void clear() { throw new UnsupportedOperationException("clear not supported!"); }
    public boolean contains(Object o) { throw new UnsupportedOperationException("contains not supported!"); }
    public boolean containsAll(Collection<?> c) { throw new UnsupportedOperationException("containsAll not supported!"); }
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!"); }
    public boolean isEmpty() { throw new UnsupportedOperationException("isEmpty not supported!"); }
    public Iterator<Map.Entry<Id, V>> iterator() { return new Iterator<Map.Entry<Id, V>>() {
      protected Iterator<Map.Entry<Id, V>> i = set.iterator();
      public boolean hasNext() { return i.hasNext(); }
      public Map.Entry<Id, V> next() { return new GCMapEntry<V>(i.next()); }
      public void remove() { i.remove(); }
    };
    }
    public boolean remove(Object o) { throw new UnsupportedOperationException("remove not supported!"); }
    public boolean removeAll(Collection<?> c) { throw new UnsupportedOperationException("removeAll not supported!"); }
    public boolean retainAll(Collection<?> c) { throw new UnsupportedOperationException("retainAll not supported!"); }
    public int size() { throw new UnsupportedOperationException("size not supported!"); }
    public Object[] toArray() { throw new UnsupportedOperationException("toArray not supported!"); }
    public <T> T[] toArray(T[] a) { throw new UnsupportedOperationException("toArray not supported!"); }
  }
  
  protected class GCMapEntry<V> implements Map.Entry<Id, V> {
    protected Map.Entry<Id, V> entry;
    
    public GCMapEntry(Map.Entry<Id, V> entry) {
      this.entry = entry;
    }
    
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public Id getKey() { return ((GCId) entry.getKey()).getId(); }
    public V getValue() { return entry.getValue(); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!"); }
    public V setValue(V value) { throw new UnsupportedOperationException("setValue not supported!"); }
  }
}

