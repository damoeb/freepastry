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

package rice.p2p.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import org.jspecify.annotations.Nullable;

/**
 *
 *
 * Class which is an implementation of a sorted map which wraps and protects an
 * internal sorted map.  Any attempts to modify the underlying map will result in
 * a UnsupportedOperationException.
 *
 * @version $Id: ImmutableSortedMap.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
@SuppressWarnings({"unchecked", "JdkObsolete"}) // JdkObsolete: public API implements SortedMap
public class ImmutableSortedMap<K, V> implements SortedMap<K, V> {

  // the internal map
  private final SortedMap<K, V> map;

  /**
   * Constructor which takes an existing map
   *
   * @param map the existing map
   */
  public ImmutableSortedMap(SortedMap<K, V> map) {
    this.map = map;
  }

  /**
    * Returns the comparator associated with this sorted map, or
   * <code>null</code> if it uses its keys' natural ordering.
   *
   * @return the comparator associated with this sorted map, or
   *          <code>null</code> if it uses its keys' natural ordering.
   */
  @Override
  public Comparator<? super K> comparator() {
    return map.comparator();
  }

  /**
   * Returns a view of the portion of this sorted map whose keys range from
   * <code>fromKey</code>, inclusive, to <code>toKey</code>, exclusive.
   *
   * @param fromKey low endpoint (inclusive) of the subMap.
   * @param toKey high endpoint (exclusive) of the subMap.
   * @return a view of the specified range within this sorted map.
   */
  @Override
  public SortedMap<K, V> subMap(K fromKey, K toKey) {
    return new ImmutableSortedMap<K, V>(map.subMap(fromKey, toKey));
  }

  /**
   * Returns a view of the portion of this sorted map whose keys are
   * strictly less than toKey.
   *
   * @param toKey high endpoint (exclusive) of the subMap.
   * @return a view of the specified initial range of this sorted map.
   */
  @Override
  public SortedMap<K, V> headMap(K toKey) {
    return new ImmutableSortedMap<K, V>(map.headMap(toKey));
  }

  /**
   * Returns a view of the portion of this sorted map whose keys are greater
   * than or equal to <code>fromKey</code>.
   *
   * @param fromKey low endpoint (inclusive) of the tailMap.
   * @return a view of the specified final range of this sorted map.
   */
  @Override
  public SortedMap<K, V> tailMap(K fromKey) {
    return new ImmutableSortedMap<K, V>(map.tailMap(fromKey));
  }

  /**
   * Returns the first (lowest) key currently in this sorted map.
   *
   * @return the first (lowest) key currently in this sorted map.
   * @throws    NoSuchElementException if this map is empty.
   */
  @Override
  public K firstKey() {
    return map.firstKey();
  }

  /**
   * Returns the last (highest) key currently in this sorted map.
   *
   * @return the last (highest) key currently in this sorted map.
   * @throws     NoSuchElementException if this map is empty.
   */
  @Override
  public K lastKey() {
    return map.lastKey();
  }

  /**
   * Returns the number of key-value mappings in this map.  If the
   * map contains more than <code>Integer.MAX_VALUE</code> elements, returns
   * <code>Integer.MAX_VALUE</code>.
   *
   * @return the number of key-value mappings in this map.
   */
  @Override
  public int size() {
    return map.size();
  }

  /**
   * Returns <code>true</code> if this map contains no key-value mappings.
   *
   * @return <code>true</code> if this map contains no key-value mappings.
   */
  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  /**
   * Returns <code>true</code> if this map contains a mapping for the specified
   * key.
   *
   * @param key key whose presence in this map is to be tested.
   * @return <code>true</code> if this map contains a mapping for the specified
   */
  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  /**
   * Returns <code>true</code> if this map maps one or more keys to the
   * specified value.
   *
   * @param value value whose presence in this map is to be tested.
   * @return <code>true</code> if this map maps one or more keys to the
   *         specified value.
  */
  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  /**
   * Returns the value to which this map maps the specified key.  Returns
   * <code>null</code> if the map contains no mapping for this key.
   *
   * @param key key whose associated value is to be returned.
   * @return the value to which this map maps the specified key, or
   *         <code>null</code> if the map contains no mapping for this key.
   */
  @Override
  public @Nullable V get(Object key) {
    return map.get(key);
  }

  // Modification Operations

  /**
   * Associates the specified value with the specified key in this map
   * (optional operation).  If the map previously contained a mapping for
   * this key, the old value is replaced by the specified value.
   *
   * @param key key with which the specified value is to be associated.
   * @param value value to be associated with the specified key.
   * @return previous value associated with specified key, or <code>null</code>
   *         if there was no mapping for key.  A <code>null</code> return can
   *         also indicate that the map previously associated <code>null</code>
   *         with the specified key, if the implementation supports
   *         <code>null</code> values.
   *
   * @throws UnsupportedOperationException if the <code>put</code> operation is
   *            not supported by this map.
   */
  @Override
  public V put(K key, V value) {
    throw new UnsupportedOperationException("put not supported by immutablemap");
  }

  /**
   * Removes the mapping for this key from this map if it is present
   * (optional operation).   More formally, if this map contains a mapping
   * from key <code>k</code> to value <code>v</code> such that
   * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping
   * is removed.
   *
   * @param key key whose mapping is to be removed from the map.
   * @return previous value associated with specified key, or <code>null</code>
   *         if there was no mapping for key.
   */
  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException("remove not supported by immutablemap");
  }

  /**
   * Copies all of the mappings from the specified map to this map
   * (optional operation).
   *
   * @param t Mappings to be stored in this map.
   */
  @Override
  public void putAll(Map<? extends K, ? extends V> t) {
    throw new UnsupportedOperationException("putall not supported by immutablemap");
  }

  /**
   * Removes all mappings from this map (optional operation).
   *
   * @throws UnsupportedOperationException clear is not supported by this
   *       map.
   */
  @Override
  public void clear() {
    throw new UnsupportedOperationException("clear not supported by immutablemap");
  }

  /**
   * Returns an unmodifiable set view of the keys contained in this map.  The set is
   * backed by the map, so changes to the map are reflected in the set.  Every
   * attempt to modify the set throws an <code>UnsupportedOperationException</code>.
   *
   * @return a set view of the keys contained in this map.
   */
  @Override
  public Set<K> keySet() {
    return Collections.unmodifiableSet(map.keySet());
  }

  /**
   * Returns an unmodifiable collection view of the values contained in this map.  The
   * collection is backed by the map, so changes to the map are reflected in it.  Every
   * attempt to modify the collection throws an <code>UnsupportedOperationException</code>.
   *
   * @return a collection view of the values contained in this map.
   */
  @Override
  public Collection<V> values() {
    return Collections.unmodifiableCollection(map.values());
  }

  /**
   * Returns an unmodifiable set view of the mappings contained in this map.  The set is
   * backed by the map, so changes to the map are reflected in it.  Every attempt to modify
   * the set or one of its entries throws an <code>UnsupportedOperationException</code>.
   *
   * @return a set view of the mappings contained in this map.
   */
  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return Collections.unmodifiableMap(map).entrySet();
  }
}
