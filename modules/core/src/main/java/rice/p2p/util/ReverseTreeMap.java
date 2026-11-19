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

import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import org.jspecify.annotations.Nullable;

/**
 *
 *
 * Class which is an implementation of a reverse tree map, maintaining tree
 * maps for both the keys and values
 *
 * @version $Id: ReverseTreeMap.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class ReverseTreeMap<K,V> {

  /**
   * The internal normal map
   */
  private SortedMap<K,V> normal = new ConcurrentRingMap<K,V>();

  /**
   * The internal value -> list of keys map
   */
  private SortedMap<V,ConcurrentRingMap<K,V>> reverse = new ConcurrentRingMap<V,ConcurrentRingMap<K,V>>();

  /**
   * The internal list of keys with null as a mapping
   */
  private SortedMap<K,V> nulls = new ConcurrentRingMap<K,V>();

  /**
   * Constructor
   *
   */
  public ReverseTreeMap() {
  }

  /**
   * Constructor
   *
   */
  protected ReverseTreeMap(SortedMap<K,V> normal, SortedMap<V,ConcurrentRingMap<K,V>> reverse, SortedMap<K,V> nulls) {
    this.normal = normal;
    this.reverse = reverse;
    this.nulls = nulls;
  }

  /**
   * returns the size
   *
   * @return the size
   */
  public int size() {
    return normal.size();
  }

  /**
   * Returns whether or not we contain the key
   *
   * @param key THe key
   * @return Whether or not we have it
   */
  public boolean containsKey(Object key) {
    return normal.containsKey(key);
  }

  /**
   * Returns the value associated with the given key
   *
   * @param key The key
   * @return The value
   */
  public synchronized @Nullable V get(Object key) {
    return normal.get(key);
  }

  /**
   * Return the set of keys
   *
   * @return The set of keys
   */
  public Set<K> keySet() {
    return normal.keySet();
  }

  /**
   * Removes any and all mappings with the given key
   *
   * @param key The key
   */
  public void remove(Object key) {
    V value = normal.remove(key);

    if (value != null) {
      ConcurrentRingMap<K,V> concurrentRingMap = Objects.requireNonNull(reverse.get(value), "every value has a reverse mapping");
      concurrentRingMap.remove(key);

      if (concurrentRingMap.size() == 0) {
        reverse.remove(value);
      }
    } else {
      nulls.remove(key);
    }
  }


  /**
   * Method which captures the insert and adds it to the
   * reverse map
   *
   * @param key The key
   * @param value The value
   */
  public void put(K key, V value) {
    V old = normal.put(key, value);

    // remove any old reverse mapping
    if (old != null) {
      ConcurrentRingMap<K,V> concurrentRingMap = Objects.requireNonNull(reverse.get(old), "every value has a reverse mapping");
      concurrentRingMap.remove(key);

      if (concurrentRingMap.size() == 0) {
        reverse.remove(old);
      }
    } else {
      nulls.remove(key);
    }

    // and add the new reverse mapping
    if (value != null) {
      ConcurrentRingMap<K,V> reverseMap = reverse.get(value);

      // creating a treeset if necessary
      if (reverseMap == null) {
        reverseMap = new ConcurrentRingMap<K,V>();
        reverse.put(value, reverseMap);
      }

      reverseMap.put(key, value);
    } else {
      nulls.put(key, null);
    }
  }

  /**
   * Method which returns the key map
   *
   * @return The key Map
   */
  public SortedMap<K,V> keyMap() {
    return normal;
  }

  /**
   * Method which returns a *cloned* tail map, or all of the values
   * after a the specified value
   *
   * @param value The start value
   * @return a head map
   */
  public SortedMap<K,V> keyTailMap(K value) {
    return normal.tailMap(value);
  }

  /**
   * Method which returns a *cloned* sub map, or all of the values
   * between the specified values
   *
   * @param start The start value
   * @param end The end value
   * @return a head map
   */
  public SortedMap<K,V> keySubMap(K start, K end) {
    return normal.subMap(start, end);
  }

  /**
   * Method which returns a headset of the values, or all the
   * keys with values up to the specified value
   *
   * @param value The maximal value
   */
  public SortedMap<K,V> valueHeadMap(V value) {
    return convert(reverse.headMap(value));
  }

  /**
   * Method which returns all of the keys which have null values
   *
   */
  public SortedMap<K,V> valueNullMap() {
    return nulls;
  }

  /**
   * Internal method which converts a map from values to key maps into a map from keys to values
   *
   * @param map The map to convert
   * @return The converted map
   */
  protected SortedMap<K,V> convert(SortedMap<V,ConcurrentRingMap<K,V>> map) {
    SortedMap<K,V> result = new ConcurrentRingMap<K,V>();
    for (V v : map.keySet())
      result.putAll(map.get(v));

    return result;
  }
}
