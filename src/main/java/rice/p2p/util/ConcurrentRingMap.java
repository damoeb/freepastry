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

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A sorted map backed by a {@link ConcurrentSkipListMap}, for the id sets and metadata indexes that one thread reads
 * while another changes them. Like the backing map, it is thread-safe and its iterators never throw
 * ConcurrentModificationException. It adds two things that ConcurrentSkipListMap lacks:
 * <ul>
 * <li>null values, which it stores as a private sentinel;</li>
 * <li>ring ranges: {@code subMap(from, to)} with {@code from > to} is a view of the keys {@code >= from} followed by
 * the keys {@code < to}, which is how an id range that wraps around the ring is scanned. Its firstKey() and
 * lastKey() are the smallest and the largest key in natural order, not in iteration order.</li>
 * </ul>
 * Views are live, except the views of a ring range (its subMap, headMap and tailMap), which are views of a copy.
 * Like a TreeMap view, a view's subMap, headMap and tailMap clamp their bounds into the view's range, and put() of
 * a key out of range throws IllegalArgumentException.
 */
public class ConcurrentRingMap<K, V> extends AbstractMap<K, V> implements SortedMap<K, V>, Serializable {
  private static final long serialVersionUID = 1L;

  /** Stands in for a null value. An enum, so that it stays a singleton through serialization. */
  private enum Null { VALUE }

  /** the whole map, shared by all views of it */
  private final ConcurrentNavigableMap<K, Object> backing;
  /** the lowest key in this view (inclusive), or null if unbounded */
  private final K lo;
  /** the key that ends this view (exclusive), or null if unbounded; below lo for a ring range */
  private final K hi;
  /** the part of the backing map that this view iterates first: all of it, or the keys {@code >= lo} of a ring */
  private final ConcurrentNavigableMap<K, Object> first;
  /** for a ring range, the keys {@code < hi}, iterated after first; otherwise null */
  private final ConcurrentNavigableMap<K, Object> second;

  /** Creates an empty map, sorted by the natural order of its keys. */
  public ConcurrentRingMap() {
    this(new ConcurrentSkipListMap<K, Object>(), null, null);
  }

  /** Creates a map with the mappings of m, sorted by m's comparator if m is a SortedMap, or naturally otherwise. */
  @SuppressWarnings("unchecked")
  public ConcurrentRingMap(Map<? extends K, ? extends V> m) {
    this(new ConcurrentSkipListMap<K, Object>(
        m instanceof SortedMap ? ((SortedMap<K, ?>) m).comparator() : null), null, null);
    putAll(m);
  }

  private ConcurrentRingMap(ConcurrentNavigableMap<K, Object> backing, K lo, K hi) {
    this.backing = backing;
    this.lo = lo;
    this.hi = hi;
    if (lo != null && hi != null && compare(lo, hi) > 0) {
      first = backing.tailMap(lo, true);
      second = backing.headMap(hi, false);
    } else if (lo != null && hi != null) {
      first = backing.subMap(lo, true, hi, false);
      second = null;
    } else if (lo != null) {
      first = backing.tailMap(lo, true);
      second = null;
    } else if (hi != null) {
      first = backing.headMap(hi, false);
      second = null;
    } else {
      first = backing;
      second = null;
    }
  }

  @SuppressWarnings("unchecked")
  private int compare(Object k1, Object k2) {
    Comparator<? super K> c = backing.comparator();
    return c == null ? ((Comparable<Object>) k1).compareTo(k2) : c.compare((K) k1, (K) k2);
  }

  private boolean isRing() {
    return second != null;
  }

  private boolean inRange(Object key) {
    if (isRing()) return compare(key, lo) >= 0 || compare(key, hi) < 0;
    return (lo == null || compare(key, lo) >= 0) && (hi == null || compare(key, hi) < 0);
  }

  /** Returns the key, moved into the range of this (non-ring) view. */
  private K clamp(K key) {
    if (lo != null && compare(key, lo) < 0) return lo;
    if (hi != null && compare(key, hi) > 0) return hi;
    return key;
  }

  private static Object mask(Object value) {
    return value == null ? Null.VALUE : value;
  }

  @SuppressWarnings("unchecked")
  private static <V> V unmask(Object stored) {
    return stored == Null.VALUE ? null : (V) stored;
  }

  @Override
  public int size() {
    return isRing() ? first.size() + second.size() : first.size();
  }

  @Override
  public boolean isEmpty() {
    return first.isEmpty() && (second == null || second.isEmpty());
  }

  @Override
  public boolean containsKey(Object key) {
    return inRange(key) && backing.containsKey(key);
  }

  @Override
  public V get(Object key) {
    return inRange(key) ? unmask(backing.get(key)) : null;
  }

  @Override
  public V put(K key, V value) {
    if (!inRange(key)) throw new IllegalArgumentException("key out of range");
    return unmask(backing.put(key, mask(value)));
  }

  @Override
  public V remove(Object key) {
    return inRange(key) ? unmask(backing.remove(key)) : null;
  }

  @Override
  public void clear() {
    first.clear();
    if (second != null) second.clear();
  }

  @Override
  public Comparator<? super K> comparator() {
    return backing.comparator();
  }

  @Override
  public K firstKey() {
    if (isRing() && !second.isEmpty()) return second.firstKey();
    return first.firstKey();
  }

  @Override
  public K lastKey() {
    if (isRing() && first.isEmpty()) return second.lastKey();
    return first.lastKey();
  }

  @Override
  public SortedMap<K, V> subMap(K fromKey, K toKey) {
    if (isRing()) return new ConcurrentRingMap<K, V>(this).subMap(fromKey, toKey);
    if (compare(fromKey, toKey) > 0) {
      if (lo == null && hi == null) return new ConcurrentRingMap<K, V>(backing, fromKey, toKey);
      return new ConcurrentRingMap<K, V>(this).subMap(fromKey, toKey);
    }
    return new ConcurrentRingMap<K, V>(backing, clamp(fromKey), clamp(toKey));
  }

  @Override
  public SortedMap<K, V> headMap(K toKey) {
    if (isRing()) return new ConcurrentRingMap<K, V>(this).headMap(toKey);
    compare(toKey, toKey); // type-check the key
    return new ConcurrentRingMap<K, V>(backing, lo, clamp(toKey));
  }

  @Override
  public SortedMap<K, V> tailMap(K fromKey) {
    if (isRing()) return new ConcurrentRingMap<K, V>(this).tailMap(fromKey);
    compare(fromKey, fromKey); // type-check the key
    return new ConcurrentRingMap<K, V>(backing, clamp(fromKey), hi);
  }

  private transient Set<Map.Entry<K, V>> entrySet;

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    if (entrySet == null) entrySet = new EntrySet();
    return entrySet;
  }

  private class EntrySet extends AbstractSet<Map.Entry<K, V>> {
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
      return new EntryIterator();
    }

    @Override
    public int size() {
      return ConcurrentRingMap.this.size();
    }

    @Override
    public boolean isEmpty() {
      return ConcurrentRingMap.this.isEmpty();
    }

    @Override
    public void clear() {
      ConcurrentRingMap.this.clear();
    }
  }

  /** Iterates first, then second, unmasking the values. */
  private class EntryIterator implements Iterator<Map.Entry<K, V>> {
    private Iterator<Map.Entry<K, Object>> current = first.entrySet().iterator();
    private boolean onSecond = second == null;
    private Map.Entry<K, Object> last;

    @Override
    public boolean hasNext() {
      if (current.hasNext()) return true;
      if (onSecond) return false;
      onSecond = true;
      current = second.entrySet().iterator();
      return current.hasNext();
    }

    @Override
    public Map.Entry<K, V> next() {
      if (!hasNext()) throw new NoSuchElementException();
      last = current.next();
      return new AbstractMap.SimpleImmutableEntry<K, V>(last.getKey(), unmask(last.getValue()));
    }

    @Override
    public void remove() {
      if (last == null) throw new IllegalStateException();
      backing.remove(last.getKey());
      last = null;
    }
  }
}
