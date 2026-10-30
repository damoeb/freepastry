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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedMap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConcurrentRingMapTest {

  /** keys 10, 20, ..., 90, the value of 50 is null */
  static ConcurrentRingMap<Integer, String> map() {
    ConcurrentRingMap<Integer, String> map = new ConcurrentRingMap<Integer, String>();
    for (int k = 90; k >= 10; k -= 10) map.put(k, k == 50 ? null : "v" + k);
    return map;
  }

  static List<Integer> keys(SortedMap<Integer, ?> map) {
    return new ArrayList<Integer>(map.keySet());
  }

  @Test
  public void iteratesInKeyOrder() {
    assertEquals(List.of(10, 20, 30, 40, 50, 60, 70, 80, 90), keys(map()));
  }

  @Test
  public void allowsNullValues() {
    ConcurrentRingMap<Integer, String> map = map();
    assertTrue(map.containsKey(50));
    assertNull(map.get(50));
    assertTrue(map.containsValue(null));
    assertNull(map.put(50, "x"));
    assertEquals("x", map.put(50, null));
    assertNull(map.remove(50));
    assertFalse(map.containsKey(50));
    assertEquals(8, map.size());
  }

  @Test
  public void subMapIsALiveView() {
    ConcurrentRingMap<Integer, String> map = map();
    SortedMap<Integer, String> sub = map.subMap(20, 50);
    assertEquals(List.of(20, 30, 40), keys(sub));
    map.put(25, "v25");
    sub.remove(30);
    assertEquals(List.of(20, 25, 40), keys(sub));
    assertFalse(map.containsKey(30));
    assertEquals(20, sub.firstKey());
    assertEquals(40, sub.lastKey());
    assertThrows(IllegalArgumentException.class, () -> sub.put(50, "out"));
    assertNull(sub.get(60));
  }

  @Test
  public void subMapOfAViewClampsItsBounds() {
    SortedMap<Integer, String> sub = map().subMap(20, 60);
    assertEquals(List.of(20, 30, 40, 50), keys(sub.subMap(0, 100)));
    assertEquals(List.of(20, 30), keys(sub.headMap(40)));
    assertEquals(List.of(40, 50), keys(sub.tailMap(40)));
    assertEquals(List.of(20, 30, 40, 50), keys(sub.tailMap(0)));
  }

  @Test
  public void subMapFromAboveToWrapsAroundTheRing() {
    ConcurrentRingMap<Integer, String> map = map();
    SortedMap<Integer, String> ring = map.subMap(70, 30);
    assertEquals(List.of(70, 80, 90, 10, 20), keys(ring));
    assertEquals(5, ring.size());
    // first and last in natural order
    assertEquals(10, ring.firstKey());
    assertEquals(90, ring.lastKey());
    assertTrue(ring.containsKey(10));
    assertFalse(ring.containsKey(50));
    assertThrows(IllegalArgumentException.class, () -> ring.put(50, "out"));
    ring.put(95, "v95");
    ring.put(5, "v5");
    assertTrue(map.containsKey(95));
    assertEquals(List.of(70, 80, 90, 95, 5, 10, 20), keys(ring));
    ring.clear();
    assertEquals(List.of(30, 40, 50, 60), keys(map));
  }

  @Test
  public void ringWithOneSideEmpty() {
    ConcurrentRingMap<Integer, String> map = map();
    assertEquals(List.of(10, 20), keys(map.subMap(95, 30)));
    assertEquals(10, map.subMap(95, 30).firstKey());
    assertEquals(20, map.subMap(95, 30).lastKey());
    assertEquals(List.of(80, 90), keys(map.subMap(80, 5)));
    assertEquals(80, map.subMap(80, 5).firstKey());
    assertEquals(90, map.subMap(80, 5).lastKey());
    assertTrue(map.subMap(95, 5).isEmpty());
    assertThrows(NoSuchElementException.class, () -> map.subMap(95, 5).firstKey());
  }

  @Test
  public void viewsOfARingAreViewsOfACopy() {
    ConcurrentRingMap<Integer, String> map = map();
    SortedMap<Integer, String> ring = map.subMap(70, 30);
    SortedMap<Integer, String> inner = ring.subMap(80, 20);
    assertEquals(List.of(80, 90, 10), keys(inner));
    assertEquals(List.of(10, 20), keys(ring.headMap(30)));
    assertEquals(List.of(80, 90), keys(ring.tailMap(80)));
    map.put(85, "v85");
    assertEquals(List.of(80, 90, 10), keys(inner));
  }

  @Test
  public void iteratorsSurviveConcurrentChanges() {
    ConcurrentRingMap<Integer, String> map = map();
    Iterator<Integer> i = map.keySet().iterator();
    List<Integer> seen = new ArrayList<Integer>();
    while (i.hasNext()) {
      int k = i.next();
      seen.add(k);
      if (k == 30) {
        map.remove(40);
        map.put(45, "v45");
      }
      if (k == 60) i.remove();
    }
    // weakly consistent: no ConcurrentModificationException, and every key that was not changed is seen once, in order
    seen.remove(Integer.valueOf(40));
    seen.remove(Integer.valueOf(45));
    assertEquals(List.of(10, 20, 30, 50, 60, 70, 80, 90), seen);
    assertEquals(List.of(10, 20, 30, 45, 50, 70, 80, 90), keys(map));
  }

  @Test
  public void copyKeepsTheOrderOfARing() {
    ConcurrentRingMap<Integer, String> copy = new ConcurrentRingMap<Integer, String>(map().subMap(70, 30));
    assertEquals(List.of(10, 20, 70, 80, 90), keys(copy));
  }

  @Test
  public void survivesJavaSerialization() throws Exception {
    ConcurrentRingMap<Integer, String> map = map();
    SortedMap<Integer, String> copy = javaRoundTrip(map);
    assertEquals(map, copy);
    assertTrue(copy.containsKey(50));
    assertNull(copy.get(50));
    assertEquals(List.of(70, 80, 90, 10, 20), keys(javaRoundTrip(map.subMap(70, 30))));
  }

  @SuppressWarnings("unchecked")
  static <T> T javaRoundTrip(T o) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(o);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      in.setObjectInputFilter(JavaSerializationFilter.getDefault());
      return (T) in.readObject();
    }
  }
}
