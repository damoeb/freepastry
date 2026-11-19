package rice.p2p.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class UnmodifiableSortedMapContractTest {
  private final TreeMap<String, Integer> backing = new TreeMap<>(Map.of("a", 1, "b", 2));
  private final SortedMap<String, Integer> testObj = Collections.unmodifiableSortedMap(backing);

  @Test
  void keySetCannotBeModified() {
    assertThrows(UnsupportedOperationException.class, () -> testObj.keySet().remove("a"));
    assertThrows(UnsupportedOperationException.class, () -> testObj.keySet().clear());
    assertEquals(2, backing.size());
  }

  @Test
  void valuesCannotBeModified() {
    assertThrows(UnsupportedOperationException.class, () -> testObj.values().remove(1));
    assertThrows(UnsupportedOperationException.class, () -> testObj.values().clear());
    assertEquals(2, backing.size());
  }

  @Test
  void entrySetCannotBeModified() {
    assertThrows(UnsupportedOperationException.class, () -> testObj.entrySet().clear());
    assertThrows(UnsupportedOperationException.class, () -> testObj.entrySet().iterator().next().setValue(9));
    assertEquals(1, backing.get("a"));
  }

  @Test
  void mutatorsAreRejected() {
    assertThrows(UnsupportedOperationException.class, () -> testObj.put("z", 9));
    assertThrows(UnsupportedOperationException.class, () -> testObj.remove("a"));
    assertThrows(UnsupportedOperationException.class, () -> testObj.putAll(Map.of("z", 9)));
    assertThrows(UnsupportedOperationException.class, () -> testObj.clear());
    assertEquals(2, backing.size());
  }

  @Test
  void rangeViewsAreImmutableAndSorted() {
    assertThrows(UnsupportedOperationException.class, () -> testObj.headMap("b").put("z", 9));
    assertThrows(UnsupportedOperationException.class, () -> testObj.tailMap("b").clear());
    assertThrows(UnsupportedOperationException.class, () -> testObj.subMap("a", "c").remove("a"));
    assertEquals("a", testObj.firstKey());
    assertEquals("b", testObj.lastKey());
    assertEquals(1, testObj.headMap("b").size());
    assertEquals(1, testObj.tailMap("b").size());
  }

  @Test
  void viewsFollowTheBackingMap() {
    backing.put("c", 3);

    assertEquals(3, testObj.keySet().size());
    assertEquals(3, testObj.values().size());
    assertEquals(3, testObj.entrySet().size());
  }
}
