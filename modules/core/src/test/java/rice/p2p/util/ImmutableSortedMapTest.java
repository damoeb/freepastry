package rice.p2p.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class ImmutableSortedMapTest {
  private final TreeMap<String, Integer> backing = new TreeMap<>(Map.of("a", 1, "b", 2));
  private final ImmutableSortedMap<String, Integer> testObj = new ImmutableSortedMap<>(backing);

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
  void viewsFollowTheBackingMap() {
    backing.put("c", 3);

    assertEquals(3, testObj.keySet().size());
    assertEquals(3, testObj.values().size());
    assertEquals(3, testObj.entrySet().size());
  }
}
