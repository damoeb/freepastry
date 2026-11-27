package rice.pastry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/** Property-based proof that jqwik runs on the JUnit platform of this build: id round trips and distance symmetry. */
class IdPropertyTest {
  @Provide
  Arbitrary<Id> ids() {
    return Arbitraries.bytes().array(byte[].class).ofSize(Id.IdBitLength / 8).map(Id::build);
  }

  @Property
  void anIdSurvivesTheByteArrayRoundTrip(@ForAll("ids") Id id) {
    assertEquals(id, Id.build(id.toByteArray()));
    assertArrayEquals(id.toByteArray(), Id.build(id.toByteArray()).toByteArray());
  }

  @Property
  void theDistanceBetweenTwoIdsIsSymmetric(@ForAll("ids") Id first, @ForAll("ids") Id second) {
    assertEquals(first.distance(second), second.distance(first));
  }
}
