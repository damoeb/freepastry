package rice.p2p.glacier;

import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.multiring.MultiringIdFactory;
import rice.p2p.multiring.RingId;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FragmentKeyRange and VersionKeyRange, and their factories, share their code in GlacierKeyRange and
 * GlacierKeyFactory: the edges, membership and the messages of the operations they do not support.
 */
class GlacierKeyRangeTest {

  private final Id ring = rice.pastry.Id.build("4242424242424242424242424242424242424242");
  private final MultiringIdFactory multiring = new MultiringIdFactory(ring, new PastryIdFactory(null));
  private final Id inside = RingId.build(ring, rice.pastry.Id.build("8100000000000000000000000000000000000000"));
  private final Id outside = RingId.build(ring, rice.pastry.Id.build("9100000000000000000000000000000000000000"));

  @Test
  void versionKeyRangeTestsTheIdOfAKey() {
    IdRange range = new VersionKeyFactory(multiring).buildIdRangeFromPrefix("8");

    assertTrue(range.containsId(new VersionKey(inside, 5)));
    assertFalse(range.containsId(new VersionKey(outside, 5)));
    assertEquals(new VersionKey(multiring.buildIdRangeFromPrefix("8").getCCWId(), 0L), range.getCCWId());
    assertEquals(new VersionKey(multiring.buildIdRangeFromPrefix("8").getCWId(), 0L), range.getCWId());
  }

  @Test
  void fragmentKeyRangeTestsTheIdOfAKey() {
    IdRange range = new FragmentKeyFactory(multiring).buildIdRangeFromPrefix("8");

    assertTrue(range.containsId(new FragmentKey(new VersionKey(inside, 5), 3)));
    assertFalse(range.containsId(new FragmentKey(new VersionKey(outside, 5), 3)));
    assertEquals(
        new FragmentKey(new VersionKey(multiring.buildIdRangeFromPrefix("8").getCCWId(), 0L), 0), range.getCCWId());
  }

  @Test
  void unsupportedOperationsNameTheirClass() {
    IdRange range = new VersionKeyFactory(multiring).buildIdRangeFromPrefix("8");

    assertEquals(
        "VersionKeyRange.mergeRange() is not supported!",
        assertThrows(RuntimeException.class, () -> range.mergeRange(range)).getMessage());
    assertEquals(
        "FragmentKeyFactory.buildId(String) is not supported!",
        assertThrows(RuntimeException.class, () -> new FragmentKeyFactory(multiring).buildId("x")).getMessage());
  }

  @Test
  void keysParseFromTheirStringForm() {
    VersionKey versionKey = new VersionKey(inside, 17);
    FragmentKey fragmentKey = new FragmentKey(versionKey, 4);

    assertEquals(versionKey, new VersionKeyFactory(multiring).buildIdFromToString(versionKey.toStringFull()));
    char[] chars = ("xx" + fragmentKey.toStringFull()).toCharArray();
    assertEquals(
        fragmentKey, new FragmentKeyFactory(multiring).buildIdFromToString(chars, 2, chars.length - 2));
  }
}
