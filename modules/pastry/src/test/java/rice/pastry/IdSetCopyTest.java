package rice.pastry;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IdSet.copy() used to share the backing map, so a change to the copy showed in the original, and the original kept
 * returning its cached hash for members it no longer had.
 */
class IdSetCopyTest {

  private static final Id A = Id.build("1000000000000000000000000000000000000000");
  private static final Id B = Id.build("5000000000000000000000000000000000000000");
  private static final Id C = Id.build("9000000000000000000000000000000000000000");
  private static final Id D = Id.build("D000000000000000000000000000000000000000");

  private static IdSet setOf(Id... ids) {
    IdSet set = new IdSet();
    for (Id id : ids) {
      set.addId(id);
    }
    return set;
  }

  @Test
  void changingTheCopyLeavesTheOriginalAndItsHashAlone() {
    IdSet original = setOf(A, B);
    byte[] hash = original.hash().clone();

    IdSet copy = original.copy();
    copy.addId(C);
    copy.removeId(A);

    assertEquals(2, original.numElements());
    assertTrue(original.isMemberId(A));
    assertFalse(original.isMemberId(C));
    assertArrayEquals(hash, original.hash());
    assertArrayEquals(setOf(B, C).hash(), copy.hash());
  }

  @Test
  void changingTheOriginalLeavesTheCopyAlone() {
    IdSet original = setOf(A, B);
    IdSet copy = original.copy();

    original.addId(C);

    assertEquals(2, copy.numElements());
    assertArrayEquals(setOf(A, B).hash(), copy.hash());
  }

  @Test
  void copyOfAWrappingSubSetKeepsItsRingOrderAndHash() {
    IdSet wrapped = setOf(A, B, C, D).subSet(new IdRange(C, B));
    IdSet copy = wrapped.copy();

    assertEquals(Arrays.asList(wrapped.asArray()), Arrays.asList(copy.asArray()));
    assertEquals(Arrays.asList(C, D, A), Arrays.asList(copy.asArray()));
    assertArrayEquals(wrapped.hash(), copy.hash());
  }
}
