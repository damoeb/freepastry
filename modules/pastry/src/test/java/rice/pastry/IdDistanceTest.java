package rice.pastry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Pins the arithmetic of the distance between two ids on the circle. */
class IdDistanceTest {
  private static IdDistance distance(int lowWord, int highWord) {
    return new IdDistance(new int[] {lowWord, 0, 0, 0, highWord});
  }

  @Test
  void aDistanceBuiltFromBytesIsReadLittleEndian() {
    byte[] bytes = new byte[Id.IdBitLength / 8];
    bytes[0] = 0x01;
    bytes[4] = 0x02;

    IdDistance actual = new IdDistance(bytes);

    assertEquals(new IdDistance(new int[] {1, 2, 0, 0, 0}), actual);
    assertArrayEquals(bytes, actual.copy());
  }

  @Test
  void blitWritesAtTheGivenOffset() {
    byte[] target = new byte[Id.IdBitLength / 8 + 3];

    distance(0x0102, 0).blit(target, 3);

    assertEquals(0x02, target[3]);
    assertEquals(0x01, target[4]);
    assertEquals(0, target[2]);
  }

  @Test
  void comparesUnsignedFromTheMostSignificantWord() {
    IdDistance small = distance(0xFFFFFFFF, 1);
    IdDistance large = distance(0, 2);

    assertTrue(small.compareTo(large) < 0);
    assertTrue(large.compareTo(small) > 0);
    assertEquals(0, small.compareTo(distance(0xFFFFFFFF, 1)));
    assertTrue(distance(0, 0x80000000).compareTo(distance(0, 1)) > 0);
  }

  @Test
  void equalDistancesHaveEqualHashCodes() {
    assertEquals(distance(7, 9), distance(7, 9));
    assertEquals(distance(7, 9).hashCode(), distance(7, 9).hashCode());
    assertNotEquals(distance(7, 9), distance(7, 10));
  }

  @Test
  void shiftingRightDividesAndFillsFromTheTop() {
    assertEquals(distance(2, 0), distance(4, 0).shift(1, 0));
    assertEquals(new IdDistance(new int[] {0, 0, 0, 0x80000000, 0}), distance(0, 1).shift(1, 0));
    assertEquals(distance(0, 0x80000000), distance(0, 0).shift(1, 1));
  }

  @Test
  void shiftingRightCanRoundUp() {
    assertEquals(distance(3, 0), distance(5, 0).shift(1, 0, true));
    assertEquals(distance(2, 0), distance(5, 0).shift(1, 0, false));
  }

  @Test
  void shiftingLeftMultipliesAndFillsFromTheBottom() {
    assertEquals(distance(8, 0), distance(4, 0).shift(-1, 0));
    assertEquals(new IdDistance(new int[] {0, 1, 0, 0, 0}), distance(0x80000000, 0).shift(-1, 0));
    assertEquals(distance(1, 0), distance(0, 0).shift(-1, 1));
  }

  @Test
  void shiftReturnsTheDistanceItself() {
    IdDistance target = distance(4, 0);

    assertSame(target, target.shift(1, 0));
    assertSame(target, target.shiftDistance(1, 0));
  }

  @Test
  void printsTheBytesFromTheMostSignificant() {
    String actual = distance(0x0A, 0).toString();

    assertEquals("< Id.distance 0x" + "00".repeat(19) + "0A >", actual);
  }
}
