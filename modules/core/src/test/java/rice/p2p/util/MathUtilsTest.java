package rice.p2p.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import org.junit.jupiter.api.Test;

class MathUtilsTest {

  @Test
  void intIsBigEndianAndSigned() {
    assertArrayEquals(new byte[] {0x01, 0x02, 0x03, 0x04}, MathUtils.intToByteArray(0x01020304));
    assertArrayEquals(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE}, MathUtils.intToByteArray(-2));
    assertEquals(-2, MathUtils.byteArrayToInt(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE}));
    assertEquals(Integer.MIN_VALUE, MathUtils.byteArrayToInt(MathUtils.intToByteArray(Integer.MIN_VALUE)));
  }

  @Test
  void longIsBigEndianAndSigned() {
    assertArrayEquals(
        new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, MathUtils.longToByteArray(0x0102030405060708L));
    assertEquals(Long.MIN_VALUE, MathUtils.byteArrayToLong(MathUtils.longToByteArray(Long.MIN_VALUE)));
    assertEquals(-1L, MathUtils.byteArrayToLong(MathUtils.longToByteArray(-1L)));
  }

  @Test
  void offsetsAreHonoured() {
    byte[] buffer = new byte[10];
    MathUtils.intToByteArray(0x0A0B0C0D, buffer, 3);
    assertEquals(0x0A0B0C0D, MathUtils.byteArrayToInt(buffer, 3));
    MathUtils.longToByteArray(0x1122334455667788L, buffer, 2);
    assertEquals(0x1122334455667788L, MathUtils.byteArrayToLong(buffer, 2));
  }

  @Test
  void shortInputIsPaddedWithTrailingZeros() {
    assertEquals(0x01020000, MathUtils.byteArrayToInt(new byte[] {0x01, 0x02}));
    assertEquals(0x0102000000000000L, MathUtils.byteArrayToLong(new byte[] {0x01, 0x02}));
  }

  @Test
  void unsignedConversionsMatchTheJdk() {
    for (int value = Byte.MIN_VALUE; value <= Byte.MAX_VALUE; value++) {
      assertEquals(Byte.toUnsignedInt((byte) value), MathUtils.uByteToInt((byte) value));
    }
    for (int value = Short.MIN_VALUE; value <= Short.MAX_VALUE; value++) {
      assertEquals(Short.toUnsignedInt((short) value), MathUtils.uShortToInt((short) value));
    }
  }

  @Test
  void modMatchesFloorModForPositiveDivisors() {
    int[] values = {Integer.MIN_VALUE, -1_000_003, -7, -1, 0, 1, 7, 1_000_003, Integer.MAX_VALUE};
    int[] divisors = {1, 2, 7, 1024, 65_537, 1 << 30};
    for (int divisor : divisors) {
      for (int value : values) {
        assertEquals(Math.floorMod(value, divisor), MathUtils.mod(value, divisor), value + " mod " + divisor);
      }
    }
  }

  @Test
  void toBase64MatchesTheJdkEncoder() {
    byte[] bytes = {0, 1, 2, (byte) 0xFF, (byte) 0x80, 42};
    assertEquals(Base64.getEncoder().encodeToString(bytes), MathUtils.toBase64(bytes));
  }
}
