package rice.pastry.adversary;

import java.math.BigInteger;
import java.util.Locale;

import rice.environment.random.RandomSource;
import rice.pastry.Id;

/**
 * Ring arithmetic on node ids as unsigned 160-bit integers, for the id placements and the oracle.
 */
final class RingIds {

  static final int ID_BITS = Id.IdBitLength;
  static final BigInteger RING_SIZE = BigInteger.ONE.shiftLeft(ID_BITS);
  private static final int HEX_DIGITS = ID_BITS / 4;
  private static final int HEX_RADIX = 16;

  private RingIds() {}

  static BigInteger toInteger(Id id) {
    return new BigInteger(id.toStringFull(), HEX_RADIX);
  }

  static Id toId(BigInteger value) {
    BigInteger onRing = value.mod(RING_SIZE);
    String hex = onRing.toString(HEX_RADIX).toUpperCase(Locale.ROOT);
    return Id.build("0".repeat(HEX_DIGITS - hex.length()) + hex);
  }

  /** A uniformly random id. */
  static Id randomId(RandomSource random) {
    byte[] material = new byte[ID_BITS / Byte.SIZE];
    random.nextBytes(material);
    return Id.build(material);
  }

  /** A uniformly random value in [0, bound). */
  static BigInteger randomBelow(RandomSource random, BigInteger bound) {
    byte[] material = new byte[ID_BITS / Byte.SIZE + 1];
    random.nextBytes(material);
    material[0] = 0;
    return new BigInteger(material).mod(bound);
  }

  /** The shorter distance between two points of the ring. */
  static BigInteger ringDistance(BigInteger first, BigInteger second) {
    BigInteger clockwise = second.subtract(first).mod(RING_SIZE);
    BigInteger counterClockwise = first.subtract(second).mod(RING_SIZE);
    return clockwise.min(counterClockwise);
  }
}
