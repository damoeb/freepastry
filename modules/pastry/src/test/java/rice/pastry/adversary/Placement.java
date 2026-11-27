package rice.pastry.adversary;

import java.math.BigInteger;

import rice.environment.random.RandomSource;
import rice.pastry.Id;

/**
 * Where the malicious nodes sit in the id space. Honest nodes are always placed uniformly.
 */
public sealed interface Placement {

  /** The id of the {@code index}-th malicious node (0-based). */
  Id maliciousId(RandomSource random, int index);

  /** Uniformly random ids: the certified-mode reality, where the CA assigns ids with a SecureRandom. */
  record Uniform() implements Placement {
    @Override
    public Id maliciousId(RandomSource random, int index) {
      return RingIds.randomId(random);
    }

    @Override
    public String toString() {
      return "UNIFORM";
    }
  }

  /**
   * Ids drawn uniformly from an arc of {@code widthFraction} of the ring centred on {@code target}: models a CA
   * compromise or the free id choice of plain mode, for the density tests.
   */
  record Cluster(Id target, double widthFraction) implements Placement {
    private static final int FRACTION_SCALE_BITS = 52;

    public Cluster {
      if (!(widthFraction > 0 && widthFraction <= 1)) {
        throw new IllegalArgumentException("widthFraction must be in (0, 1]: " + widthFraction);
      }
    }

    @Override
    public Id maliciousId(RandomSource random, int index) {
      BigInteger scale = BigInteger.ONE.shiftLeft(FRACTION_SCALE_BITS);
      BigInteger width = RingIds.RING_SIZE.multiply(BigInteger.valueOf((long) (widthFraction * scale.doubleValue())))
          .divide(scale).max(BigInteger.TWO);
      BigInteger start = RingIds.toInteger(target).subtract(width.shiftRight(1));
      return RingIds.toId(start.add(RingIds.randomBelow(random, width)));
    }

    @Override
    public String toString() {
      return "CLUSTER(" + target.toStringBare() + "," + widthFraction + ")";
    }
  }

  /**
   * Malicious ids on both sides of {@code victim}, alternating clockwise and counter-clockwise, each {@code spacing}
   * ids further out. Only meaningful with a compromised CA; kept for the leaf-set tests. The harness creates an honest
   * node with the victim id.
   */
  record Interleaved(Id victim, BigInteger spacing) implements Placement {
    @Override
    public Id maliciousId(RandomSource random, int index) {
      BigInteger step = spacing.multiply(BigInteger.valueOf(index / 2 + 1L));
      BigInteger victimValue = RingIds.toInteger(victim);
      return RingIds.toId(index % 2 == 0 ? victimValue.add(step) : victimValue.subtract(step));
    }

    @Override
    public String toString() {
      return "INTERLEAVED(" + victim.toStringBare() + ")";
    }
  }
}
