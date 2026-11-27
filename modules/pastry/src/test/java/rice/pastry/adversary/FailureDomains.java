package rice.pastry.adversary;

import rice.environment.random.RandomSource;
import rice.pastry.direct.ProximityGenerator;
import rice.pastry.direct.SimulatedNodePosition;

/**
 * {@code K} failure domains placed as regions of a Euclidean plane: each domain has a centre, and a node of the domain
 * sits within {@code radius} of it. RTT therefore correlates with the domain, and proximity neighbour selection
 * naturally prefers the own domain (monoculture). Also the {@link ProximityGenerator} of the harness network: the
 * harness names the domain of the next node with {@link #setNextDomain(int)} before it creates the node.
 */
public final class FailureDomains implements ProximityGenerator {

  /** Delay between two distinct nodes at the same point, as in the Euclidean network. */
  private static final float MIN_DELAY_MILLIS = 2.0f;
  private static final double PROXIMITY_PER_DELAY = 2.0;

  private final int domainCount;
  private final double side;
  private final double radius;
  private final double[] centreX;
  private final double[] centreY;
  private int nextDomain;
  private final RandomSource random;

  /**
   * @param domainCount number of domains {@code K}
   * @param maxDiameterMillis largest delay across the plane
   * @param radiusFraction radius of a domain region as a fraction of the side of the plane
   * @param random draws the domain centres now and the node positions later
   */
  public FailureDomains(int domainCount, int maxDiameterMillis, double radiusFraction, RandomSource random) {
    if (domainCount < 1) throw new IllegalArgumentException("domainCount must be positive: " + domainCount);
    this.domainCount = domainCount;
    this.side = maxDiameterMillis / Math.sqrt(2.0);
    this.radius = side * radiusFraction;
    this.random = random;
    centreX = new double[domainCount];
    centreY = new double[domainCount];
    for (int domain = 0; domain < domainCount; domain++) {
      centreX[domain] = random.nextDouble() * side;
      centreY[domain] = random.nextDouble() * side;
    }
  }

  public int domainCount() {
    return domainCount;
  }

  /** The domain of the node whose position {@link #generateNodeRecord()} makes next. */
  public void setNextDomain(int domain) {
    if (domain < 0 || domain >= domainCount) throw new IllegalArgumentException("no domain " + domain);
    nextDomain = domain;
  }

  @Override
  public SimulatedNodePosition generateNodeRecord() {
    double angle = random.nextDouble() * 2 * Math.PI;
    double distance = Math.sqrt(random.nextDouble()) * radius;
    return new Position(nextDomain, centreX[nextDomain] + distance * Math.cos(angle),
        centreY[nextDomain] + distance * Math.sin(angle));
  }

  /** Keeps the random the constructor got: the network simulator offers its own, which the harness does not use. */
  @Override
  public void setRandom(RandomSource ignored) {}

  /** A node position in the plane, tagged with its domain. */
  record Position(int domain, double x, double y) implements SimulatedNodePosition {
    @Override
    public float networkDelay(SimulatedNodePosition peerPosition) {
      Position peer = (Position) peerPosition;
      float delay = (float) Math.hypot(x - peer.x, y - peer.y);
      if (delay < MIN_DELAY_MILLIS && !this.equals(peer)) return MIN_DELAY_MILLIS;
      return delay;
    }

    @Override
    public float proximity(SimulatedNodePosition peerPosition) {
      return Math.round(networkDelay(peerPosition) * PROXIMITY_PER_DELAY);
    }
  }
}
