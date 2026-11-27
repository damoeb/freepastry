package rice.pastry.adversary;

import rice.environment.random.RandomSource;

/**
 * Churn during the attack window: Poisson arrivals at the rate that keeps the population stable
 * ({@code nodeCount / meanSession}), and a session time drawn for every live node at the attack start and for every
 * arrival. A node whose session ends departs.
 *
 * @param sessionTimes distribution of the session times
 * @param medianSessionMillis median session time
 * @param departure how a node departs
 */
public record Churn(SessionTimes sessionTimes, long medianSessionMillis, Departure departure) {

  /** Tail index of the heavy-tailed Pareto session times. */
  public static final double PARETO_ALPHA = 1.5;

  /** Session time distributions. */
  public enum SessionTimes {
    EXPONENTIAL,
    /** Pareto with {@link #PARETO_ALPHA}: many short sessions and a few very long ones. */
    PARETO
  }

  /**
   * How a node departs. In the direct simulator both remove the node from the network at once, and the simulator tells
   * every node at once that it is dead (it is the liveness provider), so they differ only in whether the node's own
   * {@code destroy()} runs first.
   */
  public enum Departure {
    CRASH,
    GRACEFUL
  }

  public Churn {
    if (medianSessionMillis <= 0) throw new IllegalArgumentException("medianSessionMillis must be positive");
  }

  /** Exponential session times with the given median, crash departures. */
  public static Churn exponential(long medianSessionMillis) {
    return new Churn(SessionTimes.EXPONENTIAL, medianSessionMillis, Departure.CRASH);
  }

  public double meanSessionMillis() {
    return switch (sessionTimes) {
      case EXPONENTIAL -> medianSessionMillis / Math.log(2);
      case PARETO -> PARETO_ALPHA * paretoScaleMillis() / (PARETO_ALPHA - 1);
    };
  }

  public long drawSessionMillis(RandomSource random) {
    double uniform = 1.0 - random.nextDouble(); // in (0, 1]
    double sessionMillis = switch (sessionTimes) {
      case EXPONENTIAL -> -Math.log(uniform) * meanSessionMillis();
      case PARETO -> paretoScaleMillis() / Math.pow(uniform, 1 / PARETO_ALPHA);
    };
    return Math.max(1, Math.round(sessionMillis));
  }

  /** Draws the time to the next arrival of a Poisson process with {@code ratePerMillis}. */
  static long drawInterArrivalMillis(RandomSource random, double ratePerMillis) {
    double uniform = 1.0 - random.nextDouble();
    return Math.max(1, Math.round(-Math.log(uniform) / ratePerMillis));
  }

  private double paretoScaleMillis() {
    return medianSessionMillis / Math.pow(2, 1 / PARETO_ALPHA);
  }

  String label() {
    return "median " + medianSessionMillis / 60_000 + " min " + sessionTimes.name().toLowerCase(java.util.Locale.ROOT);
  }
}
