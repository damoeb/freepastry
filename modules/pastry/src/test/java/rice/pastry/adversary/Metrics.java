package rice.pastry.adversary;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import rice.pastry.NodeHandle;

/** The arithmetic of the metrics: domain entropy, mean and percentile. */
final class Metrics {

  /** Below this many entries the normalised entropy of a set is not defined. */
  private static final int MIN_ENTRIES_FOR_DIVERSITY = 2;
  private static final double PERCENTILE_5 = 0.05;

  private Metrics() {}

  /**
   * Shannon entropy {@code H = -sum p_d log2 p_d} over the failure domains of {@code handles}, normalised by
   * {@code log2(min(domainCount, handles))}; -1 when fewer than two handles.
   */
  static double normalisedDomainEntropy(Collection<NodeHandle> handles, Oracle oracle, int domainCount) {
    if (handles.size() < MIN_ENTRIES_FOR_DIVERSITY) return -1;
    int[] perDomain = new int[domainCount];
    for (NodeHandle handle : handles) perDomain[oracle.domain(handle)]++;
    double entropy = 0;
    for (int count : perDomain) {
      if (count == 0) continue;
      double share = (double) count / handles.size();
      entropy -= share * log2(share);
    }
    double maximum = log2(Math.min(domainCount, handles.size()));
    return maximum > 0 ? entropy / maximum : -1;
  }

  static double mean(List<Double> values) {
    if (values.isEmpty()) return -1;
    double sum = 0;
    for (double value : values) sum += value;
    return sum / values.size();
  }

  /** The 5th percentile (nearest rank below), or -1 for no values. */
  static double percentile5(List<Double> values) {
    if (values.isEmpty()) return -1;
    double[] sorted = values.stream().mapToDouble(Double::doubleValue).toArray();
    Arrays.sort(sorted);
    return sorted[(int) Math.floor(PERCENTILE_5 * (sorted.length - 1))];
  }

  static double ratio(long numerator, long denominator) {
    return denominator == 0 ? -1 : (double) numerator / denominator;
  }

  private static double log2(double value) {
    return Math.log(value) / Math.log(2);
  }
}
