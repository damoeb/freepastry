package rice.pastry.adversary;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The full baseline matrix of design section 4 ("Baseline"), under {@code -Pintegration}: {@code N} in {200, 1000},
 * {@code f} in {0, 0.1, 0.2, 0.3}, behaviours {none, Drop, Drop+FastPing, Misroute+FastPing, LieInJoin,
 * PoisonRows+FastPing}, uniform placement, churn {none, median session 1 h}, 5 seeds; {@code f = 0} only with no
 * behaviour, where every behaviour is the same. Writes every run to {@code target/adversary/matrix-runs.jsonl} and the
 * cells to {@code target/adversary/baseline-matrix.jsonl} and {@code .md}; the recorded copy of the latter is the
 * test resource {@code baseline-matrix.jsonl}.
 *
 * <p>System properties: {@code adversary.nodes} (comma-separated ring sizes), {@code adversary.seeds} (seeds per
 * cell), {@code adversary.threads} (runs in parallel; each run is single-threaded and independent).
 */
@Tag("integration")
@Tag("adversary")
class AdversarialBaselineMatrixTest {

  private static final double[] FRACTIONS = {0.1, 0.2, 0.3};
  private static final long CHURN_MEDIAN_SESSION_MILLIS = 3_600_000;
  private static final double HONEST_LOOKUP_SUCCESS_MINIMUM = 0.99;
  private static final double RATIO_SLACK = 0.05;
  private static final long MATRIX_TIMEOUT_MINUTES = 60;
  /** Four N = 1000 runs fit the 1 GB heap of a test fork. */
  private static final int DEFAULT_THREADS = Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() / 2));

  static List<List<Behaviour>> behaviourSets() {
    return List.of(
        List.of(),
        List.of(new Behaviour.Drop()),
        List.of(new Behaviour.Drop(), new Behaviour.FastPing()),
        List.of(new Behaviour.Misroute(), new Behaviour.FastPing()),
        List.of(new Behaviour.LieInJoin()),
        List.of(new Behaviour.PoisonRows(), new Behaviour.FastPing()));
  }

  static List<Scenario> matrix(List<Integer> ringSizes, int seedCount) {
    List<Scenario> scenarios = new ArrayList<>();
    for (int nodes : ringSizes) {
      for (Churn churn : new Churn[] {null, Churn.exponential(CHURN_MEDIAN_SESSION_MILLIS)}) {
        List<Scenario.Builder> cells = new ArrayList<>();
        cells.add(Scenario.builder().nodes(nodes).churn(churn));
        for (double fraction : FRACTIONS) {
          for (List<Behaviour> behaviours : behaviourSets()) {
            cells.add(Scenario.builder().nodes(nodes).churn(churn).maliciousFraction(fraction).behaviours(behaviours));
          }
        }
        for (Scenario.Builder cell : cells) {
          for (int seed = 1; seed <= seedCount; seed++) scenarios.add(cell.seed(seed).build());
        }
      }
    }
    return scenarios;
  }

  @Test
  @Timeout(value = MATRIX_TIMEOUT_MINUTES, unit = TimeUnit.MINUTES)
  @DisplayName("the baseline matrix: honest cells are correct, and no cell is worse than the recorded baseline")
  void baselineMatrix() throws Exception {
    List<Integer> ringSizes = new ArrayList<>();
    for (String size : System.getProperty("adversary.nodes", "200,1000").split(",", -1)) {
      ringSizes.add(Integer.parseInt(size.trim()));
    }
    int seedCount = Integer.getInteger("adversary.seeds", 5);
    int threads = Integer.getInteger("adversary.threads", DEFAULT_THREADS);
    List<Scenario> scenarios = matrix(ringSizes, seedCount);

    long startNanos = System.nanoTime();
    Map<String, List<RunMetrics>> runsPerCell = new LinkedHashMap<>();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<RunMetrics>> futures = new ArrayList<>();
      for (Scenario scenario : scenarios) futures.add(pool.submit(() -> AdversarialRing.run(scenario)));
      for (Future<RunMetrics> future : futures) {
        RunMetrics run = future.get();
        BaselineTable.appendRun("matrix-runs", run);
        runsPerCell.computeIfAbsent(run.scenario().cellLabel(), label -> new ArrayList<>()).add(run);
      }
    } finally {
      pool.shutdownNow();
    }
    List<BaselineTable.Cell> cells = new ArrayList<>();
    for (List<RunMetrics> runs : runsPerCell.values()) cells.add(BaselineTable.summarise(runs));
    BaselineTable.write("baseline-matrix", cells);
    System.out.printf("adversary matrix: %d runs, %d threads, %d s%n", scenarios.size(), threads,
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));

    Map<String, Map<String, Double>> recorded = readRecorded();
    for (BaselineTable.Cell cell : cells) {
      if (cell.label().contains(" f=0.0 ") && cell.label().contains("churn=none")) {
        assertTrue(cell.mean("lookupSuccess") >= HONEST_LOOKUP_SUCCESS_MINIMUM, cell.toJsonLine());
      }
      Map<String, Double> baseline = recorded == null ? null : recorded.get(cell.label());
      if (baseline != null && cell.seedCount() == baseline.get("seeds").intValue()) {
        assertTrue(cell.mean("lookupSuccess") >= baseline.get("lookupSuccessMean") - RATIO_SLACK,
            "lookup success fell below the baseline: " + cell.toJsonLine());
        assertTrue(cell.mean("deliveryRatio") >= baseline.get("deliveryRatioMean") - RATIO_SLACK,
            "delivery fell below the baseline: " + cell.toJsonLine());
      }
    }
  }

  private static @Nullable Map<String, Map<String, Double>> readRecorded() {
    try {
      return BaselineTable.read(BaselineTable.MATRIX_RESOURCE);
    } catch (java.io.IOException e) {
      return null; // not recorded yet
    }
  }
}
