package rice.pastry.adversary;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The smoke subset of the baseline matrix, in the default build: N = 200, one seed, the honest cell and the Drop cell
 * at f = 0.2 (design section 4, "Test strategy"). The harness proves itself on the honest cell; the Drop cell
 * documents the gap that the secure-routing phases close: it asserts that the attack succeeds today, and that no
 * metric got worse than the recorded baseline ({@code baseline-smoke.jsonl}). When a later phase improves a metric
 * past the recorded value, it records the new value (and replaces the gap assertion by its target).
 */
@Tag("adversary")
class AdversarialRingSmokeTest {

  /** Absolute slack for a ratio against the recorded baseline: absorbs changes of unrelated code. */
  private static final double RATIO_SLACK = 0.05;

  /** The timeline of the smoke cells: shorter than the matrix's, same ring size. */
  static Scenario.Builder smokeCell() {
    return Scenario.builder().nodes(200).seed(1).stabiliseMillis(60_000).preAttackWindowMillis(60_000)
        .attackWindowMillis(300_000).lateJoins(10);
  }

  @Test
  @DisplayName("an honest ring delivers every lookup to the correct root and every late joiner converges")
  void honestRingIsCorrect() throws Exception {
    RunMetrics metrics = AdversarialRing.run(smokeCell().build());
    BaselineTable.appendRun("smoke", metrics);

    assertEquals(1.0, metrics.preAttackLookupSuccess(), metrics.toJsonLine());
    assertEquals(1.0, metrics.lookupSuccess(), metrics.toJsonLine());
    assertEquals(1.0, metrics.deliveryRatio(), metrics.toJsonLine());
    assertEquals(1.0, metrics.convergedJoinerFraction(), metrics.toJsonLine());
    assertEquals(0, metrics.recoveryMillis(), metrics.toJsonLine());
    assertEquals(0.0, metrics.maliciousShareRoutingTable(), metrics.toJsonLine());
    assertNotWorseThanBaseline(metrics);
  }

  @Test
  @DisplayName("gap, closed by secure routing: with f = 0.2 dropping nodes, lookups fail well beyond f")
  void droppingMinorityBreaksLookups() throws Exception {
    RunMetrics metrics = AdversarialRing.run(smokeCell().maliciousFraction(0.2).behaviours(new Behaviour.Drop())
        .build());
    BaselineTable.appendRun("smoke", metrics);

    assertTrue(metrics.preAttackLookupSuccess() > 0.99, metrics.toJsonLine());
    assertTrue(metrics.lookupSuccess() < 0.9, "the attack succeeds today: " + metrics.toJsonLine());
    assertTrue(metrics.droppedMessages() > 0, metrics.toJsonLine());
    assertNotWorseThanBaseline(metrics);
  }

  /** The higher-is-better ratios must not fall below the recorded run by more than {@link #RATIO_SLACK}. */
  private static void assertNotWorseThanBaseline(RunMetrics metrics) throws Exception {
    Map<String, Map<String, Double>> baseline = BaselineTable.read(BaselineTable.SMOKE_RESOURCE);
    Map<String, Double> recorded = baseline.get(BaselineTable.runKey(metrics.scenario()));
    if (recorded == null) throw new AssertionError("no recorded baseline for " + BaselineTable.runKey(metrics.scenario()));
    assertTrue(metrics.deliveryRatio() >= recorded.get("deliveryRatio") - RATIO_SLACK, metrics.toJsonLine());
    assertTrue(metrics.lookupSuccess() >= recorded.get("lookupSuccess") - RATIO_SLACK, metrics.toJsonLine());
    assertTrue(metrics.convergedJoinerFraction() >= recorded.get("convergedJoinerFraction") - RATIO_SLACK,
        metrics.toJsonLine());
    assertTrue(metrics.maliciousShareRoutingTable() <= recorded.get("maliciousShareRoutingTable") + RATIO_SLACK,
        metrics.toJsonLine());
  }
}
