package rice.pastry.adversary;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Design section 4, "Determinism": two runs with the same seed give byte-identical metric lines. */
@Tag("adversary")
class AdversarialRingDeterminismTest {

  /** Small, but with every source of randomness: malicious placement, drops, misroutes, churn, late joins. */
  private static Scenario.Builder cell() {
    return Scenario.builder().nodes(60).maliciousFraction(0.2)
        .behaviours(new Behaviour.Drop(0.5, null), new Behaviour.Misroute(), new Behaviour.FastPing(),
            new Behaviour.PoisonRows())
        .churn(Churn.exponential(600_000)).stabiliseMillis(30_000).preAttackWindowMillis(30_000)
        .attackWindowMillis(120_000).lateJoins(5).byteEstimate(true);
  }

  @Test
  @DisplayName("two runs with the same seed give byte-identical metric lines")
  void sameSeedSameLine() throws Exception {
    String first = AdversarialRing.run(cell().seed(7).build()).toJsonLine();
    String second = AdversarialRing.run(cell().seed(7).build()).toJsonLine();

    assertEquals(first, second);
  }

  @Test
  @DisplayName("another seed gives another run")
  void otherSeedOtherLine() throws Exception {
    String first = AdversarialRing.run(cell().seed(7).build()).toJsonLine();
    String other = AdversarialRing.run(cell().seed(8).build()).toJsonLine();

    assertNotEquals(first.replace("\"seed\":7", ""), other.replace("\"seed\":8", ""));
  }
}
