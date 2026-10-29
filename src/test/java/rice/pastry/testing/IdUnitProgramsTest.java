package rice.pastry.testing;

import org.junit.jupiter.api.Test;

import static rice.p2p.util.testing.MainProgram.assertContains;
import static rice.p2p.util.testing.MainProgram.assertNoneOf;
import static rice.p2p.util.testing.MainProgram.run;

/**
 * Runs the legacy main()-based checks of the Pastry Ids, which report failures only by printing a marker.
 */
public class IdUnitProgramsTest {

  @Test
  public void id() throws Exception {
    String output = run(IdUnit::main);
    assertNoneOf(output, ": FAIL");
    assertContains(output, ": PASS");
  }

  @Test
  public void idRange() throws Exception {
    String output = run(IdRangeUnit::main);
    assertNoneOf(output, "ALERT");
    assertContains(output, "IdRangeUnit test finished.");
  }

  @Test
  public void nodeId() throws Exception {
    String output = run(NodeIdUnit::main);
    // "not equal - as expected." is a passing line, the failures are reported as below
    assertNoneOf(output, "ALERT", "ERROR", "test failed!");
    assertContains(output, "--------------------------");
  }
}
