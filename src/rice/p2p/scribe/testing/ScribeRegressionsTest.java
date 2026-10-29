package rice.p2p.scribe.testing;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import rice.p2p.commonapi.testing.RegressionRun;

/**
 * The Scribe regression programs on the direct simulator (a few minutes each).
 */
@Tag("integration")
public class ScribeRegressionsTest {

  @Test
  public void scribe() throws Exception {
    RegressionRun.run(ScribeRegrTest::new);
  }

  @Test
  public void rawScribe() throws Exception {
    RegressionRun.run(RawScribeRegrTest::new);
  }
}
