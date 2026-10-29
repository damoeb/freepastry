package rice.p2p.splitstream.testing;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import rice.p2p.commonapi.testing.RegressionRun;

/**
 * The SplitStream regression program on the direct simulator (about 40 s).
 */
@Tag("integration")
public class SplitStreamRegressionTest {

  @Test
  public void splitStream() throws Exception {
    RegressionRun.run(SplitStreamRegrTest::new);
  }
}
