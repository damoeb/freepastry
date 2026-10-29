package rice.p2p.past.testing;

import java.nio.file.Path;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import rice.p2p.commonapi.testing.RegressionRun;

/**
 * The PAST regression programs on the direct simulator (about 15 s each), with their storage in a temporary directory.
 */
@Tag("integration")
public class PastRegressionsTest {

  @Test
  public void past(@TempDir Path dir) throws Exception {
    RegressionRun.run(PastRegrTest::new, (env, test) -> {
      env.getParameters().setDouble("p2p_past_successfulInsertThreshold", 1.0); // as in main()
      test.storageRoot = dir.toString();
    });
  }

  @Test
  public void rawPast(@TempDir Path dir) throws Exception {
    RegressionRun.run(RawPastRegrTest::new, (env, test) -> {
      env.getParameters().setDouble("p2p_past_successfulInsertThreshold", 1.0); // as in main()
      test.storageRoot = dir.toString();
    });
  }
}
