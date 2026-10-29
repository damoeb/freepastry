package rice.p2p.replication.testing;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import rice.p2p.commonapi.testing.RegressionRun;

/**
 * The replication regression program on the direct simulator (about 15 s).
 */
@Tag("integration")
public class ReplicationRegressionTest {

  @Test
  public void replication() throws Exception {
    RegressionRun.run(ReplicationRegrTest::new);
  }
}
