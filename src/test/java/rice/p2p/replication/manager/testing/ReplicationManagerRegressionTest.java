package rice.p2p.replication.manager.testing;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import rice.p2p.commonapi.testing.RegressionRun;

/**
 * The replication manager regression program on the direct simulator (about 80 s).
 */
@Tag("integration")
public class ReplicationManagerRegressionTest {

  @Test
  public void replicationManager() throws Exception {
    RegressionRun.run(ReplicationManagerRegrTest::new);
  }
}
