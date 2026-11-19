package rice.p2p.multiring.testing;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The multiring regression program on the direct simulator (about 45 s; its main() defaults to real sockets).
 */
@Tag("integration")
public class MultiringRegressionTest {

  @Test
  public void multiring() throws Exception {
    int protocol = MultiringRegrTest.PROTOCOL;
    MultiringRegrTest.PROTOCOL = MultiringRegrTest.PROTOCOL_DIRECT;
    Environment env = Environment.directEnvironment();
    try {
      MultiringRegrTest test = new MultiringRegrTest(env);
      test.start();
      assertTrue(test.getFailures().isEmpty(), () -> "Failed steps: " + test.getFailures());
    } finally {
      env.destroy();
      MultiringRegrTest.PROTOCOL = protocol;
    }
  }
}
