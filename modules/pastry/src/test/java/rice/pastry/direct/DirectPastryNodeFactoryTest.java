package rice.pastry.direct;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;

import static org.junit.jupiter.api.Assertions.assertSame;

class DirectPastryNodeFactoryTest {

  private Environment env;
  private PastryNodeFactory factory;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    NetworkSimulator<DirectNodeHandle, RawMessage> simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void coalesceReturnsTheGivenHandle() throws Exception {
    PastryNode local = factory.newNode();
    NodeHandle remote = factory.newNode().getLocalHandle();

    assertSame(remote, local.coalesce(remote));
  }
}
