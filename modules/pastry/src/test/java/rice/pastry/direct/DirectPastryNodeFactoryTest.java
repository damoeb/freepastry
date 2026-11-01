package rice.pastry.direct;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.PastryNodeListener;
import rice.pastry.standard.RandomNodeIdFactory;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectPastryNodeFactoryTest {

  private static final long READY_TIMEOUT_SECONDS = 10;

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

  @Test
  void bootFromAnotherThreadSetsTheNodeReadyOnTheSelectorThread() throws Exception {
    PastryNode node = factory.newNode();
    CompletableFuture<Boolean> readyOnSelector = new CompletableFuture<Boolean>();
    node.addPastryNodeListener(new PastryNodeListener() {
      @Override
      public void pastryNodeReady(PastryNode n, boolean ready) {
        if (ready) readyOnSelector.complete(env.getSelectorManager().isSelectorThread());
      }
    });

    node.boot((Object)null);

    assertTrue(readyOnSelector.get(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }
}
