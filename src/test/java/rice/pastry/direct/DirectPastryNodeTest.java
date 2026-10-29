package rice.pastry.direct;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class DirectPastryNodeTest {

  private static final int MAX_GC_ATTEMPTS = 50;

  private Environment env;
  private PastryNode node;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    NetworkSimulator<DirectNodeHandle, RawMessage> simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    PastryNodeFactory factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env);
    node = factory.newNode();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void currentNodeIsPerThread() throws Exception {
    PastryNode previous = DirectPastryNode.setCurrentNode(node);
    try {
      AtomicReference<PastryNode> seenByOther = new AtomicReference<PastryNode>(node);
      Thread other = new Thread(() -> seenByOther.set(DirectPastryNode.getCurrentNode()));
      other.start();
      other.join();

      assertSame(node, DirectPastryNode.getCurrentNode());
      assertNull(seenByOther.get());
    } finally {
      DirectPastryNode.setCurrentNode(previous);
    }
  }

  @Test
  void endedThreadThatNeverClearedItsNodeIsCollectable() throws Exception {
    Thread worker = new Thread(() -> DirectPastryNode.setCurrentNode(node));
    worker.start();
    worker.join();
    WeakReference<Thread> ref = new WeakReference<Thread>(worker);
    worker = null;

    for (int i = 0; i < MAX_GC_ATTEMPTS && ref.get() != null; i++) {
      System.gc();
    }

    assertNull(ref.get());
  }
}
