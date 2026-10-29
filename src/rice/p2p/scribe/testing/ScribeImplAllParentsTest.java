package rice.p2p.scribe.testing;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rice.environment.Environment;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.scribe.ScribeImpl;
import rice.p2p.scribe.Topic;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * Regression test: ScribeImpl.allParentsContains() used to cast the stored topic collection
 * to Vector, but addToAllParents() stores ArrayLists, so any known parent caused a
 * ClassCastException.
 */
public class ScribeImplAllParentsTest {

  /**
   * Exposes the protected addToAllParents().
   */
  static class TestableScribe extends ScribeImpl {
    TestableScribe(PastryNode node) {
      super(node, "ScribeImplAllParentsTest");
    }

    void addParent(Topic topic, NodeHandle parent) {
      addToAllParents(topic, parent);
    }
  }

  private Environment env;
  private PastryNode local;
  private PastryNode remote;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    NetworkSimulator<DirectNodeHandle, RawMessage> simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    PastryNodeFactory factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env);
    local = factory.newNode();
    remote = factory.newNode();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void allParentsContainsKnownParent() {
    TestableScribe scribe = new TestableScribe(local);
    PastryIdFactory idFactory = new PastryIdFactory(env);
    Topic topic = new Topic(idFactory, "topic");
    Topic otherTopic = new Topic(idFactory, "other topic");
    NodeHandle parent = remote.getLocalHandle();

    scribe.addParent(topic, parent);

    assertTrue(scribe.allParentsContainsParent(parent));
    assertTrue(scribe.allParentsContains(topic, parent));
    assertFalse(scribe.allParentsContains(otherTopic, parent));
  }

  @Test
  void allParentsContainsUnknownOrNullParent() {
    TestableScribe scribe = new TestableScribe(local);
    Topic topic = new Topic(new PastryIdFactory(env), "topic");

    assertFalse(scribe.allParentsContains(topic, remote.getLocalHandle()));
    assertFalse(scribe.allParentsContains(topic, null));
  }
}
