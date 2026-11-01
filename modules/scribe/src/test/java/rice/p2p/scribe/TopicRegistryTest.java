package rice.p2p.scribe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rice.p2p.scribe.ScribeTestSupport.FakeEndpoint;
import rice.p2p.scribe.ScribeTestSupport.Handle;

class TopicRegistryTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopicRegistryTest.class);

  private final Handle local = new Handle(1);
  private final Handle other = new Handle(3);
  private TopicRegistry registry;

  @BeforeEach
  void setUp() {
    FakeEndpoint fake = new FakeEndpoint(local);
    registry = new TopicRegistry(fake.endpoint, local, ScribeTestSupport.listener(), LOGGER);
  }

  @Test
  void topicsByParentIsASnapshot() {
    Topic a = ScribeTestSupport.topic(1);
    registry.addToAllParents(a, other);

    Collection<Topic> snapshot = registry.getTopicsByParent(other);
    registry.removeFromAllParents(a, other);

    assertEquals(1, snapshot.size());
    assertTrue(registry.getTopicsByParent(other).isEmpty());
  }

  @Test
  void topicsByChildIsASnapshot() {
    Topic a = ScribeTestSupport.topic(1);
    registry.addToAllChildren(a, other);

    Collection<Topic> snapshot = registry.getTopicsByChild(other);
    registry.removeFromAllChildren(a, other);

    assertEquals(1, snapshot.size());
    assertTrue(registry.getTopicsByChild(other).isEmpty());
  }

  @Test
  void destroyForgetsParentsAndChildren() {
    Topic a = ScribeTestSupport.topic(1);
    registry.addToAllParents(a, other);
    registry.addToAllChildren(a, other);

    registry.destroy();

    assertTrue(registry.getTopicsByParent(other).isEmpty());
    assertTrue(registry.getTopicsByChild(other).isEmpty());
    assertEquals(new ArrayList<Topic>(), new ArrayList<>(registry.topics()));
  }
}
