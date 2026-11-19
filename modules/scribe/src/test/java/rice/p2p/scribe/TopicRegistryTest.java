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
  private final Handle handle = new Handle(3);
  private TopicRegistry registry;

  @BeforeEach
  void setUp() {
    FakeEndpoint fake = new FakeEndpoint(local);
    registry = new TopicRegistry(fake.endpoint, local, ScribeTestSupport.listener(), LOGGER);
  }

  @Test
  void topicsByParentIsASnapshot() {
    Topic topic = ScribeTestSupport.topic(1);
    registry.addToAllParents(topic, handle);

    Collection<Topic> snapshot = registry.getTopicsByParent(handle);
    registry.removeFromAllParents(topic, handle);

    assertEquals(1, snapshot.size());
    assertTrue(registry.getTopicsByParent(handle).isEmpty());
  }

  @Test
  void topicsByChildIsASnapshot() {
    Topic topic = ScribeTestSupport.topic(1);
    registry.addToAllChildren(topic, handle);

    Collection<Topic> snapshot = registry.getTopicsByChild(handle);
    registry.removeFromAllChildren(topic, handle);

    assertEquals(1, snapshot.size());
    assertTrue(registry.getTopicsByChild(handle).isEmpty());
  }

  @Test
  void destroyForgetsParentsAndChildren() {
    Topic topic = ScribeTestSupport.topic(1);
    registry.addToAllParents(topic, handle);
    registry.addToAllChildren(topic, handle);

    registry.destroy();

    assertTrue(registry.getTopicsByParent(handle).isEmpty());
    assertTrue(registry.getTopicsByChild(handle).isEmpty());
    assertEquals(new ArrayList<Topic>(), new ArrayList<>(registry.topics()));
  }
}
