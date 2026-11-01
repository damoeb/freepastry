package rice.p2p.scribe;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.ScribeTestSupport.FakeEndpoint;
import rice.p2p.scribe.ScribeTestSupport.Handle;
import rice.p2p.scribe.ScribeTestSupport.Routed;
import rice.p2p.scribe.messaging.SubscribeAckMessage;
import rice.p2p.scribe.messaging.SubscribeMessage;
import rice.p2p.scribe.messaging.UnsubscribeMessage;

class SubscriptionManagerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionManagerTest.class);

  private final Handle local = new Handle(1);
  private FakeEndpoint fake;
  private TopicRegistry registry;
  private SubscriptionManager manager;
  private final List<Collection<Topic>> successes = new ArrayList<>();

  private final ScribeMultiClient client = new ScribeMultiClient() {
    @Override
    public boolean anycast(Topic topic, ScribeContent content) {
      return false;
    }

    @Override
    public void deliver(Topic topic, ScribeContent content) {}

    @Override
    public void childAdded(Topic topic, NodeHandle child) {}

    @Override
    public void childRemoved(Topic topic, NodeHandle child) {}

    @Override
    public void subscribeFailed(Collection<Topic> topics) {}

    @Override
    public void subscribeSuccess(Collection<Topic> topics) {
      successes.add(topics);
    }
  };

  @BeforeEach
  void setUp() {
    fake = new FakeEndpoint(local);
    registry = new TopicRegistry(fake.endpoint, local, ScribeTestSupport.listener(), LOGGER);
    manager = new SubscriptionManager(fake.endpoint, local, null, registry, () -> null, null, null, 1000, LOGGER);
  }

  /** Sends a subscribe for the topics and returns its message id. */
  private int subscribe(List<Topic> topics) {
    manager.sendSubscribe(topics, client, null, fake.remote);
    return fake.routed.stream().map(Routed::message).filter(SubscribeMessage.class::isInstance)
        .map(SubscribeMessage.class::cast).reduce((a, b) -> b).orElseThrow().getId();
  }

  private static List<List<Id>> emptyPaths(int n) {
    List<List<Id>> paths = new ArrayList<>();
    for (int i = 0; i < n; i++) paths.add(new ArrayList<>());
    return paths;
  }

  private List<Routed> unsubscribes() {
    return fake.routed.stream().filter(r -> r.message() instanceof UnsubscribeMessage).toList();
  }

  @Test
  void multiTopicAckReportsSubscribeSuccessOnce() {
    Topic a = ScribeTestSupport.topic(1);
    Topic b = ScribeTestSupport.topic(2);
    Topic c = ScribeTestSupport.topic(3);
    int id = subscribe(new ArrayList<>(List.of(a, b, c)));
    for (Topic t : List.of(a, b, c)) registry.create(t);

    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(a, b, c)), emptyPaths(3), id));

    assertEquals(1, successes.size());
  }

  @Test
  void replacedParentIsUnsubscribedOnce() {
    Topic a = ScribeTestSupport.topic(1);
    Topic b = ScribeTestSupport.topic(2);
    Handle oldParent = new Handle(3);
    int id = subscribe(new ArrayList<>(List.of(a, b)));
    for (Topic t : List.of(a, b)) registry.create(t).setParent(oldParent, new ArrayList<>());

    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(a, b)), emptyPaths(2), id));

    List<Routed> unsubscribes = unsubscribes();
    assertEquals(1, unsubscribes.size());
    assertEquals(oldParent, unsubscribes.get(0).hint());
    assertEquals(List.of(a, b), ((UnsubscribeMessage) unsubscribes.get(0).message()).topics());
  }
}
