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
  private final List<Collection<Topic>> failures = new ArrayList<>();

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
    public void subscribeFailed(Collection<Topic> topics) {
      failures.add(topics);
    }

    @Override
    public void subscribeSuccess(Collection<Topic> topics) {
      successes.add(topics);
    }
  };

  @BeforeEach
  void setUp() {
    fake = new FakeEndpoint(local);
    registry = new TopicRegistry(fake.endpoint, local, ScribeTestSupport.listener(), LOGGER);
    manager = new SubscriptionManager(fake.endpoint, local, null, registry, () -> null, null, null,
        new ScribeConfig(180000, 1000, 4096, 64, 4096, 256), () -> 0L, LOGGER);
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
    Topic topic = ScribeTestSupport.topic(1);
    Topic b = ScribeTestSupport.topic(2);
    Topic c = ScribeTestSupport.topic(3);
    int id = subscribe(new ArrayList<>(List.of(topic, b, c)));
    for (Topic t : List.of(topic, b, c)) registry.create(t);

    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(topic, b, c)), emptyPaths(3), id));

    assertEquals(1, successes.size());
  }

  @Test
  void replacedParentIsUnsubscribedOnce() {
    Topic topic = ScribeTestSupport.topic(1);
    Topic b = ScribeTestSupport.topic(2);
    Handle oldParent = new Handle(3);
    int id = subscribe(new ArrayList<>(List.of(topic, b)));
    for (Topic t : List.of(topic, b)) registry.create(t).setParent(oldParent, new ArrayList<>());

    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(topic, b)), emptyPaths(2), id));

    List<Routed> unsubscribes = unsubscribes();
    assertEquals(1, unsubscribes.size());
    assertEquals(oldParent, unsubscribes.get(0).hint());
    assertEquals(List.of(topic, b), ((UnsubscribeMessage) unsubscribes.get(0).message()).topics());
  }

  @Test
  void ackForUnknownTopicIsAnsweredWithUnsubscribe() {
    Topic topic = ScribeTestSupport.topic(1);
    int id = subscribe(new ArrayList<>(List.of(topic)));
    registry.remove(topic);

    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(topic)), emptyPaths(1), id));

    List<Routed> unsubscribes = unsubscribes();
    assertEquals(1, unsubscribes.size());
    assertEquals(fake.remote, unsubscribes.get(0).hint());
    assertEquals(List.of(topic), ((UnsubscribeMessage) unsubscribes.get(0).message()).topics());
  }

  @Test
  void ackForRootTopicIsAnsweredWithUnsubscribe() {
    Topic topic = ScribeTestSupport.topic(1);
    int id = subscribe(new ArrayList<>(List.of(topic)));
    registry.create(topic);
    fake.rootTopics.add(topic);

    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(topic)), emptyPaths(1), id));

    List<Routed> unsubscribes = unsubscribes();
    assertEquals(1, unsubscribes.size());
    assertEquals(fake.remote, unsubscribes.get(0).hint());
    assertEquals(List.of(topic), ((UnsubscribeMessage) unsubscribes.get(0).message()).topics());
  }

  @Test
  void lostMessageForCompletedSubscribeIsIgnored() {
    Topic topic = ScribeTestSupport.topic(1);
    int id = subscribe(new ArrayList<>(List.of(topic)));
    registry.create(topic);
    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(topic)), emptyPaths(1), id));

    // the timer already fired for an ack that completed the subscribe: must not throw
    manager.lostMessageReceived(new rice.p2p.scribe.messaging.SubscribeLostMessage(local, List.of(topic), id, client));
  }

  @Test
  void unsolicitedAckDoesNotReplaceTheParent() {
    Topic topic = ScribeTestSupport.topic(1);
    Handle parent = new Handle(3);
    registry.create(topic).setParent(parent, new ArrayList<>());

    // no subscribe is outstanding: neither a made-up id nor the maintenance id binds the ack to one
    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(topic)), emptyPaths(1), 42));
    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(topic)), emptyPaths(1),
        rice.p2p.scribe.maintenance.MaintainableScribe.MAINTENANCE_ID));

    assertEquals(parent, registry.getParent(topic));
    // the sender is told that we do not want it, so it does not keep us as a child
    assertEquals(List.of(fake.remote, fake.remote), unsubscribes().stream().map(Routed::hint).toList());
  }

  @Test
  void ackForAnotherSubscribesTopicDoesNotReplaceTheParent() {
    Topic asked = ScribeTestSupport.topic(1);
    Topic other = ScribeTestSupport.topic(2);
    Handle parent = new Handle(3);
    int id = subscribe(new ArrayList<>(List.of(asked)));
    registry.create(asked);
    registry.create(other).setParent(parent, new ArrayList<>());

    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(other)), emptyPaths(1), id));

    assertEquals(parent, registry.getParent(other));
  }

  @Test
  void ackFromTheCurrentParentKeepsIt() {
    Topic topic = ScribeTestSupport.topic(1);
    registry.create(topic).setParent(fake.remote, new ArrayList<>());
    List<List<Id>> path = List.of(List.of(ScribeTestSupport.id(7)));

    manager.subscribeAckReceived(new SubscribeAckMessage(fake.remote, new ArrayList<>(List.of(topic)), path,
        rice.p2p.scribe.maintenance.MaintainableScribe.MAINTENANCE_ID));

    assertEquals(fake.remote, registry.getParent(topic));
    assertEquals(List.of(), unsubscribes());
  }

  @Test
  void subscribesBeyondThePendingBoundAreRefused() {
    int subscribes = 5000;
    for (int i = 0; i < subscribes; i++) {
      Topic topic = ScribeTestSupport.topic(10 + i);
      registry.create(topic);
      manager.sendSubscribe(new ArrayList<>(List.of(topic)), client, null, fake.remote);
    }

    long sent = fake.routed.stream().filter(r -> r.message() instanceof SubscribeMessage).count();
    assertEquals(4096, sent, "subscribes sent while 4096 are outstanding");
    assertEquals(subscribes - 4096, failures.size(), "the refused ones are reported as failed");
  }
}
