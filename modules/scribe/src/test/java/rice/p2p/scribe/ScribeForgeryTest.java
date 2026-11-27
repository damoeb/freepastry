package rice.p2p.scribe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.scribe.ScribeSocketRingTest.Client;
import rice.p2p.scribe.ScribeSocketRingTest.Content;
import rice.p2p.scribe.maintenance.MaintainableScribe;
import rice.p2p.scribe.messaging.PublishMessage;
import rice.p2p.scribe.messaging.SubscribeAckMessage;
import rice.p2p.scribe.messaging.SubscribeMessage;
import rice.p2p.scribe.messaging.UnsubscribeMessage;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryEndpoint;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.standard.StandardAddress;
import rice.pastry.testing.SocketRing;

/**
 * A node of a socket ring forges Scribe tree messages: it names another node as their source, or sends messages
 * nobody asked for, or floods a node with subscriptions. The honest nodes run Scribe; the forger runs only an
 * endpoint of the same instance that sends what the test tells it to and ignores what it receives.
 */
class ScribeForgeryTest {
  private static final String INSTANCE = "forgery";
  private static final long SECONDS = 10;
  /** how long a forged message gets to take effect before the test checks that it did not */
  private static final long QUIET_MILLIS = 1500;
  /** the root of this topic is node "60", an honest one; the forger is node "D0" */
  private static final Topic TOPIC = new Topic(rice.pastry.Id.build("40"));

  private SocketRing ring;
  private List<Client> clients;
  private Forger forger;

  /** Sends what the test asks for, ignores what arrives. */
  static final class Forger implements Application {
    final Endpoint endpoint;

    Forger(PastryNode node) {
      // the address of the honest nodes' Scribe endpoints, which derives from the application's class
      endpoint = new PastryEndpoint(node, this, INSTANCE, StandardAddress.getAddress(ScribeImpl.class, INSTANCE, node.getEnvironment()));
      endpoint.setDeserializer(new ScribeMessageDeserializer(endpoint, () -> (buf, ep, type) -> Content.read(buf, ep), INSTANCE,
          LoggerFactory.getLogger(Forger.class)));
      endpoint.register();
    }

    void sendDirect(rice.p2p.commonapi.rawserialization.RawMessage message, NodeHandle to) {
      endpoint.route(null, message, to);
    }

    @Override
    public boolean forward(RouteMessage message) {
      return true;
    }

    @Override
    public void deliver(Id id, Message message) {
    }

    @Override
    public void update(NodeHandle handle, boolean joined) {
    }
  }

  @BeforeEach
  void setUp() {
    ring = new SocketRing();
  }

  @AfterEach
  void tearDown() {
    ring.close();
  }

  private static ScribeImpl scribe(PastryNode node) {
    ScribeImpl scribe = new ScribeImpl(node, INSTANCE);
    scribe.setContentDeserializer((buf, endpoint, type) -> {
      if (type == 1) {
        return Content.read(buf, endpoint);
      }
      throw new IOException("Unknown content type " + type);
    });
    return scribe;
  }

  /** Boots honest nodes "00", "80", "A0", "60" and the forger "D0", and subscribes the honest ones to the topic. */
  private void bootAndSubscribe() throws Exception {
    List<Object> apps = ring.boot(5, node -> node.getNodeId().equals(rice.pastry.Id.build("D0")) ? new Forger(node) : new Client(scribe(node)));
    clients = new ArrayList<>();
    for (Object app : apps) {
      if (app instanceof Client client) {
        clients.add(client);
      } else {
        forger = (Forger) app;
      }
    }
    for (Client client : clients) {
      client.scribe.subscribe(TOPIC, client, null, null);
    }
    for (Client client : clients) {
      ring.await("subscribe of " + client.scribe.getEndpoint().getId(), SECONDS,
          () -> client.subscribed.getCount() == 0 || client.scribe.isRoot(TOPIC));
    }
  }

  /** A node that is not the root, and so has a parent. */
  private Client child() {
    return clients.stream().filter(c -> !c.scribe.isRoot(TOPIC)).findFirst().orElseThrow();
  }

  private Client clientOf(NodeHandle handle) {
    return clients.stream().filter(c -> c.scribe.getEndpoint().getId().equals(handle.getId())).findFirst().orElseThrow();
  }

  private static NodeHandle handle(Client client) {
    return client.scribe.getEndpoint().getLocalNodeHandle();
  }

  /** Fails as soon as the condition does not hold, or passes after it held for the quiet period. */
  private void assertStays(String what, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(QUIET_MILLIS);
    while (System.nanoTime() < deadline) {
      assertTrue(condition.getAsBoolean(), what);
      Thread.sleep(20);
    }
    assertTrue(condition.getAsBoolean(), what);
    ring.assertNoExceptions();
  }

  @Test
  @DisplayName("a subscribe ack nobody asked for does not make its sender the parent")
  void unsolicitedAckDoesNotReplaceTheParent() throws Exception {
    bootAndSubscribe();
    Client victim = child();
    NodeHandle parent = victim.scribe.getParent(TOPIC);
    assertNotNull(parent);

    forger.sendDirect(new SubscribeAckMessage(forger.endpoint.getLocalNodeHandle(), List.of(TOPIC), List.of(List.of()),
        MaintainableScribe.MAINTENANCE_ID), handle(victim));
    forger.sendDirect(new SubscribeAckMessage(forger.endpoint.getLocalNodeHandle(), List.of(TOPIC), List.of(List.of()), 12345),
        handle(victim));

    assertStays("the parent stays " + parent, () -> parent.equals(victim.scribe.getParent(TOPIC)));
  }

  @Test
  @DisplayName("a subscribe ack that names another node as its source does not make that node the parent")
  void ackNamingAnotherSourceDoesNotReplaceTheParent() throws Exception {
    bootAndSubscribe();
    Client victim = child();
    NodeHandle parent = victim.scribe.getParent(TOPIC);
    NodeHandle other = clients.stream().map(ScribeForgeryTest::handle).filter(h -> !h.equals(parent) && !h.equals(handle(victim)))
        .findFirst().orElseThrow();

    forger.sendDirect(new SubscribeAckMessage(other, List.of(TOPIC), List.of(List.of()), MaintainableScribe.MAINTENANCE_ID),
        handle(victim));

    assertStays("the parent stays " + parent, () -> parent.equals(victim.scribe.getParent(TOPIC)));
  }

  @Test
  @DisplayName("an unsubscribe that names an honest child does not remove it")
  void forgedUnsubscribeKeepsTheChild() throws Exception {
    bootAndSubscribe();
    Client victimChild = child();
    Client parent = clientOf(victimChild.scribe.getParent(TOPIC));
    assertTrue(parent.scribe.getChildrenOfTopic(TOPIC).contains(handle(victimChild)));

    forger.sendDirect(new UnsubscribeMessage(handle(victimChild), List.of(TOPIC)), handle(parent));

    assertStays("the child stays", () -> parent.scribe.getChildrenOfTopic(TOPIC).contains(handle(victimChild)));
  }

  @Test
  @DisplayName("a publish that names the parent as its source but comes from another node is not delivered")
  void spoofedPublishIsNotDelivered() throws Exception {
    bootAndSubscribe();
    Client victim = child();
    NodeHandle parent = victim.scribe.getParent(TOPIC);

    forger.sendDirect(new PublishMessage(parent, TOPIC, new Content(parent, 999)), handle(victim));

    assertStays("the forged content is not delivered", () -> !victim.delivered.containsKey(999));
  }

  @Test
  @DisplayName("one subscribe message with many new topics creates no more topics on a node than the bound")
  void subscribeFloodStopsAtTheTopicBound() throws Exception {
    ring.env.getParameters().setInt("p2p_scribe_max_topics", 8);
    bootAndSubscribe();
    Client victim = child();
    PastryIdFactory idFactory = new PastryIdFactory(ring.env);
    List<Topic> topics = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      topics.add(new Topic(idFactory, "flood " + i));
    }

    forger.sendDirect(new SubscribeMessage(forger.endpoint.getLocalNodeHandle(), topics, 777, null), handle(victim));

    assertStays("no node holds more than 8 topics", () -> clients.stream().allMatch(c -> c.scribe.getTopics().size() <= 8));
  }

  @Test
  @DisplayName("with at most two children per topic every node still joins the tree and gets a publish")
  void childrenBoundKeepsTheTreeWorking() throws Exception {
    ring.env.getParameters().setInt("p2p_scribe_max_children_per_topic", 2);
    List<Client> honest = ring.boot(6, node -> new Client(scribe(node)));
    for (Client client : honest) {
      client.scribe.subscribe(TOPIC, client, null, null);
    }
    for (Client client : honest) {
      ring.await("subscribe of " + client.scribe.getEndpoint().getId(), SECONDS,
          () -> client.subscribed.getCount() == 0 || client.scribe.isRoot(TOPIC));
    }
    for (Client client : honest) {
      assertTrue(client.scribe.getChildrenOfTopic(TOPIC).size() <= 2,
          client.scribe.getEndpoint().getId() + " has " + client.scribe.getChildrenOfTopic(TOPIC));
    }
    int children = honest.stream().mapToInt(c -> c.scribe.getChildrenOfTopic(TOPIC).size()).sum();
    assertEquals(honest.size() - 1, children, "every node but the root is the child of one node");

    for (Client client : honest) {
      client.deliveries = new java.util.concurrent.CountDownLatch(1);
    }
    Client publisher = honest.get(honest.size() - 1);
    publisher.scribe.publish(TOPIC, new Content(handle(publisher), 1));
    for (Client client : honest) {
      ring.await("publish delivered at " + client.scribe.getEndpoint().getId(), SECONDS, client.deliveries);
    }
    ring.assertNoExceptions();
  }
}
