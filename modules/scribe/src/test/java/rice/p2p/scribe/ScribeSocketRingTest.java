package rice.p2p.scribe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.splitstream.Channel;
import rice.p2p.splitstream.ChannelId;
import rice.p2p.splitstream.SplitStreamImpl;
import rice.p2p.splitstream.Stripe;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;

/**
 * Scribe and SplitStream on a real socket ring on loopback: every subscribe, publish and anycast crosses the wire as
 * bytes and is read, and sent on, by the nodes on its way. The direct simulator hands the message objects over, so
 * only a ring like this one runs that code.
 */
class ScribeSocketRingTest {
  private static final int NODES = 6;
  private static final long SECONDS = 10;
  private static final short CONTENT_TYPE = 1;

  private SocketRing ring;

  @BeforeEach
  void setUp() {
    ring = new SocketRing();
  }

  @AfterEach
  void tearDown() {
    ring.close();
  }

  /** The content of the test: the node that sent it and a number. */
  record Content(NodeHandle from, int seq) implements ScribeContent {
    static Content read(InputBuffer buf, Endpoint endpoint) throws IOException {
      return new Content(endpoint.readNodeHandle(buf), buf.readInt());
    }

    @Override
    public short getType() {
      return CONTENT_TYPE;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      from.serialize(buf);
      buf.writeInt(seq);
    }
  }

  /** Counts what a node's client receives. */
  static final class Client implements ScribeMultiClient {
    final ScribeImpl scribe;
    final Map<Integer, Content> delivered = new ConcurrentHashMap<>();
    final Set<Integer> anycasts = ConcurrentHashMap.newKeySet();
    final CountDownLatch subscribed = new CountDownLatch(1);
    volatile boolean failed;
    volatile CountDownLatch deliveries = new CountDownLatch(0);
    volatile CountDownLatch anycastLatch = new CountDownLatch(0);

    Client(ScribeImpl scribe) {
      this.scribe = scribe;
    }

    @Override
    public boolean anycast(Topic topic, ScribeContent content) {
      anycasts.add(((Content) content).seq());
      anycastLatch.countDown();
      return true;
    }

    @Override
    public void deliver(Topic topic, ScribeContent content) {
      Content typedContent = (Content) content;
      if (delivered.putIfAbsent(typedContent.seq(), typedContent) == null) {
        deliveries.countDown();
      }
    }

    @Override
    public void childAdded(Topic topic, NodeHandle child) {
    }

    @Override
    public void childRemoved(Topic topic, NodeHandle child) {
    }

    @Override
    public void subscribeFailed(Collection<Topic> topics) {
      failed = true;
    }

    @Override
    public void subscribeSuccess(Collection<Topic> topics) {
      subscribed.countDown();
    }
  }

  @Test
  @DisplayName("on a six node socket ring every subscriber gets a publish, an anycast is accepted, the tree has children")
  void scribe() throws Exception {
    List<Client> clients = ring.boot(NODES, node -> {
      ScribeImpl scribe = new ScribeImpl(node, "socket-ring");
      scribe.setContentDeserializer((buf, endpoint, type) -> {
        if (type == CONTENT_TYPE) {
          return Content.read(buf, endpoint);
        }
        throw new IOException("Unknown content type " + type);
      });
      return new Client(scribe);
    });
    Topic topic = new Topic(new PastryIdFactory(ring.env), "socket ring topic");

    for (Client client : clients) {
      client.scribe.subscribe(topic, client, null, null);
    }
    for (Client client : clients) {
      ring.await("subscribe of " + client.scribe.getEndpoint().getId(), SECONDS,
          () -> client.subscribed.getCount() == 0 || client.scribe.isRoot(topic));
      assertTrue(!client.failed, "subscribe failed at " + client.scribe.getEndpoint().getId());
    }
    int children = 0;
    for (Client client : clients) {
      children += client.scribe.getChildrenOfTopic(topic).size();
    }
    assertEquals(NODES - 1, children, "every node but the root is the child of one node");

    // publish from the last node, which is not the root of every topic
    for (Client client : clients) {
      client.deliveries = new CountDownLatch(1);
    }
    Client publisher = clients.get(NODES - 1);
    publisher.scribe.publish(topic, new Content(publisher.scribe.getEndpoint().getLocalNodeHandle(), 1));
    for (Client client : clients) {
      ring.await("publish delivered at " + client.scribe.getEndpoint().getId(), SECONDS, client.deliveries);
    }

    // anycast from each node: some member accepts it
    for (int i = 0; i < NODES; i++) {
      Client from = clients.get(i);
      from.scribe.anycast(topic, new Content(from.scribe.getEndpoint().getLocalNodeHandle(), 100 + i));
    }
    ring.await("every anycast accepted", SECONDS, () -> {
      Set<Integer> accepted = ConcurrentHashMap.newKeySet();
      clients.forEach(c -> accepted.addAll(c.anycasts));
      return accepted.size() == NODES;
    });
    ring.assertNoExceptions();
  }

  @Test
  @DisplayName("on a six node socket ring SplitStream delivers every stripe's data on every node")
  void splitStream() throws Exception {
    List<SplitStreamImpl> apps = ring.boot(NODES, node -> new SplitStreamImpl(node, "socket-ring"));
    ChannelId channelId = new ChannelId(new PastryIdFactory(ring.env).buildId("socket ring channel"));
    List<Map<Integer, Boolean>> received = new ArrayList<>();
    List<Stripe[]> stripes = new ArrayList<>();
    for (SplitStreamImpl app : apps) {
      Channel channel = app.attachChannel(channelId);
      Map<Integer, Boolean> got = new ConcurrentHashMap<>();
      received.add(got);
      Stripe[] mine = channel.getStripes();
      stripes.add(mine);
      for (Stripe stripe : mine) {
        stripe.subscribe((s, data) -> got.put((int) data[0], true));
      }
    }
    int numStripes = stripes.get(0).length;
    ring.await("every stripe has a tree", SECONDS, () -> stripes.stream()
        .allMatch(mine -> java.util.Arrays.stream(mine).allMatch(s -> s.isRoot() || s.getParent() != null)));

    Stripe[] publisher = stripes.get(NODES - 1);
    for (int i = 0; i < numStripes; i++) {
      publisher[i].publish(new byte[] {(byte) i, 42});
    }
    for (int n = 0; n < NODES; n++) {
      Map<Integer, Boolean> got = received.get(n);
      ring.await("node " + n + " got the data of all " + numStripes + " stripes", SECONDS,
          () -> got.size() == numStripes);
    }
    ring.assertNoExceptions();
  }
}
