package rice.p2p.splitstream;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.scribe.ScribeImpl;
import rice.p2p.scribe.ScribeMultiClient;
import rice.p2p.scribe.Topic;
import rice.p2p.scribe.ScribeContent;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.standard.RandomNodeIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StripeTest {

  private static final int MAX_FAILED = 2;
  private static final int STRIPE_BASE = 4;

  /**
   * Counts the subscribe calls a Stripe makes instead of sending them.
   */
  static class CountingScribe extends ScribeImpl {
    int subscribes;

    CountingScribe(PastryNode node) {
      super(node, "StripeTest");
    }

    @Override
    public void subscribe(Topic topic, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
      subscribes++;
    }
  }

  private Environment env;
  private CountingScribe scribe;
  private Stripe stripe;
  private Collection<Topic> topics;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    NetworkSimulator<DirectNodeHandle, RawMessage> simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    PastryNodeFactory factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env);
    PastryNode node = factory.newNode();
    scribe = new CountingScribe(node);
    PastryIdFactory idFactory = new PastryIdFactory(env);
    Channel channel = new Channel(new ChannelId(idFactory.buildId("channel")), scribe, "StripeTest", idFactory, node.getId(), STRIPE_BASE, MAX_FAILED);
    stripe = channel.getStripes()[0];
    topics = List.of(new Topic(stripe.getStripeId().getId()));
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /**
   * Uses up exactly the immediate retries, so no delayed retry (which the simulator's clock
   * could fire at any time) is ever scheduled.
   */
  @Test
  void successResetsTheFailureCount() {
    for (int i = 0; i < MAX_FAILED; i++) {
      stripe.subscribeFailed(topics);
    }
    assertEquals(MAX_FAILED, scribe.subscribes);
    stripe.subscribeSuccess(topics);
    scribe.subscribes = 0;

    stripe.subscribeFailed(topics);

    assertEquals(1, scribe.subscribes);
  }
}
