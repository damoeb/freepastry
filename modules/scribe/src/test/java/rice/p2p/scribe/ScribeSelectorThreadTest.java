package rice.p2p.scribe;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.ScribeSocketRingTest.Client;
import rice.pastry.PastryNode;
import rice.pastry.testing.SocketRing;

/**
 * Scribe's tree state lives on the selector thread. An application that subscribes from its own thread must not touch
 * that state directly: a subscribe from the test thread raced the selector on the pending set and corrupted a HashMap
 * (ClassCastException HashMap$Node to HashMap$TreeNode in SplitStreamRegressionTest).
 */
class ScribeSelectorThreadTest {
  private static final String INSTANCE = "selector-thread";
  private static final Topic TOPIC = new Topic(rice.pastry.Id.build("40"));

  private SocketRing ring;

  /** Records on which thread the subscribe work runs. */
  static final class RecordingScribe extends ScribeImpl {
    final List<Boolean> onSelector = new CopyOnWriteArrayList<>();
    final CountDownLatch subscribed = new CountDownLatch(1);

    RecordingScribe(PastryNode node) {
      super(node, INSTANCE);
    }

    @Override
    protected void doSubscribe(java.util.Collection<Topic> topics, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
      onSelector.add(getEnvironment().getSelectorManager().isSelectorThread());
      super.doSubscribe(topics, client, content, hint);
      subscribed.countDown();
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

  @Test
  @DisplayName("a subscribe from an application thread runs on the selector thread")
  void subscribeFromAnotherThreadRunsOnTheSelector() throws Exception {
    RecordingScribe scribe = ring.boot(1, RecordingScribe::new).get(0);
    Client client = new Client(scribe);

    scribe.subscribe(TOPIC, client, null, null);

    scribe.subscribed.await(10, TimeUnit.SECONDS);
    assertEquals(List.of(true), scribe.onSelector, "the subscribe work ran on the selector thread");
    ring.assertNoExceptions();
  }
}
