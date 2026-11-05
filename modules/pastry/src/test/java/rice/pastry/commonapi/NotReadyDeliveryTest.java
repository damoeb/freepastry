package rice.pastry.commonapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.ReadyStrategy;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * In the direct simulator, a two node ring A, B where B is made not ready: an application that asks for consistent
 * routing gets no message another node routed to it, while B is not ready. Messages routed on B itself, and messages
 * for an application that does not ask for consistent routing, are delivered.
 */
class NotReadyDeliveryTest {

  static final short TYPE = 78;
  static final String CONSISTENT = "consistent";
  static final String RELAXED = "relaxed";
  static final long WAIT_SECONDS = 10;

  Environment env;
  PastryNode a;
  PastryNode b;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    PastryNodeFactory factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env),
        new EuclideanNetwork<DirectNodeHandle, RawMessage>(env), env);
    a = factory.newNode();
    b = factory.newNode();
    a.boot((Object) null);
    await(a::isReady, "A is ready");
    b.boot(a.getLocalHandle());
    await(b::isReady, "B is ready");
    await(() -> a.getLeafSet().member(b.getLocalHandle()) && b.getLeafSet().member(a.getLocalHandle()),
        "A and B have each other in their leaf sets");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("while the node is not ready, a message routed to it from another node reaches only applications that do not need consistent routing")
  void aRemoteMessageReachesOnlyARelaxedApplicationWhileNotReady() throws Exception {
    Receiver consistentAtA = onSelector(a, () -> new Receiver(a, CONSISTENT, true));
    Receiver relaxedAtA = onSelector(a, () -> new Receiver(a, RELAXED, false));
    Receiver consistentAtB = onSelector(b, () -> new Receiver(b, CONSISTENT, true));
    Receiver relaxedAtB = onSelector(b, () -> new Receiver(b, RELAXED, false));
    makeNotReady(b);

    // the same hop and delay for both, so the relaxed one arriving means the consistent one has arrived too
    onSelector(a, () -> {
      consistentAtA.endpoint.route(b.getNodeId(), new Numbered(1), null);
      relaxedAtA.endpoint.route(b.getNodeId(), new Numbered(2), null);
      return null;
    });

    await(() -> relaxedAtB.received.contains(2), "B's relaxed application got the message from A");
    assertEquals(List.of(), consistentAtB.received, "numbers B's consistent application got from A while not ready");
  }

  @Test
  @DisplayName("while the node is not ready, a message routed on the node itself is delivered")
  void aLocalMessageIsDeliveredWhileNotReady() throws Exception {
    Receiver consistentAtB = onSelector(b, () -> new Receiver(b, CONSISTENT, true));
    makeNotReady(b);

    onSelector(b, () -> {
      consistentAtB.endpoint.route(b.getNodeId(), new Numbered(3), null);
      return null;
    });

    await(() -> consistentAtB.received.contains(3), "B's consistent application got its own message");
  }

  @Test
  @DisplayName("once the node is ready again, a message routed to it from another node is delivered")
  void aRemoteMessageIsDeliveredWhenReady() throws Exception {
    Receiver consistentAtA = onSelector(a, () -> new Receiver(a, CONSISTENT, true));
    Receiver consistentAtB = onSelector(b, () -> new Receiver(b, CONSISTENT, true));

    onSelector(a, () -> {
      consistentAtA.endpoint.route(b.getNodeId(), new Numbered(4), null);
      return null;
    });

    await(() -> consistentAtB.received.contains(4), "B's consistent application got the message from A");
  }

  /** the leaf set protocol would make the node ready again at once, so a strategy that never is takes over */
  static void makeNotReady(PastryNode node) throws Exception {
    onSelector(node, () -> {
      node.setReadyStrategy(new ReadyStrategy() {
        @Override
        public void setReady(boolean r) {}

        @Override
        public boolean isReady() {
          return false;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}
      });
      return null;
    });
    assertFalse(node.isReady());
  }

  static <T> T onSelector(PastryNode node, Callable<T> task) throws Exception {
    CompletableFuture<T> result = new CompletableFuture<>();
    node.getEnvironment().getSelectorManager().invoke(() -> {
      try {
        result.complete(task.call());
      } catch (Exception | Error e) {
        result.completeExceptionally(e);
      }
    });
    return result.get(WAIT_SECONDS, TimeUnit.SECONDS);
  }

  static void await(BooleanSupplier condition, String what) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(condition.getAsBoolean(), what);
  }

  /** an application that records the numbers of the messages delivered to it */
  static class Receiver implements Application {
    final Endpoint endpoint;
    final List<Integer> received = new CopyOnWriteArrayList<>();

    Receiver(PastryNode node, String instance, boolean consistent) {
      endpoint = node.buildEndpoint(this, instance);
      endpoint.setConsistentRouting(consistent);
      endpoint.setDeserializer((buf, type, priority, sender) -> {
        if (type == TYPE) {
          return new Numbered(buf.readInt());
        }
        throw new IOException("Unknown message type: " + type);
      });
      endpoint.register();
    }

    @Override
    public void deliver(Id id, Message message) {
      received.add(((Numbered) message).number);
    }

    @Override
    public void update(NodeHandle handle, boolean joined) {}

    @Override
    public boolean forward(RouteMessage message) {
      return true;
    }
  }

  static class Numbered implements RawMessage {
    final int number;

    Numbered(int number) {
      this.number = number;
    }

    @Override
    public int getPriority() {
      return Message.LOW_PRIORITY;
    }

    @Override
    public short getType() {
      return TYPE;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(number);
    }
  }
}
