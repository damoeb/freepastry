package rice.p2p.glacier.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierMessage;
import rice.p2p.glacier.v2.messaging.GlacierNeighborResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshCompleteMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;

/**
 * A peer controls the response flag of every Glacier message it sends. A message of a response type that arrives
 * flagged as a request must cost that message, not the node.
 */
@DisplayName("Glacier message dispatcher")
class GlacierMessageDispatcherTest {
  private static final long TIMEOUT_SECONDS = 10;
  /** Version byte, uid (int) and tag (char) precede the response flag on the wire. */
  private static final int RESPONSE_FLAG_OFFSET = 1 + Integer.BYTES + Character.BYTES;

  private Environment env;
  private Endpoint endpoint;
  private NodeHandle peer;
  private final List<Throwable> reported = new CopyOnWriteArrayList<>();

  @BeforeEach
  void setUp() throws IOException {
    // a socket node, so that handles and ids are written and read the way they are on the wire
    env = new Environment();
    var original = env.getExceptionStrategy();
    env.setExceptionStrategy((source, t) -> {
      reported.add(t);
      original.handleException(source, t);
    });
    SocketNode node = SocketNode.create(env, "4000");
    endpoint = node.node.buildEndpoint(new Application() {
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
    }, "glacier-dispatch");
    peer = node.handle("8000");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  static Stream<String> responseTypes() {
    return Stream.of("neighbor response", "range response", "refresh complete", "refresh response", "response");
  }

  private GlacierMessage responseMessage(String type) {
    Id dest = peer.getId();
    IdRange range = new rice.pastry.IdRange(rice.pastry.Id.build("2000"), rice.pastry.Id.build("6000"));
    return switch (type) {
      case "neighbor response" -> new GlacierNeighborResponseMessage(1, new Id[] {dest}, new long[] {0}, peer, dest, GlacierTags.NEIGHBOR);
      case "range response" -> new GlacierRangeResponseMessage(1, range, peer, dest, GlacierTags.SYNC);
      case "refresh complete" -> new GlacierRefreshCompleteMessage(1, new VersionKey[] {new VersionKey(dest, 1)}, new int[] {0}, peer, dest, GlacierTags.REFRESH);
      case "refresh response" -> new GlacierRefreshResponseMessage(1, range, true, peer, dest, GlacierTags.REFRESH);
      case "response" -> new GlacierResponseMessage(1, new FragmentKey(new VersionKey(dest, 1), 0), true, 0, true, peer, dest, true, GlacierTags.LOOKUP);
      default -> throw new IllegalArgumentException(type);
    };
  }

  /** The message as a peer sends it with the response flag cleared, read the way the node reads it from the wire. */
  private GlacierMessage forgedAsRequest(GlacierMessage message) throws IOException {
    byte[] bytes = WireRoundTrip.bytes(message);
    assertEquals(1, bytes[RESPONSE_FLAG_OFFSET], "the response flag is where the test expects it");
    bytes[RESPONSE_FLAG_OFFSET] = 0;
    return (GlacierMessage) GlacierMessageDispatcher.deserializer(endpoint)
        .deserialize(new SimpleInputBuffer(bytes), message.getType(), 0, peer);
  }

  private void awaitSelectorLoop() throws Exception {
    CompletableFuture<Void> looped = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> looped.complete(null));
    try {
      looped.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException dropped) {
      // a destroyed selector drops the invocation, the assertions tell why
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("responseTypes")
  @DisplayName("a response type that arrives as a request is dropped and the environment survives")
  void responseTypeArrivingAsRequestIsDropped(String type) throws Exception {
    GlacierMessage forged = forgedAsRequest(responseMessage(type));
    assertFalse(forged.isResponse());
    // no collaborator is needed: a request of a response type must not reach any of them
    GlacierMessageDispatcher testObj = new GlacierMessageDispatcher(null, null, null, null, null, null, null,
        LoggerFactory.getLogger(GlacierMessageDispatcherTest.class));

    env.getSelectorManager().invoke(() -> testObj.deliver(peer.getId(), forged));

    awaitSelectorLoop();
    assertEquals(List.of(), reported);
    assertTrue(env.getSelectorManager().isAlive(), "the selector thread still runs");
  }
}
