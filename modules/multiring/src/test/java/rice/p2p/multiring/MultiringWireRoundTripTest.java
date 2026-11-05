package rice.p2p.multiring;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.multiring.messaging.MultiringContentType;
import rice.p2p.multiring.messaging.RingMessage;
import rice.p2p.scribe.ScribeImpl;
import rice.p2p.scribe.Topic;
import rice.p2p.scribe.messaging.AnycastMessage;
import rice.p2p.scribe.messaging.DropMessage;
import rice.p2p.scribe.messaging.PublishMessage;
import rice.p2p.scribe.messaging.PublishRequestMessage;
import rice.p2p.scribe.messaging.SubscribeMessage;
import rice.p2p.scribe.messaging.UnsubscribeMessage;
import rice.p2p.splitstream.SplitStreamSubscribeContent;
import rice.pastry.Id;
import rice.pastry.IdRange;
import rice.pastry.NodeSet;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;

/**
 * The values of a multiring node through the bytes of a socket ring: ring ids, handles, handle sets and id ranges,
 * and the ring message that carries another application's message, on its own and inside the Scribe messages that
 * carry it over the ring.
 */
@DisplayName("Multiring wire round trip")
class MultiringWireRoundTripTest {
  private static final String APPLICATION = "ring";
  private static final Id RING = Id.build("7777");

  private static Environment env;
  private static SocketNode local;
  private static Endpoint endpoint;
  private static NodeHandle self;
  private static NodeHandle a;
  private static NodeHandle b;

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    local = SocketNode.create(env, "4000");
    ScribeImpl scribe = new ScribeImpl(local.node, APPLICATION);
    endpoint = scribe.getEndpoint();
    scribe.setContentDeserializer((buf, ep, type) -> switch (type) {
      // type 1 is the ring message of the multiring, so SplitStream's content (also 1) is left out
      case 2 -> new SplitStreamSubscribeContent(buf);
      default -> {
        if (type != MultiringContentType.RING_MESSAGE.id()) {
          throw new IOException("Invalid type:" + type);
        }
        yield new RingMessage(buf, ep, Map.of(APPLICATION, endpoint));
      }
    });
    self = local.local();
    a = local.handle("1000");
    b = local.handle("8000");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  @ParameterizedTest
  @ValueSource(strings = { "0000", "1234", "FFFF" })
  @DisplayName("RingId")
  void ringId(String id) throws IOException {
    RingId target = RingId.build(RING, Id.build(id));

    RingId copy = WireRoundTrip.roundTrip(target, RingId::serialize, buf -> RingId.build(buf, endpoint));

    assertEquals(target, copy);
    assertEquals(RING, copy.getRingId());
  }

  @Test
  @DisplayName("MultiringNodeHandle")
  void multiringNodeHandle() throws IOException {
    for (NodeHandle handle : List.of(self, a, b)) {
      MultiringNodeHandle target = new MultiringNodeHandle(RING, handle);

      MultiringNodeHandle copy = WireRoundTrip.roundTrip(target, MultiringNodeHandle::serialize, buf -> new MultiringNodeHandle(buf, endpoint));

      assertEquals(target, copy);
      assertEquals(RING, copy.getRingId());
      assertEquals(target.getId(), copy.getId());
    }
  }

  @Test
  @DisplayName("MultiringNodeHandleSet")
  void multiringNodeHandleSet() throws IOException {
    NodeSet empty = new NodeSet();
    NodeSet one = new NodeSet();
    one.put((rice.pastry.NodeHandle) a);
    NodeSet many = new NodeSet();
    many.put((rice.pastry.NodeHandle) self);
    many.put((rice.pastry.NodeHandle) a);
    many.put((rice.pastry.NodeHandle) b);

    for (NodeSet set : List.of(empty, one, many)) {
      MultiringNodeHandleSet target = new MultiringNodeHandleSet(RING, set);

      MultiringNodeHandleSet copy =
          WireRoundTrip.roundTrip(target, MultiringNodeHandleSet::serialize, buf -> new MultiringNodeHandleSet(buf, endpoint));

      assertEquals(set.size(), copy.size());
      for (int i = 0; i < set.size(); i++) {
        assertEquals(new MultiringNodeHandle(RING, set.getHandle(i)), copy.getHandle(i));
      }
    }
  }

  @Test
  @DisplayName("MultiringIdRange")
  void multiringIdRange() throws IOException {
    List<IdRange> ranges = List.of(new IdRange(Id.build("1000"), Id.build("2000")),
        new IdRange(Id.build("F000"), Id.build("1000")), new IdRange(), new IdRange(true));

    for (IdRange range : ranges) {
      MultiringIdRange target = new MultiringIdRange(RING, range);

      MultiringIdRange copy = WireRoundTrip.roundTrip(target, MultiringIdRange::serialize, buf -> new MultiringIdRange(buf, endpoint));

      assertEquals(target, copy);
    }
  }

  private static List<RawMessage> wrapped() {
    Topic topic = new Topic(Id.build("1111"));
    return List.of(new DropMessage(self, topic), new DropMessage(null, topic),
        new PublishMessage(self, topic, new SplitStreamSubscribeContent(-9)),
        new PublishRequestMessage(self, topic, new SplitStreamSubscribeContent(4)),
        new UnsubscribeMessage(self, List.of(topic)),
        new SubscribeMessage(self, List.of(topic, new Topic(Id.build("2222"))), 3, null));
  }

  private static RingMessage ringMessage(RawMessage message) {
    return new RingMessage(RingId.build(RING, Id.build("ABCD")), message, APPLICATION);
  }

  private static RingMessage readRingMessage(rice.p2p.commonapi.rawserialization.InputBuffer buf) throws IOException {
    return new RingMessage(buf, endpoint, Map.of(APPLICATION, endpoint));
  }

  private static void assertRingMessage(RingMessage expected, RingMessage actual) throws IOException {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getApplication(), actual.getApplication());
    assertEquals(expected.getRawMessage().getClass(), actual.getRawMessage().getClass());
    assertEquals(expected.getRawMessage().getPriority(), actual.getRawMessage().getPriority());
    assertArrayEquals(WireRoundTrip.bytes(expected.getRawMessage()), WireRoundTrip.bytes(actual.getRawMessage()));
  }

  @Test
  @DisplayName("RingMessage wrapping a Scribe message")
  void ringMessageRoundTrip() throws IOException {
    for (RawMessage message : wrapped()) {
      RingMessage target = ringMessage(message);

      RingMessage copy = WireRoundTrip.roundTrip(target, MultiringWireRoundTripTest::readRingMessage);

      assertRingMessage(target, copy);
    }
  }

  @Test
  @DisplayName("RingMessage as the content of a Scribe message, over two hops")
  void ringMessageInScribeMessages() throws IOException {
    MessageDeserializer deserializer = endpoint.getDeserializer();
    Topic topic = new Topic(Id.build("3333"));
    for (RawMessage message : wrapped()) {
      RingMessage content = ringMessage(message);
      List<RawMessage> carriers = List.of(new PublishMessage(self, topic, content),
          new PublishRequestMessage(self, topic, content), new AnycastMessage(self, topic, content),
          new SubscribeMessage(self, List.of(topic), 5, content));

      for (RawMessage carrier : carriers) {
        RawMessage plain = WireRoundTrip.roundTrip(carrier, deserializer, self);
        RawMessage routed = local.route(endpoint, Id.build("5555"), carrier, self, false);

        for (RawMessage copy : List.of(plain, routed)) {
          RingMessage read = assertInstanceOf(RingMessage.class, contentOf(copy));
          assertRingMessage(content, read);
        }
      }
    }
  }

  private static rice.p2p.scribe.ScribeContent contentOf(RawMessage carrier) {
    return switch (carrier) {
      case PublishMessage m -> m.getContent();
      case PublishRequestMessage m -> m.getContent();
      case AnycastMessage m -> m.getRawContent();
      default -> throw new IllegalArgumentException(String.valueOf(carrier));
    };
  }
}
