package rice.p2p.scribe.messaging;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.ScribeImpl;
import rice.p2p.scribe.Topic;
import rice.p2p.splitstream.SplitStreamContent;
import rice.p2p.splitstream.SplitStreamSubscribeContent;
import rice.pastry.Id;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;

/**
 * Scribe's messages through the bytes of a socket ring: read where they arrive, sent on from there, and, as the
 * messages that travel inside a route message, unwrapped at a hop the way forward() does.
 */
@DisplayName("Scribe wire round trip")
class ScribeWireRoundTripTest {
  private static final short TEST_CONTENT = 3;

  /** A content type of the application on top of Scribe. */
  record TestContent(String text) implements ScribeContent {
    @Override
    public short getType() {
      return TEST_CONTENT;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeUTF(text);
    }

    static TestContent read(InputBuffer buf) throws IOException {
      return new TestContent(buf.readUTF());
    }
  }

  private static final Topic topic = new Topic(Id.build("1111"));
  private static final Topic T2 = new Topic(Id.build("2222"));
  private static final Topic T3 = new Topic(Id.build("3333"));

  private static Environment env;
  private static SocketNode local;
  private static Endpoint endpoint;
  private static MessageDeserializer deserializer;
  private static NodeHandle self;
  private static NodeHandle nodeHandle;
  private static NodeHandle b;
  private static NodeHandle c;

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    local = SocketNode.create(env, "4000");
    ScribeImpl scribe = new ScribeImpl(local.node, "wire");
    scribe.setContentDeserializer((buf, ep, type) -> switch (type) {
      case 1 -> new SplitStreamContent(buf);
      case 2 -> new SplitStreamSubscribeContent(buf);
      case TEST_CONTENT -> TestContent.read(buf);
      default -> throw new IOException("Invalid type:" + type);
    });
    endpoint = scribe.getEndpoint();
    deserializer = endpoint.getDeserializer();
    self = local.local();
    nodeHandle = local.handle("1000");
    b = local.handle("8000");
    c = local.handle("C000");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  private static void assertContent(ScribeContent expected, ScribeContent actual) throws IOException {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertEquals(expected.getClass(), actual.getClass());
    assertArrayEquals(WireRoundTrip.bytes(expected), WireRoundTrip.bytes(actual));
  }

  /** The copies a socket ring delivers: sent plain, and routed over two hops inside a route message. */
  private static <M extends RawMessage> List<M> copies(M message) throws IOException {
    M plain = WireRoundTrip.roundTrip(message, deserializer, self);
    M routed = local.route(endpoint, Id.build("5555"), message, self, false);
    M routedToHandle = local.route(endpoint, Id.build("5555"), message, self, true);
    return List.of(plain, routed, routedToHandle);
  }

  static Stream<ScribeContent> contents() {
    return Stream.of(new TestContent("hello"), new TestContent(""), new SplitStreamContent(new byte[0]),
        new SplitStreamContent(new byte[] { 1, 2, 3, -1 }), new SplitStreamSubscribeContent(7),
        new SplitStreamSubscribeContent(SplitStreamSubscribeContent.STAGE_FINAL));
  }

  @ParameterizedTest
  @MethodSource("contents")
  @DisplayName("SplitStream and test contents round trip on their own")
  void contentRoundTrip(ScribeContent content) throws IOException {
    ScribeContent copy = WireRoundTrip.roundTrip(content, buf -> switch (content.getType()) {
      case 1 -> new SplitStreamContent(buf);
      case 2 -> new SplitStreamSubscribeContent(buf);
      default -> TestContent.read(buf);
    });

    assertContent(content, copy);
  }

  private static NodeHandle named(String name) {
    return switch (name) {
      case "a" -> nodeHandle;
      case "b" -> b;
      default -> c;
    };
  }

  private static AnycastMessage anycast(List<String> toVisit, List<String> visited, boolean keepSource,
      ScribeContent content) {
    AnycastMessage msg = new AnycastMessage(self, topic, content);
    for (String name : toVisit) {
      msg.addLast(named(name));
    }
    for (String name : visited) {
      msg.addVisited(named(name));
    }
    if (!keepSource) {
      msg.remove(self);
    }
    return msg;
  }

  private static void assertAnycast(AnycastMessage expected, AnycastMessage actual) throws IOException {
    assertEquals(expected.getSource(), actual.getSource());
    assertEquals(expected.getTopic(), actual.getTopic());
    assertEquals(expected.getInitialRequestor(), actual.getInitialRequestor());
    assertEquals(new ArrayList<>(expected.toVisit), new ArrayList<>(actual.toVisit));
    assertEquals(expected.visited, actual.visited);
    assertContent(expected.getRawContent(), actual.getRawContent());
  }

  static Stream<Arguments> anycasts() {
    List<String> none = List.of();
    return Stream.of(
        Arguments.of("empty toVisit, visited is the source", none, none, true, new TestContent("x")),
        Arguments.of("toVisit and visited", List.of("a", "b"), List.of("c"), true, new TestContent("x")),
        Arguments.of("only toVisit", List.of("a"), none, false, new TestContent("x")),
        Arguments.of("empty toVisit and visited", none, none, false, new TestContent("x")),
        Arguments.of("null content", List.of("a"), List.of("b", "c"), true, null),
        Arguments.of("splitstream content", List.of("b"), List.of("a"), true,
            new SplitStreamContent(new byte[] { 5 })));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("anycasts")
  @DisplayName("AnycastMessage")
  void anycastRoundTrip(String name, List<String> toVisit, List<String> visited, boolean keepSource,
      ScribeContent content) throws IOException {
    AnycastMessage msg = anycast(toVisit, visited, keepSource, content);

    for (AnycastMessage copy : copies(msg)) {
      assertAnycast(msg, copy);
    }
  }

  @Test
  @DisplayName("AnycastMessage read at a hop is sent on with the hop as its source")
  void anycastSentOnWithNewSource() throws IOException {
    AnycastMessage msg = anycast(List.of("a", "b"), List.of("c"), true, new TestContent("x"));
    AnycastMessage read = WireRoundTrip.roundTrip(msg, deserializer, self);

    read.setSource(nodeHandle);
    read.addVisited(nodeHandle);
    read.addFirst(c);
    AnycastMessage again = WireRoundTrip.roundTrip(read, deserializer, self);

    assertAnycast(read, again);
    assertEquals(self, again.getInitialRequestor());
  }

  @Test
  @DisplayName("AnycastMessage rejects a null source")
  void anycastNullSource() {
    assertThrows(NullPointerException.class, () -> new AnycastMessage(null, topic, new TestContent("x")));
  }

  @Test
  @DisplayName("AnycastFailureMessage")
  void anycastFailure() throws IOException {
    for (ScribeContent content : (Iterable<ScribeContent>) contents()::iterator) {
      for (AnycastFailureMessage copy : copies(new AnycastFailureMessage(self, T2, content))) {
        assertEquals(self, copy.getSource());
        assertEquals(T2, copy.getTopic());
        assertContent(content, copy.getContent());
      }
    }
  }

  @Test
  @DisplayName("AnycastFailureMessage without a source")
  void anycastFailureNullSource() throws IOException {
    AnycastFailureMessage copy =
        WireRoundTrip.roundTrip(new AnycastFailureMessage(null, T2, new TestContent("x")), deserializer, self);

    assertNull(copy.getSource());
  }

  static Stream<Arguments> subscribes() {
    return Stream.of(
        Arguments.of("single topic", List.of(topic), List.of(), new TestContent("x")),
        Arguments.of("single topic, null content", List.of(topic), List.of(), null),
        Arguments.of("two topics", List.of(topic, T2), List.of("a"), new SplitStreamSubscribeContent(1)),
        Arguments.of("many topics", topics(40), List.of("a", "b"),
            new SplitStreamSubscribeContent(SplitStreamSubscribeContent.STAGE_FINAL)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("subscribes")
  @DisplayName("SubscribeMessage")
  void subscribeRoundTrip(String name, List<Topic> topics, List<String> toVisit, ScribeContent content)
      throws IOException {
    SubscribeMessage msg = new SubscribeMessage(self, topics, 42, content);
    for (String handle : toVisit) {
      msg.addLast(named(handle));
    }

    for (SubscribeMessage copy : copies(msg)) {
      assertEquals(topics, copy.getTopics());
      assertEquals(topics.get(0), copy.getTopic());
      assertEquals(42, copy.getId());
      assertEquals(self, copy.getSubscriber());
      assertAnycast(msg, copy);
    }
  }

  @Test
  @DisplayName("SubscribeMessage after a hop removed topics from it")
  void subscribeAfterRemovingTopics() throws IOException {
    SubscribeMessage msg = new SubscribeMessage(self, List.of(topic, T2, T3), 7, new TestContent("x"));
    SubscribeMessage read = WireRoundTrip.roundTrip(msg, deserializer, self);

    read.removeTopics(List.of(topic));
    SubscribeMessage again = WireRoundTrip.roundTrip(read, deserializer, self);
    read.removeTopics(List.of(T2));
    SubscribeMessage last = WireRoundTrip.roundTrip(read, deserializer, self);

    assertEquals(List.of(T2, T3), again.getTopics());
    assertEquals(T2, again.getTopic());
    assertEquals(List.of(T3), last.getTopics());
    assertEquals(T3, last.getTopic());
  }

  @Test
  @DisplayName("SubscribeMessage copy() of a message read from the wire")
  void subscribeCopyOfReadMessage() throws IOException {
    SubscribeMessage msg = new SubscribeMessage(self, List.of(topic, T2), 7, new TestContent("x"));
    msg.addLast(nodeHandle);
    SubscribeMessage read = WireRoundTrip.roundTrip(msg, deserializer, self);

    SubscribeMessage split = read.copy(List.of(T2), read.getRawContent());
    SubscribeMessage copy = WireRoundTrip.roundTrip(split, deserializer, self);

    assertEquals(List.of(T2), copy.getTopics());
    assertEquals(self, copy.getSubscriber());
    assertEquals(new ArrayList<>(split.toVisit), new ArrayList<>(copy.toVisit));
  }

  static Stream<Arguments> acks() {
    Id id = Id.build("AAAA");
    Id i2 = Id.build("BBBB");
    List<rice.p2p.commonapi.Id> empty = List.of();
    return Stream.of(
        Arguments.of("single topic, empty path", List.of(topic), List.of(empty)),
        Arguments.of("single topic, path", List.of(topic), List.of(List.<rice.p2p.commonapi.Id>of(id, i2))),
        Arguments.of("many topics, mixed paths", List.of(topic, T2, T3),
            List.of(empty, List.<rice.p2p.commonapi.Id>of(id),
                List.<rice.p2p.commonapi.Id>of(id, i2, Id.build("CCCC")))),
        Arguments.of("no topics", List.<Topic>of(), List.<List<rice.p2p.commonapi.Id>>of()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("acks")
  @DisplayName("SubscribeAckMessage")
  void subscribeAck(String name, List<Topic> topics, List<List<rice.p2p.commonapi.Id>> paths) throws IOException {
    SubscribeAckMessage msg = new SubscribeAckMessage(self, topics, paths, 9);

    for (SubscribeAckMessage copy : copies(msg)) {
      assertEquals(self, copy.getSource());
      assertEquals(9, copy.getId());
      assertEquals(topics, copy.getTopics());
      assertEquals(paths, copy.getPathsToRoot());
    }
  }

  static Stream<Integer> topicCounts() {
    return Stream.of(0, 1, 2, 300);
  }

  private static List<Topic> topics(int count) {
    List<Topic> topics = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      topics.add(new Topic(Id.build(new int[] { i, 7, 0, 0, 0 })));
    }
    return topics;
  }

  @ParameterizedTest(name = "{0} topics")
  @MethodSource("topicCounts")
  @DisplayName("SubscribeFailedMessage")
  void subscribeFailed(int count) throws IOException {
    List<Topic> topics = topics(count);

    for (SubscribeFailedMessage copy : copies(new SubscribeFailedMessage(self, topics, 11))) {
      assertEquals(self, copy.getSource());
      assertEquals(11, copy.getId());
      assertEquals(topics, copy.getTopics());
    }
  }

  @ParameterizedTest(name = "{0} topics")
  @MethodSource("topicCounts")
  @DisplayName("UnsubscribeMessage")
  void unsubscribe(int count) throws IOException {
    List<Topic> topics = topics(count);

    for (UnsubscribeMessage copy : copies(new UnsubscribeMessage(self, topics))) {
      assertEquals(self, copy.source());
      assertEquals(topics, copy.topics());
    }
  }

  @Test
  @DisplayName("UnsubscribeMessage without a source")
  void unsubscribeNullSource() throws IOException {
    UnsubscribeMessage copy = WireRoundTrip.roundTrip(new UnsubscribeMessage(null, List.of(topic)), deserializer, self);

    assertNull(copy.source());
    assertEquals(List.of(topic), copy.topics());
  }

  @Test
  @DisplayName("DropMessage")
  void drop() throws IOException {
    for (DropMessage copy : copies(new DropMessage(self, T3))) {
      assertEquals(self, copy.getSource());
      assertEquals(T3, copy.getTopic());
    }
    assertNull(WireRoundTrip.roundTrip(new DropMessage(null, T3), deserializer, self).getSource());
  }

  @ParameterizedTest
  @MethodSource("contents")
  @DisplayName("PublishMessage")
  void publish(ScribeContent content) throws IOException {
    for (PublishMessage copy : copies(new PublishMessage(self, topic, content))) {
      assertEquals(self, copy.getSource());
      assertEquals(topic, copy.getTopic());
      assertContent(content, copy.getContent());
    }
  }

  @ParameterizedTest
  @MethodSource("contents")
  @DisplayName("PublishRequestMessage")
  void publishRequest(ScribeContent content) throws IOException {
    for (PublishRequestMessage copy : copies(new PublishRequestMessage(self, topic, content))) {
      assertEquals(self, copy.getSource());
      assertEquals(topic, copy.getTopic());
      assertContent(content, copy.getContent());
    }
  }

  @Test
  @DisplayName("PublishMessage read at a hop is sent on with the hop as its source")
  void publishSentOn() throws IOException {
    PublishMessage read =
        WireRoundTrip.roundTrip(new PublishMessage(null, topic, new TestContent("x")), deserializer, self);

    read.setSource(self);
    PublishMessage again = WireRoundTrip.roundTrip(read, deserializer, self);

    assertEquals(self, again.getSource());
    assertInstanceOf(TestContent.class, again.getContent());
  }
}
