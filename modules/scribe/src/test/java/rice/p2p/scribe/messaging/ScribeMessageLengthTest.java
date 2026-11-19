package rice.p2p.scribe.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.splitstream.SplitStreamContent;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * The counts in Scribe messages come from the peer: a few bytes must not make us allocate for, or loop over,
 * billions of topics or nodes.
 */
class ScribeMessageLengthTest {
  private static final byte VERSION = 1;

  private static final Id id = (Id) Proxy.newProxyInstance(Id.class.getClassLoader(), new Class<?>[] {Id.class},
      (proxy, method, args) -> {
        throw new UnsupportedOperationException(method.getName());
      });

  /** An endpoint whose ids and node handles take no bytes, so a test only writes the counts. */
  private static final Endpoint ENDPOINT = (Endpoint) Proxy.newProxyInstance(Endpoint.class.getClassLoader(),
      new Class<?>[] {Endpoint.class}, (proxy, method, args) -> switch (method.getName()) {
        case "readId" -> id;
        case "readNodeHandle" -> null;
        default -> throw new UnsupportedOperationException(method.getName());
      });

  private static SimpleOutputBuffer out() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte(VERSION);
    return out;
  }

  private static SimpleInputBuffer in(SimpleOutputBuffer out) {
    return new SimpleInputBuffer(out.getBytes());
  }

  @Test
  void subscribeFailedRejectsAHugeTopicCount() throws IOException {
    SimpleOutputBuffer out = out();
    out.writeInt(0); // message id
    out.writeInt(Integer.MAX_VALUE);

    assertThrows(IOException.class, () -> SubscribeFailedMessage.build(in(out), ENDPOINT));
  }

  @Test
  void subscribeAckRejectsAHugeTopicCount() throws IOException {
    SimpleOutputBuffer out = out();
    out.writeInt(0); // message id
    out.writeInt(Integer.MAX_VALUE);

    assertThrows(IOException.class, () -> SubscribeAckMessage.build(in(out), ENDPOINT));
  }

  @Test
  void subscribeAckRejectsAHugePathLength() throws IOException {
    SimpleOutputBuffer out = out();
    out.writeInt(0); // message id
    out.writeInt(1); // topics
    out.writeShort((short) 0); // the topic's id type
    out.writeInt(Integer.MAX_VALUE);

    assertThrows(IOException.class, () -> SubscribeAckMessage.build(in(out), ENDPOINT));
  }

  @Test
  void unsubscribeRejectsANegativeTopicCount() throws IOException {
    SimpleOutputBuffer out = out();
    out.writeBoolean(false); // no source
    out.writeInt(-1);

    assertThrows(IOException.class, () -> UnsubscribeMessage.build(in(out), ENDPOINT));
  }

  @Test
  void anycastRejectsAHugeVisitedCount() throws IOException {
    SimpleOutputBuffer out = out();
    out.writeBoolean(false); // no source
    out.writeShort((short) 0); // the topic's id type
    out.writeInt(0); // nodes to visit
    out.writeInt(Integer.MAX_VALUE);

    assertThrows(IOException.class, () -> AnycastMessage.build(in(out), ENDPOINT, null));
  }

  @Test
  void subscribeRejectsAHugeTopicCount() throws IOException {
    SimpleOutputBuffer out = out();
    out.writeBoolean(false); // no source
    out.writeShort((short) 0); // the topic's id type
    out.writeInt(0); // nodes to visit
    out.writeInt(0); // nodes visited
    out.writeBoolean(false); // no content
    out.writeInt(0); // message id
    out.writeInt(Integer.MAX_VALUE);

    assertThrows(IOException.class, () -> SubscribeMessage.readFrom(in(out), ENDPOINT, null));
  }

  @Test
  @DisplayName("a subscribe message for one topic reads back: that topic is not repeated after the count")
  void subscribeReadsASingleTopic() throws IOException {
    SimpleOutputBuffer out = out();
    out.writeBoolean(false); // no source
    out.writeShort((short) 0); // the topic's id type
    out.writeInt(0); // nodes to visit
    out.writeInt(0); // nodes visited
    out.writeBoolean(false); // no content
    out.writeInt(0); // message id
    out.writeInt(1); // topics, the first one is the topic above

    assertEquals(1, SubscribeMessage.readFrom(in(out), ENDPOINT, null).getTopics().size());
  }

  @Test
  void splitStreamContentRejectsAHugeLength() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeInt(Integer.MAX_VALUE);

    assertThrows(IOException.class, () -> new SplitStreamContent(in(out)));
  }
}
