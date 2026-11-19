package rice.tutorial;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;
import rice.tutorial.past.MyPastContent;
import rice.tutorial.scribe.MyScribeContent;

/**
 * The tutorials' messages and contents through the bytes of a socket node, routed over two hops: the tutorials are
 * what users copy, and run on sockets.
 */
class TutorialWireRoundTripTest {
  private static Environment env;
  private static SocketNode local;
  private static Id from;
  private static Id to;

  /** Reads a message the way a tutorial's deserializer does. */
  @FunctionalInterface
  interface MsgReader {
    RawMessage read(InputBuffer buf, Endpoint endpoint) throws IOException;
  }

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    local = SocketNode.create(env, "4000");
    from = local.local().getId();
    to = rice.pastry.Id.build("9000");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  private static Endpoint endpoint(String instance, MsgReader reader) {
    Endpoint[] endpoint = new Endpoint[1];
    endpoint[0] = local.node.buildEndpoint(new Application() {
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
    }, instance);
    endpoint[0].setDeserializer((buf, type, priority, sender) -> reader.read(buf, endpoint[0]));
    return endpoint[0];
  }

  static Stream<Arguments> messages() {
    return Stream.of(
        Arguments.of("deterministicsimulator", new rice.tutorial.deterministicsimulator.MyMsg(from, to),
            (MsgReader) rice.tutorial.deterministicsimulator.MyMsg::new),
        Arguments.of("direct", new rice.tutorial.direct.MyMsg(from, to), (MsgReader) rice.tutorial.direct.MyMsg::new),
        Arguments.of("gtitm", new rice.tutorial.gtitm.MyMsg(from, to), (MsgReader) rice.tutorial.gtitm.MyMsg::new),
        Arguments.of("lesson3", new rice.tutorial.lesson3.MyMsg(from, to), (MsgReader) rice.tutorial.lesson3.MyMsg::new),
        Arguments.of("lesson4", new rice.tutorial.lesson4.MyMsg(from, to), (MsgReader) rice.tutorial.lesson4.MyMsg::new),
        Arguments.of("prioritylistener", new rice.tutorial.prioritylistener.MyMsg(from, to),
            (MsgReader) rice.tutorial.prioritylistener.MyMsg::new),
        Arguments.of("rawserialization", new rice.tutorial.rawserialization.MyMsg(from, to),
            (MsgReader) rice.tutorial.rawserialization.MyMsg::new),
        Arguments.of("ssl", new rice.tutorial.ssl.MyMsg(from, to), (MsgReader) rice.tutorial.ssl.MyMsg::new),
        Arguments.of("transportdirect", new rice.tutorial.transportdirect.MyMsg(from, to),
            (MsgReader) rice.tutorial.transportdirect.MyMsg::new),
        Arguments.of("transportlayer", new rice.tutorial.transportlayer.MyMsg(from, to),
            (MsgReader) rice.tutorial.transportlayer.MyMsg::new));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("messages")
  @DisplayName("a tutorial's message is routed over two hops")
  void myMsg(String tutorial, RawMessage message, MsgReader reader) throws IOException {
    local.route(endpoint(tutorial, reader), to, message, local.local(), false);
  }

  @Test
  @DisplayName("the forwarding tutorial's message keeps its passport, empty and with hops, over two hops")
  void forwardingPassport() throws IOException {
    Endpoint endpoint = endpoint("forwarding", rice.tutorial.forwarding.MyMsg::new);
    rice.tutorial.forwarding.MyMsg message = new rice.tutorial.forwarding.MyMsg(from, to);
    local.route(endpoint, to, message, local.local(), false);

    message.addHop(local.handle("1000"));
    message.addHop(local.handle("2000"));
    rice.tutorial.forwarding.MyMsg copy = local.route(endpoint, to, message, local.local(), true);
    assertEquals(message.toString(), copy.toString());
  }

  @Test
  void contents() throws IOException {
    Endpoint endpoint = endpoint("contents", (buf, ep) -> {
      throw new IOException("not a message");
    });
    WireRoundTrip.roundTrip(new MyPastContent(to, "some content"), buf -> new MyPastContent(buf, endpoint));
    WireRoundTrip.roundTrip(new MyPastContent(to, ""), buf -> new MyPastContent(buf, endpoint));
    WireRoundTrip.roundTrip(new MyScribeContent(local.handle("1000"), 7), buf -> new MyScribeContent(buf, endpoint));
  }
}
