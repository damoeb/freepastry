package rice.p2p.scribe;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.splitstream.SplitStreamContent;
import rice.p2p.splitstream.SplitStreamSubscribeContent;
import rice.pastry.PastryNode;
import rice.pastry.testing.StubHandleNodes;
import rice.testing.fuzz.FuzzSupport;

/**
 * Fuzzes the Scribe wire messages, with the content types of SplitStream, the family that rides on Scribe. The
 * invariant: a message or an {@link IOException}, nothing else.
 */
class ScribeFuzzTest {
  private static final short SPLITSTREAM_CONTENT = 1;
  private static final short SPLITSTREAM_SUBSCRIBE_CONTENT = 2;

  private static Environment env;
  private static PastryNode node;
  private static MessageDeserializer target;

  @BeforeAll
  static void setUp() throws IOException {
    env = Environment.directEnvironment(1);
    node = StubHandleNodes.newNode(env);
    ScribeImpl scribe = new ScribeImpl(node, "fuzz");
    scribe.setContentDeserializer((buf, endpoint, type) -> switch (type) {
      case SPLITSTREAM_CONTENT -> new SplitStreamContent(buf);
      case SPLITSTREAM_SUBSCRIBE_CONTENT -> new SplitStreamSubscribeContent(buf);
      default -> throw new IOException("Invalid type:" + type);
    });
    target = scribe.getEndpoint().getDeserializer();
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  @FuzzTest(maxDuration = "30s")
  void scribeMessage(FuzzedDataProvider data) {
    FuzzSupport.deserialize(target, data, node.getLocalHandle());
  }

  @FuzzTest(maxDuration = "30s")
  void splitStreamContent(FuzzedDataProvider data) {
    FuzzSupport.read(data, SplitStreamContent::new);
  }

  @FuzzTest(maxDuration = "30s")
  void splitStreamSubscribeContent(FuzzedDataProvider data) {
    FuzzSupport.read(data, SplitStreamSubscribeContent::new);
  }
}
