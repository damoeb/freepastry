package rice.p2p.multiring;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.multiring.messaging.RingMessage;
import rice.pastry.PastryNode;
import rice.pastry.testing.StubHandleNodes;
import rice.testing.fuzz.FuzzSupport;

/**
 * Fuzzes what a multiring node reads from the wire: ring ids, node handles, and the ring message that wraps another
 * application's message. The invariant: a value or an {@link IOException},
 * nothing else.
 */
class MultiringFuzzTest {
  private static Environment env;
  private static PastryNode node;
  private static Endpoint endpoint;

  @BeforeAll
  static void setUp() throws IOException {
    env = Environment.directEnvironment(1);
    node = StubHandleNodes.newNode(env);
    endpoint = node.buildEndpoint(new Application() {
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
    }, "fuzz");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  @FuzzTest(maxDuration = "30s")
  void ringId(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> RingId.build(buf, endpoint));
  }

  @FuzzTest(maxDuration = "30s")
  void multiringNodeHandle(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new MultiringNodeHandle(buf, endpoint));
  }

  @FuzzTest(maxDuration = "30s")
  void ringMessage(FuzzedDataProvider data) {
    java.util.Map<String, Endpoint> endpoints = java.util.Map.of(endpoint.getApplicationInstanceName(), endpoint);
    FuzzSupport.read(data, buf -> new RingMessage(buf, endpoint, endpoints));
  }
}
