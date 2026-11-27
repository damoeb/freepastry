package rice.tutorial.lookup;

import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.pastry.NodeSet;
import rice.pastry.testing.WireRoundTrip.SocketNode;
import rice.tutorial.lookup.LookupService.NodeLookupQuery;
import rice.tutorial.lookup.LookupService.NodeLookupResponse;

/** The lookup service's query and response, routed over two hops through a socket node's bytes. */
class LookupServiceWireRoundTripTest {
  private static Environment env;
  private static SocketNode local;
  private static Endpoint endpoint;

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    local = SocketNode.create(env, "4000");
    endpoint = local.node.buildEndpoint(new rice.p2p.commonapi.Application() {
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
    }, "lookup");
    endpoint.setDeserializer((buf, type, priority, sender) -> switch (type) {
      case NodeLookupQuery.TYPE -> new NodeLookupQuery(buf, endpoint);
      case NodeLookupResponse.TYPE -> new NodeLookupResponse(buf, endpoint);
      default -> throw new IOException("Unknown message type: " + type);
    });
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  @Test
  void query() throws IOException {
    local.route(endpoint, rice.pastry.Id.build("9000"), new NodeLookupQuery(local.local(), 3, 42), local.local(), false);
  }

  @Test
  void response() throws IOException {
    NodeSet nodes = new NodeSet();
    local.route(endpoint, rice.pastry.Id.build("9000"), new NodeLookupResponse(nodes, 1), local.local(), true);
    nodes.put(local.handle("1000"));
    nodes.put(local.handle("2000"));
    local.route(endpoint, rice.pastry.Id.build("9000"), new NodeLookupResponse(nodes, 2), local.local(), false);
  }
}
