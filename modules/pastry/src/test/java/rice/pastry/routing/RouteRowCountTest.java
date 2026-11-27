package rice.pastry.routing;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.PastryNode;
import rice.pastry.testing.StubHandleNodes;
import rice.pastry.pns.messages.RouteRowResponse;

class RouteRowCountTest {
  private Environment env;
  private PastryNode node;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    node = StubHandleNodes.newNode(env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void broadcastRouteRowRejectsANegativeRowLength() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeByte((byte) 0); // a node handle
    out.writeByte((byte) -1);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());

    assertThrows(IOException.class, () -> new BroadcastRouteRow(buf, node, node));
  }

  @Test
  void routeRowResponseRejectsMoreRouteSetsThanBytes() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeShort((short) 0);
    out.writeInt(Integer.MAX_VALUE);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());

    assertThrows(IOException.class, () -> new RouteRowResponse(buf, node, node.getLocalHandle(), 1));
  }
}
