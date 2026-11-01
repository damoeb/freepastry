package rice.pastry.commonapi;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSetType;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.PastryNode;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.socket.nat.probe.ProbeApp;
import rice.pastry.standard.RandomNodeIdFactory;

class PastryEndpointUnknownTypeTest {
  private static final short UNKNOWN_TYPE = 12345;

  private static final Application APPLICATION = new Application() {
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
  };

  private Environment env;
  private PastryNode node;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    node = new DirectPastryNodeFactory(new RandomNodeIdFactory(env),
        new EuclideanNetwork<DirectNodeHandle, RawMessage>(env), env).newNode();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void readIdRejectsAnUnknownTypeWithAnIOException() {
    PastryEndpoint target = new PastryEndpoint(node, APPLICATION, "test");

    assertThrows(IOException.class, () -> target.readId(new SimpleInputBuffer(new byte[0]), UNKNOWN_TYPE));
  }

  @Test
  void readNodeHandleSetRejectsAnUnknownTypeWithAnIOException() {
    PastryEndpoint target = new PastryEndpoint(node, APPLICATION, "test");

    assertThrows(IOException.class,
        () -> target.readNodeHandleSet(new SimpleInputBuffer(new byte[0]), UNKNOWN_TYPE));
  }

  @Test
  void readNodeHandleSetRejectsAMultiringSetWithAnIOException() {
    PastryEndpoint target = new PastryEndpoint(node, APPLICATION, "test");

    assertThrows(IOException.class, () -> target.readNodeHandleSet(new SimpleInputBuffer(new byte[0]),
        NodeHandleSetType.MULTIRING_NODE_HANDLE_SET.id()));
  }

  @Test
  void probeAppRejectsAnUnknownTypeWithAnIOException() {
    ProbeApp target = new ProbeApp(node, null, null);

    assertThrows(IOException.class,
        () -> target.getDeserializer().deserialize(new SimpleInputBuffer(new byte[0]), UNKNOWN_TYPE, 0, null));
  }
}
