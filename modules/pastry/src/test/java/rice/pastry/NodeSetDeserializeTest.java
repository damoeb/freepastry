package rice.pastry;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.standard.RandomNodeIdFactory;

class NodeSetDeserializeTest {
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
  void nodeSetRejectsANegativeSize() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeShort((short) -1);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());

    assertThrows(IOException.class, () -> new NodeSet(buf, node));
  }
}
