package rice.pastry.standard;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.messaging.PMessageDeserializer;

class UnknownMessageTypeTest {
  private static final short UNKNOWN_TYPE = 12345;

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

  /** Calls the pastry-typed overload, which is the one subclasses implement. */
  private static void assertRejected(PMessageDeserializer target) {
    IOException actual = assertThrows(IOException.class,
        () -> target.deserialize(new SimpleInputBuffer(new byte[0]), UNKNOWN_TYPE, 0, (NodeHandle) null));

    assertTrue(actual.getMessage().contains(String.valueOf(UNKNOWN_TYPE)), actual.getMessage());
  }

  @Test
  void joinDeserializerRejectsAnUnknownType() {
    assertRejected(new ConsistentJoinProtocol.CJPDeserializer(node));
  }

  @Test
  void periodicLeafSetDeserializerRejectsAnUnknownType() {
    assertRejected(new PeriodicLeafSetProtocol.PLSPMessageDeserializer(node));
  }

  @Test
  void standardRouteSetDeserializerRejectsAnUnknownType() {
    assertRejected(new StandardRouteSetProtocol.SRSPDeserializer(node));
  }
}
