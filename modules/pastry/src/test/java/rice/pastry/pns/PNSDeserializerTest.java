package rice.pastry.pns;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.standard.RandomNodeIdFactory;

class PNSDeserializerTest {
  private static final short UNKNOWN_TYPE = 12345;

  private Environment env;
  private ProximityNeighborSelectionApplication app;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    app = new ProximityNeighborSelectionApplication(new DirectPastryNodeFactory(new RandomNodeIdFactory(env),
        new EuclideanNetwork<DirectNodeHandle, RawMessage>(env), env).newNode());
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void rejectsAnUnknownTypeNamingTheTypeId() {
    ProximityNeighborSelectionApplication.PNSDeserializer target = app.new PNSDeserializer();

    IOException actual = assertThrows(IOException.class,
        () -> target.deserialize(new SimpleInputBuffer(new byte[0]), UNKNOWN_TYPE, 0, null));

    assertTrue(actual.getMessage().contains(String.valueOf(UNKNOWN_TYPE)), actual.getMessage());
  }
}
