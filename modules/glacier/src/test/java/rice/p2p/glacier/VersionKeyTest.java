package rice.p2p.glacier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VersionKeyTest {

  private Environment env;
  private Id id;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    id = new PastryIdFactory(env).buildId("object");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void compareToOrdersVersionsThatWouldOverflowASubtraction() {
    VersionKey low = new VersionKey(id, Long.MIN_VALUE);
    VersionKey high = new VersionKey(id, Long.MAX_VALUE);

    assertTrue(low.compareTo(high) < 0);
    assertTrue(high.compareTo(low) > 0);
    assertEquals(0, high.compareTo(new VersionKey(id, Long.MAX_VALUE)));
  }
}
