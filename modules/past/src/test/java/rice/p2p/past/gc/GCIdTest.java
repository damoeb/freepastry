package rice.p2p.past.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.commonapi.PastryIdFactory;

class GCIdTest {

  private Environment env;
  private PastryIdFactory idFactory;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    idFactory = new PastryIdFactory(env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("equals and hashCode ignore the expiration")
  void equalityIgnoresExpiration() {
    var id = idFactory.buildId("a");

    assertEquals(new ExpiringId(id, 100), new ExpiringId(id, 200));
    assertEquals(new ExpiringId(id, 100).hashCode(), new ExpiringId(id, 200).hashCode());
  }

  @Test
  @DisplayName("different underlying ids are not equal")
  void differentIdsAreNotEqual() {
    assertNotEquals(new ExpiringId(idFactory.buildId("a"), 100), new ExpiringId(idFactory.buildId("b"), 100));
  }
}
