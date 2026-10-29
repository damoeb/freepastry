package rice.p2p.past.gc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * GCIdSet.equals() used to cast its argument blindly, throwing ClassCastException
 * (or NullPointerException) instead of returning false.
 */
class GCIdSetTest {

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
  void equalsOnlyMatchingGCIdSets() {
    GCIdSet set = new GCIdSet(idFactory);

    assertEquals(new GCIdSet(idFactory), set);
    assertNotEquals(set, "not a set");
    assertNotEquals(set, null);
  }
}
