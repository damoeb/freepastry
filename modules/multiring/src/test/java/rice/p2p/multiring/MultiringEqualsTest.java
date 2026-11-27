package rice.p2p.multiring;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.NodeHandleSet;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * equals() of the multiring wrappers used to cast its argument blindly, throwing
 * ClassCastException (or NullPointerException) instead of returning false.
 */
class MultiringEqualsTest {

  private Environment env;
  private PastryIdFactory idFactory;
  private Id ringId;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    idFactory = new PastryIdFactory(env);
    ringId = idFactory.buildId("ring");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void idSetEqualsOnlyMatchingIdSets() {
    IdSet inner = idFactory.buildIdSet();
    MultiringIdSet set = new MultiringIdSet(ringId, inner);

    assertEquals(new MultiringIdSet(ringId, inner), set);
    assertNotEquals(set, "not a set");
    assertNotEquals(set, null);
  }

  @Test
  void nodeHandleSetEqualsOnlyMatchingNodeHandleSets() {
    NodeHandleSet inner = idFactory.buildNodeHandleSet();
    MultiringNodeHandleSet set = new MultiringNodeHandleSet(ringId, inner);

    assertEquals(new MultiringNodeHandleSet(ringId, inner), set);
    assertNotEquals(set, "not a set");
    assertNotEquals(set, null);
  }

  @Test
  void idRangeEqualsOnlyMatchingIdRanges() {
    Id id = idFactory.buildId("cw");
    Id ccw = idFactory.buildId("ccw");
    MultiringIdRange range = new MultiringIdRange(ringId, idFactory.buildIdRange(id, ccw));

    assertEquals(new MultiringIdRange(ringId, idFactory.buildIdRange(id, ccw)), range);
    assertNotEquals(range, "not a range");
    assertNotEquals(range, null);
  }
}
