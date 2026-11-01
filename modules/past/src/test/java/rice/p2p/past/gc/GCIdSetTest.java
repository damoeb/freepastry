package rice.p2p.past.gc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

  @Test
  void equalsComparesTheMembersOfNonEmptySets() {
    Id a = idFactory.buildId("a");
    Id b = idFactory.buildId("b");
    GCIdSet left = new GCIdSet(idFactory);
    GCIdSet right = new GCIdSet(idFactory);
    left.addId(new GCId(a, 100));
    right.addId(new GCId(a, 100));

    assertEquals(right, left);

    right.addId(new GCId(b, 100));
    left.addId(new GCId(idFactory.buildId("c"), 100));

    assertNotEquals(right, left);
  }

  @Test
  void copyHasItsOwnIdsAndExpirations() {
    Id a = idFactory.buildId("a");
    Id b = idFactory.buildId("b");
    GCIdSet original = new GCIdSet(idFactory);
    original.addId(new GCId(a, 100));

    GCIdSet copy = original.copy();
    copy.addId(new GCId(b, 200));
    copy.addId(new GCId(a, 300));

    assertEquals(1, original.numElements());
    assertFalse(original.isMemberId(new GCId(b, 200)));
    assertEquals(new GCId(a, 100), original.getIterator().next());
    assertEquals(2, copy.numElements());
  }
}
