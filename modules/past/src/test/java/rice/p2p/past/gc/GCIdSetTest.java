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
    GCIdSet gcIdSet = new GCIdSet(idFactory);

    assertEquals(new GCIdSet(idFactory), gcIdSet);
    assertNotEquals(gcIdSet, "not a set");
    assertNotEquals(gcIdSet, null);
  }

  @Test
  void equalsComparesTheMembersOfNonEmptySets() {
    Id firstId = idFactory.buildId("a");
    Id secondId = idFactory.buildId("b");
    GCIdSet left = new GCIdSet(idFactory);
    GCIdSet right = new GCIdSet(idFactory);
    left.addId(new ExpiringId(firstId, 100));
    right.addId(new ExpiringId(firstId, 100));

    assertEquals(right, left);

    right.addId(new ExpiringId(secondId, 100));
    left.addId(new ExpiringId(idFactory.buildId("c"), 100));

    assertNotEquals(right, left);
  }

  @Test
  void copyHasItsOwnIdsAndExpirations() {
    Id firstId = idFactory.buildId("a");
    Id secondId = idFactory.buildId("b");
    GCIdSet original = new GCIdSet(idFactory);
    original.addId(new ExpiringId(firstId, 100));

    GCIdSet copy = original.copy();
    copy.addId(new ExpiringId(secondId, 200));
    copy.addId(new ExpiringId(firstId, 300));

    assertEquals(1, original.numElements());
    assertFalse(original.isMemberId(new ExpiringId(secondId, 200)));
    assertEquals(new ExpiringId(firstId, 100), original.getIterator().next());
    assertEquals(2, copy.numElements());
  }
}
