package rice.p2p.glacier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FragmentKeyRange.equals(), hashCode() and toString() used to throw RuntimeException,
 * so the range could not be logged or kept in a collection.
 */
class FragmentKeyRangeTest {

  private Environment env;
  private IdRange underlying;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    PastryIdFactory idFactory = new PastryIdFactory(env);
    Id clockwiseId = idFactory.buildId("cw");
    Id counterClockwiseId = idFactory.buildId("ccw");
    underlying = idFactory.buildIdRange(clockwiseId, counterClockwiseId);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void equalsAndHashCodeFollowTheUnderlyingRange() {
    FragmentKeyRange range = new FragmentKeyRange(underlying);
    FragmentKeyRange same = new FragmentKeyRange(underlying);

    assertEquals(same, range);
    assertEquals(same.hashCode(), range.hashCode());
    assertNotEquals(range, underlying);
    assertNotEquals(range, null);
  }

  @Test
  void toStringShowsTheUnderlyingRange() {
    assertTrue(new FragmentKeyRange(underlying).toString().contains(underlying.toString()));
  }
}
