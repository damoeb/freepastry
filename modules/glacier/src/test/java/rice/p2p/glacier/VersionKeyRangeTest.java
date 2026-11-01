package rice.p2p.glacier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.IdRange;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class VersionKeyRangeTest {

  private Environment env;
  private IdRange underlying;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    PastryIdFactory idFactory = new PastryIdFactory(env);
    underlying = idFactory.buildIdRange(idFactory.buildId("cw"), idFactory.buildId("ccw"));
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void equalsFollowsTheUnderlyingRangeAndRejectsForeignObjects() {
    VersionKeyRange range = new VersionKeyRange(underlying);

    assertEquals(new VersionKeyRange(underlying), range);
    assertNotEquals(range, underlying);
    assertNotEquals(range, null);
    assertNotEquals(range, new FragmentKeyRange(underlying));
  }
}
