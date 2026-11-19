package rice.p2p.glacier.v2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class FragmentLocatorTest {

  private static final int NUM_FRAGMENTS = 4;

  private Environment env;
  private PastryIdFactory idFactory;
  private FragmentLocator testObj;
  private Id objectKey;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    idFactory = new PastryIdFactory(env);
    testObj = new FragmentLocator(idFactory, NUM_FRAGMENTS);
    objectKey = idFactory.buildId("object");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void firstFragmentOfFirstVersionLiesAtTheObjectKey() {
    assertEquals(objectKey, testObj.getFragmentLocation(objectKey, 0, 0));
  }

  @Test
  void fragmentsAreSpreadEvenlyAroundTheRing() {
    byte[] halfRing = new byte[20];
    halfRing[19] = (byte) 0x80;
    Id expected = objectKey.addToId(idFactory.buildIdDistance(halfRing));

    assertEquals(expected, testObj.getFragmentLocation(objectKey, NUM_FRAGMENTS / 2, 0));
  }

  @Test
  void laterVersionsAreShiftedAwayFromEarlierOnes() {
    assertNotEquals(testObj.getFragmentLocation(objectKey, 0, 0), testObj.getFragmentLocation(objectKey, 0, 1));
  }

  @Test
  void fragmentKeyLocationMatchesItsComponents() {
    FragmentKey key = new FragmentKey(new VersionKey(objectKey, 3), 1);

    assertEquals(testObj.getFragmentLocation(objectKey, 1, 3), testObj.getFragmentLocation(key));
  }
}
