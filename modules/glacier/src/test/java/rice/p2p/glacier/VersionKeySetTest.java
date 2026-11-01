package rice.p2p.glacier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

class VersionKeySetTest {

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
  void toStringListsTheMembers() {
    VersionKey key = new VersionKey(idFactory.buildId("object"), 7);
    VersionKeySet testObj = new VersionKeySet();
    testObj.addId(key);

    assertTrue(testObj.toString().contains(key.toString()));
  }
}
