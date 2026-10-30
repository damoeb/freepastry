package rice.p2p.glacier.v2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.glacier.ErasureCodec;
import rice.p2p.glacier.FragmentKeyFactory;
import rice.p2p.multiring.MultiringIdFactory;
import rice.p2p.multiring.MultiringNode;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.persistence.EmptyCache;
import rice.persistence.MemoryStorage;
import rice.persistence.StorageManager;
import rice.persistence.StorageManagerImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wires a GlacierImpl on a single simulated node, and checks the facade and its collaborators through
 * the debug commands, which read the state of most of them.
 */
class GlacierImplTest {

  private static final int NUM_FRAGMENTS = 8;
  private static final int NUM_SURVIVORS = 4;
  private static final String INSTANCE = "glacier-test";

  private Environment env;
  private GlacierImpl testObj;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    NetworkSimulator<DirectNodeHandle, RawMessage> simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    PastryNode pastryNode = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env).newNode();
    MultiringNode node = new MultiringNode(new PastryIdFactory(env).buildId("ring"), pastryNode);
    MultiringIdFactory ringFactory = (MultiringIdFactory) node.getIdFactory();
    IdFactory fragmentKeyFactory = new FragmentKeyFactory(ringFactory);
    StorageManager fragmentStorage = new StorageManagerImpl(fragmentKeyFactory, new MemoryStorage(fragmentKeyFactory), new EmptyCache(fragmentKeyFactory));
    StorageManager neighborStorage = new StorageManagerImpl(ringFactory, new MemoryStorage(ringFactory), new EmptyCache(ringFactory));
    GlacierPolicy policy = new GlacierDefaultPolicy(new ErasureCodec(NUM_FRAGMENTS, NUM_SURVIVORS, env), INSTANCE, env);

    testObj = new GlacierImpl(node, fragmentStorage, neighborStorage, NUM_FRAGMENTS, NUM_SURVIVORS, ringFactory, INSTANCE, policy);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void debugCommandsForAnotherInstanceAreIgnored() {
    assertNull(testObj.handleDebugCommand("glacier.other status"));
    assertNull(testObj.handleDebugCommand("status"));
  }

  @Test
  void statusReportsAnEmptyNodeThatIsResponsibleForItself() {
    String actual = testObj.handleDebugCommand("g status");

    assertTrue(actual.startsWith("Responsible for: "), actual);
    assertTrue(actual.contains("0 fragments\n0 neighbors\n0 active continuations\n0 pending requests\n"), actual);
  }

  @Test
  void showConfigReportsTheCodingAndTheRuntimeSettings() {
    testObj.setSyncMaxFragments(17);
    testObj.setRateLimit(5);

    String actual = testObj.handleDebugCommand("glacier.test show config");

    assertTrue(actual.startsWith("numFragments = 8\nnumSurvivors = 4\n"), actual);
    assertTrue(actual.contains("syncMaxFragments = 17\n"), actual);
    assertTrue(actual.endsWith("rateLimitedRequestsPerSecond = 5\n"), actual);
  }

  @Test
  void listReportsNoFragments() {
    assertEquals("0 fragment(s)\n", testObj.handleDebugCommand("g ls"));
  }

  @Test
  void trashIsEmptyWithoutATrashStorage() {
    assertEquals(0, testObj.getTrashSize());
    assertEquals(1, testObj.getReplicationFactor());
    assertEquals(INSTANCE, testObj.getInstance());
  }
}
