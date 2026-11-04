package rice.p2p.glacier.v2;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierRefreshCompleteMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshPatchMessage;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlacierBulkRefreshTest {
  /** The default config needs numSurvivors * 3 = 6 refreshed fragments to call a refresh successful. */
  private static final int NUM_FRAGMENTS = 8;
  private static final int NUM_SURVIVORS = 2;
  private static final Logger LOGGER = LoggerFactory.getLogger(GlacierBulkRefreshTest.class);

  private Environment env;
  private PastryIdFactory factory;
  private NodeHandle local;
  private List<Object> routed;
  private GlacierScheduler scheduler;
  private GlacierBulkRefresh testObj;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    GlacierConfig config = GlacierConfig.fromParameters(env.getParameters());
    Id localId = factory.buildId("local");
    local = GlacierFakes.nodeHandle(localId);
    routed = GlacierFakes.messages();
    GlacierRouter router = GlacierFakes.router(local, routed, env);
    scheduler = new GlacierScheduler(GlacierFakes.endpoint(local, routed), env, LOGGER);
    FragmentLocator locator = new FragmentLocator(factory, NUM_FRAGMENTS);

    // every fragment is held locally, but its stored object cannot be read (it vanished)
    var fragmentStorage = GlacierFakes.storage((proxy, method, args) -> switch (method.getName()) {
      case "exists" -> true;
      case "getMetadata" -> new FragmentMetadata(env.getTimeSource().currentTimeMillis() + 1000, 0, 0);
      case "getObject" -> {
        @SuppressWarnings("unchecked")
        Continuation<Object, Exception> c = (Continuation<Object, Exception>) args[1];
        c.receiveResult(null);
        yield null;
      }
      default -> null;
    });

    testObj = new GlacierBulkRefresh(config, NUM_FRAGMENTS, NUM_SURVIVORS, null, scheduler, router, locator, null, fragmentStorage, factory, env, LOGGER);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void aRefreshThatGivesUpAfterReportingSuccessDoesNotAnswerAgain() {
    Id objectId = factory.buildId("object");
    VersionKey versionKey = new VersionKey(objectId, 1);
    List<Object[]> answers = new ArrayList<>();
    Continuation<Object[], Exception> command = new Continuation<>() {
      @Override
      public void receiveResult(Object[] result) {
        answers.add(result);
      }

      @Override
      public void receiveException(Exception exception) {
        throw new AssertionError(exception);
      }
    };
    testObj.refresh(new Id[] {objectId}, new long[] {1}, new long[] {env.getTimeSource().currentTimeMillis() + 5000}, command);
    GlacierContinuation refresh = scheduler.get(0);

    // the holder reports that it refreshed all fragments: the refresh is answered with success
    refresh.syncReceiveResult(new GlacierRefreshCompleteMessage(0, new VersionKey[] {versionKey}, new int[] {NUM_FRAGMENTS}, local, factory.buildId("local"), GlacierTags.REFRESH));
    assertEquals(1, answers.size());
    assertEquals(true, answers.get(0)[0]);

    // the refresh keeps running until it gives up; that must not answer a second time
    for (int step = 0; step < 100 && !refresh.hasTerminated(); step++)
      refresh.syncTimeoutExpired();

    assertTrue(refresh.hasTerminated(), "the refresh should have given up");
    assertEquals(1, answers.size(), "the refresh was answered again when it gave up");
  }

  @Test
  void aRefreshPatchIsAnsweredWhenAFragmentVanishesBeforeItCanBeFetched() {
    VersionKey versionKey = new VersionKey(factory.buildId("object"), 1);
    GlacierRefreshPatchMessage patch = new GlacierRefreshPatchMessage(7, new VersionKey[] {versionKey}, new long[] {env.getTimeSource().currentTimeMillis() + 5000}, new byte[][] {new byte[0]}, GlacierFakes.nodeHandle(factory.buildId("sender")), factory.buildId("local"), GlacierTags.REFRESH);

    assertDoesNotThrow(() -> testObj.handlePatch(patch));

    assertEquals(1, routed.size(), "the patch was not answered");
    GlacierRefreshCompleteMessage answer = assertInstanceOf(GlacierRefreshCompleteMessage.class, routed.get(0));
    assertEquals(0, answer.getUpdates(0));
  }
}
