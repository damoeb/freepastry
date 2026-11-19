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
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.GlacierException;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlacierRetrieverTest {
  private static final int NUM_FRAGMENTS = 4;
  private static final Logger LOGGER = LoggerFactory.getLogger(GlacierRetrieverTest.class);

  private Environment env;
  private GlacierTrafficShaper trafficShaper;
  private GlacierRetriever testObj;
  private List<Object> routed;
  private FragmentKey key;
  private Manifest manifest;

  /** A trash whose lookup fails. */
  private static final class FailingTrash extends GlacierTrash {
    FailingTrash() {
      super(null, LOGGER);
    }

    @Override
    void lookupFragment(FragmentKey key, Continuation<Object, Exception> c) {
      c.receiveException(new GlacierException("trash unreadable"));
    }
  }

  /** A caller that records how it was answered. */
  private static final class RecordingCaller extends GlacierContinuation {
    final List<Object> answers = new ArrayList<>();

    @Override
    public void receiveResult(Object result) {
      answers.add(result);
    }

    @Override
    public void receiveException(Exception exception) {
      answers.add(exception);
    }

    @Override
    public void timeoutExpired() {}

    @Override
    public long getTimeout() {
      return Long.MAX_VALUE;
    }
  }

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    PastryIdFactory factory = new PastryIdFactory(env);
    GlacierConfig config = GlacierConfig.fromParameters(env.getParameters());
    NodeHandle local = GlacierFakes.nodeHandle(factory.buildId("local"));
    routed = GlacierFakes.messages();
    var endpoint = GlacierFakes.endpoint(local, routed);
    GlacierMessageSender router = GlacierFakes.router(local, routed, env);
    GlacierContinuationRegistry scheduler = new GlacierContinuationRegistry(endpoint, env, LOGGER);
    FragmentLocator locator = new FragmentLocator(factory, NUM_FRAGMENTS);
    trafficShaper = new GlacierTrafficShaper(config, NUM_FRAGMENTS, env, LOGGER);
    testObj = new GlacierRetriever(config, NUM_FRAGMENTS, 2, "test", null, endpoint, scheduler, router, locator, trafficShaper, new FailingTrash(), env, LOGGER);

    key = new FragmentKey(new VersionKey(factory.buildId("object"), 1), 0);
    manifest = new Manifest(new byte[0], new byte[NUM_FRAGMENTS][0], env.getTimeSource().currentTimeMillis() + 1000000, new byte[0], new byte[0]);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private boolean fetchWasSent() {
    return routed.stream().anyMatch(GlacierFetchMessage.class::isInstance);
  }

  @Test
  void aFailingTrashLookupFallsThroughToTheFetchFromThePrimary() {
    testObj.retrieveFragment(key, manifest, GlacierTags.SYNC_FETCH, new RecordingCaller());

    assertTrue(fetchWasSent(), "no fetch was sent to the primary holder");
  }

  @Test
  void aFailingTrashLookupDoesNotLeaveTheRequestStuckInTheTrafficShaper() {
    testObj.rateLimitedRetrieveFragment(key, manifest, GlacierTags.SYNC_FETCH, new RecordingCaller());
    assertTrue(trafficShaper.isPending(key));
    GlacierContinuation task = trafficShaper.newShaperTask();
    task.init();

    ((DirectTimeSource) env.getTimeSource()).incrementTime(1000);
    task.timeoutExpired();

    assertFalse(trafficShaper.isPending(key), "the shaper did not release the job");
    assertTrue(fetchWasSent(), "no fetch was sent to the primary holder");
  }
}
