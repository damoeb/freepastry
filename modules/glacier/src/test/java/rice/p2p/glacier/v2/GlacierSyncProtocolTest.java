package rice.p2p.glacier.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierSyncMessage;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.CachedStorage;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

/**
 * A peer chooses the fragment offset of its sync request, and the fragment ids a node stores come from the keys
 * peers send. Neither may make the node answering a sync request throw an {@link Error}, which destroys the
 * environment.
 */
@DisplayName("Glacier sync protocol")
class GlacierSyncProtocolTest {
  private static final int NUM_FRAGMENTS = 4;
  private static final int VALID_OFFSET = 1;
  private static final long TIMEOUT_SECONDS = 10;
  private static final Logger LOGGER = LoggerFactory.getLogger(GlacierSyncProtocolTest.class);

  private Environment env;
  private final List<Throwable> reported = new CopyOnWriteArrayList<>();
  private PastryIdFactory factory;
  private GlacierConfig config;
  private List<Object> routed;
  private NodeHandle requester;
  private Id localId;
  private GlacierNeighborhood neighborhood;
  private CachedStorage fragmentStorage;
  private boolean signatureValid = true;
  private GlacierSyncProtocol testObj;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    var original = env.getExceptionStrategy();
    env.setExceptionStrategy((source, t) -> {
      reported.add(t);
      original.handleException(source, t);
    });
    factory = new PastryIdFactory(env);
    config = GlacierConfig.fromParameters(env.getParameters());
    localId = factory.buildId("local");
    NodeHandle local = GlacierFakes.nodeHandle(localId);
    requester = GlacierFakes.nodeHandle(factory.buildId("requester"));
    routed = GlacierFakes.messages();
    GlacierMessageSender router = GlacierFakes.router(local, routed, env);
    FragmentLocator locator = new FragmentLocator(factory, NUM_FRAGMENTS);

    // no neighbors: this node is responsible for the whole ring
    neighborhood = new GlacierNeighborhood(
      GlacierFakes.storage((proxy, method, args) -> method.getName().equals("scan") ? factory.buildIdSet() : null),
      factory, localId, config.neighborTimeout(), env, LOGGER);
    neighborhood.determineResponsibleRange();

    GlacierPolicy policy = (GlacierPolicy) Proxy.newProxyInstance(GlacierPolicy.class.getClassLoader(), new Class<?>[] {GlacierPolicy.class}, (proxy, method, args) -> {
      if (method.getName().equals("checkSignature")) {
        return signatureValid;
      }
      return null;
    });

    fragmentStorage = new CachedStorageImpl(factory, new MemoryStorage(factory), new NoOpCache(factory));
    GlacierContinuationRegistry scheduler = new GlacierContinuationRegistry(GlacierFakes.endpoint(local, routed), env, LOGGER);
    GlacierTrafficShaper trafficShaper = new GlacierTrafficShaper(config, NUM_FRAGMENTS, env, LOGGER);
    testObj = new GlacierSyncProtocol(config, NUM_FRAGMENTS, policy, GlacierFakes.endpoint(local, routed), scheduler, router,
        locator, neighborhood, trafficShaper, fragmentStorage, factory, env, LOGGER);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /** Stores a fragment that is old enough and lives long enough to be offered to a peer that lacks it. */
  private void storeSyncableFragment(int fragmentId) throws Exception {
    long now = env.getTimeSource().currentTimeMillis();
    Manifest manifest = new Manifest(new byte[0], new byte[NUM_FRAGMENTS][0], Long.MAX_VALUE / 2, new byte[0], new byte[0]);
    FragmentKey key = new FragmentKey(new VersionKey(factory.buildId("object"), 1), fragmentId);
    FragmentMetadata metadata = new FragmentMetadata(Long.MAX_VALUE / 2, 0, now - config.syncMinQuietTime() - 1);
    CompletableFuture<Object> stored = new CompletableFuture<>();
    fragmentStorage.store(key, metadata, new FragmentAndManifest(new Fragment(0), manifest), new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        stored.complete(result);
      }

      @Override
      public void receiveException(Exception exception) {
        stored.completeExceptionally(exception);
      }
    });
    stored.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /** Delivers a sync request with an empty bloom filter (the requester lacks everything) on the selector thread. */
  private void receiveSyncRequest(int offset) throws Exception {
    GlacierSyncMessage request = new GlacierSyncMessage(1, neighborhood.getResponsibleRange(), offset,
        new FragmentBloomFilter(64, 2, env.getRandomSource()), requester, localId, GlacierTags.SYNC);
    env.getSelectorManager().invoke(() -> testObj.handleSyncRequest(request));
    CompletableFuture<Void> looped = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> looped.complete(null));
    try {
      looped.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException dropped) {
      // a destroyed selector drops the invocation, the assertions tell why
    }
  }

  private void assertNodeSurvivedAndSentNothing() {
    assertEquals(List.of(), reported);
    assertTrue(env.getSelectorManager().isAlive(), "the selector thread still runs");
    assertEquals(List.of(), routed, "nothing is sent to the requester");
  }

  @Test
  @DisplayName("a request with a valid offset gets the manifest of the fragment the requester lacks")
  void validOffsetIsAnswered() throws Exception {
    storeSyncableFragment(2);

    receiveSyncRequest(VALID_OFFSET);

    assertEquals(List.of(), reported);
    assertEquals(1, routed.size(), () -> "sent: " + routed);
    GlacierDataMessage answer = assertInstanceOf(GlacierDataMessage.class, routed.get(0));
    assertEquals(2 - VALID_OFFSET, answer.getKey(0).fragmentId());
  }

  @ParameterizedTest(name = "offset {0}")
  @ValueSource(ints = {-2 * NUM_FRAGMENTS, -1, 0, NUM_FRAGMENTS, 2 * NUM_FRAGMENTS, Integer.MIN_VALUE, Integer.MAX_VALUE})
  @DisplayName("a request with an offset outside 1..numFragments-1 is dropped and the environment survives")
  void offsetOutOfRangeIsDropped(int offset) throws Exception {
    storeSyncableFragment(0);

    receiveSyncRequest(offset);

    assertNodeSurvivedAndSentNothing();
  }

  @Test
  @DisplayName("a stored fragment whose id is not below numFragments is skipped and the environment survives")
  void storedFragmentIdOutOfRangeIsSkipped() throws Exception {
    // a peer that owns an object can sign a manifest with more fragment hashes than this node uses, and so get a
    // fragment with a large id stored
    storeSyncableFragment(NUM_FRAGMENTS + VALID_OFFSET);

    receiveSyncRequest(VALID_OFFSET);

    assertNodeSurvivedAndSentNothing();
  }

  @Test
  @DisplayName("a stored fragment whose manifest no longer verifies cancels the answer and the environment survives")
  void storedManifestWithBadSignatureCancelsTheAnswer() throws Exception {
    storeSyncableFragment(2);
    signatureValid = false;

    receiveSyncRequest(VALID_OFFSET);

    assertNodeSurvivedAndSentNothing();
  }
}
