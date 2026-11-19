package rice.p2p.glacier.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.ErasureCodec;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierRefreshCompleteMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshPatchMessage;
import rice.pastry.commonapi.PastryIdFactory;

/**
 * A refresh patch is applied to a copy of the stored manifest: a patch whose signature does not verify must leave the
 * manifest in the storage as it was, a verified one replaces it.
 */
class GlacierBulkRefreshPatchTest {
  private static final int NUM_FRAGMENTS = 8;
  private static final int NUM_SURVIVORS = 2;
  private static final long FIVE_SECONDS_MILLIS = 5000;
  private static final Logger LOGGER = LoggerFactory.getLogger(GlacierBulkRefreshPatchTest.class);

  private Environment env;
  private PastryIdFactory factory;
  private List<Object> routed;
  private GlacierBulkRefresh testObj;
  private VersionKey versionKey;
  private FragmentAndManifest stored;
  private Manifest storedManifest;
  private long storedExpiration;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    GlacierConfig config = GlacierConfig.fromParameters(env.getParameters());
    var local = GlacierFakes.nodeHandle(factory.buildId("local"));
    routed = GlacierFakes.messages();
    GlacierMessageSender router = GlacierFakes.router(local, routed, env);
    var scheduler = new GlacierContinuationRegistry(GlacierFakes.endpoint(local, routed), env, LOGGER);
    GlacierPolicy policy = new InsecureGlacierPolicy(new ErasureCodec(NUM_FRAGMENTS, NUM_SURVIVORS, env));

    versionKey = new VersionKey(factory.buildId("object"), 1);
    storedExpiration = env.getTimeSource().currentTimeMillis() + FIVE_SECONDS_MILLIS;
    storedManifest = new Manifest(new byte[0], new byte[][] {new byte[0]}, storedExpiration, new byte[0], new byte[0]);
    storedManifest.setSignature(versionKey.toByteArray());
    stored = new FragmentAndManifest(new Fragment(8), storedManifest);

    // only the first fragment of the object is held, in a storage that hands out the stored instance, as a cache does
    Map<Id, FragmentAndManifest> held = new HashMap<>(Map.of(new FragmentKey(versionKey, 0), stored));
    var fragmentStorage = GlacierFakes.storage((proxy, method, args) -> switch (method.getName()) {
      case "exists" -> held.containsKey(args[0]);
      case "getMetadata" -> new FragmentMetadata(storedExpiration, 0, 0);
      case "getObject" -> {
        @SuppressWarnings("unchecked")
        Continuation<Object, Exception> continuation = (Continuation<Object, Exception>) args[1];
        continuation.receiveResult(held.get(args[0]));
        yield null;
      }
      case "store" -> {
        @SuppressWarnings("unchecked")
        Continuation<Object, Exception> continuation = (Continuation<Object, Exception>) args[3];
        held.put((Id) args[0], (FragmentAndManifest) args[2]);
        continuation.receiveResult(true);
        yield null;
      }
      default -> null;
    });
    testObj = new GlacierBulkRefresh(config, NUM_FRAGMENTS, NUM_SURVIVORS, policy, scheduler, router, new FragmentLocator(factory, NUM_FRAGMENTS), null, fragmentStorage, factory, env, LOGGER);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private GlacierRefreshCompleteMessage patch(long expiration, byte[] signature) {
    testObj.handlePatch(new GlacierRefreshPatchMessage(7, new VersionKey[] {versionKey}, new long[] {expiration}, new byte[][] {signature},
        GlacierFakes.nodeHandle(factory.buildId("sender")), factory.buildId("local"), GlacierTags.REFRESH));
    assertEquals(1, routed.size(), "the patch was not answered");
    return assertInstanceOf(GlacierRefreshCompleteMessage.class, routed.get(0));
  }

  @Test
  @DisplayName("a patch that is not signed by the owner leaves the stored manifest as it was")
  void rejectedPatchLeavesTheStoredManifestUnchanged() {
    long extended = storedExpiration + FIVE_SECONDS_MILLIS;

    GlacierRefreshCompleteMessage answer = patch(extended, new byte[] {1, 2, 3});

    assertEquals(0, answer.getUpdates(0), "a forged patch was counted as an update");
    assertSame(storedManifest, stored.getManifest());
    assertEquals(storedExpiration, stored.getManifest().getExpiration(), "the rejected patch changed the expiration of the stored manifest");
    assertArrayEquals(versionKey.toByteArray(), stored.getManifest().getSignature(), "the rejected patch changed the signature of the stored manifest");
  }

  @Test
  @DisplayName("a patch signed by the owner replaces the stored manifest")
  void acceptedPatchReplacesTheStoredManifest() {
    long extended = storedExpiration + FIVE_SECONDS_MILLIS;

    GlacierRefreshCompleteMessage answer = patch(extended, versionKey.toByteArray());

    assertEquals(1, answer.getUpdates(0));
    assertEquals(extended, stored.getManifest().getExpiration());
  }
}
