package rice.p2p.glacier.v2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.glacier.ErasureCodec;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.VersionKey;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The manifests of the default policy must be unforgeable: only the owner of an object can sign one for it,
 * and only the owner can extend its lifetime.
 */
@DisplayName("GlacierDefaultPolicy signatures")
class GlacierDefaultPolicyTest {
  private static final int NUM_FRAGMENTS = 4;
  private static final int NUM_SURVIVORS = 2;
  private static final String INSTANCE = "glacier-policy";
  private static final long EXPIRATION = 1_000_000L;
  private static final long EXTENDED_EXPIRATION = 2_000_000L;

  private Environment env;
  private PastryIdFactory factory;
  private GlacierPolicy owner;
  private GlacierPolicy attacker;
  private GlacierPolicy verifier;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    owner = newPolicy();
    attacker = newPolicy();
    verifier = newPolicy();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private GlacierPolicy newPolicy() {
    return new GlacierDefaultPolicy(new ErasureCodec(NUM_FRAGMENTS, NUM_SURVIVORS, env), INSTANCE, env);
  }

  private static DebugContent content(rice.p2p.commonapi.Id id) {
    return new DebugContent(id, false, 0, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
  }

  private static Manifest createManifest(GlacierPolicy policy, VersionKey key, DebugContent obj) {
    Fragment[] fragments = policy.encodeObject(obj, new boolean[] {true, true, true, true});
    return policy.createManifests(key, obj, fragments, EXPIRATION)[0];
  }

  private VersionKey ownersKey() {
    return new VersionKey(factory.buildId("victim"), 0);
  }

  @Test
  @DisplayName("a manifest whose signature is the bytes of its key is rejected")
  void rejectsTheKeyBytesAsASignature() {
    VersionKey key = ownersKey();
    Manifest forged = new Manifest(new byte[32], new byte[][] {new byte[32], new byte[32], new byte[32], new byte[32]}, Long.MAX_VALUE);
    forged.setSignature(key.toByteArray());

    assertFalse(verifier.checkSignature(forged, key));
  }

  @Test
  @DisplayName("a manifest signed by a node that does not own the id is rejected (fragment squatting)")
  void rejectsAManifestOfANonOwner() {
    VersionKey key = ownersKey();
    Manifest squatted = createManifest(attacker, key, content(key.id()));

    assertFalse(verifier.checkSignature(squatted, key));
  }

  @Test
  @DisplayName("a node that does not own the object cannot extend its expiration")
  void aNonOwnerCannotExtendTheExpiration() {
    VersionKey key = ownersKey();
    Manifest original = createManifest(owner, key, content(key.id()));

    assertNull(attacker.updateManifest(key, original, EXTENDED_EXPIRATION));
  }
}
