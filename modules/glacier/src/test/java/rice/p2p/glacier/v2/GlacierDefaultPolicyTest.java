package rice.p2p.glacier.v2;

import java.security.KeyPair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.ErasureCodec;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.VersionKey;
import rice.p2p.util.SecurityUtils;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  private static KeyPair ownerKeys;
  private static KeyPair attackerKeys;
  private static KeyPair verifierKeys;

  private Environment env;
  private PastryIdFactory factory;
  private GlacierPolicy owner;
  private GlacierPolicy attacker;
  private GlacierPolicy verifier;
  private Id applicationId;

  @BeforeAll
  static void generateKeys() {
    ownerKeys = SecurityUtils.generateKeyAsymmetric();
    attackerKeys = SecurityUtils.generateKeyAsymmetric();
    verifierKeys = SecurityUtils.generateKeyAsymmetric();
  }

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    owner = newPolicy(ownerKeys);
    attacker = newPolicy(attackerKeys);
    verifier = newPolicy(verifierKeys);
    applicationId = factory.buildId("victim");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private GlacierPolicy newPolicy(KeyPair keys) {
    return new GlacierDefaultPolicy(new ErasureCodec(NUM_FRAGMENTS, NUM_SURVIVORS, env), keys, factory, INSTANCE, env);
  }

  private static DebugContent content(Id id) {
    return new DebugContent(id, false, 0, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
  }

  private static Manifest createManifest(GlacierPolicy policy, VersionKey key, DebugContent obj) {
    Fragment[] fragments = policy.encodeObject(obj, new boolean[] {true, true, true, true});
    Manifest[] manifests = policy.createManifests(key, obj, fragments, EXPIRATION);
    return manifests == null ? null : manifests[0];
  }

  private VersionKey ownersKey() {
    return new VersionKey(owner.storageId(applicationId), 0);
  }

  private Manifest ownersManifest() {
    return createManifest(owner, ownersKey(), content(applicationId));
  }

  /** What a node that is not the owner can do: sign the manifest with its own private key. */
  private static Manifest signedBy(KeyPair keys, VersionKey key, Manifest manifest) {
    Manifest result = manifest.withExpiration(manifest.getExpiration(), null);
    result.setSignature(SecurityUtils.sign(GlacierDefaultPolicy.signedBytes(key, result), keys.getPrivate()));
    return result;
  }

  @Test
  @DisplayName("the owner's manifest is accepted")
  void acceptsTheOwnersManifest() {
    assertTrue(verifier.checkSignature(ownersManifest(), ownersKey()));
  }

  @Test
  @DisplayName("a manifest whose signature is the bytes of its key is rejected")
  void rejectsTheKeyBytesAsASignature() {
    VersionKey key = ownersKey();
    Manifest forged = ownersManifest().withExpiration(Long.MAX_VALUE, key.toByteArray());

    assertFalse(verifier.checkSignature(forged, key));
  }

  @Test
  @DisplayName("a manifest signed by a node that does not own the id is rejected (fragment squatting)")
  void rejectsAManifestOfANonOwner() {
    VersionKey key = ownersKey();
    // the attacker signs validly with its own key, which does not derive the owner's id
    Manifest squatted = new Manifest(new byte[32], new byte[][] {new byte[32]}, EXPIRATION, SecurityUtils.encodePublicKey(attackerKeys.getPublic()), applicationId.toByteArray());
    squatted = signedBy(attackerKeys, key, squatted);

    assertFalse(verifier.checkSignature(squatted, key));
  }

  @Test
  @DisplayName("a manifest that names the owner's key but is signed by another node is rejected")
  void rejectsTheOwnersKeySignedByAnother() {
    VersionKey key = ownersKey();

    assertFalse(verifier.checkSignature(signedBy(attackerKeys, key, ownersManifest()), key));
  }

  @Test
  @DisplayName("a node does not create manifests for an id it does not own")
  void doesNotCreateManifestsForAnotherOwnersId() {
    assertNull(createManifest(attacker, ownersKey(), content(applicationId)));
  }

  @Test
  @DisplayName("each owner stores the same application id under its own storage id")
  void storageIdsDifferPerOwner() {
    assertNotEquals(owner.storageId(applicationId), attacker.storageId(applicationId));
    assertNotEquals(applicationId, owner.storageId(applicationId));
  }

  @Test
  @DisplayName("a node that does not own the object cannot extend its expiration")
  void aNonOwnerCannotExtendTheExpiration() {
    assertNull(attacker.updateManifest(ownersKey(), ownersManifest(), EXTENDED_EXPIRATION));
  }

  @Test
  @DisplayName("an extension signed by a node that is not the owner is rejected")
  void rejectsAnExtensionSignedByANonOwner() {
    VersionKey key = ownersKey();
    Manifest extended = signedBy(attackerKeys, key, ownersManifest().withExpiration(EXTENDED_EXPIRATION, null));

    assertFalse(verifier.checkSignature(extended, key));
  }

  @Test
  @DisplayName("an extension that keeps the old signature is rejected")
  void rejectsAnExtensionWithTheOldSignature() {
    VersionKey key = ownersKey();
    Manifest original = ownersManifest();

    assertFalse(verifier.checkSignature(original.withExpiration(EXTENDED_EXPIRATION, original.getSignature()), key));
  }

  @Test
  @DisplayName("the owner extends the expiration, and the extension is accepted")
  void theOwnerExtendsTheExpiration() {
    VersionKey key = ownersKey();

    Manifest extended = owner.updateManifest(key, ownersManifest(), EXTENDED_EXPIRATION);

    assertNotNull(extended);
    assertTrue(verifier.checkSignature(extended, key));
  }

  @Test
  @DisplayName("a manifest signed for one version is rejected for another")
  void rejectsAManifestForAnotherVersion() {
    assertFalse(verifier.checkSignature(ownersManifest(), new VersionKey(ownersKey().id(), 1)));
  }
}
