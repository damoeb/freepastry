package rice.p2p.glacier.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.IdRange;
import rice.p2p.glacier.ErasureCodec;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.p2p.glacier.v2.messaging.GlacierMessage;
import rice.p2p.glacier.v2.messaging.GlacierNeighborRequestMessage;
import rice.p2p.glacier.v2.messaging.GlacierNeighborResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeForwardMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshCompleteMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshPatchMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshProbeMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierSyncMessage;
import rice.p2p.glacier.v2.messaging.GlacierTimeoutMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;
import rice.persistence.EmptyCache;
import rice.persistence.MemoryStorage;
import rice.persistence.StorageManager;
import rice.persistence.StorageManagerImpl;

/**
 * Every Glacier protocol message through the bytes of a socket ring, with the deserializer of a GlacierImpl that is
 * built on a socket node: read where it arrives, sent on from there, and, as the messages are routed to a key, wrapped
 * in a route message that a hop reads and unwraps.
 */
@DisplayName("Glacier messages on the wire")
class GlacierMessageWireRoundTripTest {
  private static final char TAG = 'x';
  private static final int UID = 4711;
  private static final int NUM_FRAGMENTS = 8;
  private static final int NUM_SURVIVORS = 4;
  private static final String INSTANCE = "glacier-wire";
  private static final int SENT_MESSAGE_TYPES = 14;

  private static Environment env;
  private static SocketNode socketNode;
  private static NodeHandle self;
  private static NodeHandle peer;
  private static Endpoint endpoint;
  private static Id dest;
  private static IdRange range;

  @BeforeAll
  static void setUp() throws Exception {
    env = new Environment();
    socketNode = SocketNode.create(env, "4000");
    self = socketNode.local();
    peer = socketNode.handle("8000");
    PastryIdFactory factory = new PastryIdFactory(env);
    StorageManager fragments = new StorageManagerImpl(factory, new MemoryStorage(factory), new EmptyCache(factory));
    StorageManager neighbors = new StorageManagerImpl(factory, new MemoryStorage(factory), new EmptyCache(factory));
    GlacierImpl glacier = new GlacierImpl(socketNode.node, fragments, neighbors, NUM_FRAGMENTS, NUM_SURVIVORS, factory,
        INSTANCE, new InsecureGlacierPolicy(new ErasureCodec(NUM_FRAGMENTS, NUM_SURVIVORS, env)));
    Field field = GlacierImpl.class.getDeclaredField("endpoint");
    field.setAccessible(true);
    endpoint = (Endpoint) field.get(glacier);
    dest = Id.build("9000");
    range = new rice.pastry.IdRange(Id.build("2000"), Id.build("6000"));
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  private static VersionKey versionKey(String id, long version) {
    return new VersionKey(Id.build(id), version);
  }

  private static FragmentKey fragmentKey(String id, long version, int fragmentId) {
    return new FragmentKey(versionKey(id, version), fragmentId);
  }

  private static FragmentKey[] fragmentKeys(int n) {
    FragmentKey[] keys = new FragmentKey[n];
    for (int i = 0; i < n; i++) {
      keys[i] = fragmentKey(Integer.toHexString(0x1000 + i), i, i);
    }
    return keys;
  }

  private static VersionKey[] versionKeys(int n) {
    VersionKey[] keys = new VersionKey[n];
    for (int i = 0; i < n; i++) {
      keys[i] = versionKey(Integer.toHexString(0x1000 + i), 7L * i);
    }
    return keys;
  }

  private static Fragment fragment(int size) {
    Fragment f = new Fragment(size);
    for (int i = 0; i < size; i++) {
      f.getPayload()[i] = (byte) (i * 31);
    }
    return f;
  }

  private static Manifest manifest(boolean signed) {
    byte[][] hashes = new byte[NUM_FRAGMENTS][20];
    for (int i = 0; i < hashes.length; i++) {
      Arrays.fill(hashes[i], (byte) (i + 1));
    }
    byte[] objectHash = new byte[20];
    Arrays.fill(objectHash, (byte) 3);
    Manifest m = new Manifest(objectHash, hashes, 1_700_000_000_000L, new byte[] {5, 5, 5}, new byte[] {6, 6});
    if (signed) {
      m.setSignature(new byte[] {9, 8, 7});
    }
    return m;
  }

  /** The message through the transport-level read, and routed to its destination over two hops. */
  private static <M extends GlacierMessage> M send(M message) throws IOException {
    M direct = WireRoundTrip.roundTrip(message, endpoint.getDeserializer(), peer);
    assertCommon(message, direct);
    M routed = socketNode.route(endpoint, message.getDestination(), message, peer, false);
    assertCommon(message, routed);
    M routedToHandle = socketNode.route(endpoint, message.getDestination(), message, peer, true);
    assertCommon(message, routedToHandle);
    return routed;
  }

  private static void assertCommon(GlacierMessage expected, GlacierMessage actual) {
    assertEquals(expected.getClass(), actual.getClass());
    assertEquals(expected.getUID(), actual.getUID());
    assertEquals(expected.getTag(), actual.getTag());
    assertEquals(expected.isResponse(), actual.isResponse());
    assertEquals(expected.getDestination(), actual.getDestination());
    assertEquals(expected.getSource().getId(), actual.getSource().getId());
    assertEquals(expected.getType(), actual.getType());
  }

  private static void assertSameKeys(FragmentKey[] expected, int count, IntFunction<FragmentKey> actual) {
    assertEquals(expected.length, count);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual.apply(i));
    }
  }

  static Stream<Integer> counts() {
    return Stream.of(0, 1, 2, 17);
  }

  @ParameterizedTest(name = "{0} keys")
  @MethodSource("counts")
  void dataMessageWithFragmentsAndManifests(int n) throws IOException {
    FragmentKey[] keys = fragmentKeys(n);
    Fragment[] fragments = new Fragment[n];
    Manifest[] manifests = new Manifest[n];
    for (int i = 0; i < n; i++) {
      fragments[i] = fragment(i * 3);
      manifests[i] = manifest(i % 2 == 0);
    }
    GlacierDataMessage actual = send(new GlacierDataMessage(UID, keys, fragments, manifests, self, dest, true, TAG));
    assertSameKeys(keys, actual.numKeys(), actual::getKey);
    for (int i = 0; i < n; i++) {
      assertArrayEquals(fragments[i].getPayload(), actual.getFragment(i).getPayload());
      assertArrayEquals(manifests[i].getObjectHash(), actual.getManifest(i).getObjectHash());
      assertArrayEquals(manifests[i].getSignature(), actual.getManifest(i).getSignature());
      assertArrayEquals(manifests[i].getOwnerKey(), actual.getManifest(i).getOwnerKey());
      assertArrayEquals(manifests[i].getName(), actual.getManifest(i).getName());
      assertEquals(manifests[i].getExpiration(), actual.getManifest(i).getExpiration());
    }
  }

  @Test
  void dataMessageWithNullFragmentsAndManifests() throws IOException {
    FragmentKey[] keys = fragmentKeys(3);
    Fragment[] fragments = {null, fragment(5), null};
    Manifest[] manifests = {manifest(false), null, null};
    GlacierDataMessage actual = send(new GlacierDataMessage(UID, keys, fragments, manifests, self, dest, false, TAG));
    assertNull(actual.getFragment(0));
    assertArrayEquals(fragments[1].getPayload(), actual.getFragment(1).getPayload());
    assertNull(actual.getFragment(2));
    assertArrayEquals(manifests[0].getObjectHash(), actual.getManifest(0).getObjectHash());
    assertNull(actual.getManifest(1));
    assertNull(actual.getManifest(2));
  }

  @Test
  void dataMessageWithEmptyFragmentAndSingleKeyConstructor() throws IOException {
    GlacierDataMessage actual = send(
        new GlacierDataMessage(UID, fragmentKey("1234", 1, 2), new Fragment(0), manifest(true), self, dest, true, TAG));
    assertEquals(1, actual.numKeys());
    assertEquals(0, actual.getFragment(0).getPayload().length);
  }

  @ParameterizedTest(name = "{0} keys")
  @MethodSource("counts")
  void fetchMessage(int n) throws IOException {
    FragmentKey[] keys = fragmentKeys(n);
    GlacierFetchMessage actual = send(
        new GlacierFetchMessage(UID, keys, GlacierFetchMessage.FETCH_FRAGMENT_AND_MANIFEST, self, dest, TAG));
    assertSameKeys(keys, actual.getNumKeys(), actual::getKey);
    assertEquals(GlacierFetchMessage.FETCH_FRAGMENT_AND_MANIFEST, actual.getRequest());
  }

  @Test
  void fetchMessageForSingleKey() throws IOException {
    GlacierFetchMessage actual = send(
        new GlacierFetchMessage(UID, fragmentKey("77", 3, 5), GlacierFetchMessage.FETCH_MANIFEST, self, dest, TAG));
    assertEquals(fragmentKey("77", 3, 5), actual.getKey(0));
    assertEquals(GlacierFetchMessage.FETCH_MANIFEST, actual.getRequest());
  }

  @ParameterizedTest(name = "{0} keys")
  @MethodSource("counts")
  void queryMessage(int n) throws IOException {
    FragmentKey[] keys = fragmentKeys(n);
    GlacierQueryMessage actual = send(new GlacierQueryMessage(UID, keys, self, dest, TAG));
    assertSameKeys(keys, actual.numKeys(), actual::getKey);
  }

  @ParameterizedTest(name = "{0} keys")
  @MethodSource("counts")
  void responseMessage(int n) throws IOException {
    FragmentKey[] keys = fragmentKeys(n);
    long[] lifetimes = new long[n];
    boolean[] haveIt = new boolean[n];
    boolean[] authoritative = new boolean[n];
    for (int i = 0; i < n; i++) {
      lifetimes[i] = Long.MAX_VALUE - i;
      haveIt[i] = i % 2 == 0;
      authoritative[i] = i % 3 == 0;
    }
    GlacierResponseMessage actual = send(
        new GlacierResponseMessage(UID, keys, haveIt, lifetimes, authoritative, self, dest, true, TAG));
    assertSameKeys(keys, actual.numKeys(), actual::getKey);
    for (int i = 0; i < n; i++) {
      assertEquals(haveIt[i], actual.hasFragment(i));
      assertEquals(authoritative[i], actual.getAuthoritative(i));
      assertEquals(lifetimes[i], actual.getExpiration(i));
    }
  }

  @Test
  void responseMessageForSingleKey() throws IOException {
    GlacierResponseMessage actual = send(
        new GlacierResponseMessage(UID, fragmentKey("55", 1, 1), true, 99L, false, self, dest, false, TAG));
    assertTrue(actual.hasFragment(0));
    assertFalse(actual.getAuthoritative(0));
    assertEquals(99L, actual.getExpiration(0));
  }

  @Test
  void neighborRequestMessage() throws IOException {
    GlacierNeighborRequestMessage actual = send(new GlacierNeighborRequestMessage(UID, range, self, dest, TAG));
    assertEquals(range, actual.getRequestedRange());
  }

  @ParameterizedTest(name = "{0} neighbors")
  @MethodSource("counts")
  void neighborResponseMessage(int n) throws IOException {
    rice.p2p.commonapi.Id[] neighbors = new rice.p2p.commonapi.Id[n];
    long[] lastSeen = new long[n];
    for (int i = 0; i < n; i++) {
      neighbors[i] = Id.build(Integer.toHexString(0x2000 + i));
      lastSeen[i] = 1_000L * i;
    }
    GlacierNeighborResponseMessage actual = send(
        new GlacierNeighborResponseMessage(UID, neighbors, lastSeen, self, dest, TAG));
    assertEquals(n, actual.numNeighbors());
    for (int i = 0; i < n; i++) {
      assertEquals(neighbors[i], actual.getNeighbor(i));
      assertEquals(lastSeen[i], actual.getLastSeen(i));
    }
  }

  static Stream<IdRange> ranges() {
    return Stream.of(new rice.pastry.IdRange(Id.build("2000"), Id.build("6000")), new rice.pastry.IdRange(true),
        new rice.pastry.IdRange(false), new rice.pastry.IdRange(Id.build("F000"), Id.build("1000")));
  }

  @ParameterizedTest
  @MethodSource("ranges")
  void rangeQueryMessage(IdRange r) throws IOException {
    assertEquals(r, send(new GlacierRangeQueryMessage(UID, r, self, dest, TAG)).getRequestedRange());
  }

  @ParameterizedTest
  @MethodSource("ranges")
  void rangeResponseMessage(IdRange r) throws IOException {
    assertEquals(r, send(new GlacierRangeResponseMessage(UID, r, self, dest, TAG)).getCommonRange());
  }

  @ParameterizedTest
  @MethodSource("ranges")
  void rangeForwardMessage(IdRange r) throws IOException {
    GlacierRangeForwardMessage actual = send(new GlacierRangeForwardMessage(UID, r, peer, self, dest, TAG));
    assertEquals(r, actual.getRequestedRange());
    assertEquals(peer.getId(), actual.getRequestor().getId());
  }

  @ParameterizedTest(name = "{0} keys")
  @MethodSource("counts")
  void refreshCompleteMessage(int n) throws IOException {
    VersionKey[] keys = versionKeys(n);
    int[] updates = new int[n];
    for (int i = 0; i < n; i++) {
      updates[i] = Integer.MAX_VALUE - i;
    }
    GlacierRefreshCompleteMessage actual = send(new GlacierRefreshCompleteMessage(UID, keys, updates, self, dest, TAG));
    assertEquals(n, actual.numKeys());
    for (int i = 0; i < n; i++) {
      assertEquals(keys[i], actual.getKey(i));
      assertEquals(updates[i], actual.getUpdates(i));
    }
  }

  @ParameterizedTest(name = "{0} keys")
  @MethodSource("counts")
  void refreshPatchMessage(int n) throws IOException {
    VersionKey[] keys = versionKeys(n);
    long[] lifetimes = new long[n];
    byte[][] signatures = new byte[n][];
    for (int i = 0; i < n; i++) {
      lifetimes[i] = 5_000L * i;
      signatures[i] = new byte[i % 3 == 0 ? 0 : i];
      Arrays.fill(signatures[i], (byte) i);
    }
    GlacierRefreshPatchMessage actual = send(
        new GlacierRefreshPatchMessage(UID, keys, lifetimes, signatures, self, dest, TAG));
    assertEquals(n, actual.numKeys());
    for (int i = 0; i < n; i++) {
      assertEquals(keys[i], actual.getKey(i));
      assertEquals(lifetimes[i], actual.getLifetime(i));
      assertArrayEquals(signatures[i], actual.getSignature(i));
    }
  }

  @Test
  void refreshProbeMessage() throws IOException {
    Id requested = Id.build("ABCD");
    assertEquals(requested, send(new GlacierRefreshProbeMessage(UID, requested, self, dest, TAG)).getRequestedId());
  }

  @ParameterizedTest
  @MethodSource("ranges")
  void refreshResponseMessage(IdRange r) throws IOException {
    for (boolean online : new boolean[] {true, false}) {
      GlacierRefreshResponseMessage actual = send(new GlacierRefreshResponseMessage(UID, r, online, self, dest, TAG));
      assertEquals(r, actual.getRange());
      assertEquals(online, actual.isOnline());
    }
  }

  static Stream<BloomFilter> bloomFilters() {
    Environment e = new Environment();
    try {
      BloomFilter filled = new BloomFilter(1024, 3, e.getRandomSource());
      filled.add(new byte[] {1, 2, 3});
      return Stream.of(filled, new BloomFilter(64, 1, e.getRandomSource()));
    } finally {
      e.destroy();
    }
  }

  @ParameterizedTest
  @MethodSource("bloomFilters")
  void syncMessage(BloomFilter bloom) throws IOException {
    GlacierSyncMessage actual = send(new GlacierSyncMessage(UID, range, 12, bloom, self, dest, TAG));
    assertEquals(range, actual.getRange());
    assertEquals(12, actual.getOffsetFID());
    assertEquals(bloom.toString(), actual.getBloomFilter().toString());
    assertEquals(bloom.contains(new byte[] {1, 2, 3}), actual.getBloomFilter().contains(new byte[] {1, 2, 3}));
  }

  @Test
  @DisplayName("the local timeout message is never sent")
  void timeoutMessageIsNotSerializable() {
    GlacierTimeoutMessage timeout = new GlacierTimeoutMessage(UID, self);
    assertThrows(RuntimeException.class, () -> timeout.serialize(new SimpleOutputBuffer()));
    assertThrows(IOException.class,
        () -> endpoint.getDeserializer().deserialize(new SimpleInputBuffer(new byte[0]), timeout.getType(), 0, peer));
  }

  @Test
  void everySentMessageTypeHasATest() {
    assertEquals(SENT_MESSAGE_TYPES + 1, GlacierMessage.class.getPermittedSubclasses().length);
  }
}
