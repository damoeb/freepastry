package rice.p2p.glacier.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;

/**
 * The values Glacier puts into messages and storage, through the bytes of a socket ring: serialized, read back with the
 * node's endpoint (so ids and handles are read the way a ring reads them), serialized again and read again.
 */
@DisplayName("Glacier values on the wire")
class GlacierValueWireRoundTripTest {
  private static final int HASH_LENGTH = 20;

  private static Environment env;
  private static SocketNode socketNode;
  private static NodeHandle peer;
  private static Endpoint endpoint;

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    socketNode = SocketNode.create(env, "4000");
    peer = socketNode.handle("8000");
    endpoint = socketNode.node.buildEndpoint(new Application() {
      @Override
      public boolean forward(RouteMessage message) {
        return true;
      }

      @Override
      public void deliver(rice.p2p.commonapi.Id id, Message message) {
      }

      @Override
      public void update(rice.p2p.commonapi.NodeHandle handle, boolean joined) {
      }
    }, "glacier-values");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  private static byte[] filled(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (seed + i * 7);
    }
    return bytes;
  }

  private static Manifest manifest(int fragments, int hashLength, byte[] signature) {
    byte[][] hashes = new byte[fragments][];
    for (int i = 0; i < fragments; i++) {
      hashes[i] = filled(hashLength, i);
    }
    Manifest m = new Manifest(filled(HASH_LENGTH, 99), hashes, 1_700_000_000_000L, filled(300, 5), filled(40, 6));
    m.setSignature(signature);
    return m;
  }

  private static void assertSameManifest(Manifest expected, Manifest actual) {
    assertArrayEquals(expected.getObjectHash(), actual.getObjectHash());
    assertEquals(expected.getExpiration(), actual.getExpiration());
    assertArrayEquals(expected.getSignature(), actual.getSignature());
    assertArrayEquals(expected.getOwnerKey(), actual.getOwnerKey());
    assertArrayEquals(expected.getName(), actual.getName());
    assertEquals(expected.getFragmentHashes().length, actual.getFragmentHashes().length);
    for (int i = 0; i < expected.getFragmentHashes().length; i++) {
      assertArrayEquals(expected.getFragmentHash(i), actual.getFragmentHash(i));
    }
  }

  @ParameterizedTest(name = "{0} bytes")
  @ValueSource(ints = {0, 1, 255, 256, 70_000})
  void fragment(int size) throws IOException {
    Fragment original = new Fragment(size);
    System.arraycopy(filled(size, 5), 0, original.getPayload(), 0, size);
    Fragment copy = WireRoundTrip.roundTrip(original, Fragment::serialize, Fragment::new);
    assertArrayEquals(original.getPayload(), copy.getPayload());
  }

  @Test
  void fragmentKeyAndVersionKey() throws IOException {
    VersionKey version = new VersionKey(Id.build("ABCD"), Long.MAX_VALUE);
    assertEquals(version, WireRoundTrip.roundTrip(version, VersionKey::serialize, buf -> new VersionKey(buf, endpoint)));
    VersionKey negative = new VersionKey(Id.build(), -1L);
    assertEquals(negative, WireRoundTrip.roundTrip(negative, VersionKey::serialize, buf -> new VersionKey(buf, endpoint)));
    FragmentKey key = new FragmentKey(version, Integer.MAX_VALUE);
    assertEquals(key, WireRoundTrip.roundTrip(key, FragmentKey::serialize, buf -> new FragmentKey(buf, endpoint)));
    FragmentKey first = new FragmentKey(negative, 0);
    assertEquals(first, WireRoundTrip.roundTrip(first, FragmentKey::serialize, buf -> new FragmentKey(buf, endpoint)));
  }

  @Test
  void fragmentMetadata() throws IOException {
    FragmentMetadata original = new FragmentMetadata(Long.MAX_VALUE, 0L, -5L);
    assertEquals(original, WireRoundTrip.roundTrip(original, FragmentMetadata::serialize, FragmentMetadata::new));
  }

  @ParameterizedTest(name = "{0} fragment hashes")
  @ValueSource(ints = {1, 2, 8, 32})
  void manifestWithAndWithoutSignature(int fragments) throws IOException {
    for (byte[] signature : new byte[][] {null, new byte[0], filled(128, 1)}) {
      Manifest original = manifest(fragments, HASH_LENGTH, signature);
      assertSameManifest(original, WireRoundTrip.roundTrip(original, Manifest::serialize, Manifest::new));
    }
  }

  @Test
  void bloomFilter() throws IOException {
    BloomFilter filled = new BloomFilter(2048, 4, env.getRandomSource());
    filled.add(filled(10, 1));
    filled.add(filled(33, 2));
    BloomFilter copy = WireRoundTrip.roundTrip(filled, BloomFilter::serialize, BloomFilter::new);
    assertEquals(filled.toString(), copy.toString());
    assertEquals(true, copy.contains(filled(10, 1)));
    assertEquals(true, copy.contains(filled(33, 2)));
    BloomFilter small = new BloomFilter(16, 1, env.getRandomSource());
    assertEquals(small.toString(), WireRoundTrip.roundTrip(small, BloomFilter::serialize, BloomFilter::new).toString());
  }

  @Test
  void fragmentAndManifestWithAndWithoutSignature() throws IOException {
    for (byte[] signature : new byte[][] {null, filled(40, 3)}) {
      FragmentAndManifest original = new FragmentAndManifest(new Fragment(0), manifest(4, HASH_LENGTH, signature));
      FragmentAndManifest copy = WireRoundTrip.roundTrip(original, FragmentAndManifest::new);
      assertEquals(0, copy.getFragment().getPayload().length);
      assertSameManifest(original.getManifest(), copy.getManifest());
    }
  }

  @ParameterizedTest(name = "mutable={0}")
  @ValueSource(booleans = {true, false})
  void debugContent(boolean mutable) throws IOException {
    for (byte[] payload : new byte[][] {new byte[0], filled(300, 4)}) {
      DebugContent original = new DebugContent(Id.build("1234"), mutable, 17L, payload);
      DebugContent copy = WireRoundTrip.roundTrip(original, buf -> new DebugContent(buf, endpoint));
      assertEquals(original.getId(), copy.getId());
      assertEquals(original.isMutable(), copy.isMutable());
      assertEquals(original.getVersion(), copy.getVersion());
      assertArrayEquals(payload, copy.getPayload());
    }
  }

  @Test
  void debugContentHandleIsReadableFieldByField() throws IOException {
    DebugContentHandle original = new DebugContentHandle(Id.build("4321"), 9L, 123_456L, peer);
    InputBuffer in = new SimpleInputBuffer(WireRoundTrip.bytes(original));
    assertEquals(original.id(), endpoint.readId(in, in.readShort()));
    assertEquals(original.version(), in.readLong());
    assertEquals(original.expiration(), in.readLong());
    assertEquals(peer.getId(), endpoint.readNodeHandle(in).getId());
    assertEquals(0, in.bytesRemaining());
  }

  @Test
  void glacierContentHandleIsReadableFieldByField() throws IOException {
    for (byte[] signature : new byte[][] {null, filled(16, 6)}) {
      Manifest manifest = manifest(8, HASH_LENGTH, signature);
      GlacierContentHandle original = new GlacierContentHandle(Id.build("4321"), 9L, peer, manifest);
      InputBuffer in = new SimpleInputBuffer(WireRoundTrip.bytes(original));
      assertEquals(original.id(), endpoint.readId(in, in.readShort()));
      assertEquals(original.version(), in.readLong());
      assertEquals(peer.getId(), endpoint.readNodeHandle(in).getId());
      assertSameManifest(manifest, new Manifest(in));
      assertEquals(0, in.bytesRemaining());
    }
  }
}
