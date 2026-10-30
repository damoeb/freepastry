package rice.p2p.glacier.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import rice.p2p.glacier.Fragment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Round trips of what Glacier persists: fragments with their manifests, neighbor timestamps and fragment metadata.
 */
class GlacierStorageSerializerTest {

  private final GlacierStorageSerializer testObj = new GlacierStorageSerializer();

  private static Manifest manifest(long expiration) {
    Manifest manifest = new Manifest(new byte[] {1, 2, 3}, new byte[][] {{4, 5}, {6, 7}}, expiration);
    manifest.setSignature(new byte[] {8, 9, 10, 11});
    return manifest;
  }

  private static SimpleInputBuffer reread(SimpleOutputBuffer buf) {
    return new SimpleInputBuffer(buf.getBytes(), 0, buf.getWritten());
  }

  @Test
  void writesAndReadsAFragmentAndItsManifest() throws IOException {
    Fragment fragment = new Fragment(5);
    System.arraycopy(new byte[] {9, 8, 7, 6, 5}, 0, fragment.getPayload(), 0, 5);
    FragmentAndManifest expected = new FragmentAndManifest(fragment, manifest(123456789L));
    SimpleOutputBuffer buf = new SimpleOutputBuffer();

    testObj.writeObject(expected, buf);
    FragmentAndManifest actual = (FragmentAndManifest) testObj.readObject(reread(buf));

    assertArrayEquals(expected.getFragment().getPayload(), actual.getFragment().getPayload());
    assertArrayEquals(expected.getManifest().getObjectHash(), actual.getManifest().getObjectHash());
    assertArrayEquals(expected.getManifest().getFragmentHashes(), actual.getManifest().getFragmentHashes());
    assertArrayEquals(expected.getManifest().getSignature(), actual.getManifest().getSignature());
    assertEquals(123456789L, actual.getManifest().getExpiration());
  }

  @Test
  void writesAndReadsANeighborTimestamp() throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();

    testObj.writeObject(Long.valueOf(987654321L), buf);

    assertEquals(987654321L, testObj.readObject(reread(buf)));
  }

  @Test
  void writesAndReadsFragmentMetadata() throws IOException {
    FragmentMetadata expected = new FragmentMetadata(3L, 2L, 1L);
    SimpleOutputBuffer buf = new SimpleOutputBuffer();

    testObj.writeMetadata(expected, buf);
    FragmentMetadata actual = (FragmentMetadata) testObj.readMetadata(reread(buf));

    assertEquals(0, expected.compareTo(actual));
    assertEquals(3L, actual.currentExpiration());
    assertEquals(2L, actual.previousExpiration());
    assertEquals(1L, actual.storedSince());
  }

  @Test
  void refusesOtherObjects() {
    assertThrows(IOException.class, () -> testObj.writeObject("text", new SimpleOutputBuffer()));
    assertThrows(IOException.class, () -> testObj.writeMetadata("text", new SimpleOutputBuffer()));
  }
}
