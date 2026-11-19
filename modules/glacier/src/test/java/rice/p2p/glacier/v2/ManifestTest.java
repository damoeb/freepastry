package rice.p2p.glacier.v2;

import java.util.Arrays;

import java.io.IOException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.p2p.glacier.Fragment;
import rice.p2p.util.HashUtils;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManifestTest {

  private static final Logger LOG = LoggerFactory.getLogger(ManifestTest.class);

  @Test
  void validatesAFragmentAgainstItsSha256Hash() {
    Fragment fragment = new Fragment(8);
    fragment.getPayload()[0] = 42;
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {HashUtils.hash(fragment.getPayload())}, 0, new byte[0], new byte[0]);
    manifest.setSignature(new byte[0]); // toStringFull(), logged on a mismatch, needs one

    assertTrue(manifest.validatesFragment(fragment, 0, LOG));

    fragment.getPayload()[1] = 1;
    assertFalse(manifest.validatesFragment(fragment, 0, LOG));
  }

  @Test
  void toStringFullOfAnUnsignedManifestDoesNotThrow() {
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {new byte[HashUtils.LENGTH]}, 0, new byte[0], new byte[0]);

    assertTrue(manifest.toStringFull().contains("signature  = [null]"));
  }

  @Test
  void rejectsAFragmentHashOfTheOldSha1Length() {
    Fragment fragment = new Fragment(8);
    byte[] sha1Sized = Arrays.copyOf(HashUtils.hash(fragment.getPayload()), 20);
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {sha1Sized}, 0, new byte[0], new byte[0]);
    manifest.setSignature(new byte[0]); // toStringFull(), logged on a mismatch, needs one

    assertFalse(manifest.validatesFragment(fragment, 0, LOG));
  }

  private static Manifest roundTrip(Manifest manifest) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    manifest.serialize(out);
    return new Manifest(new SimpleInputBuffer(out.getBytes()));
  }

  @Test
  @DisplayName("an unsigned manifest serializes and reads back with a null signature")
  void roundTripsAnUnsignedManifest() throws IOException {
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {new byte[HashUtils.LENGTH]}, 7, new byte[] {1, 2, 3}, new byte[] {4, 5});

    Manifest actual = roundTrip(manifest);

    assertNull(actual.getSignature());
    assertEquals(7, actual.getExpiration());
    assertArrayEquals(manifest.getFragmentHash(0), actual.getFragmentHash(0));
    assertArrayEquals(new byte[] {1, 2, 3}, actual.getOwnerKey());
    assertArrayEquals(new byte[] {4, 5}, actual.getName());
  }

  private static SimpleOutputBuffer manifestUpToTheOwnerKey() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(7); // expiration
    out.writeInt(0); // object hash length
    out.writeInt(1); // fragment hashes
    out.writeInt(1); // fragment hash length
    out.writeInt(-1); // no signature
    out.writeByte((byte) 0); // the fragment hash
    return out;
  }

  @Test
  @DisplayName("an owner key longer than any encoded RSA key is rejected when read")
  void rejectsAnOversizedOwnerKey() throws IOException {
    SimpleOutputBuffer out = manifestUpToTheOwnerKey();
    out.writeInt(Manifest.MAX_OWNER_KEY_LENGTH + 1);
    out.write(new byte[Manifest.MAX_OWNER_KEY_LENGTH + 1], 0, Manifest.MAX_OWNER_KEY_LENGTH + 1);
    out.writeInt(0);

    assertThrows(IOException.class, () -> new Manifest(new SimpleInputBuffer(out.getBytes())));
  }

  @Test
  @DisplayName("a name longer than the bound is rejected when read")
  void rejectsAnOversizedName() throws IOException {
    SimpleOutputBuffer out = manifestUpToTheOwnerKey();
    out.writeInt(0);
    out.writeInt(Manifest.MAX_NAME_LENGTH + 1);
    out.write(new byte[Manifest.MAX_NAME_LENGTH + 1], 0, Manifest.MAX_NAME_LENGTH + 1);

    assertThrows(IOException.class, () -> new Manifest(new SimpleInputBuffer(out.getBytes())));
  }

  @Test
  @DisplayName("a negative owner key length is rejected when read")
  void rejectsANegativeOwnerKeyLength() throws IOException {
    SimpleOutputBuffer out = manifestUpToTheOwnerKey();
    out.writeInt(-2);

    assertThrows(IOException.class, () -> new Manifest(new SimpleInputBuffer(out.getBytes())));
  }

  @Test
  @DisplayName("withExpiration() returns a patched copy and leaves the manifest unchanged")
  void withExpirationLeavesTheOriginalUnchanged() {
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {new byte[HashUtils.LENGTH]}, 7, new byte[] {1}, new byte[] {2});
    manifest.setSignature(new byte[] {3});

    Manifest actual = manifest.withExpiration(8, new byte[] {4});

    assertEquals(8, actual.getExpiration());
    assertArrayEquals(new byte[] {4}, actual.getSignature());
    assertArrayEquals(new byte[] {1}, actual.getOwnerKey());
    assertEquals(7, manifest.getExpiration());
    assertArrayEquals(new byte[] {3}, manifest.getSignature());
  }

  @Test
  @DisplayName("a manifest without fragment hashes, which serialize() cannot write, is rejected when read")
  void rejectsAManifestWithoutFragmentHashes() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(7); // expiration
    out.writeInt(0); // object hash length
    out.writeInt(0); // fragment hashes
    out.writeInt(HashUtils.LENGTH); // fragment hash length
    out.writeInt(-1); // no signature

    assertThrows(IOException.class, () -> new Manifest(new SimpleInputBuffer(out.getBytes())));
  }

  @Test
  @DisplayName("a signed manifest keeps its signature through a round trip")
  void roundTripsASignedManifest() throws IOException {
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {new byte[HashUtils.LENGTH]}, 7, new byte[] {1, 2, 3}, new byte[] {4, 5});
    manifest.setSignature(new byte[] {1, 2, 3});

    assertArrayEquals(new byte[] {1, 2, 3}, roundTrip(manifest).getSignature());
  }
}
