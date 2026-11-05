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
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {HashUtils.hash(fragment.getPayload())}, 0);
    manifest.setSignature(new byte[0]); // toStringFull(), logged on a mismatch, needs one

    assertTrue(manifest.validatesFragment(fragment, 0, LOG));

    fragment.getPayload()[1] = 1;
    assertFalse(manifest.validatesFragment(fragment, 0, LOG));
  }

  @Test
  void toStringFullOfAnUnsignedManifestDoesNotThrow() {
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {new byte[HashUtils.LENGTH]}, 0);

    assertTrue(manifest.toStringFull().contains("signature  = [null]"));
  }

  @Test
  void rejectsAFragmentHashOfTheOldSha1Length() {
    Fragment fragment = new Fragment(8);
    byte[] sha1Sized = Arrays.copyOf(HashUtils.hash(fragment.getPayload()), 20);
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {sha1Sized}, 0);
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
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {new byte[HashUtils.LENGTH]}, 7);

    Manifest actual = roundTrip(manifest);

    assertNull(actual.getSignature());
    assertEquals(7, actual.getExpiration());
    assertArrayEquals(manifest.getFragmentHash(0), actual.getFragmentHash(0));
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
    Manifest manifest = new Manifest(new byte[HashUtils.LENGTH], new byte[][] {new byte[HashUtils.LENGTH]}, 7);
    manifest.setSignature(new byte[] {1, 2, 3});

    assertArrayEquals(new byte[] {1, 2, 3}, roundTrip(manifest).getSignature());
  }
}
