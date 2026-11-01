package rice.p2p.glacier.v2;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.p2p.glacier.Fragment;
import rice.p2p.util.HashUtils;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
