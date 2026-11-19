package rice.p2p.glacier.v2;

import java.io.IOException;
import java.util.Arrays;

import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.ErasureCodec;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.VersionKey;
import rice.p2p.past.PastContent;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.util.HashUtils;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * FOR TESTS ONLY. The policy Glacier used to ship as its default: the "signature" of a manifest is the bytes
 * of its key, so anybody can forge a manifest and extend any object. It lives in the test sources so that no
 * deployment can pick it up; real nodes use {@link GlacierDefaultPolicy}.
 */
final class InsecureGlacierPolicy implements GlacierPolicy {
  private final ErasureCodec codec;

  InsecureGlacierPolicy(ErasureCodec codec) {
    this.codec = codec;
  }

  @Override
  public Id storageId(Id applicationId) {
    return applicationId;
  }

  @Override
  public boolean checkSignature(Manifest manifest, VersionKey key) {
    return manifest.getSignature() != null && Arrays.equals(manifest.getSignature(), key.toByteArray());
  }

  @Override
  public Fragment[] encodeObject(PastContent content, boolean[] generateFragment) {
    try {
      SimpleOutputBuffer outputBuffer = serialize(content);
      return codec.encode(outputBuffer.getBytes(), outputBuffer.getWritten(), generateFragment);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public Manifest[] createManifests(VersionKey key, PastContent content, Fragment[] fragments, long expiration) {
    try {
      SimpleOutputBuffer outputBuffer = serialize(content);
      byte[][] fragmentHash = new byte[fragments.length][];
      for (int i = 0; i < fragments.length; i++)
        fragmentHash[i] = HashUtils.hash(fragments[i].getPayload());
      Manifest manifest = new Manifest(HashUtils.hash(outputBuffer.getBytes(), 0, outputBuffer.getWritten()), fragmentHash, expiration, new byte[0], new byte[0]);
      manifest.setSignature(key.toByteArray());
      Manifest[] manifests = new Manifest[fragments.length];
      Arrays.fill(manifests, manifest);
      return manifests;
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public Manifest updateManifest(VersionKey key, Manifest manifest, long newExpiration) {
    if (!checkSignature(manifest, key))
      return null;
    return manifest.withExpiration(newExpiration, key.toByteArray());
  }

  @Override
  public PastContent decodeObject(Fragment[] fragments, Endpoint endpoint, PastContentDeserializer deserializer) {
    return codec.decode(fragments, endpoint, deserializer);
  }

  private static SimpleOutputBuffer serialize(PastContent content) throws IOException {
    SimpleOutputBuffer outputBuffer = new SimpleOutputBuffer();
    outputBuffer.writeShort(content.getType());
    content.serialize(outputBuffer);
    return outputBuffer;
  }
}
