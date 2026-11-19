/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/
package rice.p2p.glacier.v2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.Arrays;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.glacier.ErasureCodec;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.VersionKey;
import rice.p2p.past.PastContent;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.util.HashUtils;
import rice.p2p.util.MathUtils;
import rice.p2p.util.SecurityUtils;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * The policy for real deployments: manifests are signed by the owner of the object, and nobody else can sign
 * or extend one.
 *
 * <p>Each node is an owner with its own RSA key pair. An object the application calls {@code a} is stored
 * under the id {@code buildId(first 20 bytes of SHA-256("FreePastry Glacier id v1" || len(K) || K || N))}, where
 * {@code K} is the owner's encoded public key ({@link SecurityUtils#encodePublicKey}), {@code len(K)} its length
 * as a 4-byte big-endian int, and the name {@code N} is {@code a.toByteArray()}. The manifest carries {@code K} and
 * {@code N}, so any node can recompute the id and check that the key in the manifest owns it. The signature is
 * {@link SecurityUtils#sign} (SHA256withRSA) over {@link #signedBytes(VersionKey, Manifest)}: the key's id and
 * version, the object hash, the fragment hashes, the expiration, {@code K} and {@code N}.
 *
 * <p>A manifest is accepted only if the id derives from its key and name and the signature verifies under
 * that key. Fragment squatting needs an id derived from the squatter's key, which is a different id, and
 * extending the lifetime needs a new signature, which only the owner can make.
 */
public class GlacierDefaultPolicy implements GlacierPolicy {

  /** Separates the id derivation from other uses of the hash. */
  private static final byte[] ID_DOMAIN = "FreePastry Glacier id v1".getBytes(StandardCharsets.UTF_8);

  /** Separates the signed manifest from other signed data. */
  private static final byte[] SIGNATURE_DOMAIN = "FreePastry Glacier manifest v1".getBytes(StandardCharsets.UTF_8);

  private final ErasureCodec codec;
  private final KeyPair owner;
  private final byte[] encodedOwnerKey;
  private final IdFactory factory;
  private final Logger logger;

  /**
   * @param owner the key pair of this node as an owner; keep it, a node with another key pair can neither
   *     find nor refresh the objects it stored before
   * @param factory the factory of the ids Glacier stores under, the one {@link GlacierImpl} gets
   */
  public GlacierDefaultPolicy(ErasureCodec codec, KeyPair owner, IdFactory factory, String instance, Environment env) {
    this.codec = codec;
    this.owner = owner;
    this.encodedOwnerKey = SecurityUtils.encodePublicKey(owner.getPublic());
    this.factory = factory;
    logger = env.getLogManager().getLogger(GlacierDefaultPolicy.class, instance);
  }

  @Override
  public Id storageId(Id applicationId) {
    return deriveId(encodedOwnerKey, applicationId.toByteArray());
  }

  private Id deriveId(byte[] ownerKey, byte[] name) {
    return factory.buildId(HashUtils.hashToIdMaterial(ID_DOMAIN, MathUtils.intToByteArray(ownerKey.length), ownerKey, name));
  }

  @Override
  public boolean checkSignature(Manifest manifest, VersionKey key) {
    byte[] signature = manifest.getSignature();
    if (signature == null || manifest.getOwnerKey().length == 0)
      return false;

    if (!deriveId(manifest.getOwnerKey(), manifest.getName()).equals(key.id())) {
      logger.warn("Manifest for {} names an owner that does not own the id", key);
      return false;
    }

    try {
      PublicKey ownerKey = SecurityUtils.decodePublicKey(manifest.getOwnerKey());
      return SecurityUtils.verify(signedBytes(key, manifest), signature, ownerKey);
    } catch (SecurityException e) {
      logger.warn("Cannot verify the manifest for {}: {}", key, e.toString());
      return false;
    }
  }

  /**
   * The canonical encoding the owner signs: a domain tag, then the key's id and version, the object hash, the
   * number of fragment hashes and each of them, the expiration, the owner key and the name. Every byte array
   * is prefixed with its length as a 4-byte int.
   */
  static byte[] signedBytes(VersionKey key, Manifest manifest) {
    try {
      SimpleOutputBuffer outputBuffer = new SimpleOutputBuffer();
      writeBytes(outputBuffer, SIGNATURE_DOMAIN);
      writeBytes(outputBuffer, key.id().toByteArray());
      outputBuffer.writeLong(key.version());
      writeBytes(outputBuffer, manifest.getObjectHash());
      byte[][] fragmentHashes = manifest.getFragmentHashes();
      outputBuffer.writeInt(fragmentHashes.length);
      for (byte[] fragmentHash : fragmentHashes)
        writeBytes(outputBuffer, fragmentHash);
      outputBuffer.writeLong(manifest.getExpiration());
      writeBytes(outputBuffer, manifest.getOwnerKey());
      writeBytes(outputBuffer, manifest.getName());
      return Arrays.copyOf(outputBuffer.getBytes(), outputBuffer.getWritten());
    } catch (IOException e) {
      throw new IllegalStateException("Writing to memory failed", e);
    }
  }

  private static void writeBytes(SimpleOutputBuffer outputBuffer, byte[] bytes) throws IOException {
    outputBuffer.writeInt(bytes.length);
    outputBuffer.write(bytes, 0, bytes.length);
  }

  private Manifest sign(VersionKey key, Manifest unsigned) {
    unsigned.setSignature(SecurityUtils.sign(signedBytes(key, unsigned), owner.getPrivate()));
    return unsigned;
  }

  @Override
  public PastContent decodeObject(Fragment[] fragments, Endpoint endpoint, PastContentDeserializer deserializer) {
    return codec.decode(fragments, endpoint, deserializer);
  }

  @Override
  public Manifest[] createManifests(VersionKey key, PastContent content, Fragment[] fragments, long expiration) {
    if (!storageId(content.getId()).equals(key.id())) {
      logger.warn("Cannot create manifests for {}: not the storage id of {}", key, content.getId());
      return null;
    }

    try {
      SimpleOutputBuffer outputBuffer = new SimpleOutputBuffer();
      outputBuffer.writeShort(content.getType());
      content.serialize(outputBuffer);
      return createManifestsHelper(key, content.getId().toByteArray(), outputBuffer.getBytes(), outputBuffer.getWritten(), fragments, expiration);
    } catch (IOException ioException) {
      logger.atWarn().log(() -> "Cannot serialize object: "+ioException);
      return null;
    }
  }

  private Manifest[] createManifestsHelper(VersionKey key, byte[] name, byte[] bytes, int length, Fragment[] fragments, long expiration) {

    /* Compute the hash values. */

    byte[][] fragmentHash = new byte[fragments.length][];
    for (int i = 0; i < fragments.length; i++) {
      fragmentHash[i] = HashUtils.hash(fragments[i].getPayload());
    }

    byte[] objectHash = HashUtils.hash(bytes, 0, length);

    /* Create the manifest: it covers all fragments, so one signature serves them all */

    Manifest manifest = sign(key, new Manifest(objectHash, fragmentHash, expiration, encodedOwnerKey, name));
    Manifest[] manifests = new Manifest[fragments.length];
    Arrays.fill(manifests, manifest);

    return manifests;
  }

  @Override
  public Fragment[] encodeObject(PastContent content, boolean[] generateFragment) {
    logger.trace("Serialize object: {}", content);

    try {
      SimpleOutputBuffer outputBuffer = new SimpleOutputBuffer();
      outputBuffer.writeShort(content.getType());
      content.serialize(outputBuffer);
      return encodeObjectHelper(content, outputBuffer.getBytes(), outputBuffer.getWritten(), generateFragment);
    } catch (IOException ioException) {
      logger.atWarn().log(() -> "Cannot serialize object: "+ioException);
      return null;
    }
  }


  private Fragment[] encodeObjectHelper(PastContent content, byte[] bytes, int length, boolean[] generateFragment) {
    logger.trace("Create fragments: {}", content);
    Fragment[] fragments = codec.encode(bytes, length, generateFragment);
    logger.trace("Completed: {}", content);

    return fragments;
  }

  @Override
  public Manifest updateManifest(VersionKey key, Manifest manifest, long newExpiration) {
    if (!Arrays.equals(manifest.getOwnerKey(), encodedOwnerKey)) {
      logger.warn("Cannot extend {}: this node does not own it", key);
      return null;
    }
    if (!checkSignature(manifest, key))
      return null;

    return sign(key, new Manifest(manifest.getObjectHash(), manifest.getFragmentHashes(), newExpiration, manifest.getOwnerKey(), manifest.getName()));
  }
}
