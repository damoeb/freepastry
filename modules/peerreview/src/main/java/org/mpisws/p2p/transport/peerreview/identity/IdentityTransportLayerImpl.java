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
package org.mpisws.p2p.transport.peerreview.identity;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.mpisws.p2p.pki.x509.X509Serializer;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.table.KeyValueStore;
import org.mpisws.p2p.transport.table.TableTransportLayer;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.Serializer;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.InputBuffer;

import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.NO_CERTIFICATE;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SIGNATURE_BAD;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SIGNATURE_OK;

/**
 * TODO: make it store known certs to a file, make it periodically check the revocation server.
 *
 * Extends the TableTLi, but uses Certs rather than general objects.
 * Also, sends huge messages by opening temp tcp-socket.
 *
 * @author Jeff Hoye
 *
 */
public class IdentityTransportLayerImpl<Identifier, I> extends
     TableTransportLayer<Identifier, I, X509Certificate>
     implements IdentityTransport<Identifier, I> {

  /**
   * The algorithm of all PeerReview signatures. Changed from SHA1withRSA: this is part of the wire
   * protocol, nodes with different algorithms cannot verify each other.
   */
  public static final String DEFAULT_SIGNATURE_ALGORITHM = "SHA256withRSA";
  /**
   * Signature size for a 2048-bit RSA key (was 96, i.e. 768-bit keys). The actual size is derived from
   * the local certificate's key, see {@link #getSignatureSizeBytes()}; all nodes must use the same key size.
   */
  public static final short DEFAULT_SIGNATURE_SIZE = 256;
  String signatureAlgorithm = DEFAULT_SIGNATURE_ALGORITHM;
  short signatureSize = DEFAULT_SIGNATURE_SIZE;

  Signature signer;

  /**
   * How many verifiers {@link #getVerifier} keeps. An evicted verifier is built again from its certificate.
   */
  public static final int MAX_CACHED_VERIFIERS = 1024;

  /** The most recently used verifiers by identity; access to it is synchronized on the map. */
  final Map<I, Signature> verifiers = new LinkedHashMap<I, Signature>(16, 0.75f, true) {
    @Override
    protected boolean removeEldestEntry(Map.Entry<I, Signature> eldest) {
      return size() > MAX_CACHED_VERIFIERS;
    }
  };

  HashProvider hasher;

  /**
   * TODO: Use a param to load the store from a file.
   * @param buf must be null: loading a persistent store is not implemented and throws
   */
  static <H, I> KeyValueStore<I, X509Certificate> loadTableStore(I localId, X509Certificate localCert, Serializer<I> iSerializer, X509Serializer cSerializer, InputBuffer buf) {
    MyStore<H, I> ret = new MyStore<H, I>();
    ret.put(localId, localCert);
    if (buf != null) {
      // load store from the file
      throw new RuntimeException("Persistent version not implemented.");
    }

    return ret;
  }

  static class MyStore<H, I> extends HashMap<I, X509Certificate> implements KeyValueStore<I, X509Certificate> {
    IdentityTransportCallback<H, I> callback;
    @Override
    public X509Certificate put(I key, X509Certificate value) {
      X509Certificate ret = super.put(key, value);
      if (ret == null && callback != null) {
        callback.notifyCertificateAvailable(key);
      }
      return ret;
    }}

  public IdentityTransportLayerImpl(
      Serializer<I> iSerializer, X509Serializer cSerializer, I localId,
      X509Certificate localCert, PrivateKey localPrivate, TransportLayer<Identifier, ByteBuffer> transportLayer,
      HashProvider hasher, Environment env) throws InvalidKeyException, NoSuchAlgorithmException {
    super(iSerializer, cSerializer, loadTableStore(localId, localCert, iSerializer, cSerializer, null), transportLayer, env);
    this.tl = transportLayer;
    transportLayer.setCallback(this);
    this.hasher = hasher;
    this.logger = env.getLogManager().getLogger(IdentityTransportLayerImpl.class, null);
    this.errorHandler = new DefaultErrorHandler<Identifier>(this.logger);

    if (localCert.getPublicKey() instanceof RSAPublicKey) {
      signatureSize = (short)((((RSAPublicKey)localCert.getPublicKey()).getModulus().bitLength()+7)/8);
    }
    signer = Signature.getInstance(signatureAlgorithm);
    signer.initSign(localPrivate);
  }

  @Override
  public void setCallback(
      TransportLayerCallback<Identifier, ByteBuffer> callback) {
    ((MyStore<Identifier, I>)knownValues).callback = (IdentityTransportCallback<Identifier, I>)callback;
    super.setCallback(callback);
  }

  /**
   * CERT_REQUEST, int requestId, Identifier
   */
  @Override
  public Cancellable requestCertificate(final Identifier source,
      final I principal, final Continuation<X509Certificate, Exception> continuation,
      Map<String, Object> options) {
    return super.requestValue(source, principal, continuation, options);
  }

  @Override
  public boolean hasCertificate(I identifier) {
    return super.hasKey(identifier);
  }

  @Override
  public byte[] sign(byte[] bytes) {
    try {
      signer.update(bytes);
      byte[] ret = signer.sign();
      logger.atTrace().log(() -> "Signature of "+java.util.Base64.getEncoder().encodeToString(bytes)+" was "+java.util.Base64.getEncoder().encodeToString(ret));
      return ret;
    } catch (SignatureException signatureException) {
      RuntimeException throwMe = new RuntimeException("Couldn't sign "+bytes.length+" bytes");
      throwMe.initCause(signatureException);
      throw throwMe;
    }
  }

  @Override
  public int verify(I id, byte[] msg, byte[] signature) {
    logger.atTrace().log(() -> "Verify:"+id+" "+msg.length+" "+java.util.Base64.getEncoder().encodeToString(msg)+" == "+signature.length+" "+java.util.Base64.getEncoder().encodeToString(signature));
    Signature verifier = getVerifier(id);
    if (verifier == null) return NO_CERTIFICATE;

    try {
      synchronized(verifier) {
        verifier.update(msg);
        if (verifier.verify(signature)) {
          return SIGNATURE_OK;
        }
      }
    } catch (SignatureException signatureException) {
      // a malformed signature (e.g. wrong length) comes from the peer: it is a failed verification, not an error
      logger.atDebug().log(() -> "Malformed signature from "+id+": "+signatureException.getMessage());
    }
    return SIGNATURE_BAD;
  }

  /**
   * Returns null if we don't know the cert for the identifier.
   *
   * @throws RuntimeException if the verifier cannot be created from the certificate
   */
  public Signature getVerifier(I identifier) {
    synchronized (verifiers) {
      Signature ret = verifiers.get(identifier);
      try {
        if (ret == null) {
          if (knownValues.containsKey(identifier)) {
            X509Certificate cert = knownValues.get(identifier);
            ret = Signature.getInstance(signatureAlgorithm);
            ret.initVerify(cert);
            verifiers.put(identifier, ret);
          }
        }
      } catch (NoSuchAlgorithmException | InvalidKeyException e) {
        throw new RuntimeException(e);
      }
      return ret;
    }
  }

  @Override
  public short getSignatureSizeBytes() {
    return signatureSize;
  }

  @Override
  public byte[] getEmptyHash() {
    return hasher.getEmptyHash();
  }

  @Override
  public short getHashSizeBytes() {
    return hasher.getHashSizeBytes();
  }

  @Override
  public byte[] hash(long seq, short type, byte[] nodeHash, byte[] contentHash) {
    return hasher.hash(seq, type, nodeHash, contentHash);
  }

  @Override
  public byte[] hash(ByteBuffer... hashMe) {
    return hasher.hash(hashMe);
  }
}
