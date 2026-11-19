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

package rice.pastry.standard;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.NodeIdFactory;

/**
 * Hands out the node id that a certificate authority (CA) bound to this node's certificate. The id is
 * the common name of the node's X.509 certificate; {@link #generateNodeId()} checks that the CA signed
 * the certificate, that both certificates are valid now, and that the node holds the certificate's
 * private key, and fails otherwise, so a node cannot start with an id it was not granted. The same id
 * is returned on every call and every restart. See {@link NodeIdCertificate} for the whole design,
 * including how peers check the id.
 * <p>
 * The key store is the one {@code org.mpisws.p2p.pki.x509.CertificateAuthorityTool} writes with {@code -store}: the
 * node's private key and certificate under {@link NodeIdCertificate#NODE_KEY_ALIAS}, the CA certificate
 * under {@link NodeIdCertificate#CA_CERT_ALIAS}.
 *
 * @version $Id: CertifiedNodeIdFactory.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class CertifiedNodeIdFactory implements NodeIdFactory {

  private final X509Certificate nodeCert;

  private final X509Certificate caCert;

  private final PrivateKey nodeKey;

  private final Logger logger;

  /**
   * @param nodeCert the node's certificate; its subject is the node id
   * @param nodeKey the private key of nodeCert
   * @param caCert the trust anchor that must have issued nodeCert
   */
  public CertifiedNodeIdFactory(X509Certificate nodeCert, PrivateKey nodeKey, X509Certificate caCert, Environment env) {
    this.nodeCert = nodeCert;
    this.nodeKey = nodeKey;
    this.caCert = caCert;
    this.logger = env.getLogManager().getLogger(CertifiedNodeIdFactory.class, null);
  }

  /**
   * @param store a key store with the entries described in the class comment
   */
  public static CertifiedNodeIdFactory fromKeyStore(KeyStore store, char[] keyPassword, Environment env) throws GeneralSecurityException {
    Certificate[] chain = store.getCertificateChain(NodeIdCertificate.NODE_KEY_ALIAS);
    Certificate ca = store.getCertificate(NodeIdCertificate.CA_CERT_ALIAS);
    if (chain == null || chain.length == 0 || !(chain[0] instanceof X509Certificate node)) {
      throw new CertificateException("The key store has no node certificate under '" + NodeIdCertificate.NODE_KEY_ALIAS + "'");
    }
    if (!(ca instanceof X509Certificate caCert)) {
      throw new CertificateException("The key store has no CA certificate under '" + NodeIdCertificate.CA_CERT_ALIAS + "'");
    }
    if (!(store.getKey(NodeIdCertificate.NODE_KEY_ALIAS, keyPassword) instanceof PrivateKey key)) {
      throw new CertificateException("The key store has no private key under '" + NodeIdCertificate.NODE_KEY_ALIAS + "'");
    }
    return new CertifiedNodeIdFactory(node, key, caCert, env);
  }

  /**
   * @param file a PKCS12 key store, as written by {@code CertificateAuthorityTool -store}
   * @param password the store and key password (blank for {@code CertificateAuthorityTool -store})
   */
  public static CertifiedNodeIdFactory load(Path file, char[] password, String keyStoreType, Environment env) throws IOException, GeneralSecurityException {
    KeyStore store = KeyStore.getInstance(keyStoreType);
    try (InputStream in = Files.newInputStream(file)) {
      store.load(in, password);
    }
    return fromKeyStore(store, password, env);
  }

  /**
   * @return the node id the CA granted
   * @throws SecurityException if the certificate is not valid for this CA now, or the node does not hold its key
   */
  @Override
  public Id generateNodeId() {
    try {
      Id id = NodeIdCertificate.verify(nodeCert, caCert, Instant.now());
      requireKeyPair(nodeKey, nodeCert.getPublicKey());
      return id;
    } catch (GeneralSecurityException e) {
      logger.warn("The node id certificate is not valid: {}", e.getMessage());
      throw new SecurityException("The node id certificate is not valid: " + e.getMessage(), e);
    }
  }

  /** a signature made with the private key must verify with the certificate's public key */
  private static void requireKeyPair(PrivateKey key, PublicKey pub) throws GeneralSecurityException {
    String algorithm = switch (key.getAlgorithm()) {
      case "RSA" -> "SHA256withRSA";
      case "EC" -> "SHA256withECDSA";
      default -> throw new CertificateException("Unsupported node key algorithm " + key.getAlgorithm());
    };
    byte[] challenge = {1, 2, 3, 4};
    Signature signer = Signature.getInstance(algorithm);
    signer.initSign(key);
    signer.update(challenge);
    byte[] signature = signer.sign();
    Signature verifier = Signature.getInstance(algorithm);
    verifier.initVerify(pub);
    verifier.update(challenge);
    if (!verifier.verify(signature)) {
      throw new CertificateException("The node's private key does not belong to its certificate");
    }
  }
}
