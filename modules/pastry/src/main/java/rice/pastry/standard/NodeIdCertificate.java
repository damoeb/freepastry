package rice.pastry.standard;

import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;

import org.jspecify.annotations.Nullable;
import rice.pastry.Id;

/**
 * Binding of a node id to an X.509 certificate: the certificate's subject is exactly
 * {@code CN=<Id.toStringFull()>}, and a certificate authority (CA) signed it.
 * <p>
 * Design, shared by {@link CertifiedNodeIdFactory}, {@code rice.pastry.socket.CertifiedIdBindStrategy}
 * and {@code org.mpisws.p2p.pki.x509.CAToolImpl}:
 * <ul>
 * <li>What the CA signs: one ordinary X.509 certificate per node, carrying the node's public key, a
 * validity period and the node id as the common name. Issuing is {@code CAToolImpl.signNodeId()}. The
 * certificate is the same one the TLS layer authenticates with, so there is no second signature format.</li>
 * <li>Where it lives: in the node's PKCS12 key store, as the chain of the {@link #NODE_KEY_ALIAS} entry
 * (written by {@code CAToolImpl -store}), next to the CA's certificate under {@link #CA_CERT_ALIAS}, which is
 * the trust anchor. {@code CertifiedNodeIdFactory} loads both.</li>
 * <li>Verifying a certificate ({@link #verify}): issued by the trust anchor (issuer name and signature),
 * both certificates within their validity period, and a subject that is a well-formed node id. The
 * result is the node id, so a node cannot choose its own id.</li>
 * <li>Node side: {@code CertifiedNodeIdFactory} verifies the certificate, and that the private key
 * belongs to it, before it hands out the id.</li>
 * <li>Peer side: {@code SSLTransportLayerImpl} with client authentication required validates the peer's
 * chain against the trust store and reports the subject as {@code SSLTransportLayer.OPTION_CERT_SUBJECT};
 * {@code CertifiedIdBindStrategy} accepts a node handle only if its id equals that subject
 * ({@link #matches}). On a mismatch the binding is refused and a warning is logged.</li>
 * </ul>
 * Limits: the trust anchor signs node certificates directly (no intermediate CAs, no revocation beyond
 * expiry), and UDP datagrams stay outside TLS, so they remain unauthenticated.
 */
public final class NodeIdCertificate {

  /** key store alias of the node's private key and its certificate chain, as written by {@code CAToolImpl -store} */
  public static final String NODE_KEY_ALIAS = "private";

  /** key store alias of the CA certificate that is the trust anchor */
  public static final String CA_CERT_ALIAS = "cert";

  private static final String SUBJECT_PREFIX = "CN=";

  private NodeIdCertificate() {
  }

  /**
   * @return the certificate subject (RFC 2253) that binds a certificate to this id
   */
  public static String subjectFor(Id id) {
    return SUBJECT_PREFIX + id.toStringFull();
  }

  /**
   * @param subject the common name of a certificate, as reported by {@code SSLTransportLayer.OPTION_CERT_SUBJECT}
   *     (without {@code CN=}), or null if the connection carries no certificate
   * @return true if this is the common name of a certificate for id
   */
  public static boolean matches(Id id, @Nullable Object subject) {
    return subject instanceof String s && id.toStringFull().equals(s);
  }

  /**
   * Checks that a CA bound the certificate to a node id, and returns the id.
   *
   * @param cert the node's certificate
   * @param ca the trust anchor
   * @param now the time to check the validity periods against
   * @return the node id in the certificate's subject
   * @throws CertificateException if the certificate was not issued by ca, is not valid at now,
   *     or its subject is not a node id
   */
  public static Id verify(X509Certificate cert, X509Certificate ca, Instant now) throws CertificateException {
    if (!cert.getIssuerX500Principal().equals(ca.getSubjectX500Principal())) {
      throw new CertificateException("Node id certificate issued by " + cert.getIssuerX500Principal() + ", not by the trusted CA " + ca.getSubjectX500Principal());
    }
    Date at = Date.from(now);
    ca.checkValidity(at);
    cert.checkValidity(at);
    try {
      cert.verify(ca.getPublicKey());
    } catch (GeneralSecurityException e) {
      throw new CertificateException("Node id certificate is not signed by the trusted CA", e);
    }
    return idOf(cert);
  }

  /**
   * The node id in the subject, without checking who signed the certificate.
   */
  static Id idOf(X509Certificate cert) throws CertificateException {
    String subject = cert.getSubjectX500Principal().getName();
    int hexLength = Id.IdBitLength / 4;
    if (!subject.startsWith(SUBJECT_PREFIX) || subject.length() != SUBJECT_PREFIX.length() + hexLength) {
      throw new CertificateException("Certificate subject '" + subject + "' is not CN=<" + hexLength + " hex digits>");
    }
    String hex = subject.substring(SUBJECT_PREFIX.length());
    for (int i = 0; i < hex.length(); i++) {
      if (Character.digit(hex.charAt(i), 16) < 0) {
        throw new CertificateException("Certificate subject '" + subject + "' is not a node id");
      }
    }
    Id id = Id.build(hex);
    if (!id.toStringFull().equals(hex)) {
      throw new CertificateException("Certificate subject '" + subject + "' is not a node id");
    }
    return id;
  }
}
