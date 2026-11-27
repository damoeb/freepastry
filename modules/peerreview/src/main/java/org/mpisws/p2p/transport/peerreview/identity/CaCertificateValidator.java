package org.mpisws.p2p.transport.peerreview.identity;

import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.security.auth.x500.X500Principal;

/**
 * Accepts a certificate for a principal only if the trusted CA signed it, it is valid now, and its subject is
 * {@code CN=<common name of the principal>}, the way {@code CertificateAuthorityTool} issues certificates.
 *
 * @param <I> the principal (identifier) type
 */
public class CaCertificateValidator<I> implements CertificateValidator<I> {
  private final X509Certificate caCertificate;
  private final Function<I, String> commonNameOf;
  private final Supplier<Instant> clock;

  /**
   * @param caCertificate the trust anchor, e.g. the CA certificate in the node's key store
   * @param commonNameOf the common name the CA puts in the certificate of a principal (for a node id:
   *     {@code Id.toStringFull()})
   * @param clock the current time, to check validity periods against
   */
  public CaCertificateValidator(X509Certificate caCertificate, Function<I, String> commonNameOf, Supplier<Instant> clock) {
    this.caCertificate = caCertificate;
    this.commonNameOf = commonNameOf;
    this.clock = clock;
  }

  @Override
  public void validate(I principal, X509Certificate certificate) throws CertificateException {
    if (!certificate.getIssuerX500Principal().equals(caCertificate.getSubjectX500Principal())) {
      throw new CertificateException("Certificate for " + principal + " issued by " + certificate.getIssuerX500Principal() + ", not by the trusted CA " + caCertificate.getSubjectX500Principal());
    }
    try {
      certificate.verify(caCertificate.getPublicKey());
    } catch (GeneralSecurityException e) {
      throw new CertificateException("Certificate for " + principal + " is not signed by the trusted CA", e);
    }
    Date now = Date.from(clock.get());
    caCertificate.checkValidity(now);
    certificate.checkValidity(now);
    X500Principal expected = new X500Principal("CN=" + commonNameOf.apply(principal));
    if (!certificate.getSubjectX500Principal().equals(expected)) {
      throw new CertificateException("Certificate subject " + certificate.getSubjectX500Principal() + " is not " + expected);
    }
  }
}
