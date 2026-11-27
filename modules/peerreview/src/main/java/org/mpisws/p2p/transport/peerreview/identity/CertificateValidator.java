package org.mpisws.p2p.transport.peerreview.identity;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Decides whether a certificate that a peer returned may be stored as the certificate of a principal. A peer can
 * return any certificate, so without this check a colluder could make a node accept its own key for an honest
 * principal, and then forge that principal's authenticators.
 *
 * @param <I> the principal (identifier) type
 */
@FunctionalInterface
public interface CertificateValidator<I> {
  /**
   * @param principal the principal the certificate was requested for
   * @param certificate the certificate a peer returned
   * @throws CertificateException if the certificate is not one the trusted CA issued for principal, or is not valid now
   */
  void validate(I principal, X509Certificate certificate) throws CertificateException;
}
