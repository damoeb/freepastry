package org.mpisws.p2p.pki.x509;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.security.cert.CertificateException;
import java.security.NoSuchProviderException;
import org.junit.jupiter.api.BeforeAll;
import rice.testing.fuzz.FuzzSupport;

/**
 * Fuzzes the certificate a peer sends: a length-prefixed DER encoding. The invariant: a certificate or an
 * {@link java.io.IOException}, nothing else.
 */
class PkiFuzzTest {
  private static X509Serializer target;

  @BeforeAll
  static void setUp() throws CertificateException, NoSuchProviderException {
    target = new X509Serializer();
  }

  @FuzzTest(maxDuration = "30s")
  void x509Certificate(FuzzedDataProvider data) {
    FuzzSupport.read(data, target::deserialize);
  }
}
