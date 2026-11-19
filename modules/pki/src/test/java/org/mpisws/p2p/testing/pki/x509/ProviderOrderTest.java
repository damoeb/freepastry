package org.mpisws.p2p.testing.pki.x509;

import java.security.Provider;
import java.security.Security;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import org.mpisws.p2p.pki.x509.X509Serializer;
import rice.p2p.util.SecurityUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Using the crypto helpers must not change the JVM-wide JCA provider list: BouncyCastle is passed explicitly where needed.
 */
public class ProviderOrderTest {

  @Test
  public void usingTheCryptoClassesLeavesTheProviderListAlone() throws Exception {
    List<Provider> before = Arrays.asList(Security.getProviders());

    SecurityUtils.encryptSymmetric(new byte[] {1}, SecurityUtils.generateKeySymmetric());
    new X509Serializer();
    CertificateAuthorityTool.generateKeyPair();

    assertEquals(before, Arrays.asList(Security.getProviders()));
    assertNull(Security.getProvider(SecurityUtils.BOUNCY_CASTLE.getName()));
  }
}
