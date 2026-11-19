package org.mpisws.p2p.testing.transportlayer;

import java.security.KeyStore;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import rice.pastry.Id;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;
import rice.pastry.standard.NodeIdCertificate;
import rice.pastry.standard.RandomNodeIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** The TLS trust store of a certified node holds the CA certificate and nothing else: a node's own leaf is no trust anchor. */
class CertifiedTrustStoreTest {
  @Test
  void trustStoreHoldsOnlyTheCaCertificate() throws Exception {
    CertificateAuthorityTool authority = CertifiedAdmissionTest.ca();
    rice.environment.Environment environment = new rice.environment.Environment();
    try {
      Id id = new RandomNodeIdFactory(environment).generateNodeId();
      KeyStore nodeStore = CertifiedAdmissionTest.store(authority, id);

      KeyStore testObj = CertifiedSocketPastryNodeFactory.trustStoreOf(nodeStore);

      List<String> aliases = Collections.list(testObj.aliases());
      assertEquals(1, aliases.size(), "only the CA: " + aliases);
      assertEquals(authority.getCertificate(), testObj.getCertificate(aliases.get(0)));
      assertFalse(testObj.isKeyEntry(aliases.get(0)), "no private key in the trust store");
      assertFalse(testObj.containsAlias(NodeIdCertificate.NODE_KEY_ALIAS));
    } finally {
      environment.destroy();
    }
  }
}
