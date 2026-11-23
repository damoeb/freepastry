package org.mpisws.p2p.testing.pki.x509;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.CertPathBuilder;
import java.security.cert.CertStore;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXCertPathBuilderResult;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Set;
import javax.security.auth.x500.X500Principal;

import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v1CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.pki.x509.CertificateAuthorityTool;
import rice.p2p.util.SecurityUtils;
import rice.pastry.Id;
import rice.pastry.standard.NodeIdCertificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An existing CA re-issues its certificate as version 3 for the same key and subject; the node certificates it
 * issued before stay valid under the new CA certificate.
 */
class CAReissueTest {

  @TempDir
  Path dir;

  /** a CA certificate as earlier versions of the tool wrote it: version 1, serial 1, no extensions */
  private static X509Certificate version1CA(KeyPair caKeys, Instant now) throws Exception {
    X500Principal name = new X500Principal("CN=OldCA");
    JcaX509v1CertificateBuilder builder = new JcaX509v1CertificateBuilder(name, BigInteger.ONE,
        Date.from(now.minus(1, ChronoUnit.DAYS)), Date.from(now.plus(1, ChronoUnit.DAYS)), name, caKeys.getPublic());
    return new JcaX509CertificateConverter().setProvider(SecurityUtils.BOUNCY_CASTLE).getCertificate(
        builder.build(new JcaContentSignerBuilder(CertificateAuthorityTool.DEFAULT_SIGNATURE_ALGORITHM).setProvider(SecurityUtils.BOUNCY_CASTLE).build(caKeys.getPrivate())));
  }

  private static X509Certificate pathBuiltTo(X509Certificate target, X509Certificate anchor) throws Exception {
    X509CertSelector selector = new X509CertSelector();
    selector.setCertificate(target);
    PKIXBuilderParameters parameters = new PKIXBuilderParameters(Set.of(new TrustAnchor(anchor, null)), selector);
    parameters.setRevocationEnabled(false);
    parameters.addCertStore(CertStore.getInstance("Collection", new CollectionCertStoreParameters(List.of(target))));
    PKIXCertPathBuilderResult result = (PKIXCertPathBuilderResult) CertPathBuilder.getInstance("PKIX").build(parameters);
    return result.getTrustAnchor().getTrustedCert();
  }

  @Test
  void nodeCertificateOfAVersion1CaVerifiesUnderTheReissuedCertificate() throws Exception {
    Instant now = Instant.now();
    KeyPair caKeys = CertificateAuthorityTool.generateKeyPair();
    X509Certificate oldCa = version1CA(caKeys, now);
    Id id = CertificateAuthorityTool.randomNodeId();
    X509Certificate nodeCert = CertificateAuthorityTool.sign(id.toStringFull(), CertificateAuthorityTool.generateKeyPair().getPublic(),
        now.minus(1, ChronoUnit.HOURS), now.plus(1, ChronoUnit.DAYS), BigInteger.TEN, oldCa, caKeys.getPrivate());

    X509Certificate reissued = CertificateAuthorityTool.reissueCA(oldCa, caKeys, now.minus(1, ChronoUnit.HOURS), now.plus(3650, ChronoUnit.DAYS));

    assertEquals(3, reissued.getVersion());
    assertTrue(reissued.getBasicConstraints() >= 0, "CA:true");
    assertTrue(reissued.getKeyUsage()[5], "keyCertSign");
    assertEquals(oldCa.getSubjectX500Principal(), reissued.getSubjectX500Principal());
    assertEquals(oldCa.getPublicKey(), reissued.getPublicKey());
    assertEquals(id, NodeIdCertificate.verify(nodeCert, reissued, now));
    assertEquals(reissued, pathBuiltTo(nodeCert, reissued), "PKIX finds the path through the authority key identifier");
  }

  @Test
  void reissuingTheStoredCaKeepsItsNodeCertificatesValid() throws Exception {
    String name = new File(dir.toFile(), "TestCA").getPath();
    CertificateAuthorityTool before = CertificateAuthorityTool.getCATool(name, "pw".toCharArray());
    X509Certificate nodeCert = before.signRandomNodeId(CertificateAuthorityTool.generateKeyPair().getPublic());

    CertificateAuthorityTool after = CertificateAuthorityTool.reissueCATool(name, "pw".toCharArray());

    // within the same second the new certificate equals the old one (same key, subject, serial and validity)
    assertEquals(before.getCertificate().getPublicKey(), after.getCertificate().getPublicKey());
    assertEquals(after.getCertificate(), CertificateAuthorityTool.getCATool(name, "pw".toCharArray()).getCertificate(), "the store holds the re-issued certificate");
    NodeIdCertificate.verify(nodeCert, after.getCertificate(), Instant.now());
    assertEquals(after.getCertificate(), pathBuiltTo(nodeCert, after.getCertificate()));
    // the reloaded key still signs certificates that verify under the re-issued CA
    after.signRandomNodeId(CertificateAuthorityTool.generateKeyPair().getPublic()).verify(after.getCertificate().getPublicKey());
  }

  @Test
  void caChoosesDistinctRandomIds() {
    assertNotEquals(CertificateAuthorityTool.randomNodeId(), CertificateAuthorityTool.randomNodeId());
  }
}
