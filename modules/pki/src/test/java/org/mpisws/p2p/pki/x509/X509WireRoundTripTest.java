package org.mpisws.p2p.pki.x509;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Writes certificates with {@link X509Serializer}, reads them back from the bytes, writes the copy and reads it
 * again, as a node that receives a certificate and passes it on does.
 */
class X509WireRoundTripTest {
  @TempDir
  Path dir;

  private static byte[] serialized(X509Serializer serializer, X509Certificate cert) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    serializer.serialize(cert, out);
    return out.getBytes();
  }

  @Test
  void aNodeCertificateAndTheCaCertificateRoundTrip() throws Exception {
    X509Serializer serializer = new X509Serializer();
    CertificateAuthorityTool certificateAuthorityTool = CertificateAuthorityTool.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray());
    X509Certificate node = certificateAuthorityTool.sign("node", CertificateAuthorityTool.generateKeyPair().getPublic());

    for (X509Certificate cert : new X509Certificate[] {node, certificateAuthorityTool.getCertificate()}) {
      byte[] first = serialized(serializer, cert);
      SimpleInputBuffer input = new SimpleInputBuffer(first);
      X509Certificate copy = serializer.deserialize(input);
      byte[] second = serialized(serializer, copy);

      assertEquals(cert, copy);
      assertEquals(0, input.bytesRemaining());
      assertArrayEquals(first, second);
      assertEquals(cert, serializer.deserialize(new SimpleInputBuffer(second)));
      copy.verify(certificateAuthorityTool.getCertificate().getPublicKey());
    }
  }

  @Test
  void aCertificateIsReadWithoutTheBytesThatFollow() throws Exception {
    X509Serializer serializer = new X509Serializer();
    CertificateAuthorityTool certificateAuthorityTool = CertificateAuthorityTool.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray());
    X509Certificate first = certificateAuthorityTool.sign("first", CertificateAuthorityTool.generateKeyPair().getPublic());
    X509Certificate second = certificateAuthorityTool.sign("second", CertificateAuthorityTool.generateKeyPair().getPublic());
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    serializer.serialize(first, out);
    serializer.serialize(second, out);
    SimpleInputBuffer input = new SimpleInputBuffer(out.getBytes());

    assertEquals(first, serializer.deserialize(input));
    assertEquals(second, serializer.deserialize(input));
    assertEquals(0, input.bytesRemaining());
  }
}
