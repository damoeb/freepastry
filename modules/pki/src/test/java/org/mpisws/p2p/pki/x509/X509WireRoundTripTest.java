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
 * Writes certificates with {@link X509SerializerImpl}, reads them back from the bytes, writes the copy and reads it
 * again, as a node that receives a certificate and passes it on does.
 */
class X509WireRoundTripTest {
  @TempDir
  Path dir;

  private static byte[] serialized(X509SerializerImpl serializer, X509Certificate cert) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    serializer.serialize(cert, out);
    return out.getBytes();
  }

  @Test
  void aNodeCertificateAndTheCaCertificateRoundTrip() throws Exception {
    X509SerializerImpl serializer = new X509SerializerImpl();
    CAToolImpl ca = CAToolImpl.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray());
    X509Certificate node = ca.sign("node", CAToolImpl.generateKeyPair().getPublic());

    for (X509Certificate cert : new X509Certificate[] {node, ca.getCertificate()}) {
      byte[] first = serialized(serializer, cert);
      SimpleInputBuffer in = new SimpleInputBuffer(first);
      X509Certificate copy = serializer.deserialize(in);
      byte[] second = serialized(serializer, copy);

      assertEquals(cert, copy);
      assertEquals(0, in.bytesRemaining());
      assertArrayEquals(first, second);
      assertEquals(cert, serializer.deserialize(new SimpleInputBuffer(second)));
      copy.verify(ca.getCertificate().getPublicKey());
    }
  }

  @Test
  void aCertificateIsReadWithoutTheBytesThatFollow() throws Exception {
    X509SerializerImpl serializer = new X509SerializerImpl();
    CAToolImpl ca = CAToolImpl.getCATool(new File(dir.toFile(), "TestCA").getPath(), "pw".toCharArray());
    X509Certificate first = ca.sign("first", CAToolImpl.generateKeyPair().getPublic());
    X509Certificate second = ca.sign("second", CAToolImpl.generateKeyPair().getPublic());
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    serializer.serialize(first, out);
    serializer.serialize(second, out);
    SimpleInputBuffer in = new SimpleInputBuffer(out.getBytes());

    assertEquals(first, serializer.deserialize(in));
    assertEquals(second, serializer.deserialize(in));
    assertEquals(0, in.bytesRemaining());
  }
}
