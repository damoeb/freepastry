package org.mpisws.p2p.pki.x509;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

class X509SerializerImplTest {

  @Test
  void rejectsACertificateLongerThanTheMessage() throws Exception {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(Integer.MAX_VALUE);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());
    X509SerializerImpl target = new X509SerializerImpl();

    assertThrows(IOException.class, () -> target.deserialize(buf));
  }

  @Test
  void rejectsANegativeCertificateLength() throws Exception {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(-1);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());
    X509SerializerImpl target = new X509SerializerImpl();

    assertThrows(IOException.class, () -> target.deserialize(buf));
  }
}
