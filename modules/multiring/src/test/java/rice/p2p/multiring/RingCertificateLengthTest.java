package rice.p2p.multiring;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

/**
 * A ring certificate is fetched from wherever its ring publishes it: its counts and lengths must not make us
 * allocate gigabytes.
 */
class RingCertificateLengthTest {
  private static final int CERTIFICATE_FORMAT_VERSION = 1;
  private static final int ID_BYTES = 20;

  /** Writes a certificate up to the number of bootstrap addresses, then lets the test write the rest. */
  private interface Rest {
    void write(DataOutputStream out) throws IOException;
  }

  private static ByteArrayInputStream certificate(int bootstraps, Rest rest) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(new GZIPOutputStream(bytes))) {
      out.writeInt(CERTIFICATE_FORMAT_VERSION);
      out.writeUTF("ring");
      out.writeInt(ID_BYTES);
      out.write(new byte[ID_BYTES]);
      out.writeInt(0); // protocol
      out.writeInt(0); // port
      out.writeLong(0); // version
      out.writeInt(bootstraps);
      rest.write(out);
    }
    return new ByteArrayInputStream(bytes.toByteArray());
  }

  @Test
  void rejectsAHugeBootstrapCount() throws IOException {
    ByteArrayInputStream stream = certificate(Integer.MAX_VALUE, out -> { });

    IOException actual = assertThrows(IOException.class, () -> RingCertificate.readFromStream(stream));

    assertTrue(actual.getMessage().contains("Invalid length"), actual.getMessage());
  }

  @Test
  void rejectsANegativeBootstrapCount() throws IOException {
    ByteArrayInputStream stream = certificate(-1, out -> { });

    assertThrows(IOException.class, () -> RingCertificate.readFromStream(stream));
  }

  @Test
  void rejectsAHugeKeyLength() throws IOException {
    ByteArrayInputStream stream = certificate(0, out -> {
      out.writeBoolean(false); // no log server
      out.writeInt(Integer.MAX_VALUE);
    });

    IOException actual = assertThrows(IOException.class, () -> RingCertificate.readFromStream(stream));

    assertTrue(actual.getMessage().contains("Invalid length"), actual.getMessage());
  }
}
