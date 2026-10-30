package rice.pastry.standard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.util.SecurityUtils;
import rice.pastry.Id;

/**
 * Round trips of the node id certificate format.
 */
class CertifiedNodeIdFactoryTest {

  private Environment env;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void readsTheNodeIdOfAGeneratedCertificate() throws IOException {
    Id expected = new RandomNodeIdFactory(env).generateNodeId();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    CertifiedNodeIdFactory.generateCertificate(expected, bytes, SecurityUtils.generateKeyAsymmetric().getPrivate());
    Id actual = CertifiedNodeIdFactory.readCertificate(new ByteArrayInputStream(bytes.toByteArray()));

    assertEquals(expected, actual);
  }

  @Test
  void refusesAnotherFormatVersion() {
    byte[] bytes = {0, 0, 0, 99};

    assertThrows(IOException.class, () -> CertifiedNodeIdFactory.readCertificate(new ByteArrayInputStream(bytes)));
  }
}
