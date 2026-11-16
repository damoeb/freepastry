package org.mpisws.p2p.testing.transportlayer.peerreview;

import java.nio.file.Path;
import java.security.interfaces.RSAPublicKey;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.pki.x509.CATool;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransportLayerImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs the PeerReview scenarios of {@link PRRegressionTest}: alice and bob exchange messages, carol witnesses both.
 * Each scenario runs for {@link PRRegressionTest#DEFAULT_MILLIS_TO_FINISH} ms of wall-clock time (the simulated
 * clock runs in real time), over an in-memory transport, so they are integration tests.
 */
@Tag("integration")
public class PeerReviewScenariosTest {

  /** Signature size of the 2048-bit RSA keys of {@link CATool#generateKeyPair()} */
  private static final int SIGNATURE_SIZE = CATool.DEFAULT_KEY_SIZE / 8;

  @Test
  public void correctNodesStayTrusted(@TempDir Path dir) throws Exception {
    run(new PRRegressionTest(PRRegressionTest.DEFAULT_MILLIS_TO_FINISH, dir.toFile()));
  }

  @Test
  public void extraMessageExposesSender(@TempDir Path dir) throws Exception {
    run(new PRNonconform1(dir.toFile()));
  }

  @Test
  public void modifiedMessageExposesSender(@TempDir Path dir) throws Exception {
    run(new PRNonconform2(dir.toFile()));
  }

  @Test
  public void forkedLogExposesNode(@TempDir Path dir) throws Exception {
    run(new PRInconsistent1(dir.toFile()));
  }

  private static void run(PRRegressionTest scenario) throws InterruptedException {
    scenario.await(PRRegressionTest.MAIN_TIMEOUT_MILLIS);

    // the signatures are sized after the nodes' (2048-bit) keys, not the former fixed 96 bytes
    assertEquals(3, scenario.getIdentityTransports().size());
    for (IdentityTransportLayerImpl<PRRegressionTest.HandleImpl, PRRegressionTest.IdImpl> tl : scenario.getIdentityTransports()) {
      assertEquals(IdentityTransportLayerImpl.DEFAULT_SIGNATURE_SIZE, tl.getSignatureSizeBytes());
      assertEquals(SIGNATURE_SIZE, tl.getSignatureSizeBytes());
    }
    for (PRRegressionTest.Player player : scenario.playerTable.values())
      assertEquals(CATool.DEFAULT_KEY_SIZE, ((RSAPublicKey) player.cert.getPublicKey()).getModulus().bitLength());
  }
}
