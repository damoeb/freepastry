package org.mpisws.p2p.transport.peerreview.audit;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocol;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.selector.TimerTask;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests that the progress timer keeps ticking while work is pending. */
public class AuditProtocolProgressTimerTest {

  private static AuditProtocol<RawSerializable, RawSerializable> newAudit(Environment env) {
    AtomicInteger calls = new AtomicInteger();
    PeerReview<RawSerializable, RawSerializable> peerreview = AuditProtocolDestroyTest.stub(PeerReview.class, env, calls);
    IdentityTransport<RawSerializable, RawSerializable> transport = AuditProtocolDestroyTest.stub(IdentityTransport.class, env, calls);
    EvidenceTransferProtocol<RawSerializable, RawSerializable> evidenceTransfer = AuditProtocolDestroyTest.stub(EvidenceTransferProtocol.class, env, calls);
    return new AuditProtocol<RawSerializable, RawSerializable>(peerreview, null, null, null, transport, null, evidenceTransfer, null);
  }

  @Test
  public void progressTimerReschedulesWhileAnInvestigationIsPending() {
    Environment env = new Environment();
    try {
      AuditProtocol<RawSerializable, RawSerializable> audit = newAudit(env);
      RawSerializable target = buf -> { };
      audit.activeInvestigation.put(target, new ActiveInvestigationInfo<>(target, 0, Long.MAX_VALUE, null, null));
      audit.scheduleProgressTimer();
      TimerTask first = audit.progressTimer;

      audit.makeProgressTimerExpired();

      assertNotNull(audit.progressTimer);
      assertNotSame(first, audit.progressTimer);
    } finally {
      env.destroy();
    }
  }

  @Test
  public void progressTimerStopsWhenNothingIsPending() {
    Environment env = new Environment();
    try {
      AuditProtocol<RawSerializable, RawSerializable> audit = newAudit(env);
      audit.scheduleProgressTimer();

      audit.makeProgressTimerExpired();

      assertNull(audit.progressTimer);
    } finally {
      env.destroy();
    }
  }
}
