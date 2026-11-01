package org.mpisws.p2p.transport.peerreview.audit;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocol;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawSerializable;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests that a destroyed AuditProtocol schedules nothing more. */
public class AuditProtocolImplDestroyTest {

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals", "ReferenceEquality"}) // a stub has no type arguments to check, and equals() is proxy identity
  static <T> T stub(Class<?> type, Environment env, AtomicInteger calls) {
    return (T) Proxy.newProxyInstance(AuditProtocolImplDestroyTest.class.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getEnvironment":
          return env;
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        default:
          calls.incrementAndGet();
          return method.getReturnType() == long.class ? (Object) 0L : null;
      }
    });
  }

  @Test
  public void destroyedAuditProtocolDoesNotAuditAgain() {
    Environment env = new Environment();
    try {
      AtomicInteger calls = new AtomicInteger();
      PeerReview<RawSerializable, RawSerializable> peerreview = stub(PeerReview.class, env, calls);
      IdentityTransport<RawSerializable, RawSerializable> transport = stub(IdentityTransport.class, env, calls);
      EvidenceTransferProtocol<RawSerializable, RawSerializable> evidenceTransfer = stub(EvidenceTransferProtocol.class, env, calls);
      AuditProtocolImpl<RawSerializable, RawSerializable> audit =
          new AuditProtocolImpl<RawSerializable, RawSerializable>(peerreview, null, null, null, transport, null, evidenceTransfer, null);
      audit.destroy();
      int before = calls.get();

      audit.auditsTimerExpired();

      assertEquals(before, calls.get());
    } finally {
      env.destroy();
    }
  }
}
