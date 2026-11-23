/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/
package org.mpisws.p2p.transport.peerreview.audit;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.peerreview.IdentifierExtractor;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocol;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.message.AuthResponse;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessageType;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests that an AUTHRESP widens the range of the ongoing investigation of its subject, and that only
 * authenticators signed by the subject enter the investigation.
 */
public class AuditProtocolAuthResponseTest {

  /** Serves as both the handle and the identifier. */
  static class Id implements RawSerializable {
    final int id;

    Id(int id) {
      this.id = id;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(id);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Id that && that.id == id;
    }

    @Override
    public int hashCode() {
      return id;
    }
  }

  /** The signature the stub accepts as the subject's; any other signature is a forgery. */
  static final byte[] GOOD_SIGNATURE = {2};
  static final byte[] FORGED_SIGNATURE = {3};

  Environment env;
  AuditProtocol<Id, Id> audit;
  boolean hasSubjectCertificate = true;
  final List<Id> certificateRequests = new ArrayList<>();

  /**
   * A stub of the interface: getEnvironment(), getTime() and getIdentifierExtractor() work, hasCertificate() answers
   * {@link #hasSubjectCertificate}, verify(subject, authenticator) accepts {@link #GOOD_SIGNATURE} only,
   * requestCertificate() is recorded, the rest is a no-op.
   */
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals", "ReferenceEquality"}) // the caller picks the generic interface type; a stub has no type arguments to check, and equals() is proxy identity
  <T> T stub(Class<?> type) {
    return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getEnvironment":
          return env;
        case "getTime":
          return env.getTimeSource().currentTimeMillis();
        case "getIdentifierExtractor":
          return (IdentifierExtractor<Id, Id>) h -> h;
        case "hasCertificate":
          return hasSubjectCertificate;
        case "verify":
          if (args.length == 2 && args[1] instanceof Authenticator authenticator) {
            return Arrays.equals(GOOD_SIGNATURE, authenticator.getSignature());
          }
          return defaultValue(method.getReturnType());
        case "requestCertificate":
          certificateRequests.add((Id) args[1]);
          return null;
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        default:
          return defaultValue(method.getReturnType());
      }
    });
  }

  static Object defaultValue(Class<?> type) {
    if (type == boolean.class) return false;
    if (type == long.class) return 0L;
    if (type == int.class) return 0;
    if (type == short.class) return (short)0;
    if (type == byte.class) return (byte)0;
    if (type == double.class) return 0d;
    if (type == float.class) return 0f;
    if (type == char.class) return (char)0;
    return null;
  }

  @BeforeEach
  public void setUp() {
    env = new Environment();
    PeerReview<Id, Id> peerreview = stub(PeerReview.class);
    IdentityTransport<Id, Id> transport = stub(IdentityTransport.class);
    EvidenceTransferProtocol<Id, Id> evidenceTransfer = stub(EvidenceTransferProtocol.class);
    audit = new AuditProtocol<Id, Id>(peerreview, null, null, null, transport, null, evidenceTransfer, null);
  }

  @AfterEach
  public void tearDown() {
    env.destroy();
  }

  static Authenticator auth(long seq) {
    return new Authenticator(seq, new byte[] {1}, GOOD_SIGNATURE);
  }

  static Authenticator forged(long seq) {
    return new Authenticator(seq, new byte[] {1}, FORGED_SIGNATURE);
  }

  @Test
  public void authResponseHasItsOwnType() {
    assertEquals(PeerReviewMessageType.AUTHRESP.id(), new AuthResponse<Id>(new Id(1), auth(1), auth(2)).getType());
  }

  @Test
  public void authResponseUpdatesTheInvestigation() {
    Id subject = new Id(7);
    audit.investigate(subject, 0);
    Authenticator from = auth(10);
    Authenticator authenticator = auth(20);

    audit.handleIncomingDatagram(new Id(8), new AuthResponse<Id>(subject, from, authenticator));

    PendingInvestigation<Id> investigation = audit.activeInvestigation.get(subject);
    assertSame(from, investigation.getAuthFrom());
    assertSame(authenticator, investigation.getAuthTo());

    // a later response only widens the range
    audit.handleIncomingDatagram(new Id(9), new AuthResponse<Id>(subject, auth(15), auth(30)));
    assertSame(from, investigation.getAuthFrom());
    assertEquals(30, investigation.getAuthTo().getSeq());
  }

  @Test
  public void authResponseWithAForgedFromIsIgnored() {
    Id subject = new Id(7);
    audit.investigate(subject, 0);

    audit.handleIncomingDatagram(new Id(8), new AuthResponse<Id>(subject, forged(10), auth(20)));

    PendingInvestigation<Id> investigation = audit.activeInvestigation.get(subject);
    assertNull(investigation.getAuthFrom());
    assertNull(investigation.getAuthTo());
  }

  @Test
  public void authResponseWithAForgedToIsIgnored() {
    Id subject = new Id(7);
    audit.investigate(subject, 0);
    audit.handleIncomingDatagram(new Id(8), new AuthResponse<Id>(subject, auth(10), auth(20)));

    // a forged authenticator beyond the end of the subject's log must not widen the range
    audit.handleIncomingDatagram(new Id(9), new AuthResponse<Id>(subject, auth(5), forged(Long.MAX_VALUE)));

    PendingInvestigation<Id> investigation = audit.activeInvestigation.get(subject);
    assertEquals(10, investigation.getAuthFrom().getSeq());
    assertEquals(20, investigation.getAuthTo().getSeq());
  }

  @Test
  public void authResponseWithoutTheSubjectsCertificateIsIgnoredAndTheCertificateRequested() {
    Id subject = new Id(7);
    audit.investigate(subject, 0);
    hasSubjectCertificate = false;

    audit.handleIncomingDatagram(new Id(8), new AuthResponse<Id>(subject, auth(10), auth(20)));

    PendingInvestigation<Id> investigation = audit.activeInvestigation.get(subject);
    assertNull(investigation.getAuthFrom());
    assertNull(investigation.getAuthTo());
    assertEquals(List.of(subject), certificateRequests);
  }
}
