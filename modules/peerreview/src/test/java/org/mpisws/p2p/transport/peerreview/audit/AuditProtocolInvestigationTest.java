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
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.peerreview.IdentifierExtractor;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocol;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.message.AuthRequest;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tests that an investigation which times out asks the witnesses again.
 */
public class AuditProtocolInvestigationTest {

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

  Environment env;
  AuditProtocol<Id, Id> audit;
  /** the messages sent to the witnesses */
  final List<Object> toWitnesses = new ArrayList<Object>();

  /** A stub of the interface: getEnvironment(), getTime(), getIdentifierExtractor() and sendMessageToWitnesses() work, the rest returns null, 0 or false. */
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"}) // the caller picks the generic interface type; a stub has no type arguments to check
  <T> T stub(Class<?> type) {
    return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getEnvironment":
          return env;
        case "getTime":
          return env.getTimeSource().currentTimeMillis();
        case "getIdentifierExtractor":
          return (IdentifierExtractor<Id, Id>) h -> h;
        case "sendMessageToWitnesses":
          toWitnesses.add(args[1]);
          return null;
        default:
          Class<?> ret = method.getReturnType();
          if (ret == boolean.class) return false;
          if (ret == int.class) return 0;
          if (ret == long.class) return 0L;
          return null;
      }
    });
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

  @Test
  public void timedOutInvestigationAsksTheWitnessesAgain() {
    Id subject = new Id(7);
    // no authenticators yet, and the timeout has passed
    audit.activeInvestigation.put(subject, new PendingInvestigation<Id>(subject, 100, 0, null, null));

    audit.makeProgressOnInvestigations();

    assertEquals(1, toWitnesses.size());
    assertInstanceOf(AuthRequest.class, toWitnesses.get(0));
  }
}
