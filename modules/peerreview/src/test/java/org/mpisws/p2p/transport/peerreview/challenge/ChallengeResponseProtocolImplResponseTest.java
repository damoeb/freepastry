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
package org.mpisws.p2p.transport.peerreview.challenge;

import java.io.IOException;
import java.lang.reflect.Proxy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.ResponseMessage;
import org.mpisws.p2p.transport.peerreview.replay.record.RecordLayer;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Tests how a RESPONSE is handled.
 */
public class ChallengeResponseProtocolImplResponseTest {

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
  }

  Environment env;
  ChallengeResponseProtocolImpl<Id, Id> protocol;

  /** A stub of the interface whose methods all return null, 0 or false. */
  @SuppressWarnings("unchecked")
  static <T> T stub(Class<T> type) {
    return (T) Proxy.newProxyInstance(ChallengeResponseProtocolImplResponseTest.class.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
      Class<?> ret = method.getReturnType();
      if (ret == boolean.class) return false;
      if (ret == int.class) return 0;
      if (ret == long.class) return 0L;
      if (ret == short.class) return (short)0;
      return null;
    });
  }

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() {
    env = RecordLayer.generateEnvironment();
    IdentityTransport<Id, Id> transport = stub(IdentityTransport.class);
    PeerReviewImpl<Id, Id> peerreview = new PeerReviewImpl<Id, Id>(transport, env, null, null, h -> h, null);
    // the info store has no evidence at all
    PeerInfoStore<Id, Id> infoStore = stub(PeerInfoStore.class);
    protocol = new ChallengeResponseProtocolImpl<Id, Id>(peerreview, transport, infoStore, null, null, null, null);
  }

  @AfterEach
  public void tearDown() {
    env.destroy();
  }

  @Test
  public void responseWithoutAMatchingChallengeIsDiscarded() {
    ResponseMessage<Id> response = new ResponseMessage<Id>(new Id(1), new Id(2), 42, null);

    assertDoesNotThrow(() -> protocol.handleResponse(response, null));
  }
}
