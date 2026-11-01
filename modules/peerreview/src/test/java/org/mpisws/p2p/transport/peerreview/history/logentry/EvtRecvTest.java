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
package org.mpisws.p2p.transport.peerreview.history.logentry;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import org.mpisws.p2p.transport.util.Serializer;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EvtRecvTest {

  static final byte[] MESSAGE = {10, 11, 12, 13, 14, 15, 16, 17};
  static final int RELEVANT = 3;
  static final HashProvider HASHER = new SHA256HashProvider();

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

  static final Serializer<Id> ID_SERIALIZER = new Serializer<Id>() {
    @Override
    public void serialize(Id i, OutputBuffer buf) throws IOException {
      i.serialize(buf);
    }

    @Override
    public Id deserialize(InputBuffer buf) throws IOException {
      return new Id(buf.readInt());
    }
  };

  @Test
  public void partlyRelevantRecvKeepsTheRelevantBytesAndHashesTheRest() throws Exception {
    EvtRecv<Id> evtRecv = new EvtRecv<Id>(new Id(1), 7, ByteBuffer.wrap(MESSAGE), RELEVANT, HASHER);

    assertArrayEquals(new byte[] {10, 11, 12}, evtRecv.getPayload());
    assertArrayEquals(HASHER.hash(ByteBuffer.wrap(MESSAGE, RELEVANT, MESSAGE.length - RELEVANT)), evtRecv.getHash());
  }

  @Test
  public void partlyRelevantRecvSurvivesARoundTrip() throws Exception {
    EvtRecv<Id> evtRecv = new EvtRecv<Id>(new Id(1), 7, ByteBuffer.wrap(MESSAGE), RELEVANT, HASHER);
    ByteBuffer serialized = evtRecv.serialize();
    byte[] bytes = new byte[serialized.remaining()];
    serialized.get(bytes);

    EvtRecv<Id> read = new EvtRecv<Id>(new SimpleInputBuffer(bytes), ID_SERIALIZER, HASHER.getHashSizeBytes());

    assertEquals(7, read.getSenderSeq());
    assertArrayEquals(evtRecv.getPayload(), read.getPayload());
    assertArrayEquals(evtRecv.getHash(), read.getHash());
  }
}
