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

package rice.p2p.glacier.v2.messaging;

import rice.p2p.commonapi.rawserialization.MessageTypeId;
import rice.p2p.commonapi.rawserialization.MessageTypeIndex;

/**
 * The wire type ids of Glacier's messages, all read by Glacier's message deserializer.
 * {@link #TIMEOUT} is a local timer message that never goes on the wire, but keeps its id reserved.
 */
public enum GlacierMessageType implements MessageTypeId {
  /** {@link GlacierDataMessage} */
  DATA(1),
  /** {@link GlacierFetchMessage} */
  FETCH(2),
  /** {@link GlacierNeighborRequestMessage} */
  NEIGHBOR_REQUEST(3),
  /** {@link GlacierNeighborResponseMessage} */
  NEIGHBOR_RESPONSE(4),
  /** {@link GlacierQueryMessage} */
  QUERY(5),
  /** {@link GlacierRangeForwardMessage} */
  RANGE_FORWARD(6),
  /** {@link GlacierRangeQueryMessage} */
  RANGE_QUERY(7),
  /** {@link GlacierRangeResponseMessage} */
  RANGE_RESPONSE(8),
  /** {@link GlacierRefreshCompleteMessage} */
  REFRESH_COMPLETE(9),
  /** {@link GlacierRefreshPatchMessage} */
  REFRESH_PATCH(10),
  /** {@link GlacierRefreshProbeMessage} */
  REFRESH_PROBE(11),
  /** {@link GlacierRefreshResponseMessage} */
  REFRESH_RESPONSE(12),
  /** {@link GlacierResponseMessage} */
  RESPONSE(13),
  /** {@link GlacierSyncMessage} */
  SYNC(14),
  /** {@link GlacierTimeoutMessage}, local only */
  TIMEOUT(15);

  private static final MessageTypeIndex<GlacierMessageType> INDEX = MessageTypeIndex.of(values());

  private final short id;

  GlacierMessageType(int id) {
    this.id = (short) id;
  }

  @Override
  public short id() {
    return id;
  }

  /**
   * @param id a wire id
   * @return the type with that id
   * @throws IllegalArgumentException if no Glacier message has that id
   */
  public static GlacierMessageType of(short id) {
    return INDEX.get(id);
  }

  /**
   * @param id a wire id
   * @return the type with that id, or {@code null} if no Glacier message has it
   */
  public static GlacierMessageType find(short id) {
    return INDEX.find(id);
  }
}
