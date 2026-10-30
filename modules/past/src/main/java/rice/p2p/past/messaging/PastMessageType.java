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

package rice.p2p.past.messaging;

import rice.p2p.commonapi.rawserialization.MessageTypeId;
import rice.p2p.commonapi.rawserialization.MessageTypeIndex;

/**
 * The wire type ids of Past's and GCPast's messages. They share one scope: a GCPast endpoint reads
 * both with one deserializer. {@link #MESSAGE_LOST} and {@link #GC_COLLECT} are local messages
 * that never go on the wire, but keep their ids reserved.
 */
public enum PastMessageType implements MessageTypeId {
  /** {@link CacheMessage} */
  CACHE(1),
  /** {@link FetchHandleMessage} */
  FETCH_HANDLE(2),
  /** {@link FetchMessage} */
  FETCH(3),
  /** {@link InsertMessage} */
  INSERT(4),
  /** {@link LookupHandlesMessage} */
  LOOKUP_HANDLES(5),
  /** {@link LookupMessage} */
  LOOKUP(6),
  /** {@link MessageLostMessage}, local only */
  MESSAGE_LOST(7),
  /** {@link GCCollectMessage}, local only */
  GC_COLLECT(8),
  /** {@link GCInsertMessage} */
  GC_INSERT(9),
  /** {@link GCLookupHandlesMessage} */
  GC_LOOKUP_HANDLES(10),
  /** {@link GCRefreshMessage} */
  GC_REFRESH(11);

  private static final MessageTypeIndex<PastMessageType> INDEX = MessageTypeIndex.of(values());

  private final short id;

  PastMessageType(int id) {
    this.id = (short) id;
  }

  @Override
  public short id() {
    return id;
  }

  /**
   * @param id a wire id
   * @return the type with that id, or {@code null} if no Past message has it
   */
  public static PastMessageType find(short id) {
    return INDEX.find(id);
  }
}
