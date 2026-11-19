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

package rice.p2p.commonapi;

import org.jspecify.annotations.Nullable;
import rice.p2p.commonapi.rawserialization.MessageTypeId;
import rice.p2p.commonapi.rawserialization.MessageTypeIndex;

/**
 * The type ids of the {@link Id} implementations, written before an Id and passed to {@link Endpoint#readId}. The endpoints stack (multiring and GC endpoints wrap a Pastry endpoint), so all Ids share one scope.
 */
public enum IdType implements MessageTypeId {
  /** {@code rice.pastry.Id} */
  PASTRY_ID(1),
  /** {@code rice.p2p.multiring.RingId} */
  RING_ID(2),
  /** {@code rice.p2p.past.gc.ExpiringId} */
  GC_ID(3),
  /** {@code rice.p2p.glacier.VersionKey} */
  VERSION_KEY(41),
  /** {@code rice.p2p.glacier.FragmentKey} */
  FRAGMENT_KEY(42);

  private static final MessageTypeIndex<IdType> INDEX = MessageTypeIndex.of(values());

  private final short id;

  IdType(int id) {
    this.id = (short) id;
  }

  @Override
  public short id() {
    return id;
  }

  /**
   * @param id a type id
   * @return the type with that id
   * @throws IllegalArgumentException if no type has that id
   */
  public static IdType of(short id) {
    return INDEX.get(id);
  }

  /**
   * @param id a type id
   * @return the type with that id, or {@code null} if no type has it
   */
  public static @Nullable IdType find(short id) {
    return INDEX.find(id);
  }
}
