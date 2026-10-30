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

package rice.p2p.commonapi.rawserialization;

import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Maps the wire ids of one {@link MessageTypeId} registry back to its constants. Building the index
 * fails if two constants share an id, so a registry with a duplicate id does not load.
 *
 * @param <E> the registry enum
 */
public final class MessageTypeIndex<E extends Enum<E> & MessageTypeId> {

  private final Map<Short, E> byId;

  private MessageTypeIndex(Map<Short, E> byId) {
    this.byId = byId;
  }

  /**
   * @param constants the registry's {@code values()}
   * @return the index of the constants by id
   * @throws IllegalStateException if two constants share an id
   */
  @SafeVarargs
  public static <E extends Enum<E> & MessageTypeId> MessageTypeIndex<E> of(E... constants) {
    Map<Short, E> byId = new HashMap<>();
    for (E constant : constants) {
      E previous = byId.putIfAbsent(constant.id(), constant);
      if (previous != null) {
        throw new IllegalStateException("Type id " + constant.id() + " is used by both " + previous
            + " and " + constant + " in " + constant.getDeclaringClass().getName());
      }
    }
    return new MessageTypeIndex<>(Map.copyOf(byId));
  }

  /**
   * @param id a wire id
   * @return the constant with that id
   * @throws IllegalArgumentException if no constant has that id
   */
  public E get(short id) {
    E constant = byId.get(id);
    if (constant == null) {
      throw new IllegalArgumentException("Unknown type:" + id);
    }
    return constant;
  }

  /**
   * @param id a wire id
   * @return the constant with that id, or {@code null} if no constant has it
   */
  public @Nullable E find(short id) {
    return byId.get(id);
  }
}
