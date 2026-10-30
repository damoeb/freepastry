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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Checks {@link MessageTypeIndex}. {@code MessageTypeRegistriesTest} in freepastry-tutorials checks
 * the registries themselves, because it sees every module.
 */
class MessageTypeIdTest {

  private enum Clean implements MessageTypeId {
    ONE(1), TWO(2);

    private final short id;

    Clean(int id) {
      this.id = (short) id;
    }

    @Override
    public short id() {
      return id;
    }
  }

  private enum Clash implements MessageTypeId {
    ONE(1), ALSO_ONE(1);

    private final short id;

    Clash(int id) {
      this.id = (short) id;
    }

    @Override
    public short id() {
      return id;
    }
  }

  @Test
  void indexFindsConstantsById() {
    MessageTypeIndex<Clean> index = MessageTypeIndex.of(Clean.values());

    assertSame(Clean.TWO, index.get((short) 2));
    assertNull(index.find((short) 3));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> index.get((short) 3));
    assertEquals("Unknown type:3", e.getMessage());
  }

  @Test
  void indexRejectsDuplicateIds() {
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> MessageTypeIndex.of(Clash.values()));
    assertTrue(e.getMessage().contains("ONE") && e.getMessage().contains("ALSO_ONE"), e.getMessage());
  }
}
