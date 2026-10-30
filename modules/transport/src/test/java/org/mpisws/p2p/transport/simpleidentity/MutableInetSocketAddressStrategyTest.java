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
package org.mpisws.p2p.transport.simpleidentity;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;

import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that {@link MutableInetSocketAddressStrategy} serializes the address again when it changed, and reuses the
 * bytes while it stays the same.
 */
public class MutableInetSocketAddressStrategyTest {

  static final int PORT = 1234;

  /** Writes the four bytes of the IPv4 address, no port. */
  static class AddressSerializer implements Serializer<InetSocketAddress> {
    int calls;

    @Override
    public void serialize(InetSocketAddress i, OutputBuffer b) throws IOException {
      calls++;
      byte[] addr = i.getAddress().getAddress();
      b.write(addr, 0, addr.length);
    }

    @Override
    public InetSocketAddress deserialize(InputBuffer b, InetSocketAddress i, Map<String, Object> options) {
      throw new UnsupportedOperationException();
    }
  }

  static class SwitchableStrategy extends MutableInetSocketAddressStrategy {
    InetAddress current;

    SwitchableStrategy(Serializer<InetSocketAddress> serializer, InetAddress initial) {
      super(PORT, serializer);
      current = initial;
    }

    @Override
    protected InetAddress getLocalAddress() {
      return current;
    }
  }

  @Test
  public void reserializesOnlyWhenTheAddressChanges() throws IOException {
    InetAddress first = InetAddress.getByAddress(new byte[] {10, 0, 0, 1});
    InetAddress second = InetAddress.getByAddress(new byte[] {10, 0, 0, 2});
    AddressSerializer serializer = new AddressSerializer();
    SwitchableStrategy strategy = new SwitchableStrategy(serializer, first);

    assertArrayEquals(first.getAddress(), strategy.getLocalIdentifierBytes());
    assertArrayEquals(first.getAddress(), strategy.getLocalIdentifierBytes());
    assertEquals(1, serializer.calls, "an unchanged address should reuse the bytes");

    strategy.current = second;
    assertArrayEquals(second.getAddress(), strategy.getLocalIdentifierBytes(), "a changed address must be advertised");
    assertArrayEquals(second.getAddress(), strategy.getLocalIdentifierBytes());
    assertEquals(2, serializer.calls);
  }
}
