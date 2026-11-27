package org.mpisws.p2p.transport.multiaddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

class MultiInetSocketAddressTest {

  private static byte[] serialized(int numAddresses) throws IOException {
    InetSocketAddress[] addresses = new InetSocketAddress[numAddresses];
    for (int i = 0; i < numAddresses; i++) {
      addresses[i] = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9000 + i);
    }
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    new MultiInetSocketAddress(addresses).serialize(out);
    return out.getBytes();
  }

  @Test
  void buildAcceptsAsManyAddressesAsAllowed() throws IOException {
    MultiInetSocketAddress actual = MultiInetSocketAddress.build(new SimpleInputBuffer(serialized(2)), 2);

    assertEquals(2, actual.getNumAddresses());
  }

  @Test
  void buildRejectsMoreAddressesThanAllowed() throws IOException {
    byte[] wire = serialized(3);

    assertThrows(IOException.class, () -> MultiInetSocketAddress.build(new SimpleInputBuffer(wire), 2));
  }

  @Test
  void buildRejectsAHugeAddressCountWithoutTheLimit() {
    byte[] wire = {Byte.MAX_VALUE};

    assertThrows(IOException.class, () -> MultiInetSocketAddress.build(new SimpleInputBuffer(wire)));
  }

  @Test
  void buildRejectsANegativeOrEmptyAddressCount() {
    assertThrows(IOException.class, () -> MultiInetSocketAddress.build(new SimpleInputBuffer(new byte[] {-1}), 2));
    assertThrows(IOException.class, () -> MultiInetSocketAddress.build(new SimpleInputBuffer(new byte[] {0}), 2));
  }
}
