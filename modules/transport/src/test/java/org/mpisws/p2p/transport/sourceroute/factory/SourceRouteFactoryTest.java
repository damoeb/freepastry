package org.mpisws.p2p.transport.sourceroute.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

class SourceRouteFactoryTest {
  private static final int MAX_HOPS = 3;

  private static List<InetSocketAddress> hops(int n) throws IOException {
    List<InetSocketAddress> ret = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      ret.add(new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, (byte) (i + 1)}), 9000 + i));
    }
    return ret;
  }

  private static byte[] serialized(SourceRoute<InetSocketAddress> route) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    route.serialize(out);
    return out.getBytes();
  }

  @Test
  void wireSerializedLengthMatchesTheBytesWritten() throws IOException {
    SourceRoute<InetSocketAddress> target = new WireSourceRouteFactory().getSourceRoute(hops(3));

    assertEquals(serialized(target).length, target.getSerializedLength());
  }

  @Test
  void wireFactoryAcceptsARouteWithinTheMaximum() throws IOException {
    byte[] wire = serialized(new WireSourceRouteFactory().getSourceRoute(hops(MAX_HOPS)));

    SourceRoute<InetSocketAddress> actual =
        new WireSourceRouteFactory(MAX_HOPS).build(new SimpleInputBuffer(wire), null, null);

    assertEquals(MAX_HOPS, ((WireSourceRoute) actual).getPath().size());
  }

  @Test
  void wireFactoryRejectsMoreHopsThanAllowed() throws IOException {
    byte[] wire = serialized(new WireSourceRouteFactory().getSourceRoute(hops(MAX_HOPS + 1)));

    assertThrows(IOException.class,
        () -> new WireSourceRouteFactory(MAX_HOPS).build(new SimpleInputBuffer(wire), null, null));
  }

  @Test
  void wireFactoryRejectsANegativeOrEmptyHopCount() {
    WireSourceRouteFactory target = new WireSourceRouteFactory(MAX_HOPS);

    assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(new byte[] {-1}), null, null));
    assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(new byte[] {0}), null, null));
  }

  @Test
  void multiAddressFactoryRejectsMoreHopsThanAllowed() {
    MultiAddressSourceRouteFactory target = new MultiAddressSourceRouteFactory(MAX_HOPS);

    assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(new byte[] {Byte.MAX_VALUE}), null, null));
    assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(new byte[] {-5}), null, null));
  }
}
