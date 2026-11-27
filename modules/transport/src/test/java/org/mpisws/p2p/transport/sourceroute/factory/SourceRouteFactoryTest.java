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
  void wireFactoryRejectsARouteThatRepeatsAHop() throws IOException {
    List<InetSocketAddress> h = hops(3);
    WireSourceRouteFactory target = new WireSourceRouteFactory(MAX_HOPS + 2);
    List<List<InetSocketAddress>> repeating = List.of(
        List.of(h.get(0), h.get(1), h.get(1), h.get(2)),
        List.of(h.get(0), h.get(1), h.get(2), h.get(1), h.get(2)),
        List.of(h.get(0), h.get(0)));

    for (List<InetSocketAddress> route : repeating) {
      byte[] wire = serialized(target.getSourceRoute(route));
      assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(wire), null, null));
    }
  }

  private static InetSocketAddress v6(int lastByte, int port) throws IOException {
    byte[] address = new byte[16];
    address[0] = 0x20;
    address[1] = 0x01;
    address[15] = (byte) lastByte;
    return new InetSocketAddress(InetAddress.getByAddress(address), port);
  }

  @Test
  void wireRouteWithIpv6AndMixedHopsRoundTrips() throws IOException {
    WireSourceRouteFactory target = new WireSourceRouteFactory();
    List<InetSocketAddress> mixed = List.of(v6(1, 9000), hops(1).get(0), v6(2, 65535));
    for (List<InetSocketAddress> path : List.of(List.of(v6(1, 9000)), List.of(v6(1, 9000), v6(2, 9001)), mixed)) {
      SourceRoute<InetSocketAddress> route = target.getSourceRoute(path);
      byte[] wire = serialized(route);

      SourceRoute<InetSocketAddress> read = target.build(new SimpleInputBuffer(wire), null, null);

      assertEquals(route, read);
      assertEquals(wire.length, route.getSerializedLength());
      SourceRoute<InetSocketAddress> reversed = target.reverse(route);
      assertEquals(reversed, target.build(new SimpleInputBuffer(serialized(reversed)), null, null));
    }
  }

  @Test
  void wireFactoryRejectsAnIpv6RouteThatRepeatsAHop() throws IOException {
    WireSourceRouteFactory target = new WireSourceRouteFactory();
    byte[] wire = serialized(target.getSourceRoute(List.of(v6(1, 9000), v6(2, 9000), v6(1, 9000))));

    assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(wire), null, null));
  }

  @Test
  void wireFactoryRejectsAnUnknownAddressFamily() throws IOException {
    WireSourceRouteFactory target = new WireSourceRouteFactory();
    for (byte family : new byte[] {0, 5, 7, 16, -1}) {
      byte[] wire = serialized(target.getSourceRoute(hops(2)));
      wire[1] = family;
      assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(wire), null, null));
    }
  }

  @Test
  void wireFactoryRejectsATruncatedHop() throws IOException {
    WireSourceRouteFactory target = new WireSourceRouteFactory();
    byte[] wire = serialized(target.getSourceRoute(List.of(v6(1, 9000), v6(2, 9001))));
    byte[] truncated = java.util.Arrays.copyOf(wire, wire.length - 5);

    assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(truncated), null, null));
  }

  @Test
  void wireFactoryAcceptsDirectRelayedAndReversedRoutes() throws IOException {
    WireSourceRouteFactory target = new WireSourceRouteFactory();
    for (int n = 1; n <= 3; n++) {
      SourceRoute<InetSocketAddress> route = target.getSourceRoute(hops(n));
      for (SourceRoute<InetSocketAddress> sourceRoute : List.of(route, target.reverse(route))) {
        SourceRoute<InetSocketAddress> read = target.build(new SimpleInputBuffer(serialized(sourceRoute)), null, null);
        assertEquals(sourceRoute, read);
      }
    }
  }

  @Test
  void multiAddressFactoryRejectsMoreHopsThanAllowed() {
    MultiAddressSourceRouteFactory target = new MultiAddressSourceRouteFactory(MAX_HOPS);

    assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(new byte[] {Byte.MAX_VALUE}), null, null));
    assertThrows(IOException.class, () -> target.build(new SimpleInputBuffer(new byte[] {-5}), null, null));
  }
}
