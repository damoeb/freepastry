package org.mpisws.p2p.transport.fuzz;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mpisws.p2p.transport.liveness.PingMessage;
import org.mpisws.p2p.transport.liveness.PongMessage;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.simpleidentity.InetSocketAddressSerializer;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.factory.SourceRouteConfig;
import org.mpisws.p2p.transport.sourceroute.factory.WireSourceRouteFactory;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Writes the values of the transport layers to bytes and reads them back the way a hop does: the reader must consume
 * exactly the bytes the writer produced and the copy must write the same bytes again. The socket and message layers
 * built on these values are in {@link TransportLayersWireRoundTripTest}.
 */
class TransportWireRoundTripTest {
  @FunctionalInterface
  interface Writer<T> {
    void write(T value, OutputBuffer buf) throws IOException;
  }

  @FunctionalInterface
  interface Reader<T> {
    T read(InputBuffer buf) throws IOException;
  }

  static <T> byte[] bytes(T value, Writer<? super T> writer) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    writer.write(value, out);
    return out.getBytes();
  }

  private static <T> T readAll(byte[] bytes, Reader<T> reader, Object value) throws IOException {
    SimpleInputBuffer input = new SimpleInputBuffer(bytes);
    T read = reader.read(input);
    assertNotNull(read, () -> "read " + value);
    assertEquals(0, input.bytesRemaining(), () -> "bytes left after reading " + value);
    return read;
  }

  /** Serialize, read, serialize the copy, assert the same bytes, read again; returns the second copy. */
  static <T> T roundTrip(T value, Writer<? super T> writer, Reader<? extends T> reader) throws IOException {
    byte[] first = bytes(value, writer);
    T copy = readAll(first, reader::read, value);
    byte[] second = bytes(copy, writer);
    assertArrayEquals(first, second, () -> "the copy of " + value + " serializes to the same bytes");
    return readAll(second, reader::read, value);
  }

  private static InetSocketAddress v4(int lastOctet, int port) throws IOException {
    return new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 1, 2, (byte) lastOctet}), port);
  }

  private static InetSocketAddress v6(int lastByte, int port) throws IOException {
    byte[] address = new byte[16];
    address[0] = 0x20;
    address[1] = 0x01;
    address[15] = (byte) lastByte;
    return new InetSocketAddress(InetAddress.getByAddress(address), port);
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE, 1_700_000_000_000L})
  @DisplayName("Ping and Pong are the 8 bytes of their time, which is what the liveness layer reads after its header")
  void pingAndPong(long sentTime) throws IOException {
    byte[] ping = bytes(new PingMessage(sentTime), PingMessage::serialize);
    byte[] pong = bytes(new PongMessage(sentTime), PongMessage::serialize);

    assertEquals(Long.BYTES, ping.length);
    assertEquals(sentTime, ByteBuffer.wrap(ping).getLong());
    assertArrayEquals(ping, pong);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 80, 32767, 32768, 65535})
  void inetSocketAddressV4AndV6(int port) throws IOException {
    for (InetSocketAddress address : List.of(v4(7, port), v6(9, port))) {
      InetSocketAddress read = roundTrip(address, InetSocketAddressSerializer::serializeAddress,
          InetSocketAddressSerializer::deserializeAddress);

      assertEquals(address, read);
      assertEquals(bytes(address, InetSocketAddressSerializer::serializeAddress).length,
          new InetSocketAddressSerializer().getSerializedLength(address));
    }
  }

  @Test
  @DisplayName("MultiInetSocketAddress with one address, with every count up to the hard maximum, mixing v4 and v6")
  void multiInetSocketAddress() throws IOException {
    for (int count = 1; count <= MultiInetSocketAddress.HARD_MAX_NUM_ADDRESSES; count++) {
      InetSocketAddress[] addresses = new InetSocketAddress[count];
      for (int i = 0; i < count; i++) {
        addresses[i] = i % 2 == 0 ? v4(i + 1, 9000 + i) : v6(i + 1, 9000 + i);
      }
      MultiInetSocketAddress address = new MultiInetSocketAddress(addresses);

      MultiInetSocketAddress read = roundTrip(address, MultiInetSocketAddress::serialize,
          MultiInetSocketAddress::build);

      assertEquals(count, read.getNumAddresses());
      for (int i = 0; i < count; i++) {
        assertEquals(addresses[i], read.getAddress(i));
      }
      assertEquals(bytes(address, MultiInetSocketAddress::serialize).length, address.getSerializedLength());
    }
  }

  @Test
  void multiInetSocketAddressWithALimit() throws IOException {
    MultiInetSocketAddress address = new MultiInetSocketAddress(v4(1, 1), v4(2, 2));

    MultiInetSocketAddress read = roundTrip(address, MultiInetSocketAddress::serialize,
        buf -> MultiInetSocketAddress.build(buf, 2));

    assertEquals(address, read);
  }

  private static List<InetSocketAddress> wireHops(int count) throws IOException {
    List<InetSocketAddress> hops = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      hops.add(v4(i + 1, 9000 + i));
    }
    return hops;
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, SourceRouteConfig.DEFAULT_MAX_NUM_HOPS})
  void wireSourceRoute(int hopCount) throws IOException {
    WireSourceRouteFactory factory = new WireSourceRouteFactory();
    SourceRoute<InetSocketAddress> route = factory.getSourceRoute(wireHops(hopCount));

    SourceRoute<InetSocketAddress> read = roundTrip(route, SourceRoute::serialize,
        buf -> factory.build(buf, v4(100, 1), v4(101, 2)));

    assertEquals(route, read);
    assertEquals(bytes(route, SourceRoute::serialize).length, route.getSerializedLength());
    SourceRoute<InetSocketAddress> reversed = factory.reverse(read);
    assertEquals(factory.reverse(route), roundTrip(reversed, SourceRoute::serialize,
        buf -> factory.build(buf, v4(100, 1), v4(101, 2))));
  }

  private static List<MultiInetSocketAddress> multiHops(int count) throws IOException {
    List<MultiInetSocketAddress> hops = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      hops.add(i % 2 == 0 ? new MultiInetSocketAddress(v4(i + 1, 9000 + i))
          : new MultiInetSocketAddress(v4(i + 1, 9000 + i), v6(i + 1, 9000 + i)));
    }
    return hops;
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, SourceRouteConfig.DEFAULT_MAX_NUM_HOPS})
  @DisplayName("MultiAddress source route: the two-hop route is only its count and is rebuilt from the local and last hop")
  void multiAddressSourceRoute(int hopCount) throws IOException {
    MultiAddressSourceRouteFactory factory = new MultiAddressSourceRouteFactory();
    List<MultiInetSocketAddress> hops = multiHops(hopCount);
    SourceRoute<MultiInetSocketAddress> route = factory.getSourceRoute(hops);
    // a node reads a route it is the last hop of, from the node that sent it
    MultiInetSocketAddress local = hops.get(hopCount - 1);
    MultiInetSocketAddress lastHop = hops.get(0);

    SourceRoute<MultiInetSocketAddress> read = roundTrip(route, SourceRoute::serialize,
        buf -> factory.build(buf, local, lastHop));

    assertEquals(route, read);
    assertEquals(bytes(route, SourceRoute::serialize).length, route.getSerializedLength());
    if (hopCount == 2) {
      assertEquals(1, bytes(route, SourceRoute::serialize).length);
    }
  }

  @Test
  @DisplayName("A source route read at a hop and reversed for the answer reads again at the other end")
  void reversedMultiAddressRouteReads() throws IOException {
    MultiAddressSourceRouteFactory factory = new MultiAddressSourceRouteFactory();
    List<MultiInetSocketAddress> hops = multiHops(3);
    SourceRoute<MultiInetSocketAddress> route = factory.getSourceRoute(hops);
    SourceRoute<MultiInetSocketAddress> atLast = roundTrip(route, SourceRoute::serialize,
        buf -> factory.build(buf, hops.get(2), hops.get(1)));

    SourceRoute<MultiInetSocketAddress> reversed = factory.reverse(atLast);
    SourceRoute<MultiInetSocketAddress> atFirst = roundTrip(reversed, SourceRoute::serialize,
        buf -> factory.build(buf, hops.get(0), hops.get(1)));

    assertEquals(reversed, atFirst);
    assertTrue(atFirst.getFirstHop().equals(hops.get(2)));
  }
}
