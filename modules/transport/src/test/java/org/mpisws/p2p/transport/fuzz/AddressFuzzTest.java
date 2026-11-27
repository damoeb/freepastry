package org.mpisws.p2p.transport.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.net.InetSocketAddress;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.simpleidentity.InetSocketAddressSerializer;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.factory.WireSourceRouteFactory;
import rice.testing.fuzz.FuzzSupport;

/**
 * Fuzzes the address and source-route readers of the transport layers: the multi-address, source-route and identity
 * layers read them from the first bytes of every message and socket a peer sends. The invariant: a value or an
 * {@link java.io.IOException}, nothing else.
 */
class AddressFuzzTest {
  private static final InetSocketAddress LOCAL = InetSocketAddress.createUnresolved("local", 1);
  private static final InetSocketAddress LAST_HOP = InetSocketAddress.createUnresolved("hop", 2);

  @FuzzTest(maxDuration = "30s")
  void multiInetSocketAddress(FuzzedDataProvider data) {
    FuzzSupport.read(data, MultiInetSocketAddress::build);
  }

  @FuzzTest(maxDuration = "30s")
  void inetSocketAddress(FuzzedDataProvider data) {
    FuzzSupport.read(data, InetSocketAddressSerializer::deserializeAddress);
  }

  @FuzzTest(maxDuration = "30s")
  void wireSourceRoute(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new WireSourceRouteFactory().build(buf, LOCAL, LAST_HOP));
  }

  @FuzzTest(maxDuration = "30s")
  void multiAddressSourceRoute(FuzzedDataProvider data) {
    MultiInetSocketAddress local = new MultiInetSocketAddress(LOCAL);
    MultiInetSocketAddress lastHop = new MultiInetSocketAddress(LAST_HOP);
    FuzzSupport.read(data, buf -> new MultiAddressSourceRouteFactory().build(buf, local, lastHop));
  }
}
