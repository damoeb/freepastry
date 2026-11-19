package org.mpisws.p2p.transport.sourceroute;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.multiaddress.SimpleAddressStrategy;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;

class AddressPolicyForwardStrategyTest {
  private static final List<InetSocketAddress> PUBLIC_NODE = List.of(addr("203.0.113.5", 9001));
  private static final List<InetSocketAddress> LOOPBACK_NODE = List.of(addr("127.0.0.1", 9001));

  private static InetSocketAddress addr(String address, int port) {
    try {
      return new InetSocketAddress(InetAddress.getByName(address), port);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"127.0.0.1", "127.1.2.3", "::1", "169.254.169.254", "fe80::1", "0.0.0.0", "::", "224.0.0.1",
      "ff02::1"})
  void aNodeOnAPublicAddressRefuses(String address) {
    assertFalse(AddressPolicyForwardStrategy.isAcceptable(PUBLIC_NODE, addr(address, 80)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"203.0.113.6", "10.0.0.7", "192.168.1.20", "2001:db8::7"})
  void aNodeOnAPublicAddressAccepts(String address) {
    assertTrue(AddressPolicyForwardStrategy.isAcceptable(PUBLIC_NODE, addr(address, 9002)));
  }

  @Test
  void aNodeOnLoopbackMayForwardToLoopback() {
    assertTrue(AddressPolicyForwardStrategy.isAcceptable(LOOPBACK_NODE, addr("127.0.0.1", 9002)));
  }

  @Test
  void aNodeNeverForwardsToItself() {
    assertFalse(AddressPolicyForwardStrategy.isAcceptable(LOOPBACK_NODE, addr("127.0.0.1", 9001)));
    assertFalse(AddressPolicyForwardStrategy.isAcceptable(PUBLIC_NODE, addr("203.0.113.5", 9001)));
  }

  @Test
  void portZeroAndUnresolvedAddressesAreRefused() {
    assertFalse(AddressPolicyForwardStrategy.isAcceptable(PUBLIC_NODE, addr("203.0.113.6", 0)));
    assertFalse(AddressPolicyForwardStrategy.isAcceptable(PUBLIC_NODE, InetSocketAddress.createUnresolved("x", 1)));
  }

  @Test
  void theMultiAddressStrategyChecksTheAddressThatIsContacted() {
    MultiInetSocketAddress local = new MultiInetSocketAddress(addr("203.0.113.5", 9001), addr("10.0.0.5", 9001));
    AddressPolicyForwardStrategy<MultiInetSocketAddress> strategy =
        AddressPolicyForwardStrategy.forMultiInetSocketAddress(local, new SimpleAddressStrategy());
    // behind the same NAT the inner address is contacted: a peer's private address is fine, loopback is not
    MultiInetSocketAddress peer = new MultiInetSocketAddress(addr("203.0.113.5", 9002), addr("10.0.0.6", 9002));
    MultiInetSocketAddress spoofed = new MultiInetSocketAddress(addr("203.0.113.5", 9002), addr("127.0.0.1", 6379));

    MultiAddressSourceRouteFactory routes = new MultiAddressSourceRouteFactory();
    assertTrue(strategy.forward(peer, routes.getSourceRoute(local, peer), false, null));
    assertFalse(strategy.forward(spoofed, routes.getSourceRoute(local, spoofed), false, null));
  }
}
