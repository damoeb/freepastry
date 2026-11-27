package org.mpisws.p2p.transport.sourceroute;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.multiaddress.AddressStrategy;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;

/**
 * Forwards a source route only to a next hop whose address could be a peer: never to the wildcard or a multicast
 * address, never to the node itself, and to a loopback or link-local address only when the node has such an address
 * itself (a test ring on 127.0.0.1). Without it, a node relays datagrams and sockets to any address a route names,
 * for example to a service on its own loopback interface or to a cloud metadata endpoint.
 *
 * @param <Identifier> the address type of the layer below the source route layer
 */
public class AddressPolicyForwardStrategy<Identifier> implements SourceRouteForwardStrategy<Identifier> {
  private final List<InetSocketAddress> localAddresses;
  private final Function<Identifier, InetSocketAddress> contactedAddress;

  /**
   * @param localAddresses the addresses of this node
   * @param contactedAddress the address the layer below contacts for a next hop
   */
  public AddressPolicyForwardStrategy(
      Collection<InetSocketAddress> localAddresses, Function<Identifier, InetSocketAddress> contactedAddress) {
    this.localAddresses = List.copyOf(localAddresses);
    this.contactedAddress = contactedAddress;
  }

  public static AddressPolicyForwardStrategy<InetSocketAddress> forInetSocketAddress(InetSocketAddress local) {
    return new AddressPolicyForwardStrategy<>(List.of(local), Function.identity());
  }

  /**
   * Checks the address that {@code addressStrategy} picks for the next hop, as the multi-address layer below does.
   */
  public static AddressPolicyForwardStrategy<MultiInetSocketAddress> forMultiInetSocketAddress(
      MultiInetSocketAddress local, AddressStrategy addressStrategy) {
    return new AddressPolicyForwardStrategy<>(
        addresses(local), nextHop -> addressStrategy.getAddress(local, nextHop));
  }

  /** All addresses of {@code address}, outermost first. */
  public static List<InetSocketAddress> addresses(MultiInetSocketAddress address) {
    List<InetSocketAddress> ret = new ArrayList<>(address.getNumAddresses());
    for (int i = 0; i < address.getNumAddresses(); i++) {
      ret.add(address.getAddress(i));
    }
    return ret;
  }

  @Override
  public boolean forward(
      Identifier nextHop, SourceRoute<Identifier> sourceRoute, boolean socket, @Nullable Map<String, Object> options) {
    return isAcceptable(localAddresses, contactedAddress.apply(nextHop));
  }

  /**
   * Whether a node with the addresses {@code local} may contact {@code target} on behalf of someone else.
   */
  public static boolean isAcceptable(Collection<InetSocketAddress> local, InetSocketAddress target) {
    InetAddress address = target.getAddress();
    if (address == null || target.getPort() == 0 || local.contains(target)) {
      return false;
    }
    if (address.isAnyLocalAddress() || address.isMulticastAddress()) {
      return false;
    }
    if (address.isLoopbackAddress()) {
      return anyLocal(local, InetAddress::isLoopbackAddress);
    }
    if (address.isLinkLocalAddress()) {
      return anyLocal(local, InetAddress::isLinkLocalAddress);
    }
    return true;
  }

  private static boolean anyLocal(Collection<InetSocketAddress> local, Predicate<InetAddress> test) {
    for (InetSocketAddress address : local) {
      InetAddress ip = address.getAddress();
      if (ip != null && test.test(ip)) {
        return true;
      }
    }
    return false;
  }
}
