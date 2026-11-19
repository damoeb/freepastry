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
package rice.pastry.socket;

import static rice.environment.params.ConfigChecks.requireNonNegative;
import static rice.environment.params.ConfigChecks.requirePositive;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import rice.environment.params.Parameters;

/**
 * The parameters a {@link SocketPastryNodeFactory} reads.
 *
 * @param findPortMaxTries {@code nat_find_port_max_tries}: the most firewall ports tried when opening
 *        one
 * @param natAppName {@code nat_app_name}: the application name registered with the firewall
 * @param bindAddress {@code socket_bindAddress}, optional: the address to bind to when the caller gives
 *        none; null if absent
 * @param allowLoopback {@code pastry_socket_allow_loopback}: accept the loopback address when the
 *        operating system offers it as the local address
 * @param knownNetworkAddress {@code pastry_socket_known_network_address}: a host connected to, to find
 *        a non-loopback local address
 * @param knownNetworkAddressPort {@code pastry_socket_known_network_address_port}: the port of that
 *        host
 * @param maxOpenSockets {@code pastry_socket_scm_max_open_sockets}: the limit of the socket limiting
 *        layer
 * @param seed {@code rice_socket_seed}: treat every booting node as a seed of the ring
 * @param incrementPortAfterConstruction {@code pastry_socket_increment_port_after_construction}: use
 *        the next port for the next node
 * @param hasExternalAddress whether {@code external_address} is set
 * @param externalAddress {@code external_address}, optional: the address of this node as seen from the
 *        Internet; null if absent or if its host cannot be resolved
 */
public record SocketFactoryConfig(
    int findPortMaxTries,
    String natAppName,
    String bindAddress,
    boolean allowLoopback,
    String knownNetworkAddress,
    int knownNetworkAddressPort,
    int maxOpenSockets,
    boolean seed,
    boolean incrementPortAfterConstruction,
    boolean hasExternalAddress,
    InetSocketAddress externalAddress) {

  /**
   * @throws IllegalArgumentException if findPortMaxTries is negative or maxOpenSockets is not positive
   * @throws UnknownHostException as {@link Parameters#getInetSocketAddress(String)} does
   */
  public static SocketFactoryConfig fromParameters(Parameters p) throws UnknownHostException {
    boolean hasExternalAddress = p.contains("external_address");
    return new SocketFactoryConfig(
      requireNonNegative("nat_find_port_max_tries", p.getInt("nat_find_port_max_tries")),
      p.getString("nat_app_name"),
      p.contains("socket_bindAddress") ? p.getString("socket_bindAddress") : null,
      p.getBoolean("pastry_socket_allow_loopback"),
      p.getString("pastry_socket_known_network_address"),
      p.getInt("pastry_socket_known_network_address_port"),
      requirePositive("pastry_socket_scm_max_open_sockets", p.getInt("pastry_socket_scm_max_open_sockets")),
      p.getBoolean("rice_socket_seed"),
      p.getBoolean("pastry_socket_increment_port_after_construction"),
      hasExternalAddress,
      hasExternalAddress ? p.getInetSocketAddress("external_address") : null);
  }
}
