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
package rice.pastry.socket.nat.rendezvous;

import java.util.List;

import rice.environment.params.Parameters;

/**
 * The parameters of a {@link RendezvousSocketPastryNodeFactory}. The {@code rendezvous_test_*} values
 * simulate firewalled nodes for testing.
 *
 * @param testMakesBootstrap {@code rendezvous_test_makes_bootstrap}: never simulate a firewall for the
 *        first node
 * @param testFirewall {@code rendezvous_test_firewall}: simulate a firewall for some nodes
 * @param testNumFirewalled {@code rendezvous_test_num_firewalled}: the fraction of nodes, from 0 to 1,
 *        that get a simulated firewall
 * @param simulateFirewall {@code rendezvous_simulate_firewall}, optional, default false: simulate a
 *        firewall for every node
 * @param natNetworkPrefixes {@code nat_network_prefixes}: the address prefixes, separated by
 *        {@code ;} in the parameter, that are not routable on the Internet
 */
public record RendezvousConfig(
    boolean testMakesBootstrap,
    boolean testFirewall,
    float testNumFirewalled,
    boolean simulateFirewall,
    List<String> natNetworkPrefixes) {

  public RendezvousConfig {
    natNetworkPrefixes = List.copyOf(natNetworkPrefixes);
  }

  public static RendezvousConfig fromParameters(Parameters p) {
    return new RendezvousConfig(
      p.getBoolean("rendezvous_test_makes_bootstrap"),
      p.getBoolean("rendezvous_test_firewall"),
      p.getFloat("rendezvous_test_num_firewalled"),
      p.contains(RendezvousSocketPastryNodeFactory.SIMULATE_FIREWALL)
          && p.getBoolean(RendezvousSocketPastryNodeFactory.SIMULATE_FIREWALL),
      List.of(p.getString("nat_network_prefixes").split(";")));
  }
}
