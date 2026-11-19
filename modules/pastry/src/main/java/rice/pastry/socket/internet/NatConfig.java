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
package rice.pastry.socket.internet;

import java.util.List;
import java.util.Locale;

import rice.environment.params.Parameters;

/**
 * The NAT and firewall policies of an {@link InternetPastryNodeFactory}. The policies are the raw
 * parameter values; {@link InternetPastryNodeFactory#getFireWallPolicyVariable(String)} maps them.
 *
 * @param natHandlerClass {@code nat_handler_class}, optional: the {@link rice.pastry.socket.nat.FirewallPortOpener} implementation to
 *        load; null for a {@link rice.pastry.socket.nat.NoOpFirewallPortOpener}
 * @param natSearchPolicy {@code nat_search_policy}: when to look for a NAT ({@code never},
 *        {@code prefix} or {@code always})
 * @param natStatePolicy {@code nat_state_policy}: what to do when the requested firewall port is taken
 *        ({@code overwrite}, {@code fail} or {@code change})
 * @param firewallTestPolicy {@code firewall_test_policy}: when to check connectivity through the
 *        firewall ({@code never}, {@code boot}, {@code prefix} or {@code always})
 */
public record NatConfig(String natHandlerClass, String natSearchPolicy, String natStatePolicy, String firewallTestPolicy) {

  /**
   * The values {@link InternetPastryNodeFactory#getFireWallPolicyVariable(String)} knows.
   */
  private static final List<String> POLICIES =
      List.of("prefix", "change", "never", "overwrite", "always", "boot", "fail", "rendezvous");

  /**
   * @throws IllegalArgumentException if a policy is not one of the known values
   */
  public static NatConfig fromParameters(Parameters parameters) {
    return new NatConfig(
      parameters.contains("nat_handler_class") ? parameters.getString("nat_handler_class") : null,
      policy(parameters, "nat_search_policy"),
      policy(parameters, "nat_state_policy"),
      policy(parameters, "firewall_test_policy"));
  }

  private static String policy(Parameters parameters, String key) {
    String value = parameters.getString(key);
    if (value == null || !POLICIES.contains(value.toLowerCase(Locale.ROOT))) {
      throw new IllegalArgumentException(key + " must be one of " + POLICIES + ", but is " + value);
    }
    return value;
  }
}
