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
package rice.pastry;

import static rice.environment.params.ConfigChecks.positive;

import rice.environment.params.Parameters;

/**
 * The overlay geometry and protocol version that a {@link PastryNode} and its {@link PastryNodeFactory}
 * use. A node builds it from its environment's parameters; get it with {@link PastryNode#getConfig()}.
 *
 * @param rtMax {@code pastry_rtMax}: the number of node handles kept per routing table entry
 * @param rtBaseBitLength {@code pastry_rtBaseBitLength}: the number of bits per routing table digit
 * @param lSetSize {@code pastry_lSetSize}: the size of the leaf set
 * @param routeMsgVersion {@code pastry_protocol_router_routeMsgVersion}: the version of the route
 *        messages this node sends
 */
public record PastryConfig(byte rtMax, byte rtBaseBitLength, byte lSetSize, byte routeMsgVersion) {

  /**
   * @throws IllegalArgumentException if rtMax, rtBaseBitLength or lSetSize is not positive
   */
  public static PastryConfig fromParameters(Parameters p) {
    return new PastryConfig(
      (byte)positive("pastry_rtMax", p.getInt("pastry_rtMax")),
      (byte)positive("pastry_rtBaseBitLength", p.getInt("pastry_rtBaseBitLength")),
      (byte)positive("pastry_lSetSize", p.getInt("pastry_lSetSize")),
      (byte)p.getInt("pastry_protocol_router_routeMsgVersion"));
  }
}
