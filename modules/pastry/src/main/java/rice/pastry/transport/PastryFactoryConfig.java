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
package rice.pastry.transport;

import static rice.environment.params.ConfigChecks.requireNonNegative;

import rice.environment.params.Parameters;

/**
 * The parameters a {@link TransportPastryNodeFactory} and its subclasses read when they build nodes.
 *
 * @param leafSetMaintFreq {@code pastry_leafSetMaintFreq}: the leaf set maintenance period, in
 *        seconds; 0 means never
 * @param routeSetMaintFreq {@code pastry_routeSetMaintFreq}: the routing table maintenance period, in
 *        seconds; 0 means never
 * @param usePns {@code transport_use_pns}: pick routing table entries by proximity (PNS)
 * @param multipleNodes {@code pastry_factory_multipleNodes}: give every node its own environment, so
 *        that one JVM can run several nodes
 * @param selectorPerNode {@code pastry_factory_selectorPerNode}: give every node its own selector
 *        thread (with multipleNodes)
 * @param processorPerNode {@code pastry_factory_processorPerNode}: give every node its own processor
 *        thread (with multipleNodes)
 */
public record PastryFactoryConfig(
    int leafSetMaintFreq,
    int routeSetMaintFreq,
    boolean usePns,
    boolean multipleNodes,
    boolean selectorPerNode,
    boolean processorPerNode) {

  /**
   * @throws IllegalArgumentException if a maintenance period is negative
   */
  public static PastryFactoryConfig fromParameters(Parameters parameters) {
    return new PastryFactoryConfig(
      requireNonNegative("pastry_leafSetMaintFreq", parameters.getInt("pastry_leafSetMaintFreq")),
      requireNonNegative("pastry_routeSetMaintFreq", parameters.getInt("pastry_routeSetMaintFreq")),
      parameters.getBoolean("transport_use_pns"),
      parameters.getBoolean("pastry_factory_multipleNodes"),
      parameters.getBoolean("pastry_factory_selectorPerNode"),
      parameters.getBoolean("pastry_factory_processorPerNode"));
  }
}
