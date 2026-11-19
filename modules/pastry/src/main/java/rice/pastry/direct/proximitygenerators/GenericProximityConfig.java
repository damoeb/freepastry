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
package rice.pastry.direct.proximitygenerators;

import static rice.environment.params.ConfigChecks.requirePositive;

import rice.environment.params.Parameters;

/**
 * The parameters of a {@link GenericProximityGenerator}, which reads its topology from a GT-ITM style
 * delay matrix.
 *
 * @param maxOverlaySize {@code pastry_direct_gtitm_max_overlay_size}: the number of nodes the matrix
 *        holds
 * @param minDelay {@code pastry_direct_min_delay}: the smallest delay, in milliseconds; read as a float
 *        here
 * @param delayFactor {@code pastry_direct_gtitm_delay_factor}: the factor applied to every matrix entry
 * @param matrixFile {@code pastry_direct_gtitm_matrix_file}: the matrix file, used when the caller
 *        passes none
 * @param nodesPerStub {@code pastry_direct_gtitm_nodes_per_stub}: the number of overlay nodes placed on
 *        each matrix node
 */
public record GenericProximityConfig(
    int maxOverlaySize,
    float minDelay,
    float delayFactor,
    String matrixFile,
    int nodesPerStub) {

  /**
   * @throws IllegalArgumentException if maxOverlaySize or nodesPerStub is not positive
   */
  public static GenericProximityConfig fromParameters(Parameters p) {
    return new GenericProximityConfig(
      requirePositive("pastry_direct_gtitm_max_overlay_size", p.getInt("pastry_direct_gtitm_max_overlay_size")),
      p.getFloat("pastry_direct_min_delay"),
      p.getFloat("pastry_direct_gtitm_delay_factor"),
      p.getString("pastry_direct_gtitm_matrix_file"),
      requirePositive("pastry_direct_gtitm_nodes_per_stub", p.getInt("pastry_direct_gtitm_nodes_per_stub")));
  }
}
