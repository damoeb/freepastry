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
package rice.p2p.past;

import static rice.environment.params.ConfigChecks.inRange;
import static rice.environment.params.ConfigChecks.requirePositive;

import rice.environment.params.Parameters;

/**
 * The parameters of a {@link PastImpl}.
 *
 * @param messageTimeout {@code p2p_past_messageTimeout}: how long to wait for a reply, in milliseconds
 * @param successfulInsertThreshold {@code p2p_past_successfulInsertThreshold}: the fraction of
 *        replicas, from 0 to 1, that must store an object for an insert to succeed
 */
public record PastConfig(int messageTimeout, double successfulInsertThreshold) {

  /**
   * @throws IllegalArgumentException if the timeout is not positive or the threshold is outside [0, 1]
   */
  public static PastConfig fromParameters(Parameters p) {
    return new PastConfig(
      requirePositive("p2p_past_messageTimeout", p.getInt("p2p_past_messageTimeout")),
      inRange("p2p_past_successfulInsertThreshold", p.getDouble("p2p_past_successfulInsertThreshold"), 0, 1));
  }
}
