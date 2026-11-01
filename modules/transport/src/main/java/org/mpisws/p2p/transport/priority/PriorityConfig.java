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
package org.mpisws.p2p.transport.priority;

import static rice.environment.params.ConfigChecks.positive;

import rice.environment.params.Parameters;

/**
 * The tuning parameters of a {@link PriorityTransportLayerImpl}.
 *
 * @param maxMsgSize {@code pastry_socket_writer_max_msg_size}: the largest message, in bytes, that is
 *        queued on the primary socket; larger ones are sent on their own socket, up to the big message
 *        limit
 * @param maxBigMsgSize {@code pastry_socket_writer_max_big_msg_size}: the largest message, in bytes, that
 *        is accepted or sent at all; a peer announcing a bigger one on a big message socket is disconnected
 * @param maxQueueLength {@code pastry_socket_writer_max_queue_length}: the most messages queued per
 *        destination
 */
public record PriorityConfig(int maxMsgSize, int maxQueueLength, int maxBigMsgSize) {

  /**
   * @throws IllegalArgumentException if a value is not positive
   */
  public static PriorityConfig fromParameters(Parameters p) {
    return new PriorityConfig(
      positive("pastry_socket_writer_max_msg_size", p.getInt("pastry_socket_writer_max_msg_size")),
      positive("pastry_socket_writer_max_queue_length", p.getInt("pastry_socket_writer_max_queue_length")),
      positive("pastry_socket_writer_max_big_msg_size", p.getInt("pastry_socket_writer_max_big_msg_size")));
  }
}
