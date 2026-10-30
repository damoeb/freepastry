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
package org.mpisws.p2p.transport.wire;

import static rice.environment.params.ConfigChecks.positive;

import rice.environment.params.Parameters;

/**
 * The tuning parameters of a {@link WireTransportLayerImpl} and its {@link TCPLayer} and
 * {@link UDPLayerImpl}.
 *
 * @param forceBindAddress {@code wire_forceBindAddress}, optional, default false: bind outgoing
 *        sockets to the bind address too (needed for modelnet)
 * @param maxOpenSockets {@code pastry_socket_scm_max_open_sockets}: the number of open TCP sockets at
 *        which the layer starts closing others
 * @param socketBufferSize {@code pastry_socket_scm_socket_buffer_size}: TCP send and receive buffer
 *        size, in bytes
 * @param tcpNoDelay {@code transport_tcp_no_delay}, optional, default false: disable Nagle's algorithm
 * @param datagramReceiveBufferSize {@code transport_wire_datagram_receive_buffer_size}: UDP receive
 *        buffer size, in bytes
 * @param datagramSendBufferSize {@code transport_wire_datagram_send_buffer_size}: UDP send buffer size,
 *        in bytes; also the size of the UDP read buffer
 */
public record WireConfig(
    boolean forceBindAddress,
    int maxOpenSockets,
    int socketBufferSize,
    boolean tcpNoDelay,
    int datagramReceiveBufferSize,
    int datagramSendBufferSize) {

  /**
   * @throws IllegalArgumentException if a buffer size is not positive
   */
  public static WireConfig fromParameters(Parameters p) {
    return new WireConfig(
      p.contains("wire_forceBindAddress") && p.getBoolean("wire_forceBindAddress"),
      p.getInt("pastry_socket_scm_max_open_sockets"),
      positive("pastry_socket_scm_socket_buffer_size", p.getInt("pastry_socket_scm_socket_buffer_size")),
      p.contains("transport_tcp_no_delay") && p.getBoolean("transport_tcp_no_delay"),
      positive("transport_wire_datagram_receive_buffer_size", p.getInt("transport_wire_datagram_receive_buffer_size")),
      positive("transport_wire_datagram_send_buffer_size", p.getInt("transport_wire_datagram_send_buffer_size")));
  }
}
