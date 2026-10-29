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
/**
 * The layered transport framework under Pastry. A
 * {@link org.mpisws.p2p.transport.TransportLayer} sends messages and opens
 * {@link org.mpisws.p2p.transport.P2PSocket}s to an {@code Identifier}; layers are stacked so that
 * each one adds its own header, translates the identifier type of the layer below (for example
 * {@code InetSocketAddress} to {@code SourceRoute}) and reports upwards through a
 * {@link org.mpisws.p2p.transport.TransportLayerCallback}. Outcomes are delivered to
 * {@link org.mpisws.p2p.transport.MessageCallback} and {@link org.mpisws.p2p.transport.SocketCallback};
 * options in a {@code Map<String, Object>} are passed down and applied by the layers that understand
 * them. The subpackages contain the individual layers.
 * <p>
 * Threading: layers are driven by the selector thread; socket IO is selected with
 * {@link org.mpisws.p2p.transport.P2PSocket#register}, and callbacks, including those of a request that
 * fails immediately, can run before the requesting method returns.
 */
package org.mpisws.p2p.transport;
