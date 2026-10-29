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

package rice.pastry.dist;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.UnresolvedAddressException;
import java.util.*;

import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;
import rice.pastry.*;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.socket.nat.rendezvous.RendezvousSocketPastryNodeFactory;

/**
 * Static helper that creates a distributed (socket-based) pastry node factory
 * for a given protocol: {@link #PROTOCOL_SOCKET} or {@link #PROTOCOL_RENDEZVOUS}.
 *
 * @version $Id: DistPastryNodeFactory.java,v 1.8 2003/12/22 03:24:46 amislove
 *      Exp $
 * @author Alan Mislove
 */
public class DistPastryNodeFactory {

  // choices of protocols
  public static int PROTOCOL_SOCKET = 2;
  public static int PROTOCOL_RENDEZVOUS = 3;

  public static int PROTOCOL_DEFAULT = PROTOCOL_SOCKET;

  /**
   * Creates a distributed pastry node factory for the given protocol.
   *
   * @param nf the factory that generates the ids of new nodes
   * @param protocol the protocol to use ({@link #PROTOCOL_SOCKET} or {@link #PROTOCOL_RENDEZVOUS})
   * @param port the port number the first node is bound to
   * @param env the environment
   * @return a factory for the given protocol
   * @throws IllegalArgumentException if the protocol is not supported
   * @throws IOException if the factory cannot be created
   */
  public static SocketPastryNodeFactory getFactory(NodeIdFactory nf, int protocol, int port, Environment env) throws IOException {
    if (protocol == PROTOCOL_SOCKET) {
      return new SocketPastryNodeFactory(nf, port, env);
    }
    if (protocol == PROTOCOL_RENDEZVOUS) {
      return new RendezvousSocketPastryNodeFactory(nf, port, env, false);
    }
    
    throw new IllegalArgumentException("Unsupported Protocol " + protocol);
  }
}

