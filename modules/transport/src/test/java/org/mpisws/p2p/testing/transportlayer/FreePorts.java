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
package org.mpisws.p2p.testing.transportlayer;

import java.io.Closeable;
import java.io.IOException;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * Finds ports for tests that bind a WireTransportLayer. That layer binds UDP (without SO_REUSEADDR) and TCP to the
 * same port and uses it as its identifier, so it cannot bind port 0; and a port that {@code new ServerSocket(0)}
 * reports free for TCP may still be taken for UDP.
 */
public final class FreePorts {
  static final int ATTEMPTS = 100;

  private FreePorts() {}

  /** Returns a port on the address that both a UDP and a TCP socket could just bind. */
  public static int freePort(InetAddress address) throws IOException {
    return freePorts(address, 1)[0];
  }

  /** Returns count distinct ports on the address that both a UDP and a TCP socket could just bind. */
  public static int[] freePorts(InetAddress address, int count) throws IOException {
    List<Closeable> held = new ArrayList<Closeable>();
    try {
      int[] ports = new int[count];
      for (int i = 0; i < count; i++) {
        ports[i] = holdFreePort(address, held);
      }
      return ports;
    } finally {
      for (Closeable closeable : held) closeable.close();
    }
  }

  /** Binds a UDP and a TCP socket to the same ephemeral port, adds both to held and returns the port. */
  private static int holdFreePort(InetAddress address, List<Closeable> held) throws IOException {
    BindException last = null;
    for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
      DatagramSocket udp = new DatagramSocket(null);
      udp.setReuseAddress(false);
      udp.bind(new InetSocketAddress(address, 0));
      int port = udp.getLocalPort();
      ServerSocket tcp = new ServerSocket();
      try {
        tcp.setReuseAddress(true);
        tcp.bind(new InetSocketAddress(address, port));
        held.add(udp);
        held.add(tcp);
        return port;
      } catch (BindException e) {
        last = e;
        tcp.close();
        udp.close();
      }
    }
    throw last;
  }
}
