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
package org.mpisws.p2p.transport.sourceroute.factory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * Wire format: hop count (byte), then per hop the address length (byte, 4 or 16), the address bytes and the port
 * (unsigned short).
 */
public class WireSourceRoute extends SourceRoute<InetSocketAddress> {
  static final int IPV4_BYTES = 4;
  static final int IPV6_BYTES = 16;
  /** Bytes of a hop besides its address: the address-length byte (4 or 16) and the port. */
  static final int HOP_OVERHEAD_BYTES = Byte.BYTES + Short.BYTES;

  WireSourceRoute(InetSocketAddress local, InetSocketAddress remote) {
    super(local, remote);
  }

  WireSourceRoute(List<InetSocketAddress> path) {
    super(path);
  }

  WireSourceRoute(InetSocketAddress address) {
    super(address);
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte) path.size());
    for (InetSocketAddress address : path) {
      byte[] addressBytes = address.getAddress().getAddress();
      if (addressBytes.length != IPV4_BYTES && addressBytes.length != IPV6_BYTES) {
        throw new IOException("Incorrect number of bytes for IP address, expecting 4 or 16, got " + addressBytes.length);
      }
      buf.writeByte((byte) addressBytes.length);
      buf.write(addressBytes, 0, addressBytes.length);
      buf.writeShort((short) address.getPort());
    }
  }

  @Override
  public int getSerializedLength() {
    int ret = 1; // numhops

    for (InetSocketAddress address : path) {
      ret += HOP_OVERHEAD_BYTES + address.getAddress().getAddress().length;
    }

    return ret;
  }

  List<InetSocketAddress> getPath() {
    return path;
  }
}
