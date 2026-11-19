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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;

/**
 * We have to read the first byte and see if this is a
 * passthrough (the layer higher than us asked to open it) socket or a
 * primary (our layer tried to open it) socket, or a socket for BIG messages.
 */
final class IncomingSocketDispatcher<Identifier> implements P2PSocketReceiver<Identifier> {
  private final P2PSocket<Identifier> s;
  private final PeerMessageWriterContext<Identifier> ctx;

  IncomingSocketDispatcher(P2PSocket<Identifier> socket, PeerMessageWriterContext<Identifier> ctx) {
    this.s = socket;
    this.ctx = ctx;
  }

  @Override
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
    if (socket != s) throw new IllegalArgumentException("Sockets not equal!!! s:"+s+" socket:"+socket);
    if (canWrite || !canRead) throw new IllegalArgumentException("Should only be able to read! canRead:"+canRead+" canWrite:"+canWrite);
    // the first thing we need to do is to find out if this is a primary socket or a passthrough
    ByteBuffer hdr = ByteBuffer.allocate(1);
    int ret;
    try {
      ret = (int)socket.read(hdr);
    } catch (IOException ioe) {
      socket.close();
      return;
    }
    switch (ret) {
    case -1:
      // closed... strange
      socket.close();
      break;
    case 0:
      // reregister
      socket.register(true, false, this);
      break;
    case 1:
      // success
      hdr.flip();
      dispatch(hdr.get());
      break;
    default:
      //Whisky Tango Foxtrot?
      socket.close();
      throw new IllegalStateException("Read "+ret+" bytes?  Not good.  Expected to read 1 byte.");
    }
  }

  private void dispatch(byte val) throws IOException {
    switch(val) {
    case PriorityTransportLayer.PASSTHROUGH_SOCKET_B:
      ctx.callback.get().incomingSocket(s);
      break;
    case PriorityTransportLayer.PRIMARY_SOCKET_B:
      ctx.logger.debug("Opened Primary Socket from {}", s.getIdentifier());
      ctx.entityManagers.apply(s.getIdentifier()).primarySocketAvailable(s, null);
      break;
    case PriorityTransportLayer.BIG_MSG_SOCKET_B:
      ctx.logger.debug("Opened BIG Message Socket from {}", s.getIdentifier());
      ctx.entityManagers.apply(s.getIdentifier()).handleBigMsgSocket(s);
      break;
    default:
      ctx.logger.warn("Closing socket from {}, unknown socket header:{}", s.getIdentifier(), val);
      s.close();
      break;
    }
  }

  @Override
  public void receiveException(P2PSocket<Identifier> socket, Exception e) {
    ctx.errorHandler.get().receivedException(socket.getIdentifier(), e);
  }
}
