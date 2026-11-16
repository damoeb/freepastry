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
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;

/**
 * Opens our side of the primary socket to a node: once the layer below delivers the socket, writes the
 * {@link PriorityTransportLayer#PRIMARY_SOCKET} header and hands the socket to the node's
 * {@link EntityManager}. Failures go to the EntityManager too, which then retries or gives up.
 */
final class PrimarySocketOpener<Identifier> implements SocketCallback<Identifier> {
  private final Identifier i;

  /**
   * the receipt the EntityManager holds as its pendingSocket
   */
  private final SocketRequestHandleImpl<Identifier> handle;

  private final EntityManagerContext<Identifier> ctx;

  PrimarySocketOpener(Identifier i, SocketRequestHandleImpl<Identifier> handle, EntityManagerContext<Identifier> ctx) {
    this.i = i;
    this.handle = handle;
    this.ctx = ctx;
  }

  @Override
  public void receiveResult(SocketRequestHandle<Identifier> cancellable, final P2PSocket<Identifier> sock) {
    handle.setSubCancellable(() -> {
      sock.close();
      return true;
    });
    sock.register(false, true, new HeaderWriter());
  }

  @Override
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
    if (handle.getSubCancellable() != null && s != handle.getSubCancellable()) throw new IllegalArgumentException(
        "s != handle.getSubCancellable() must be a bug. s:"+
        s+" sub:"+handle.getSubCancellable());
    ctx.entityManagers.apply(s.getIdentifier()).receiveSocketException(handle, ex);
  }

  /**
   * Writes the primary socket header, then hands the socket over.
   */
  private final class HeaderWriter implements P2PSocketReceiver<Identifier> {
    ByteBuffer writeMe = ByteBuffer.wrap(PriorityTransportLayer.PRIMARY_SOCKET);

    @Override
    public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
      if (canRead || !canWrite) throw new IllegalArgumentException("expected to write!  canRead:"+canRead+" canWrite:"+canWrite);
      ctx.logger.debug("Opened Primary socket {} to {}", socket, i);
      if (socket.write(writeMe) == -1) {
        ctx.entityManagers.apply(i).stopLivenessChecker();
        ctx.entityManagers.apply(socket.getIdentifier()).receiveSocketException(handle, new org.mpisws.p2p.transport.ClosedChannelException("Channel closed while writing."));
        return;
      }
      if (writeMe.hasRemaining()) {
        socket.register(false, true, this);
      } else {
        ctx.entityManagers.apply(socket.getIdentifier()).primarySocketAvailable(socket, handle);
      }
    }

    @Override
    public void receiveException(P2PSocket<Identifier> socket, Exception e) {
      ctx.entityManagers.apply(socket.getIdentifier()).receiveSocketException(handle, e);
    }

    @Override
    public String toString() {
      return "PriorityTLi: Primary Socket shim to "+i;
    }
  }
}
