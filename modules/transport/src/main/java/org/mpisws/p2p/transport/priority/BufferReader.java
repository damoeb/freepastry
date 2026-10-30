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
import java.nio.channels.ClosedChannelException;

import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;

/**
 * Reads into the buf, or closes the socket.
 *
 * Together with {@link SizeReader} this reads the framed messages of a primary socket: a 4-byte size,
 * then the message, then the next size.
 *
 * @author Jeff Hoye
 */
class BufferReader<Identifier> implements P2PSocketReceiver<Identifier> {
  final EntityManager<Identifier> owner;
  final EntityManagerContext<Identifier> ctx;
  ByteBuffer buf;

  public BufferReader(EntityManager<Identifier> owner, int size, P2PSocket<Identifier> socket) {
    this.owner = owner;
    this.ctx = owner.context();
    buf = ByteBuffer.allocate(size);
    socket.register(true, false, this);
  }

  public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
    if (canWrite || !canRead) throw new IllegalStateException(owner+" Expected only to read. canRead:"+canRead+" canWrite:"+canWrite+" socket:"+socket);

    try {
      if (socket.read(buf) == -1) {
        owner.closeMe(socket);
        return;
      }
    } catch (IOException ioe) {
      receiveException(socket, ioe);
      return;
    }

    if(buf.remaining() == 0) {
      buf.flip();
      done(socket);
    } else {
      socket.register(true, false, this);
    }
  }

  public void receiveException(P2PSocket<Identifier> socket, Exception e) {

    if (e instanceof ClosedChannelException) {
      return;
    }

    boolean printError = true;

    if (e instanceof NodeIsFaultyException) {
      printError = false;
    }

    if (e instanceof IOException) {
      if (e.getMessage().equals("An established connection was aborted by the software in your host machine")) {
        printError = false;
      }
    }

    if (printError) {
      ctx.errorHandler.get().receivedException(socket.getIdentifier(), e);
    }
    owner.closeMe(socket);
  }

  public void done(P2PSocket<Identifier> socket) throws IOException {
    ctx.logger.debug("{} read message of size {} from {}", owner, buf.capacity(), socket);
    ctx.listeners.notifyRead(buf.capacity(), socket.getIdentifier(), socket.getOptions());
    ctx.callback.get().messageReceived(socket.getIdentifier(), buf, socket.getOptions());
    new SizeReader<Identifier>(owner, socket);
  }

  public String toString() {
    return "BufferReader{"+buf+"}";
  }
}
