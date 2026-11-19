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
package org.mpisws.p2p.transport.rendezvous;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import rice.Continuation;

/**
 * The trick here is that this layer is at some level, say InetSocketAddress, but must pass around very High-Level
 * Identifiers, such as a NodeHandle for the rendezvous strategy to do its job, but maybe this can just be the
 * RendezvousContact, and it can be casted.
 *
 * protocol:
 * byte CONNECTOR_SOCKET
 *   HighIdentifier target = serializer.deserialize(sib);
 *   HighIdentifier opener = serializer.deserialize(sib);
 *   int uid = sib.readInt();
 *
 * byte ACCEPTOR_SOCKET
 *   HighIdentifier target = serializer.deserialize(sib);
 *   HighIdentifier opener = serializer.deserialize(sib);
 *   int uid = sib.readInt();
 *
 * UDP: We send these messages over TCP/routing unless we believe the Firewall has a temporarily open port
 * to us (see the following)
 *
 * The firewall will open the port for a few seconds after a message is sent.  However it uses an
 * ephemeral port.  So, when we receive a message from a new port, we don't know who it's coming from yet,
 * and if it is a ping and we send back a pong, the higher layer will try to send it to the outer-most
 * addr, but this won't work if the node is NATted and didn't configure forwarding.
 *
 *  Ex:
 *  Nancy is NATted and her external address:port is x:0 because there is no forwarding.
 *
 *  Here's what this layer sees
 *  x:5000 -> Alice : UDP // we don't know who x:5000 is, and we don't know what kind of message it is
 *  Alice -> x:0 : Pong // we need to translate x:0 into x:5000
 *
 *  To accomplish this we'll use a series of Hash tables, and also tag the incoming addr/port on the options
 *  of incoming UDP packets.  When Liveness or Identity makes an immediate response to
 *  UDP packets we will get our tag back, but when normal traffic is sent we need to remember the mapping.
 *
 *  Note that there may be several NATted nodes behind the same firewall who all advertise the same bogus
 *  port of x:0, so we can't use the external port as a key in the table.
 *
 *  We can assume that every sendMessage either has our tag, or a highIdentifier in the tag (set by lower identity), or both
 *  If there is both then we'll make a binding of the tag, and the highIdentifier to the port
 *  If there is only a tag, then we'll send it to the tag
 *  If there is only a highIdentifier, then we may be forced to TCP/route
 *
 *  Also, we need to remember how recently we got a UDP from the node, and if it's been too long, assume that the
 *  hole has closed, and we need to shift back to TCP (much slower)
 *
 * @author Jeff Hoye
 */


/**
 * Writes a byte then notifies the continuation.
 * @author Jeff Hoye
 */
class ByteWriter<Identifier> implements P2PSocketReceiver<Identifier> {
  ByteBuffer bytesToWrite;
  Continuation<P2PSocket<Identifier>, Exception> callMeWhenDone;

  public ByteWriter(byte writeMe, Continuation<P2PSocket<Identifier>, Exception> callMeWhenDone) {
    this(makeByteArray(writeMe), callMeWhenDone);
  }

  public ByteWriter(byte[] writeMe, Continuation<P2PSocket<Identifier>, Exception> callMeWhenDone) {
    this.bytesToWrite = ByteBuffer.wrap(writeMe);
    this.callMeWhenDone = callMeWhenDone;
  }

  @Override
  public void receiveSelectResult(P2PSocket<Identifier> socket,
      boolean canRead, boolean canWrite) throws IOException {
    long bytesWritten = socket.write(bytesToWrite);
    if (bytesWritten < 0) {
      socket.close();
      callMeWhenDone.receiveException(new ClosedChannelException("Socket "+socket+" closed."));
      return;
    }
    if (bytesToWrite.hasRemaining()) {
      socket.register(false, true, this);
      return;
    }

    // done, call the continuation
    callMeWhenDone.receiveResult(socket);
  }

  @Override
  public void receiveException(P2PSocket<Identifier> socket,
      Exception ioe) {
    socket.close();
    callMeWhenDone.receiveException(ioe);
  }

  private static byte[] makeByteArray(byte writeMe) {
    byte[] foo = new byte[1];
    foo[0] = writeMe;
    return foo;
  }
}
