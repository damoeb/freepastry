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

import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.util.InsufficientBytesException;
import org.mpisws.p2p.transport.util.SocketInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

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

import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.PILOT_PING;
import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.PILOT_PONG_BYTES;
import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.PILOT_REQUEST;

class IncomingPilot<Identifier, HighIdentifier extends RendezvousContact> extends AbstractPilot<Identifier, HighIdentifier> {
  /**
   * Used to read the initial connection information, then re-constructed each time to read pings.
   * Always ready to read the pings.
   */
  IncomingPilot(RendezvousTransportLayerImpl<Identifier, HighIdentifier> layer, P2PSocket<Identifier> socket) throws IOException {
    super(layer);
    this.socket = socket;
    sib = new SocketInputBuffer(socket);
    receiveSelectResult(socket, true, true);
  }

  protected void requestSocket(HighIdentifier requestor, int uid) throws IOException {
    logger.trace("Requesting socket from: {}requestor:{} uid:{}", i, requestor, uid);

    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    sob.writeByte(PILOT_REQUEST);
    serializer.serialize(requestor,sob);
    sob.writeInt(uid);

    enqueue(sob.getByteBuffer());
  }

  @Override
  protected void read() throws IOException {
    if (i == null) {
      // only do this the first time
      try {
        i = serializer.deserialize(socketInputBuffer());
        logger.trace("Received incoming Pilot from {}", i);
      } catch (InsufficientBytesException ibe) {
        socket().register(true, false, this);
        return;
      }
      socketInputBuffer().clear();
      incomingPilots.put(i,this);
      layer.notifyIncomingPilotAdded(i);

      // NOTE, it's not important to put a return here, because maybe the node sent a ping while waiting for this step,
      // just rely on the recovery to properly re-register this
    }

    try {
      byte msgType = socketInputBuffer().readByte();
      switch(msgType) {
      case PILOT_PING:
        logger.trace("{} received ping", this);
        socketInputBuffer().clear();
        enqueue(ByteBuffer.wrap(PILOT_PONG_BYTES));
        read();  // read the next thing, or re-register if there isn't enough to read
        break;
      }
    } catch (InsufficientBytesException ibe) {
      socket().register(true, false, this);
      return;
    } catch (IOException ioe) {
      cancel();
    }
  }

  @Override
  public boolean cancel() {
    return super.cancel();
  }

  @Override
  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
    if (i != null) {
      logger.trace("Shutdown of incoming pilot {}", socket);
      IncomingPilot<Identifier, HighIdentifier> pilot = incomingPilots.remove(i);
      if (pilot != null) layer.notifyIncomingPilotRemoved(i);
    }
    socket.close();
  }

  @Override
  public void run() {
    // nothing for now, not scheduled


  }
}
