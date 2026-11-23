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

import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.ACCEPTOR_SOCKET;
import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.CONNECTION_RESPONSE_FAILURE;
import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.CONNECTION_RESPONSE_SUCCESS;
import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.CONNECTOR_SOCKET;
import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.NORMAL_SOCKET;
import static org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl.PILOT_SOCKET;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.sourceroute.Forwarder;
import org.mpisws.p2p.transport.util.InsufficientBytesException;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.mpisws.p2p.transport.util.SocketInputBuffer;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.slf4j.Logger;
import rice.Continuation;
import rice.p2p.util.tuples.Tuple;

/**
 * What a rendezvous node, or any node, does with a socket that arrives from the lower layer: the first byte says
 * whether it is a normal socket, a connector or an acceptor (the headers of a rendezvous request) or a pilot. A
 * connector is kept until its acceptor comes, an acceptor is matched with its connector and both are forwarded to each
 * other, or, if the local node asked for it, the acceptor is delivered to whoever asked.
 *
 * protocol: see {@link RendezvousTransportLayerImpl}. Runs on the selector thread.
 */
class IncomingSocketHandler<Identifier, HighIdentifier extends RendezvousContact> {
  private final RendezvousTransportLayerImpl<Identifier, HighIdentifier> layer;
  private final Logger logger;
  private final ContactDeserializer<Identifier, HighIdentifier> serializer;
  private final HighIdentifier localNodeHandle;
  private final RendezvousStrategy<HighIdentifier> rendezvousStrategy;
  private final Map<HighIdentifier, IncomingPilot<Identifier, HighIdentifier>> incomingPilots;
  private final WaitingConnectors<Identifier, HighIdentifier> waitingConnectors;
  private final ExpectedIncomingSockets<Identifier, HighIdentifier> expectedIncomingSockets;

  IncomingSocketHandler(RendezvousTransportLayerImpl<Identifier, HighIdentifier> layer) {
    this.layer = layer;
    this.logger = layer.logger;
    this.serializer = layer.serializer;
    this.localNodeHandle = layer.localNodeHandle;
    this.rendezvousStrategy = layer.rendezvousStrategy;
    this.incomingPilots = layer.incomingPilots;
    this.waitingConnectors = layer.waitingConnectors;
    this.expectedIncomingSockets = layer.expectedIncomingSockets;
  }

  void incomingSocket(P2PSocket<Identifier> incomingSocket) throws IOException {
    logger.trace("incomingSocket({})", incomingSocket);

    new P2PSocketReceiver<Identifier>() {

      @Override
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
        logger.trace("incomingSocket({}).rSR({},{})", socket, canRead, canWrite);
        // read byte, switch on it
        ByteBuffer buf = ByteBuffer.allocate(1);
        long bytesRead = socket.read(buf);

        if (bytesRead == 0) {
          // try again
          socket.register(true, false, this);
          return;
        }

        if (bytesRead < 0) {
          // input was closed
          socket.close();
          return;
        }

        // could check that bytesRead == 1, but we know it is
        buf.flip();
        byte socketType = buf.get();
        switch(socketType) {
        case NORMAL_SOCKET:
          logger.trace("incomingSocket({}).rSR({},{}):NORMAL", socket, canRead, canWrite);
          layer.callback.incomingSocket(socket);
          return;
        case CONNECTOR_SOCKET:
          readConnectHeader(socket);
          return;
        case ACCEPTOR_SOCKET:
          readAcceptHeader(socket);
          return;
        case PILOT_SOCKET:
          new IncomingPilot<>(layer, socket);
          return;
        }
      }

      @Override
      public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {

      }
    }.receiveSelectResult(incomingSocket, true, false);
  }

  private void readConnectHeader(P2PSocket<Identifier> socket) throws IOException {
    logger.trace("readConnectHeader({})", socket);
    final SocketInputBuffer sib = new SocketInputBuffer(socket);
    P2PSocketReceiver<Identifier> receiver = new P2PSocketReceiver<Identifier>() {

      @Override
      public void receiveSelectResult(P2PSocket<Identifier> socket,
          boolean canRead, boolean canWrite) throws IOException {
        // a header split across reads throws InsufficientBytesException: sib keeps what it read and rewinds, and we wait for more

        try {
          HighIdentifier target = serializer.deserialize(sib);
          HighIdentifier opener = serializer.deserialize(sib);
          int uid = sib.readInt();

          logger.trace("readConnectHeader({},{},{},{})", socket, target, opener, uid);
          if (!waitingConnectors.put(opener, target, uid, socket)) return;

          if (incomingPilots.containsKey(target)) {
            logger.trace("I'm the rendezvous for {} to {} and I have a pilot.", opener, target);
            IncomingPilot<Identifier, HighIdentifier> pilot = incomingPilots.get(target);
            pilot.requestSocket(opener,uid);
          } else {
            logger.info("I'm the rendezvous for {} to {} and I don't have a pilot.", opener, target);
            rendezvousStrategy.openChannel(target, localNodeHandle, opener, uid, null, socket.getOptions());
          }
        } catch (InsufficientBytesException ibe) {
          socket.register(true, false, this);
        }
      }

      @Override
      public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
        // what to do here?  close the socket?
        logger.warn("error in readConnectHeader({}) closing.", socket, ioe);
        socket.close();
      }
    };

    receiver.receiveSelectResult(socket, true, false);
  }

  private void readAcceptHeader(P2PSocket<Identifier> acceptorSocket) throws IOException {
    logger.trace("readAcceptHeader({})", acceptorSocket);
    final SocketInputBuffer sib = new SocketInputBuffer(acceptorSocket);
    P2PSocketReceiver<Identifier> receiver = new P2PSocketReceiver<Identifier>() {

      @Override
      public void receiveSelectResult(final P2PSocket<Identifier> acceptorSocket,
          boolean canRead, boolean canWrite) throws IOException {
        // a header split across reads throws InsufficientBytesException: sib keeps what it read and rewinds, and we wait for more

        try {
          final HighIdentifier target = serializer.deserialize(sib);
          final HighIdentifier opener = serializer.deserialize(sib);
          final int uid = sib.readInt();

          if (opener.equals(localNodeHandle)) {
            // I requested this, look up in expectedIncomingSockets
            final Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>> deliverSocketToMe = expectedIncomingSockets.remove(target, uid);
            if (deliverSocketToMe == null) {
              logger.warn("Got accept socket to me, that I'm not expecting: t:{} o:{} uid:{} {}", target, opener, uid, acceptorSocket);
              new ByteWriter<>(CONNECTION_RESPONSE_FAILURE,new Continuation<P2PSocket<Identifier>, Exception>(){

                @Override
                public void receiveException(Exception exception) {
                  // just ignore this, it's not important
                }

                @Override
                public void receiveResult(P2PSocket<Identifier> result) {
                  result.close();
                }}).receiveSelectResult(acceptorSocket, false, true);
              // send failure
              return;
            }

            new ByteWriter<>(CONNECTION_RESPONSE_SUCCESS,new Continuation<P2PSocket<Identifier>, Exception>() {
              @Override
              public void receiveException(Exception exception) {
                deliverSocketToMe.first().receiveException(deliverSocketToMe.second(), exception);
              }

              @Override
              public void receiveResult(P2PSocket<Identifier> result) {
                // need to return a wrapper with the proper options from deliverSocketToMe.b().getOptions(), do we need to merge these?  Probably
                deliverSocketToMe.first().receiveResult(deliverSocketToMe.second(), new SocketWrapperSocket<Identifier, Identifier>(result.getIdentifier(),result,logger,layer.errorHandler,OptionsFactory.merge(deliverSocketToMe.second().getOptions(),result.getOptions())));
              }}).receiveSelectResult(acceptorSocket, false, true);
            return;
          }

          logger.trace("readAcceptHeader({},{},{},{})", acceptorSocket, target, opener, uid);

          // I'm just the middleman, look up the value in connectSockets

          final P2PSocket<Identifier> connectorSocket = waitingConnectors.remove(opener, target, uid);

          if (connectorSocket == null) {
            logger.debug("writing failed bytes in readAcceptHeader({},{},{},{})", acceptorSocket, target, opener, uid);
            // write failed to socket
            P2PSocketReceiver<Identifier> acceptorFailed = new ByteWriter<>(CONNECTION_RESPONSE_FAILURE,
                new Continuation<P2PSocket<Identifier>, Exception>() {
                  @Override
                  public void receiveResult(P2PSocket<Identifier> result) {
                    // send the failure, then close
                    result.close();
                  }
                  @Override
                  public void receiveException(Exception exception) {
                    logger.warn("Error writing failed bytes in readAcceptHeader({},{},{},{})", acceptorSocket, target, opener, uid, exception);
                    acceptorSocket.close();
                  }
                });
            acceptorFailed.receiveSelectResult(acceptorSocket, false, true);
          } else {
            // when both sockets set themselves in this structure, then we can begin forwarding.
            class ForwardSockets {
              @Nullable P2PSocket<Identifier> connector;
              @Nullable P2PSocket<Identifier> acceptor;
            }
            final ForwardSockets forwardSockets = new ForwardSockets();

            logger.trace("writing success bytes in readAcceptHeader({},{},{},{})", acceptorSocket, target, opener, uid);
            // write success to connectorSocket/acceptorSocket
            // when both writes succeed, bridge them
            P2PSocketReceiver<Identifier> connectorSuccess = new ByteWriter<>(CONNECTION_RESPONSE_SUCCESS,
                new Continuation<P2PSocket<Identifier>, Exception>(){
              @Override
              public void receiveResult(P2PSocket<Identifier> result) {
                // done, set up the forwarder
                logger.trace("Connector socket complete, setting up forwarding. readAcceptHeader({},{},{},{})", acceptorSocket, target, opener, uid);

                forwardSockets.connector = result;
                if (forwardSockets.acceptor != null) createForwarder(forwardSockets.connector,forwardSockets.acceptor,opener,target,uid);
              }
              @Override
              public void receiveException(Exception exception) {
                logger.warn("Error writing failed bytes in readAcceptHeader({},{},{},{})", acceptorSocket, target, opener, uid, exception);
                // the connector is automatically closed
                acceptorSocket.close();
              }
            });
            connectorSuccess.receiveSelectResult(connectorSocket, false, true);

            P2PSocketReceiver<Identifier> acceptorSuccess = new ByteWriter<>(CONNECTION_RESPONSE_SUCCESS,
                new Continuation<P2PSocket<Identifier>, Exception>(){
              @Override
              public void receiveResult(P2PSocket<Identifier> result) {
                // done, set up the forwarder
                logger.trace("Acceptor socket complete, setting up forwarding. readAcceptHeader({},{},{},{})", acceptorSocket, target, opener, uid);

                forwardSockets.acceptor = result;
                if (forwardSockets.connector != null) createForwarder(forwardSockets.connector, forwardSockets.acceptor,opener,target,uid);
              }
              @Override
              public void receiveException(Exception exception) {
                logger.warn("Error writing failed bytes in readAcceptHeader({},{},{},{})", acceptorSocket, target, opener, uid, exception);
                // the connector is automatically closed
                connectorSocket.close();
              }
            });
            acceptorSuccess.receiveSelectResult(acceptorSocket, false, true);

          }
        } catch (InsufficientBytesException ibe) {
          acceptorSocket.register(true, false, this);
        }
      }

      @Override
      public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
        // what to do here?  close the socket?
        logger.warn("error in readConnectHeader({}) closing.", socket, ioe);
        socket.close();
      }
    };

    receiver.receiveSelectResult(acceptorSocket, true, false);
  }

  private void createForwarder(P2PSocket<Identifier> socket, P2PSocket<Identifier> b, HighIdentifier connector, HighIdentifier acceptor, int uid) {
    logger.atDebug().log(() -> "createForwarder("+socket+","+b+","+connector+","+acceptor+","+uid+")");
    // a relayed socket lives as long as the connection it carries: the handshake deadline no longer applies
    socket.handshakeCompleted();
    b.handshakeCompleted();
    new Forwarder<Identifier>(null,socket,b,logger);
  }
}
