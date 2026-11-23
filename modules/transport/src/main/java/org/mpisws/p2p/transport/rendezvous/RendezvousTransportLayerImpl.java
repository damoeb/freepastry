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
import java.net.BindException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.sourceroute.Forwarder;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.InsufficientBytesException;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.mpisws.p2p.transport.util.SocketInputBuffer;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.p2p.util.tuples.Tuple;
import rice.selector.SelectorLoop;

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
public class RendezvousTransportLayerImpl<Identifier, HighIdentifier extends RendezvousContact> implements
    TransportLayer<Identifier, ByteBuffer>, TransportLayerCallback<Identifier, ByteBuffer>, PilotManager<HighIdentifier>,
    RendezvousTransportLayer<HighIdentifier> {

  public static final byte NORMAL_SOCKET = 0; // used when normally opening a channel (bypassing rendezvous)
  public static final byte CONNECTOR_SOCKET = 1; // sent to the rendezvous server
  public static final byte ACCEPTOR_SOCKET = 2; // used when openChannel() is called
  public static final byte PILOT_SOCKET = 3; // forms a pilot connection


  public static final byte CONNECTION_RESPONSE_FAILURE = 0; // forms a pilot connection
  public static final byte CONNECTION_RESPONSE_SUCCESS = 1; // forms a pilot connection

  // UDP Tagging
  public static final long NO_TAG = EphemeralIdentifierRegistry.NO_TAG;
  public static final String TAG_KEY = "RendezvousTransportLayer.UDP_TAG";
  /**
   * The message came from the overlay, rather than a lower layer
   */
  public static final String FROM_OVERLAY = "rendezvous.from_overlay";

  /**
   * Value should be a HighIdentifier
   */
  public static final String OPTION_USE_PILOT = "USE_PILOT";

  /**
   * options.get(RENDEZVOUS_CONTACT_STRING) returns a RendezvousContact
   */
  private final String RENDEZVOUS_CONTACT_STRING;  // usually: identity.node_handle_to_index

  private final TransportLayer<Identifier, ByteBuffer> transportLayer;
  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  private TransportLayerCallback<Identifier, ByteBuffer> callback;
  private final PilotFinder<HighIdentifier> pilotFinder;
  private final RendezvousStrategy<HighIdentifier> rendezvousStrategy;
  private final ResponseStrategy<Identifier> responseStrategy;
  final HighIdentifier localNodeHandle;
  final Logger logger;
  final ContactDeserializer<Identifier, HighIdentifier> serializer;
  final SelectorLoop selectorManager;
  private final RandomSource random;
  private final EphemeralIdentifierRegistry<Identifier, HighIdentifier> ephemeralDB;
  private final ContactDirectStrategy<HighIdentifier> contactDirectStrategy;
  private ErrorHandler<Identifier> errorHandler;

  public RendezvousTransportLayerImpl(
      TransportLayer<Identifier, ByteBuffer> transportLayer,
      String RENDEZVOUS_CONTACT_STRING,
      HighIdentifier myRendezvousContact,
      ContactDeserializer<Identifier, HighIdentifier> deserializer,
      RendezvousGenerationStrategy<HighIdentifier> rendezvousGenerator,
      PilotFinder<HighIdentifier> pilotFinder,
      RendezvousStrategy<HighIdentifier> rendezvousStrategy,
      ResponseStrategy<Identifier> responseStrategy,
      ContactDirectStrategy<HighIdentifier> contactDirectStrategy,
      Environment env) {
    this.random = env.getRandomSource();
    this.selectorManager = env.getSelectorManager();
    this.transportLayer = transportLayer;
    this.localNodeHandle = myRendezvousContact;
    this.serializer = deserializer;
    this.RENDEZVOUS_CONTACT_STRING = RENDEZVOUS_CONTACT_STRING;
    this.pilotFinder = pilotFinder;
    this.rendezvousStrategy = rendezvousStrategy;
    this.responseStrategy = responseStrategy;
    this.contactDirectStrategy = contactDirectStrategy;
    this.ephemeralDB = new EphemeralIdentifierRegistry<Identifier, HighIdentifier>(env,EphemeralIdentifierRegistry.DEFAULT_STALE_PORT_MILLIS); // TODO: make this a configurable parameter
    this.logger = env.getLogManager().getLogger(RendezvousTransportLayerImpl.class, null);
    this.errorHandler = new DefaultErrorHandler<Identifier>(logger);

    transportLayer.setCallback(this);
  }

  @Override
  public SocketRequestHandle<Identifier> openSocket(final Identifier identifier, final SocketCallback<Identifier> deliverSocketToMe, final @Nullable Map<String, Object> options) {
    logger.trace("openSocket({},{},{})", identifier, deliverSocketToMe, options);

    final SocketRequestHandle<Identifier> handle = new SocketRequestHandleImpl<Identifier>(identifier,options,logger);

    // no options, or no contact in them, means the caller addresses the lower layer directly (as sendMessage does)
    final HighIdentifier contact = getHighIdentifier(options);

    if (contact == null || contact.canContactDirect() || contactDirectStrategy.canContactDirect(contact)) {
      if (logger.isTraceEnabled()) {
        String text = "null";
        if (contact != null) {
          text = contact+" strat:"+contactDirectStrategy.canContactDirect(contact);
        }
        logger.trace("openSocket("+identifier+","+deliverSocketToMe+","+options+") contact:"+text);
      }

      // write NORMAL_SOCKET and continue
      transportLayer.openSocket(identifier, new SocketCallback<Identifier>(){
        @Override
        public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> openedSocket) {
          openedSocket.register(false, true, new ByteWriter<>(NORMAL_SOCKET, new Continuation<P2PSocket<Identifier>, Exception>() {
            @Override
            public void receiveResult(P2PSocket<Identifier> socket) {
              deliverSocketToMe.receiveResult(handle, socket);
            }

            @Override
            public void receiveException(Exception exception) {
              deliverSocketToMe.receiveException(handle, exception);
            }
          }));
        }

        @Override
        public void receiveException(SocketRequestHandle<Identifier> request, Exception ex) {
          deliverSocketToMe.receiveException(handle, ex);
        }
      }, options);
      return handle;
    } else {
      if (options != null && options.containsKey(OPTION_USE_PILOT)) {
        HighIdentifier middleMan = (HighIdentifier)options.get(OPTION_USE_PILOT);
        // this is normally used when a node is joining, wo you can't route to
        logger.trace("Opening socket to {} OPTION_USE_PILOT->{}", contact, middleMan);
        openSocketViaPilot(contact, middleMan, handle, deliverSocketToMe, options);
        return handle;
      } else {
        if (localNodeHandle.canContactDirect()) {
          // see if the node already has a pilot to me
          if (openSocketUsingPilotToMe(contact, handle, deliverSocketToMe, options)) return handle;
          // see if the node should have a pilot to a node I know
          if (openSocketUsingPilotFinder(contact, handle, deliverSocketToMe, options)) return handle;
          openSocketUsingRouting(contact, handle, deliverSocketToMe, options);
          return handle;
        } else {
          if (openSocketUsingPilotFinder(contact, handle, deliverSocketToMe, options)) return handle;

          // pick random outgoing pilot, and openConnection via him
          ArrayList<HighIdentifier> myPilots = new ArrayList<HighIdentifier>(outgoingPilots.keySet());
          if (myPilots.isEmpty()) deliverSocketToMe.receiveException(handle, new IllegalStateException("No available outgoing pilots."));
          HighIdentifier middleMan = myPilots.get(random.nextInt(myPilots.size()));
          openSocketViaPilot(contact, middleMan, handle, deliverSocketToMe, options);
          return handle;
        }
      }
    }
  }

  private void openSocketUsingRouting(final HighIdentifier contact,
      final SocketRequestHandle<Identifier> handle,
      final SocketCallback<Identifier> deliverSocketToMe,
      final @Nullable Map<String, Object> options) {
    // route to the node to open the socket to me
    logger.trace("opening a socket using routing to {}", contact);

    final int uid = random.nextInt();
    putExpectedIncomingSocket(contact, uid, deliverSocketToMe, handle);

    rendezvousStrategy.openChannel(contact, localNodeHandle, localNodeHandle, uid, new Continuation<Integer, Exception>() {

      @Override
      public void receiveResult(Integer result) {
        // don't need to do anything
      }

      @Override
      public void receiveException(Exception exception) {
        logger.info("openSocket({},{},{})", contact, deliverSocketToMe, options, exception);
        deliverSocketToMe.receiveException(handle, exception);
      }
    }, options);
  }

  private boolean openSocketUsingPilotFinder(HighIdentifier contact,
      SocketRequestHandle<Identifier> handle,
      SocketCallback<Identifier> deliverSocketToMe, @Nullable Map<String, Object> options) {
    HighIdentifier middleMan = pilotFinder.findPilot(contact);
    if (middleMan == null) {
      return false;
    } else {
      // use middleman
      logger.trace("opening a socket to {} via {}", contact, middleMan);
      openSocketViaPilot(contact, middleMan, handle, deliverSocketToMe, options);
      return true;
    }
  }

  /**
   * Return true there was a pilot to me.
   */
  protected boolean openSocketUsingPilotToMe(HighIdentifier contact,
      SocketRequestHandle<Identifier> handle,
      SocketCallback<Identifier> deliverSocketToMe, @Nullable Map<String, Object> options) {
    int uid = random.nextInt();
    putExpectedIncomingSocket(contact, uid, deliverSocketToMe, handle);

    if (incomingPilots.containsKey(contact)) {
      // use the pilot if possible
      logger.debug("Opening socket to firewalled node that I have a pilot to: {} uid:{}", contact, uid);
      try {
        incomingPilots.get(contact).requestSocket(localNodeHandle, uid);
      } catch (IOException ioe) {
        removeExpectedIncomingSocket(contact, uid);
        deliverSocketToMe.receiveException(handle, ioe);
      }
      return true;
    }
    return false;
  }

  protected void openSocketViaPilot(
      final HighIdentifier destination,
      final HighIdentifier middleMan,
      final SocketRequestHandle<Identifier> handle,
      final SocketCallback<Identifier> deliverSocketToMe,
      final @Nullable Map<String, Object> options) {

    if (middleMan.equals(localNodeHandle)) {
      throw new IllegalArgumentException("openSocketViaPilot("+destination+","+middleMan+","+handle+","+deliverSocketToMe+","+options+") can't use self as rendezvous.");
    }

    final int uid = random.nextInt();
    logger.atDebug().log(() -> "openSocketViaPilot<"+uid+">("+destination+","+middleMan+","+handle+","+deliverSocketToMe+","+options+")");

    // build header
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      sob.writeByte(CONNECTOR_SOCKET);
      serializer.serialize(destination, sob);
      serializer.serialize(localNodeHandle, sob);
      sob.writeInt(uid);
    } catch (IOException ioe) {
      deliverSocketToMe.receiveException(handle, ioe);
    }

    final ByteBuffer writeBuffer = sob.getByteBuffer(); // to write all the connection info
    final ByteBuffer readBuffer = ByteBuffer.allocate(1);  // to read success

    // open the socket
    transportLayer.openSocket(serializer.convert(middleMan), new SocketCallback<Identifier>() {
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> openedSocket) {

        try {
          new P2PSocketReceiver<Identifier>() {

            @Override
            public void receiveSelectResult(P2PSocket<Identifier> socket,
                boolean canRead, boolean canWrite) throws IOException {
              if (writeBuffer.hasRemaining()) {
                // write the header
                long bytesWritten = socket.write(writeBuffer);
                if (bytesWritten < 0) {
                  deliverSocketToMe.receiveException(handle, new ClosedChannelException("Channel closed detected to <"+uid+"> "+destination+" via "+middleMan+" in "+RendezvousTransportLayerImpl.this));
                  return;
                }
                if (writeBuffer.hasRemaining()) {
                  socket.register(false, true, this);
                  return;
                }
              }
              if (!writeBuffer.hasRemaining()) {
                // read for the response
                if (readBuffer.hasRemaining()) {
                  long bytesRead = socket.read(readBuffer);
                  if (bytesRead < 0) {
                    deliverSocketToMe.receiveException(handle, new ClosedChannelException("Channel closed detected to <"+uid+"> "+destination+" via "+middleMan+" in "+RendezvousTransportLayerImpl.this));
                    return;
                  }
                  if (readBuffer.hasRemaining()) {
                    socket.register(true, false, this);
                    return;
                  }
                }

                // interpret the response
                readBuffer.flip();
                byte response = readBuffer.get();
                switch(response) {
                case CONNECTION_RESPONSE_SUCCESS:
                  logger.atDebug().log(() -> "success in openSocketViaPilot<"+uid+">("+destination+","+middleMan+","+handle+","+deliverSocketToMe+","+options+")");
                  deliverSocketToMe.receiveResult(handle, socket);
                  return;
                default:
                  deliverSocketToMe.receiveException(handle, new ClosedChannelException("Failed to connect to <"+uid+"> "+destination+" via "+middleMan+" in "+RendezvousTransportLayerImpl.this+" response:"+response));
                  return;
                }
              }
            }

            @Override
            public void receiveException(P2PSocket<Identifier> socket,
                Exception ioe) {
              deliverSocketToMe.receiveException(handle, ioe);
            }
          }.receiveSelectResult(openedSocket, false, true);
        } catch (IOException ioe) {
          deliverSocketToMe.receiveException(handle, ioe);
        }
      }
      @Override
      public void receiveException(SocketRequestHandle<Identifier> request,
          Exception ex) {
        deliverSocketToMe.receiveException(handle, ex);
      }
    }, options);
  }

  @Override
  public void incomingSocket(P2PSocket<Identifier> incomingSocket) throws IOException {
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
          callback.incomingSocket(socket);
          return;
        case CONNECTOR_SOCKET:
          readConnectHeader(socket);
          return;
        case ACCEPTOR_SOCKET:
          readAcceptHeader(socket);
          return;
        case PILOT_SOCKET:
          new IncomingPilot<>(RendezvousTransportLayerImpl.this, socket);
          return;
        }
      }

      @Override
      public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {

      }
    }.receiveSelectResult(incomingSocket, true, false);
  }

  protected void readConnectHeader(P2PSocket<Identifier> socket) throws IOException {
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
          putConnectSocket(opener, target, uid, socket);

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

  protected void readAcceptHeader(P2PSocket<Identifier> acceptorSocket) throws IOException {
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
            final Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>> deliverSocketToMe = removeExpectedIncomingSocket(target, uid);
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
                deliverSocketToMe.first().receiveResult(deliverSocketToMe.second(), new SocketWrapperSocket<Identifier, Identifier>(result.getIdentifier(),result,logger,errorHandler,OptionsFactory.merge(deliverSocketToMe.second().getOptions(),result.getOptions())));
              }}).receiveSelectResult(acceptorSocket, false, true);
            return;
          }

          logger.trace("readAcceptHeader({},{},{},{})", acceptorSocket, target, opener, uid);

          // I'm just the middleman, look up the value in connectSockets

          final P2PSocket<Identifier> connectorSocket = removeConnectSocket(opener, target, uid);

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

  Map<HighIdentifier, Map<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>>> expectedIncomingSockets =
    new HashMap<HighIdentifier, Map<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>>>();

  protected void putExpectedIncomingSocket(HighIdentifier contact, int uid,
      SocketCallback<Identifier> deliverSocketToMe, SocketRequestHandle<Identifier> requestHandle) {
    logger.atTrace().log(() -> "putExpectedIncomingSocket("+contact+"@"+System.identityHashCode(contact)+","+uid+","+deliverSocketToMe+","+requestHandle+")");
    Map<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>> socketsByUid = expectedIncomingSockets.get(contact);
    if (socketsByUid == null) {
      socketsByUid = new HashMap<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>>();
      expectedIncomingSockets.put(contact, socketsByUid);
    }

    if (socketsByUid.containsKey(uid)) {
      throw new IllegalStateException("putExpectedIncomingSockets("+contact+","+uid+","+deliverSocketToMe+") already contains "+socketsByUid.get(uid));
    }

    socketsByUid.put(uid, new Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>(deliverSocketToMe, requestHandle));
  }

  protected @Nullable Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>> removeExpectedIncomingSocket(HighIdentifier target, int uid) {
    logger.atTrace().log(() -> "removeExpectedIncomingSocket("+target+"@"+System.identityHashCode(target)+","+uid+")");

    Map<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>> socketsByUid = expectedIncomingSockets.get(target);
    if (socketsByUid == null) {
      if (logger.isTraceEnabled()) {
        String text = "";
        for (HighIdentifier highIdentifier : expectedIncomingSockets.keySet()) {
          text+=" "+highIdentifier;
        }
        logger.trace("removeExpectedIncomingSocket("+target+"@"+System.identityHashCode(target)+","+uid+") had no first level entry for target"+text);
      }
      return null;
    }
    Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>> ret = socketsByUid.get(uid);

    if (ret == null) {
      if (logger.isTraceEnabled()) {
          String s = "";
        for (Integer h : socketsByUid.keySet()) {
          s+=" "+h;
        }
        logger.trace("removeExpectedIncomingSocket("+target+"@"+System.identityHashCode(target)+","+uid+") had no uid for target"+s);
      }
    } else {
      socketsByUid.remove(uid);
    }
    if (socketsByUid.isEmpty()) expectedIncomingSockets.remove(target);

    return ret;
  }



  protected void createForwarder(P2PSocket<Identifier> socket, P2PSocket<Identifier> b, HighIdentifier connector, HighIdentifier acceptor, int uid) {
    logger.atDebug().log(() -> "createForwarder("+socket+","+b+","+connector+","+acceptor+","+uid+")");
    // a relayed socket lives as long as the connection it carries: the handshake deadline no longer applies
    socket.handshakeCompleted();
    b.handshakeCompleted();
    new Forwarder<Identifier>(null,socket,b,logger);
  }

  /**
   * requestor, target, uid -> socket
   */
  Map<HighIdentifier, Map<HighIdentifier, Map<Integer, P2PSocket<Identifier>>>> connectSockets =
    new HashMap<HighIdentifier, Map<HighIdentifier, Map<Integer, P2PSocket<Identifier>>>>();

  /**
   * This map stores the connect socket until the corresponding accept socket arrives
   */
  public void putConnectSocket(HighIdentifier requestor, HighIdentifier target, int uid, P2PSocket<Identifier> socket) {
    Map<HighIdentifier, Map<Integer, P2PSocket<Identifier>>> socketsByTarget = connectSockets.get(requestor);
    if (socketsByTarget == null) {
      socketsByTarget = new HashMap<HighIdentifier, Map<Integer, P2PSocket<Identifier>>>();
      connectSockets.put(requestor, socketsByTarget);
    }

    Map<Integer, P2PSocket<Identifier>> socketsByUid = socketsByTarget.get(target);
    if (socketsByUid == null) {
      socketsByUid = new HashMap<Integer, P2PSocket<Identifier>>();
      socketsByTarget.put(target, socketsByUid);
    }

    P2PSocket<Identifier> three = socketsByUid.get(uid);
    if (three != null) {
      // error, we have a problem, because there is already a socket registered here with the same uid!!!
      logger.warn("error in storeConnectSocket() there is already a connector with the same UID!!!, dropping the new one.  Old:{} new:{}", three, socket);
      socket.close();
      return;
    }

    socketsByUid.put(uid, socket);

    // TODO: connectSockets entries whose acceptor never connects are never removed; add a timeout
  }

  public @Nullable P2PSocket<Identifier> removeConnectSocket(HighIdentifier requestor, HighIdentifier target, int uid) {
    Map<HighIdentifier, Map<Integer, P2PSocket<Identifier>>> socketsByTarget = connectSockets.get(requestor);
    if (socketsByTarget == null) {
      return null;
    }

    Map<Integer, P2PSocket<Identifier>> socketsByUid = socketsByTarget.get(target);
    if (socketsByUid == null) {
      return null;
    }

    // clean this up
    P2PSocket<Identifier> three = socketsByUid.remove(uid);
    if (socketsByUid.isEmpty()) socketsByTarget.remove(target);
    if (socketsByTarget.isEmpty()) connectSockets.remove(requestor);

    return three;
  }

  @Override
  public void openChannel(HighIdentifier requestor, HighIdentifier middleMan, int uid) {
    logger.info("openChannel({},{},{})", requestor, middleMan, uid);
    openAcceptSocket(requestor, middleMan, uid);
  }

  /**
   * We are a firewalled node and got a connect request, now time to respond to it
   */
  protected void openAcceptSocket(final HighIdentifier requestor, final HighIdentifier middleMan, final int uid) {
    logger.trace("openAcceptSocket({},{},{})", requestor, middleMan, uid);
    // TODO: there is a case where the requestor can be contacted directly, in this case, just do that, but may have to change
    // some other parts of the code:
      // 1) Send message to middleman, rather than open socket
      // 2) Change Pilot Request to include the middleman and requestor, or, better yet, make PILOT_CONNECT_DIRECT
      // 3) Set accept the socket directly

    if (!middleMan.canContactDirect()) {
      throw new IllegalArgumentException("openAcceptSocket("+requestor+","+middleMan+","+uid+") middleMan is firewalled.");
    }


    // build header
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      sob.writeByte(ACCEPTOR_SOCKET);
      serializer.serialize(localNodeHandle, sob);
      serializer.serialize(requestor, sob);
      sob.writeInt(uid);
    } catch (IOException ioe) {
      logger.warn("Error serializing in openAcceptSocket({},{},{})", requestor, middleMan, uid, ioe);
      return;
    }

    final ByteBuffer writeBuffer = sob.getByteBuffer(); // to write all the connection info
    final ByteBuffer readBuffer = ByteBuffer.allocate(1);  // to read success


    transportLayer.openSocket(serializer.convert(middleMan), new SocketCallback<Identifier>() {
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> openedSocket) {
        try {
          new P2PSocketReceiver<Identifier>() {

            @Override
            public void receiveSelectResult(P2PSocket<Identifier> socket,
                boolean canRead, boolean canWrite) throws IOException {
              if (writeBuffer.hasRemaining()) {
                // write the header
                long bytesWritten = socket.write(writeBuffer);
                if (bytesWritten < 0) {
                  logger.warn("Channel closed in openAcceptSocket({},{},{})", requestor, middleMan, uid);
                  return;
                }
                if (writeBuffer.hasRemaining()) {
                  socket.register(false, true, this);
                  return;
                }
              }
              if (!writeBuffer.hasRemaining()) {
                // read for the response
                if (readBuffer.hasRemaining()) {
                  long bytesRead = socket.read(readBuffer);
                  if (bytesRead < 0) {
                    logger.warn("Channel closed in openAcceptSocket({},{},{})", requestor, middleMan, uid);
                    return;
                  }
                  if (readBuffer.hasRemaining()) {
                    socket.register(true, false, this);
                    return;
                  }
                }

                // interpret the response
                readBuffer.flip();
                byte response = readBuffer.get();
                switch(response) {
                case CONNECTION_RESPONSE_SUCCESS:
                  logger.trace("success in openAcceptSocket({},{},{})", requestor, middleMan, uid);
                  callback.incomingSocket(socket);
                  return;
                default:
                  logger.warn("Failed to connect in openAcceptSocket({},{},{})", requestor, middleMan, uid);
                  return;
                }
              }
            }

            @Override
            public void receiveException(P2PSocket<Identifier> s,
                Exception ex) {
              logger.warn("Failure opening socket in openAcceptSocket({},{},{})", requestor, middleMan, uid, ex);
            }
          }.receiveSelectResult(openedSocket, false, true);
        } catch (IOException ioe) {
          logger.warn("Exception in openAcceptSocket({},{},{})", requestor, middleMan, uid, ioe);
        }
      }

      @Override
      public void receiveException(SocketRequestHandle<Identifier> request,
          Exception ex) {
        if (ex instanceof BindException) {
          // Address already in use: no further information
          openAcceptSocket(requestor, middleMan, uid);
          return;
        }
        logger.warn("Failure opening socket in openAcceptSocket({},{},{})", requestor, middleMan, uid, ex);
      }
    }, OptionsFactory.addOption(null, RENDEZVOUS_CONTACT_STRING, requestor));

  }

  // ************************** UDP **********************
  /**
   * Usually called from the higher level app, who probably used routing to get the message here.
   */
  @Override
  public void messageReceivedFromOverlay(HighIdentifier highIdentifier, ByteBuffer msg, @Nullable Map<String, Object> options) throws IOException {
    logger.trace("messageReceivedFromOverlay({},{},{})", highIdentifier, msg, options);
    messageReceived(serializer.convert(highIdentifier),msg,OptionsFactory.addOption(options, FROM_OVERLAY, true));
  }

  @Override
  public void messageReceived(Identifier identifier, ByteBuffer msg, @Nullable Map<String, Object> options) throws IOException {
    logger.debug("messageReceived({},{},{})", identifier, msg, options);
    if (options != null && options.containsKey(FROM_OVERLAY) && ((Boolean)options.get(FROM_OVERLAY)) == true) {
      // do nothing
    } else {
      // this will be the ephemeral UDP port
      responseStrategy.messageReceived(identifier, msg, options);
      options = OptionsFactory.addOption(options, TAG_KEY, ephemeralDB.getTagForEphemeral(identifier));
    }
    callback.messageReceived(identifier, msg, options);
  }

  protected @Nullable HighIdentifier getHighIdentifier(@Nullable Map<String, Object> options) {
    if (options == null) return null;
    return (HighIdentifier)options.get(RENDEZVOUS_CONTACT_STRING);
  }

  protected long getTag(@Nullable Map<String, Object> options) {
    if (options == null) return NO_TAG;
    Object ret = options.get(TAG_KEY);
    if (ret == null) return NO_TAG;
    return ((Long)ret).longValue();
  }

  /**
   * What to do if firewalled?
   *   ConnectRequest UDP only?  For now always use UDP_AND_TCP
   */
  @Override
  public @Nullable MessageRequestHandle<Identifier, ByteBuffer> sendMessage(Identifier identifier, ByteBuffer m, final @Nullable MessageCallback<Identifier, ByteBuffer> deliverAckToMe, @Nullable Map<String, Object> options) {
    logger.trace("sendMessage({},{},{},{})", identifier, m, deliverAckToMe, options);

    HighIdentifier high = getHighIdentifier(options);
    if (high == null) {
      // try to use a tag
      long tag = getTag(options);
      if (tag != NO_TAG) {
        Identifier ephemeral = ephemeralDB.getEphemeral(tag, identifier);
        if (ephemeral != null) identifier = ephemeral;
      }

      responseStrategy.messageSent(identifier, m, options);
      return transportLayer.sendMessage(identifier, m, deliverAckToMe, options);
    } else {
      if (high.canContactDirect() || contactDirectStrategy.canContactDirect(high)) {
        // this is the typical case
        // pass-through, need to allow for null during bootstrap, we assume pass-through works
        responseStrategy.messageSent(identifier, m, options);
        return transportLayer.sendMessage(identifier, m, deliverAckToMe, options);
      } else {
        // rendezvous (we know it's a NATted node)

        // fetch the tag from the options
        long tag = getTag(options);

        // attempt to map from highIdentifier to tag
        if (tag != NO_TAG) {
          // we know high != null, and !high.canContactDirect()
          ephemeralDB.mapHighToTag(high, tag);
        }

        // see if we have a valid ephemeral addr:port for this NATted highIdentifier
        Identifier ephemeral = ephemeralDB.getEphemeral(high);

        if (ephemeral != null && responseStrategy.sendDirect(ephemeral, m, options)) {
          // send directly if the strategy allows for it
          logger.debug("Sending directly on ephemeral {} for {}", ephemeral, high);
          responseStrategy.messageSent(ephemeral, m, options);
          return transportLayer.sendMessage(ephemeral, m, deliverAckToMe, options);
        }
        logger.debug("Not sending directly on ephemeral {} for {}", ephemeral, high);

        final MessageRequestHandleImpl<Identifier, ByteBuffer> ret = new MessageRequestHandleImpl<Identifier, ByteBuffer>(identifier, m, options);
        MessageCallback<HighIdentifier, ByteBuffer> ack;
        if (deliverAckToMe == null) {
          ack = null;
        } else {
          ack = new MessageCallback<HighIdentifier, ByteBuffer>(){
            @Override
            public void ack(MessageRequestHandle<HighIdentifier, ByteBuffer> msg) {
              deliverAckToMe.ack(ret);
            }
            @Override
            public void sendFailed(MessageRequestHandle<HighIdentifier, ByteBuffer> msg, Exception reason) {
              deliverAckToMe.sendFailed(ret, reason);
            }
          };
        }

        // check to see if we have a pilot to the node, if so, use it
        if (incomingPilots.containsKey(high)) {
          options = OptionsFactory.addOption(options, OPTION_USE_PILOT, high);
        }

        ret.setSubCancellable(rendezvousStrategy.sendMessage(high, m, ack, options));
        return ret;
      }
    }
  }

  @Override
  public String toString() {
    return "RendezvousTL{"+localNodeHandle+"}";
  }

  @Override
  public void acceptMessages(boolean enabled) {
    transportLayer.acceptMessages(enabled);
  }
  @Override
  public void acceptSockets(boolean enabled) {
    transportLayer.acceptSockets(enabled);
  }
  @Override
  public Identifier getLocalIdentifier() {
    return transportLayer.getLocalIdentifier();
  }
  @Override
  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }
  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }
  @Override
  public void destroy() {
    transportLayer.destroy();
  }

  // *************Pilot Sockets (used to connect leafset members) ******************
  // *************** outgoing Pilots, only used by NATted nodes ********************
  final Map<HighIdentifier, OutgoingPilot<Identifier, HighIdentifier>> outgoingPilots =
    new HashMap<HighIdentifier, OutgoingPilot<Identifier, HighIdentifier>>();

  // listener
  final List<OutgoingPilotListener<HighIdentifier>> opListeners = new CopyOnWriteArrayList<OutgoingPilotListener<HighIdentifier>>();
  protected void notifyOutgoingPilotAdded(HighIdentifier highIdentifier) {
    // avoid cme
    for (OutgoingPilotListener<HighIdentifier> outgoingPilotListener : opListeners) {
      outgoingPilotListener.pilotOpening(highIdentifier);
    }
  }
  protected void notifyOutgoingPilotRemoved(HighIdentifier highIdentifier) {
    // avoid cme
    for (OutgoingPilotListener<HighIdentifier> outgoingPilotListener : opListeners) {
      outgoingPilotListener.pilotClosed(highIdentifier);
    }
  }

  @Override
  public void addOutgoingPilotListener(OutgoingPilotListener<HighIdentifier> listener) {
    opListeners.add(listener);
  }

  @Override
  public void removeOutgoingPilotListener(OutgoingPilotListener<HighIdentifier> listener) {
    opListeners.remove(listener);
  }

  /**
   * Only used by NATted node.
   *
   * Opens a pilot socket to a "lifeline" node.  These are usually nodes near the local node in the id space.
   */
  @Override
  public SocketRequestHandle<HighIdentifier> openPilot(final HighIdentifier highIdentifier,
      final Continuation<SocketRequestHandle<HighIdentifier>, Exception> deliverAckToMe) {
    logger.debug("openPilot({})", highIdentifier);
    if (!highIdentifier.canContactDirect()) throw new IllegalArgumentException("can't open pilot to natted node:"+highIdentifier);
    OutgoingPilot<Identifier, HighIdentifier> outgoingPilot;
    @Nullable Map<String, Object> options;

    // make sure we don't open multiple pilots
    synchronized(outgoingPilots) {
      if (outgoingPilots.containsKey(highIdentifier)) {
        logger.trace("openPilot({}) already had one.", highIdentifier);
        if (deliverAckToMe != null) deliverAckToMe.receiveResult(outgoingPilots.get(highIdentifier));
        return outgoingPilots.get(highIdentifier);
      }

      options = serializer.getOptions(highIdentifier);
      outgoingPilot = new OutgoingPilot<>(this, highIdentifier, options);
      outgoingPilots.put(highIdentifier, outgoingPilot);
    }
    outgoingPilot.init(); // don't want to do this while holding a lock

    notifyOutgoingPilotAdded(highIdentifier);

    final OutgoingPilot<Identifier, HighIdentifier> o = outgoingPilot;
    o.setCancellable(transportLayer.openSocket(serializer.convert(highIdentifier), new SocketCallback<Identifier>(){
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> socket) {
        o.setSocket(socket);
        if (deliverAckToMe != null) deliverAckToMe.receiveResult(o);
      }

      @Override
      public void receiveException(SocketRequestHandle<Identifier> request, Exception ex) {
        o.receiveException(ex);
        if (deliverAckToMe != null) deliverAckToMe.receiveException(ex);
      }
    }, options));

    return o;
  }

  @Override
  public void closePilot(HighIdentifier highIdentifier) {
    logger.debug("closePilot({})", highIdentifier);
    OutgoingPilot<Identifier, HighIdentifier> closeMe;
    synchronized(outgoingPilots) {
      closeMe = outgoingPilots.remove(highIdentifier);
    }

    // notifyOutgoingPilotRemoved(i) is called in cancel()
    if (closeMe != null) {
      closeMe.cancel();
    }
  }

  public static final byte PILOT_PING = 1;
  public static final byte PILOT_PONG = 2;
  public static final byte PILOT_REQUEST = 3;

  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] PILOT_PING_BYTES = {PILOT_PING};
  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] PILOT_PONG_BYTES = {PILOT_PONG};
  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] PILOT_SOCKET_BYTES = {PILOT_SOCKET};

  public static final int PILOT_PING_PERIOD = 5000;



  // ********* incoming Pilots, only used by non-NATted nodes *************
  final Map<HighIdentifier, IncomingPilot<Identifier, HighIdentifier>> incomingPilots = new HashMap<HighIdentifier, IncomingPilot<Identifier, HighIdentifier>>();

  // listener
  final List<IncomingPilotListener<HighIdentifier>> ipListeners = new CopyOnWriteArrayList<IncomingPilotListener<HighIdentifier>>();
  protected void notifyIncomingPilotAdded(HighIdentifier highIdentifier) {
    // avoid cme
    for (IncomingPilotListener<HighIdentifier> incomingPilotListener : ipListeners) {
      incomingPilotListener.pilotOpening(highIdentifier);
    }
  }
  protected void notifyIncomingPilotRemoved(HighIdentifier highIdentifier) {
    // avoid cme
    for (IncomingPilotListener<HighIdentifier> incomingPilotListener : ipListeners) {
      incomingPilotListener.pilotClosed(highIdentifier);
    }
  }

  @Override
  public void addIncomingPilotListener(IncomingPilotListener<HighIdentifier> listener) {
    ipListeners.add(listener);
  }

  @Override
  public void removeIncomingPilotListener(IncomingPilotListener<HighIdentifier> listener) {
    ipListeners.remove(listener);
  }


}
