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
package rice.tutorial.transportlayer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.selector.TimerTask;

public class BandwidthLimitingTransportLayer<Identifier> implements
    TransportLayer<Identifier, ByteBuffer>,
    TransportLayerCallback<Identifier, ByteBuffer> {

  /**
   * The lower level transport layer.
   */
  private final TransportLayer<Identifier, ByteBuffer> transportLayer;

  /**
   * The environment
   */
  private final Environment environment;
  private final Logger logger;


  /**
   * The size of the bucket.
   */
  private final int BUCKET_SIZE;
  /**
   * How often the bucket is refilled.
   */
  private final int BUCKET_TIME_LIMIT;

  /**
   * When this goes to zero, don't send messages
   */
  private int bucket;

  private ErrorHandler<Identifier> errorHandler;

  /**
   * To send 10K/second specify use
   *
   * 10240,1000
   *
   * @param bucketSize bytes to send
   * @param bucketTimeLimitMillis how often the bucket is refilled, in millis
   */
  public BandwidthLimitingTransportLayer(
      TransportLayer<Identifier, ByteBuffer> transportLayer,
      int bucketSize, int bucketTimeLimitMillis,
      Environment env) {
    this.environment = env;
    this.transportLayer = transportLayer;
    BUCKET_SIZE = bucketSize;
    BUCKET_TIME_LIMIT = bucketTimeLimitMillis;
    logger = env.getLogManager().getLogger(BandwidthLimitingTransportLayer.class, null);
    this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    transportLayer.setCallback(this);

    environment.getSelectorManager().getTimer().schedule(new TimerTask(){
      @Override
      public void run() {
        // always synchronize on "this" before modifying the bucket
        synchronized(this) {
          bucket = BUCKET_SIZE;
          for (BandwidthLimitingSocket s : sockets) {
            s.notifyBandwidthRefilled();
          }
        }
      }
    }, 0, BUCKET_TIME_LIMIT);
  }

  @Override
  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(Identifier identifier, ByteBuffer message,
      final MessageCallback<Identifier, ByteBuffer> deliverAckToMe, Map<String, Object> options) {

    final MessageRequestHandleImpl<Identifier, ByteBuffer> returnMe =
      new MessageRequestHandleImpl<Identifier, ByteBuffer>(identifier, message, options);

    boolean success = true;
    synchronized(this) {
      if (message.remaining() > bucket) {
        success = false;
      } else {
        bucket-=message.remaining();
      }
    }
    if (!success) {
      logger.debug("Dropping message {} because not enough bandwidth:{}", message, bucket);
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(returnMe, new NotEnoughBandwidthException(bucket, message.remaining()));
      return returnMe;
    }

    returnMe.setSubCancellable(transportLayer.sendMessage(identifier,message,new MessageCallback<Identifier, ByteBuffer>() {
      @Override
      public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
        if (deliverAckToMe != null) deliverAckToMe.ack(returnMe);
      }

      @Override
      public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception reason) {
        if (deliverAckToMe != null) deliverAckToMe.sendFailed(returnMe, reason);
      }
    },options));
    return returnMe;
  }

  @Override
  public SocketRequestHandle<Identifier> openSocket(Identifier identifier, final SocketCallback<Identifier> deliverSocketToMe, Map<String, Object> options) {
    final SocketRequestHandleImpl<Identifier> returnMe = new SocketRequestHandleImpl<Identifier>(identifier,options, logger);
    returnMe.setSubCancellable(transportLayer.openSocket(identifier, new SocketCallback<Identifier>(){
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> socket) {
        deliverSocketToMe.receiveResult(returnMe, new BandwidthLimitingSocket(socket));
      }

      @Override
      public void receiveException(SocketRequestHandle<Identifier> cancellable, Exception exception) {
        deliverSocketToMe.receiveException(returnMe, exception);
      }
    }, options));
    return returnMe;
  }

  TransportLayerCallback<Identifier, ByteBuffer> callback;
  @Override
  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  @Override
  public void incomingSocket(P2PSocket<Identifier> socket) throws IOException {
    callback.incomingSocket(new BandwidthLimitingSocket(socket));
  }

  /**
   * Keep track of all of the BandwidthLimitingSockets
   */
  Collection<BandwidthLimitingSocket> sockets = new ArrayList<BandwidthLimitingSocket>();

  class BandwidthLimitingSocket extends SocketWrapperSocket<Identifier, Identifier> {
    public BandwidthLimitingSocket(P2PSocket<Identifier> socket) {
      super(socket.getIdentifier(), socket,
          BandwidthLimitingTransportLayer.this.logger,
          BandwidthLimitingTransportLayer.this.errorHandler,
          socket.getOptions());
      synchronized(BandwidthLimitingTransportLayer.this) {
        sockets.add(this);
      }
    }

    @Override
    public long write(ByteBuffer source) throws IOException {
      if (source.remaining() <= bucket) {
        long bytesWritten = super.write(source);
        if (bytesWritten >= 0) {
          // EOF is usually -1
          synchronized(this) {
            bucket -= (int) bytesWritten;
          }
        }
        return bytesWritten;
      }

      logger.debug("Limiting {} to {} bytes.", socket, bucket);

      // The user is trying to write more than is allowed.  To handle this we
      // will copy the allowed amount into a temporary ByteBuffer and pass that
      // to the next layer.  It is critical set the proper position of source
      // before returning.  First, let's record the original position.
      int originalPosition = source.position();

      // now we create allowedBuffer, whose size is "bucket"
      ByteBuffer allowedBuffer = ByteBuffer.wrap(source.array(), source.arrayOffset() + originalPosition, bucket);

      // try to write our allowedBuffer
      long bytesWritten = super.write(allowedBuffer);

      if (bytesWritten < 0) {
        // there was a problem
        return bytesWritten;
      }

      // allocate the bandwidth
      synchronized(this) {
        bucket -= (int) bytesWritten;
      }

      // we need to properly set the position
      source.position(originalPosition+(int)bytesWritten);
      return bytesWritten;
    }

    /**
     * Store the write requester.  If this variable is not null
     * it means that the storedWriter wants to write, but there
     * wasn't enough bandwidth.
     */
    P2PSocketReceiver<Identifier> storedWriter;

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<Identifier> receiver) {
      // this variable is what we will pass to super.register()
      boolean canWrite = wantToWrite;

      // if the user wants to write, and the bucket is empty, set our temp variable to false
      if (wantToWrite == true && bucket == 0) {
        canWrite = false;
        storedWriter = receiver;
      }

      // only call super.register() if we have something to do
      if (wantToRead || canWrite) super.register(wantToRead, canWrite, receiver);
    }

    /**
     * Register and clear the storedWriter
     */
    public void notifyBandwidthRefilled() {
      if (storedWriter != null) {
        P2PSocketReceiver<Identifier> writerToResume = storedWriter;
        storedWriter = null;
        super.register(false, true, writerToResume);
      }
    }

    @Override
    public void close() {
      super.close();
      synchronized(BandwidthLimitingTransportLayer.this) {
        sockets.remove(this);
      }
    }

    @Override
    public void shutdownOutput() {
      super.shutdownOutput();
      synchronized(BandwidthLimitingTransportLayer.this) {
        sockets.remove(this);
      }
    }
  }

  @Override
  public void acceptMessages(boolean shouldAccept) {
    transportLayer.acceptMessages(shouldAccept);
  }

  @Override
  public void acceptSockets(boolean shouldAccept) {
    transportLayer.acceptSockets(shouldAccept);
  }

  @Override
  public Identifier getLocalIdentifier() {
    return transportLayer.getLocalIdentifier();
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
    transportLayer.setErrorHandler(handler);
  }

  @Override
  public void destroy() {
    transportLayer.destroy();
  }

  @Override
  public void messageReceived(Identifier identifier, ByteBuffer message, Map<String, Object> options) throws IOException {
    // notify listeners
    callback.messageReceived(identifier, message, options);
  }

  public static PastryNodeFactory exampleA(int bindport, Environment env,
      NodeIdFactory nidFactory, final int bucketSize, final int bucketTimeLimitMillis) throws IOException {

    // anonymously extend SPNF
    PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env) {

      /**
       * Override buildWireTransportLayer to return the BandwidthLimitingTL wrapping
       * the default wire implementation.
       */
      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> buildWireTransportLayer(
          InetSocketAddress innermostAddress, PastryNode pastryNode) throws IOException {
        // get the default layer
        TransportLayer<InetSocketAddress, ByteBuffer> wireTransportLayer =
          super.buildWireTransportLayer(innermostAddress, pastryNode);

        // wrap it with our layer
        return new BandwidthLimitingTransportLayer<InetSocketAddress>(
            wireTransportLayer, bucketSize, bucketTimeLimitMillis, pastryNode.getEnvironment());
      }
    };
    return factory;
  }

  public static PastryNodeFactory exampleB(int bindport, Environment env,
      NodeIdFactory nidFactory, final int bucketSize, final int bucketTimeLimitMillis) throws IOException {
    PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env) {

      @Override
      protected TransLivenessProximity<MultiInetSocketAddress, ByteBuffer> getSourceRouteManagerLayer(
          TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> sourceRouteTransportLayer,
          LivenessProvider<SourceRoute<MultiInetSocketAddress>> livenessProvider,
          Pinger<SourceRoute<MultiInetSocketAddress>> pinger,
          PastryNode pastryNode,
          MultiInetSocketAddress proxyAddress,
          MultiAddressSourceRouteFactory sourceRouteFactory) throws IOException {

        // get the default layer
        final TransLivenessProximity<MultiInetSocketAddress, ByteBuffer> sourceRouteManagerLayer =
          super.getSourceRouteManagerLayer(
            sourceRouteTransportLayer, livenessProvider, pinger, pastryNode, proxyAddress, sourceRouteFactory);

        // wrap the default layer with our layer
        final BandwidthLimitingTransportLayer<MultiInetSocketAddress> bandwidthLimitingLayer =
          new BandwidthLimitingTransportLayer<MultiInetSocketAddress>(
            sourceRouteManagerLayer.getTransportLayer(), bucketSize, bucketTimeLimitMillis, pastryNode.getEnvironment());

        return new TransLivenessProximity<MultiInetSocketAddress, ByteBuffer>(){
          @Override
          public TransportLayer<MultiInetSocketAddress, ByteBuffer> getTransportLayer() {
            return bandwidthLimitingLayer;
          }
          @Override
          public LivenessProvider<MultiInetSocketAddress> getLivenessProvider() {
            return sourceRouteManagerLayer.getLivenessProvider();
          }
          @Override
          public ProximityProvider<MultiInetSocketAddress> getProximityProvider() {
            return sourceRouteManagerLayer.getProximityProvider();
          }
        };
      }
    };
    return factory;
  }
}
