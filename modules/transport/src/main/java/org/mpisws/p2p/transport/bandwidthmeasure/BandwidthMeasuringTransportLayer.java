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
package org.mpisws.p2p.transport.bandwidthmeasure;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.util.tuples.Tuple;
import rice.selector.TimerTask;

/**
 * Measure the bandwidth of the connections.
 *
 * @author Jeff Hoye
 */
public class BandwidthMeasuringTransportLayer<Identifier> implements
  TransportLayer<Identifier, ByteBuffer>,
  TransportLayerCallback<Identifier, ByteBuffer> {

  TransportLayer<Identifier, ByteBuffer> tl;
  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  TransportLayerCallback<Identifier, ByteBuffer> callback;

  Logger logger;
  ErrorHandler<Identifier> errorHandler;


  // current measured
  public static final int CUR_DOWN = 0;
  public static final int CUR_UP = 1;
  public static final int CUR_SATURATED = 2;

  // last measured
  public static final int LAST_DOWN = 3;
  public static final int LAST_UP = 4;
  public static final int LAST_SATURATED = 5;

  public static final int NUM_VALS = 6;

  public static final int SATURATED = 1;
  public static final int NOT_SATURATED = 0;

  /**
   * Current measure, last measure
   */
  final Map<Identifier, Tuple<int[], Collection<MySocket>>> measured = new HashMap<Identifier, Tuple<int[], Collection<MySocket>>>();
  private final rice.environment.time.TimeSource time;

  public BandwidthMeasuringTransportLayer(int measurementPeriod, TransportLayer<Identifier, ByteBuffer> transportLayer, Environment env) {
    this.tl = transportLayer;
    transportLayer.setCallback(this);
    this.logger = env.getLogManager().getLogger(BandwidthMeasuringTransportLayer.class, null);
    this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    this.time = env.getTimeSource();
    this.lastMeasure = time.currentTimeMillis();

    env.getSelectorManager().schedule(new TimerTask() {

      @Override
      public void run() {
        measure();
      }

    },measurementPeriod,measurementPeriod);
  }

  /**
   * Last time measure was called.
   */
  long lastMeasure;

  protected void measure() {

    synchronized(measured) {
      long now = time.currentTimeMillis();
      int diff = (int)(now - lastMeasure);
      lastMeasure = now;
      for (Identifier identifier : measured.keySet()) {
        Tuple<int[], Collection<MySocket>> pair = measured.get(identifier);
        int[] vals = pair.first();
        vals[LAST_DOWN] = vals[CUR_DOWN]*1000/diff;
        vals[LAST_UP] = vals[CUR_UP]*1000/diff;
        vals[LAST_SATURATED] = vals[CUR_SATURATED];
        vals[CUR_DOWN] = 0;
        vals[CUR_UP] = 0;
        vals[CUR_SATURATED] = NOT_SATURATED;

        // see if anyone was waiting to write
        if (vals[LAST_SATURATED] == NOT_SATURATED) {
          for (MySocket mySocket : pair.second()) {
            if (mySocket.wantsToWrite()) {
              vals[LAST_SATURATED] = SATURATED;
              break;
            }
          }
        }
      }
    }
  }

  /**
   * Should be holding the lock on measured
   */
  public Tuple<int[], Collection<MySocket>> getOrCreateMeasurements(Identifier identifier) {
    Tuple<int[], Collection<MySocket>> ret = measured.get(identifier);
    if (ret == null) {
      ret = new Tuple<int[], Collection<MySocket>>(new int[NUM_VALS], new ArrayList<MySocket>());
      measured.put(identifier, ret);
    }
    return ret;
  }

  /**
   * The bandwidth an Identifier used.
   *
   * @param down downstream bytes/sec
   * @param up upstream bytes/sec
   * @param saturated if the upstream was saturated
   */
  public record BandwidthUsed(int down, int up, boolean saturated) {
  }

  /**
   * For all Identifiers that used any bandwidth since the measurementPeriod, returns:
   * downstream bytes/sec, upstream bytes/sec, if the upstream was saturated
   */
  public Map<Identifier, BandwidthUsed> getBandwidthUsed() {
    synchronized(measured) {
      HashMap<Identifier, BandwidthUsed> ret = new HashMap<Identifier, BandwidthUsed>();
      for (Identifier identifier : measured.keySet()) {
        Tuple<int[], Collection<MySocket>> pair = measured.get(identifier);
        int[] vals = pair.first();
        ret.put(identifier,new BandwidthUsed(vals[LAST_DOWN], vals[LAST_UP], vals[LAST_SATURATED] == SATURATED));
      }
      return ret;
    }
  }

  @Override
  public void incomingSocket(P2PSocket<Identifier> socket) throws IOException {
    callback.incomingSocket(new MySocket(socket.getIdentifier(),socket,logger,errorHandler,socket.getOptions()));
  }

  class MySocket extends SocketWrapperSocket<Identifier, Identifier> {

    @Override
    public void close() {
      synchronized(measured) {
        getOrCreateMeasurements(identifier).second().remove(this);
      }
      super.close();
    }

    @Override
    public void receiveException(P2PSocket<Identifier> socket, Exception e) {
      synchronized(measured) {
        getOrCreateMeasurements(identifier).second().remove(this);
      }
      super.receiveException(socket, e);
    }

    /**
     * true if we are waiting to write.
     */
    public boolean wantsToWrite() {
      return (writer != null);
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite,
        P2PSocketReceiver<Identifier> receiver) {
      super.register(wantToRead, wantToWrite, receiver);
    }

    public MySocket(Identifier identifier, P2PSocket<Identifier> socket,
        Logger logger, ErrorHandler<Identifier> errorHandler,
        @Nullable Map<String, Object> options) {
      super(identifier, socket, logger, errorHandler, options);
      synchronized(measured) {
        getOrCreateMeasurements(identifier).second().add(this);
      }
    }

    @Override
    public long write(ByteBuffer srcs) throws IOException {
      long ret = super.write(srcs);
      synchronized(measured) {
        int[] vals = getOrCreateMeasurements(getIdentifier()).first();
        if (srcs.hasRemaining()) {
          vals[CUR_SATURATED] = SATURATED;
        }
        vals[CUR_UP] = (int) (vals[CUR_UP] + ret);
      }
      return ret;
    }

    @Override
    public long read(ByteBuffer srcs) throws IOException {
      long ret = super.read(srcs);
      synchronized(measured) {
        int[] vals = getOrCreateMeasurements(getIdentifier()).first();
        vals[CUR_DOWN] = (int) (vals[CUR_DOWN] + ret);
      }
      return ret;
    }

  }

  @Override
  public void messageReceived(Identifier identifier, ByteBuffer msg,
      @Nullable Map<String, Object> options) throws IOException {
    synchronized (measured) {
      getOrCreateMeasurements(identifier).first()[CUR_DOWN]+=msg.remaining();
    }
    callback.messageReceived(identifier, msg, options);
  }

  @Override
  public @Nullable MessageRequestHandle<Identifier, ByteBuffer> sendMessage(Identifier identifier,
      ByteBuffer msg, @Nullable MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
      @Nullable Map<String, Object> options) {
    synchronized (measured) {
      getOrCreateMeasurements(identifier).first()[CUR_UP]+=msg.remaining();
    }
    return tl.sendMessage(identifier, msg, deliverAckToMe, options);
  }


  @Override
  public SocketRequestHandle<Identifier> openSocket(final Identifier identifier,
      final SocketCallback<Identifier> deliverSocketToMe,
      final @Nullable Map<String, Object> options) {
    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(identifier,options,logger);
    ret.setSubCancellable(tl.openSocket(identifier, new SocketCallback<Identifier>() {

      @Override
      public void receiveException(SocketRequestHandle<Identifier> request,
          Exception ex) {
        deliverSocketToMe.receiveException(request, ex);
      }

      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> socket) {
        deliverSocketToMe.receiveResult(ret, new MySocket(identifier,socket,logger,errorHandler,options));
      }

    }, options));
    return ret;
  }

  @Override
  public void acceptMessages(boolean enabled) {
    tl.acceptMessages(enabled);
  }

  @Override
  public void acceptSockets(boolean enabled) {
    tl.acceptSockets(enabled);
  }

  @Override
  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  @Override
  public void setCallback(
      TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  @Override
  public void destroy() {
    tl.destroy();
  }

}
