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
package org.mpisws.p2p.transport.limitsockets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

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
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.selector.Timer;
import rice.selector.TimerTask;

/**
 * Automatically closes sockets based on LRU.
 *
 * Uses the LinkedHashMap to perform the LRU policy.
 *
 * @author Jeff Hoye
 */
public class LimitSocketsTransportLayer<Identifier, MessageType> implements TransportLayer<Identifier, MessageType>, TransportLayerCallback<Identifier, MessageType> {
  /** A socket whose output was shut down is closed after this delay. */
  private static final int CLOSE_AFTER_SHUTDOWN_MILLIS = 3000;

  int MAX_SOCKETS;
  private final TransportLayer<Identifier, MessageType> transportLayer;
  private final LinkedHashMap<LSSocket, LSSocket> cache;
  private final Logger logger;
  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  private TransportLayerCallback<Identifier, MessageType> callback;
  private final Timer timer;
  private ErrorHandler<Identifier> handler;


  public LimitSocketsTransportLayer(int maxSockets, TransportLayer<Identifier, MessageType> transportLayer, ErrorHandler<Identifier> handler, Environment env) {
    this.MAX_SOCKETS = maxSockets;
    this.transportLayer = transportLayer;
    this.logger = env.getLogManager().getLogger(LimitSocketsTransportLayer.class, null);
    this.timer = env.getSelectorManager().getTimer();
    this.cache = new LinkedHashMap<LSSocket, LSSocket>(MAX_SOCKETS,0.75f,true);
    this.handler = handler;
    if (this.handler == null) {
      this.handler = new DefaultErrorHandler<Identifier>(logger);
    }

    transportLayer.setCallback(this);
  }

  @Override
  public SocketRequestHandle<Identifier> openSocket(final Identifier identifier, final SocketCallback<Identifier> deliverSocketToMe, final @Nullable Map<String, Object> options) {
    if (logger.isTraceEnabled()) logger.trace("{}.openSocket({},{},{})", LimitSocketsTransportLayer.this, identifier, deliverSocketToMe, options, new Exception("Stack Trace"));

    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(identifier, options, logger) {
      @Override
      public boolean cancel() {
        logger.trace("{}.openSocket({},{}):{}.cancel()", this, identifier, deliverSocketToMe, this);
        return super.cancel();
      }

      @Override
      public String toString() {
        return LimitSocketsTransportLayer.this+"RequestHandle.openSocket("+identifier+","+deliverSocketToMe+","+options+")";
      }
    };

    ret.setSubCancellable(transportLayer.openSocket(identifier, new SocketCallback<Identifier>(){
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> socket) {
        logger.trace("{}.openSocket({},{}):{}.receiveResult()", this, identifier, deliverSocketToMe, ret);
        deliverSocketToMe.receiveResult(ret, wrapAndTrackSocket(socket));
      }
      @Override
      public void receiveException(SocketRequestHandle<Identifier> request, Exception ex) {
        logger.atTrace().log(() -> this+".openSocket("+identifier+","+deliverSocketToMe+"):"+ret+".receiveException("+ex+")");
        deliverSocketToMe.receiveException(ret, ex);
      }
      @Override
      public String toString() {
        return LimitSocketsTransportLayer.this+"SocketCallback.openSocket("+identifier+","+deliverSocketToMe+","+options+")";
      }

    }, options));

    return ret;
  }

  @Override
  public void incomingSocket(P2PSocket<Identifier> socket) throws IOException {
    logger.trace("{}.incomingSocket({})", this, socket);
    callback.incomingSocket(wrapAndTrackSocket(socket));
  }

  protected LSSocket wrapAndTrackSocket(P2PSocket<Identifier> socket) {
    LSSocket ret = new LSSocket(socket);
    cache.put(ret,ret);
    closeIfNecessary();
    return ret;
  }

  protected void closeIfNecessary() {
    Collection<LSSocket> closeMe = new ArrayList<LSSocket>();
    synchronized(cache) {
      while (cache.size() > MAX_SOCKETS) {
        Iterator<LSSocket> iterator = cache.keySet().iterator();
        closeMe.add(iterator.next());
        iterator.remove();
      }
    }
    for (LSSocket lSSocket : closeMe) {
      lSSocket.forceClose();
    }
  }

  class LSSocket extends SocketWrapperSocket<Identifier, Identifier> {
    boolean closed = false;
    boolean forcedClose = false;
    public LSSocket(P2PSocket<Identifier> socket) {
      super(socket.getIdentifier(), socket, LimitSocketsTransportLayer.this.logger, handler, socket.getOptions());
    }

    /**
     * Called when we force a socket closed.
     */
    public void forceClose() {
      logger.debug("{}.forceClose()", this);
      forcedClose = true;
      super.shutdownOutput();
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            close();
          } catch (Exception ignored) {
            // do nothing, it's probably already closed anyway
          }
        }
      }, CLOSE_AFTER_SHUTDOWN_MILLIS);
    }

    /**
     * Called by the higher layer
     */
    @Override
    public void close() {
      logger.trace("{}.close()", this);
      closed = true;
      cache.remove(this);
      super.close();
    }

    @Override
    public long read(ByteBuffer dsts) throws IOException {
      if (!closed) touch(this);
      try {
        return super.read(dsts);
      } catch (IOException ioe) {
        close();
        throw ioe;
      }
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<Identifier> receiver) {
      if (forcedClose) {
        if (wantToWrite) {
          receiver.receiveException(this, new ClosedChannelException("Limit Sockets forced close. "+this));
        }
        if (wantToRead) {
          super.register(true, false, receiver);
        }
        return;
      }
      if (!closed) touch(this);
      super.register(wantToRead, wantToWrite, receiver);
    }

    @Override
    public long write(ByteBuffer srcs) throws IOException {
      if (forcedClose) throw new ClosedChannelException("Limit Sockets forced close. "+this);
      if (!closed) touch(this);
      try {
        return super.write(srcs);
      } catch (IOException ioe) {
        close();
        throw ioe;
      }
    }

    @Override
    public String toString() {
      return LimitSocketsTransportLayer.this.toString()+"$LSSocket<"+identifier+">["+(closed?"closed":"open")+"]@"+System.identityHashCode(this)+socket.toString();
    }
  }

  public void touch(LSSocket socket) {
    synchronized(cache) {
      if (cache.get(socket) == null) {
        cache.put(socket, socket);
        closeIfNecessary();
      }
    }
  }

  @Override
  public String toString() {
    return "LimitSocks<"+cache.size()+">";
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
  public @Nullable MessageRequestHandle<Identifier, MessageType> sendMessage(Identifier identifier, MessageType messageType, @Nullable MessageCallback<Identifier, MessageType> deliverAckToMe, @Nullable Map<String, Object> options) {
    return transportLayer.sendMessage(identifier, messageType, deliverAckToMe, options);
  }

  @Override
  public void messageReceived(Identifier identifier, MessageType messageType, @Nullable Map<String, Object> options) throws IOException {
    callback.messageReceived(identifier, messageType, options);
  }

  @Override
  public void setCallback(TransportLayerCallback<Identifier, MessageType> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.handler = handler;
    transportLayer.setErrorHandler(handler);
  }

  @Override
  public void destroy() {
    transportLayer.destroy();
  }


}
