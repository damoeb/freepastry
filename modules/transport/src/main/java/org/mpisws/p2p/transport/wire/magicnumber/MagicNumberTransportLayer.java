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
package org.mpisws.p2p.transport.wire.magicnumber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.ListenableTransportLayer;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.TransportLayerListener;
import org.mpisws.p2p.transport.util.ByteBuffers;
import org.mpisws.p2p.transport.util.DefaultCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.wire.exception.StalledSocketException;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.selector.TimerTask;

/**
 * This class eliminates random connections by dropping all messages/sockets that don't
 * correlate to a specific header.  It is the simplest example of how to add a new
 * layer into the mpisws transport layer system.
 *
 * @author Jeff Hoye
 */
public class MagicNumberTransportLayer<Identifier> implements
    TransportLayer<Identifier, ByteBuffer>,
    TransportLayerCallback<Identifier, ByteBuffer>,
    ListenableTransportLayer<Identifier> {

  private final Logger logger;
  private final Environment environment;

  private TransportLayerCallback<Identifier, ByteBuffer> callback;
  private ErrorHandler<Identifier> errorHandler;
  private final TransportLayer<Identifier, ByteBuffer> wire;

  private final byte[] HEADER;
  private final int SOCKET_TIMEOUT;

  /**
   *
   * @param errorHandler call me when there is a problem
   * @param headerBytes the header to expect from a socket/packet
   * @param timeOut how long to wait before killing a socket that is not sending (milliseconds)
   */
  public MagicNumberTransportLayer(TransportLayer<Identifier, ByteBuffer> wtl,
      Environment env,
      ErrorHandler<Identifier> errorHandler,
      byte[] headerBytes,
      int timeOut) {
    this.logger = env.getLogManager().getLogger(MagicNumberTransportLayer.class, null);
    this.environment = env;
    this.wire = wtl;
    this.HEADER = headerBytes;
    this.SOCKET_TIMEOUT = timeOut;

    this.errorHandler = errorHandler;

    this.callback = new DefaultCallback<Identifier, ByteBuffer>(logger);

    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    }

    wire.setCallback(this);
  }

  @Override
  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    wire.setErrorHandler(handler);
    if (handler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
      return;
    }
    this.errorHandler = handler;
  }

  @Override
  public void acceptMessages(boolean enabled) {
    wire.acceptMessages(enabled);
  }

  @Override
  public void acceptSockets(boolean enabled) {
    wire.acceptSockets(enabled);
  }

  @Override
  public Identifier getLocalIdentifier() {
    return wire.getLocalIdentifier();
  }

  @Override
  public SocketRequestHandle<Identifier> openSocket(final Identifier identifier,
      final SocketCallback<Identifier> deliverSocketToMe,
      @Nullable Map<String, Object> options) {
    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");

    final SocketRequestHandleImpl<Identifier> cancellable = new SocketRequestHandleImpl<Identifier>(identifier, options, logger);

    cancellable.setSubCancellable(wire.openSocket(identifier, new SocketCallback<Identifier>(){
      @Override
      @SuppressWarnings("ReferenceEquality") // the same instance is meant
      public void receiveResult(SocketRequestHandle<Identifier> request, final P2PSocket<Identifier> result) {
        if (cancellable.getSubCancellable() != null && request != cancellable.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+request+" sub:"+cancellable.getSubCancellable());

        cancellable.setSubCancellable(() -> {
          result.close();
          return true;
        });

        result.register(false, true, new P2PSocketReceiver<Identifier>(){
          ByteBuffer buf = ByteBuffer.wrap(HEADER);
          @Override
          public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
            if (canRead) throw new IOException("Never asked to read!");
            if (!canWrite) throw new IOException("Can't write!");
            long ret = socket.write(buf);
            if (ret < 0) {
              socket.close();
              return;
            }
            notifyListenersWrite((int)ret, socket.getIdentifier(), socket.getOptions(), false, true);
            if (buf.hasRemaining()) {
              socket.register(false, true, this);
            } else {
              deliverSocketToMe.receiveResult(cancellable, socket);
            }
          }
          @Override
          public void receiveException(P2PSocket<Identifier> socket, Exception e) {
            deliverSocketToMe.receiveException(cancellable, e);
          }
        });
      }
      @Override
      @SuppressWarnings("ReferenceEquality") // the same instance is meant
      public void receiveException(SocketRequestHandle<Identifier> c, Exception exception) {
        if (cancellable.getSubCancellable() != null && c != cancellable.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+cancellable.getSubCancellable());
        deliverSocketToMe.receiveException(cancellable, exception);
      }
    }, options));

    return cancellable;
  }

  @Override
  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(
      final Identifier identifier,
      final ByteBuffer m,
      final @Nullable MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
      @Nullable Map<String, Object> options) {

    // build a new ByteBuffer with the header
    byte[] msgWithHeader = new byte[HEADER.length+m.remaining()];
    System.arraycopy(HEADER, 0, msgWithHeader, 0, HEADER.length);
    m.get(msgWithHeader, HEADER.length, m.remaining());

    logger.debug("sendMessage({},{})", identifier, m);

    final MessageRequestHandleImpl<Identifier, ByteBuffer> cancellable
      = new MessageRequestHandleImpl<Identifier, ByteBuffer>(identifier, m, options);

    final ByteBuffer buf = ByteBuffer.wrap(msgWithHeader);
    cancellable.setSubCancellable(wire.sendMessage(identifier,
        buf,
        new MessageCallback<Identifier, ByteBuffer>() {

          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
            if (cancellable.getSubCancellable() != null && msg != cancellable.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+cancellable.getSubCancellable());
            if (deliverAckToMe != null) deliverAckToMe.ack(cancellable);
            notifyListenersWrite(HEADER.length, identifier, cancellable.getOptions(), false, false);  // non-passthrough part
            notifyListenersWrite(buf.limit()-HEADER.length, identifier, cancellable.getOptions(), true, false); // passthrough part
          }

          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception ex) {
            if (cancellable.getSubCancellable() != null && msg != cancellable.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+cancellable.getSubCancellable());
            if (deliverAckToMe == null) {
              errorHandler.receivedException(identifier, ex);
            } else {
              deliverAckToMe.sendFailed(cancellable, ex);
            }
          }
        },
        options));

    return cancellable;
  }

  @Override
  public void destroy() {
    wire.destroy();
  }

  @Override
  public void incomingSocket(P2PSocket<Identifier> socket) throws IOException {
    socket.register(true, false, new VerifyHeaderReceiver(socket));
  }

  @Override
  public void messageReceived(Identifier identifier, ByteBuffer msg, @Nullable Map<String, Object> options) throws IOException {
    logger.debug("messageReceived({},{})", identifier, msg);

    if (msg.remaining() < HEADER.length) {
      errorHandler.receivedUnexpectedData(identifier, ByteBuffers.arrayForRead(msg), ByteBuffers.arrayStart(msg), null);
      return;
    }
    byte[] header = new byte[HEADER.length];

    msg.get(header);

    int remaining = msg.remaining();
    if (Arrays.equals(HEADER, header)) {
      notifyListenersRead(HEADER.length, identifier, options, false, false);  // non-passthrough part
      notifyListenersRead(remaining, identifier, options, true, false); // passthrough part
      callback.messageReceived(identifier, msg, options);
      return;
    }

    notifyListenersRead(HEADER.length, identifier, options, false, false);  // non-passthrough part
    notifyListenersRead(remaining, identifier, options, true, false); // passthrough part

    errorHandler.receivedUnexpectedData(identifier, ByteBuffers.arrayForRead(msg), ByteBuffers.arrayStart(msg), null);
  }

  protected class VerifyHeaderReceiver extends TimerTask implements P2PSocketReceiver<Identifier> {
    ByteBuffer buf = ByteBuffer.allocate(HEADER.length);

    P2PSocket<Identifier> socket;

    /**
     * Set once the header timeout fired; the socket close it triggers must not be reported again
     */
    boolean stalled = false;

    public VerifyHeaderReceiver(P2PSocket<Identifier> socket) {
      this.socket = socket;
      environment.getSelectorManager().getTimer().schedule(this,SOCKET_TIMEOUT);
    }

    @Override
    public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
      if (stalled) return; // already reported as a StalledSocketException
      errorHandler.receivedException(socket.getIdentifier(), ioe);
    }

    @Override
    public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
      // TODO: Optimization: Check array at each step, to fail faster
      // no read timeout of its own is needed here: the constructor schedules this receiver's run(), which closes the socket
      // if the header has not arrived within SOCKET_TIMEOUT, and the header's arrival cancels that timer
      if (canWrite) throw new IOException("Never asked to write!");
      if (!canRead) throw new IOException("Can't read!");
      long bytesRead;
      if ((bytesRead = socket.read(buf)) < 0) {
        socket.close();
        return;
      }
      notifyListenersRead((int)bytesRead, socket.getIdentifier(), socket.getOptions(),false,true);
      if (buf.hasRemaining()) {
        socket.register(true, false, this);
      } else {
        if (Arrays.equals(HEADER, buf.array())) {
          // header matched
          cancel();
          callback.incomingSocket(socket);
        } else {
          cancel();
          errorHandler.receivedUnexpectedData(socket.getIdentifier(), buf.array(), 0, null);
        }
      }
    }

    @Override
    public void run() {
      stalled = true;
      socket.close();
      errorHandler.receivedException(socket.getIdentifier(), new StalledSocketException(socket.getIdentifier(), "Timeout on incoming socket expired."));
    }

    @Override
    public String toString() {
      return MagicNumberTransportLayer.this+" VHR";
    }
  }

  // ******************************** TransportLayerListeners *******************************
  final List<TransportLayerListener<Identifier>> listeners = new CopyOnWriteArrayList<TransportLayerListener<Identifier>>();
  @Override
  public void addTransportLayerListener(
      TransportLayerListener<Identifier> listener) {
    listeners.add(listener);
  }

  @Override
  public void removeTransportLayerListener(
      TransportLayerListener<Identifier> listener) {
    listeners.remove(listener);
  }

  public void notifyListenersRead(int bytesRead, Identifier identifier,
      @Nullable Map<String, Object> options, boolean passthrough, boolean socket) {
    for (TransportLayerListener<Identifier> transportLayerListener : listeners) {
      transportLayerListener.read(bytesRead, identifier, options, passthrough, socket);
    }
  }

  public void notifyListenersWrite(int bytesRead, Identifier identifier,
      @Nullable Map<String, Object> options, boolean passthrough, boolean socket) {
    for (TransportLayerListener<Identifier> transportLayerListener : listeners) {
      transportLayerListener.wrote(bytesRead, identifier, options, passthrough, socket);
    }
  }
}
