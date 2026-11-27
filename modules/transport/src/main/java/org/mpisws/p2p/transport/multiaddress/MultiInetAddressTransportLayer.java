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
package org.mpisws.p2p.transport.multiaddress;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.util.ByteBuffers;
import org.mpisws.p2p.transport.util.DefaultCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.InsufficientBytesException;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketInputBuffer;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * This transport layer that can allow a node behind a NAT to talk to nodes outside or inside the firewall.
 *
 * Could conceivably be used to allow a node with multiple NICs pick the best address for different nodes.
 * Perhaps this would be useful if you were running a node on the NAT.
 *
 * This class adds an epoch and a list of InetSocketAddresses, and also disambiguates between them
 * for the lower layer.  This is useful in situations where the node is behind a NAT and is addressed
 * differently for nodes outside the NAT as inside.
 *
 *
 * Optimization: Pre-serialize localAddress in the ctor, then just mem-copy it to the buffer
 *
 * @author Jeff Hoye
 *
 */
public class MultiInetAddressTransportLayer
    implements TransportLayerCallback<InetSocketAddress, ByteBuffer>, TransportLayer<MultiInetSocketAddress, ByteBuffer> {

  TransportLayer<InetSocketAddress, ByteBuffer> wire;
  MultiInetSocketAddress localAddress;
  TransportLayerCallback<MultiInetSocketAddress, ByteBuffer> callback;
  ErrorHandler<MultiInetSocketAddress> errorHandler;
  Logger logger;
  AddressStrategy strategy;
  private final int maxNumAddresses;


  public MultiInetAddressTransportLayer(
      MultiInetSocketAddress localAddress,
      TransportLayer<InetSocketAddress, ByteBuffer> wire,
      Environment env,
      ErrorHandler<MultiInetSocketAddress> handler,
      AddressStrategy strategy) {
    this.logger = env.getLogManager().getLogger(MultiInetAddressTransportLayer.class, null);
    this.maxNumAddresses = MultiInetAddressConfig.fromParameters(env.getParameters()).maxNumAddresses();
    this.wire = wire;
    this.errorHandler = handler;
    this.localAddress = localAddress;
    this.strategy = strategy;
    if (wire == null) throw new IllegalArgumentException("TransportLayer<InetSocketAddress, ByteBuffer> wire must be non-null");
    if (localAddress == null) throw new IllegalArgumentException("EpochInetSocketAddress localAddress must be non-null");

    this.callback = new DefaultCallback<MultiInetSocketAddress, ByteBuffer>(env);

    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<MultiInetSocketAddress>(logger);
    }
    if (this.strategy == null) {
      this.strategy = new SimpleAddressStrategy();
    }

    wire.setCallback(this);
  }

  public AddressStrategy getAddressStrategy() {
    return strategy;
  }

  // ******************************** Sockets **************************
  @Override
  public @Nullable SocketRequestHandle<MultiInetSocketAddress> openSocket(
      final MultiInetSocketAddress address,
      final SocketCallback<MultiInetSocketAddress> deliverSocketToMe,
      @Nullable Map<String, Object> options) {

    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");

    final SocketRequestHandleImpl<MultiInetSocketAddress> handle = new SocketRequestHandleImpl<MultiInetSocketAddress>(address, options, logger);

    logger.info("openSocket({},{},{})", address, deliverSocketToMe, options);
    SimpleOutputBuffer sob = new SimpleOutputBuffer(localAddress.getSerializedLength());
    try {
      localAddress.serialize(sob);
    } catch (IOException ioe) {
      deliverSocketToMe.receiveException(handle, ioe);
      return null;
    }
    final ByteBuffer buffer = sob.getByteBuffer();

    InetSocketAddress addr = strategy.getAddress(getLocalIdentifier(), address);

    handle.setSubCancellable(wire.openSocket(addr,
        new SocketCallback<InetSocketAddress>(){

      @Override
      @SuppressWarnings("ReferenceEquality") // the same instance is meant
      public void receiveResult(SocketRequestHandle<InetSocketAddress> request, final P2PSocket<InetSocketAddress> result) {
        if (handle.getSubCancellable() != null && request != handle.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+request+" sub:"+handle.getSubCancellable());

        handle.setSubCancellable(() -> {
          result.close();
          return true;
        });

        logger.trace("openSocket({}):receiveResult({})", address, result);
        result.register(false, true, new P2PSocketReceiver<InetSocketAddress>() {
          @Override
          public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
              boolean canRead, boolean canWrite) throws IOException {
            if (canRead || !canWrite) throw new IOException("Expected to write! "+canRead+","+canWrite);

            // do the work
            if (socket.write(buffer) < 0) {
              deliverSocketToMe.receiveException(handle, new ClosedChannelException("Remote node closed socket while opening.  Try again."));
              return;
            }

            // keep working or pass up the new socket
            if (buffer.hasRemaining()) {
              socket.register(false, true, this);
            } else {
              deliverSocketToMe.receiveResult(handle, new SocketWrapperSocket<MultiInetSocketAddress, InetSocketAddress>(address, socket, logger, errorHandler, socket.getOptions()));
            }
          }

          @Override
          public void receiveException(P2PSocket<InetSocketAddress> socket,
              Exception e) {
            deliverSocketToMe.receiveException(handle, e);
          }
        });

      }
      @Override
      @SuppressWarnings("ReferenceEquality") // the same instance is meant
      public void receiveException(SocketRequestHandle<InetSocketAddress> c, Exception exception) {
        if (handle.getSubCancellable() != null && c != handle.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+handle.getSubCancellable());
        deliverSocketToMe.receiveException(handle, exception);
      }
    }, options));
    return handle;
  }

  @Override
  public void incomingSocket(P2PSocket<InetSocketAddress> incomingSocket) throws IOException {
    logger.debug("incomingSocket({})", incomingSocket);

    final SocketInputBuffer sib = new SocketInputBuffer(incomingSocket);
    incomingSocket.register(true, false, new P2PSocketReceiver<InetSocketAddress>() {

      @Override
      public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
          boolean canRead, boolean canWrite) throws IOException {
        logger.trace("incomingSocket({}):receiveSelectResult()", socket);
        if (canWrite || !canRead) throw new IOException("Expected to read! "+canRead+","+canWrite);
        try {
          MultiInetSocketAddress eisa = MultiInetSocketAddress.build(sib, maxNumAddresses);
          logger.trace("Read {}", eisa);
          callback.incomingSocket(new SocketWrapperSocket<MultiInetSocketAddress, InetSocketAddress>(eisa, socket, logger, errorHandler, socket.getOptions()));
        } catch (InsufficientBytesException ibe) {
          socket.register(true, false, this);
        } catch (IOException e) {
          if (logger.isInfoEnabled()) errorHandler.receivedException(new MultiInetSocketAddress(socket.getIdentifier()), e);
        }
      }

      @Override
      public void receiveException(P2PSocket<InetSocketAddress> socket,Exception e) {
        errorHandler.receivedException(new MultiInetSocketAddress(socket.getIdentifier()), e);
      }
    });

  }

  // ************************** Messages ***************************
  @Override
  public MessageRequestHandle<MultiInetSocketAddress, ByteBuffer> sendMessage(
      final MultiInetSocketAddress address,
      final ByteBuffer m,
      final @Nullable MessageCallback<MultiInetSocketAddress, ByteBuffer> deliverAckToMe,
      @Nullable Map<String, Object> options) {

    logger.debug("sendMessage({},{})", address, m);

    final MessageRequestHandleImpl<MultiInetSocketAddress, ByteBuffer> handle
      = new MessageRequestHandleImpl<MultiInetSocketAddress, ByteBuffer>(address, m, options);
    final ByteBuffer buf;
    SimpleOutputBuffer sob = new SimpleOutputBuffer(m.remaining() + localAddress.getSerializedLength());
    try {
      localAddress.serialize(sob);
      sob.write(ByteBuffers.array(m), ByteBuffers.arrayPosition(m), m.remaining());
    } catch (IOException ioe) {
      if (deliverAckToMe == null) {
        errorHandler.receivedException(address, ioe);
      } else {
        deliverAckToMe.sendFailed(handle, ioe);
      }
      return handle;
    }
    buf = ByteBuffer.wrap(sob.getBytes());

    handle.setSubCancellable(wire.sendMessage(
        strategy.getAddress(getLocalIdentifier(), address),
        buf,
        new MessageCallback<InetSocketAddress, ByteBuffer>() {

          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void ack(MessageRequestHandle<InetSocketAddress, ByteBuffer> msg) {
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe != null) deliverAckToMe.ack(handle);
          }

          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void sendFailed(MessageRequestHandle<InetSocketAddress, ByteBuffer> msg, Exception ex) {
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe == null) {
              if (ex instanceof NodeIsFaultyException) return;
              errorHandler.receivedException(address, ex);
            } else {
              deliverAckToMe.sendFailed(handle, ex);
            }
          }
        },
        options));
    return handle;
  }

  @Override
  public String toString() {
    return "MultiInetAddrTL{"+localAddress+"}";
  }

  @Override
  public void messageReceived(InetSocketAddress address, ByteBuffer msg, @Nullable Map<String, Object> options) throws IOException {
    logger.debug("messageReceived({},{})", address, msg);
    // read numAddresses
    byte[] array = ByteBuffers.arrayForRead(msg);
    int pos = ByteBuffers.arrayPosition(msg);
    SimpleInputBuffer sib = new SimpleInputBuffer(array, pos);

    MultiInetSocketAddress eisa;
    try {
      eisa = MultiInetSocketAddress.build(sib, maxNumAddresses);
    } catch (IOException ioe) {
      errorHandler.receivedUnexpectedData(new MultiInetSocketAddress(address), array, pos, null);
      return;
    }

    // make sure to leave m at the proper position
    ByteBuffers.setArrayPosition(msg, array.length - sib.bytesRemaining());

    callback.messageReceived(eisa, msg, options);
  }

  // ***************************** Other ******************************
  @Override
  public MultiInetSocketAddress getLocalIdentifier() {
    return localAddress;
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
  public void destroy() {
    wire.destroy();
  }

  @Override
  public void setCallback(TransportLayerCallback<MultiInetSocketAddress, ByteBuffer> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<MultiInetSocketAddress> handler) {
    this.errorHandler = handler;
  }
}
