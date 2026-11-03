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
package org.mpisws.p2p.transport.sourceroute;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.mpisws.p2p.transport.util.ByteBuffers;
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
 * This layer can only send/receive messages from a SourceRoute and determine liveness.
 * It does not manage current routes to nodes.
 *
 * @author Jeff Hoye
 */
public class SourceRouteTransportLayerImpl<Identifier> implements
    SourceRouteTransportLayer<Identifier>,
    TransportLayerCallback<Identifier, ByteBuffer> {

  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  TransportLayerCallback<SourceRoute<Identifier>, ByteBuffer> callback;
  ErrorHandler<SourceRoute<Identifier>> errorHandler;
  TransportLayer<Identifier, ByteBuffer> etl;
  Environment environment;
  Logger logger;
  SourceRoute<Identifier> localIdentifier;
  Collection<SourceRouteTap<Identifier>> taps;
  SourceRouteFactory<Identifier> srFactory;
  SourceRouteForwardStrategy<Identifier> forwardSourceRouteStrategy;

  public SourceRouteTransportLayerImpl(
      SourceRouteFactory<Identifier> srFactory,
      TransportLayer<Identifier, ByteBuffer> etl,
      SourceRouteForwardStrategy<Identifier> fSRs,
      Environment env,
      ErrorHandler<SourceRoute<Identifier>> errorHandler) {
    this.etl = etl;
    this.environment = env;
    this.logger = env.getLogManager().getLogger(SourceRouteTransportLayerImpl.class, null);
    this.srFactory = srFactory;
    this.errorHandler = errorHandler;
    localIdentifier = this.srFactory.getSourceRoute(etl.getLocalIdentifier());
    taps = new ArrayList<SourceRouteTap<Identifier>>();

    this.forwardSourceRouteStrategy = fSRs;

    if (this.forwardSourceRouteStrategy == null) {
      this.forwardSourceRouteStrategy = new DefaultForwardSourceRouteStrategy<Identifier>();
    }

    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<SourceRoute<Identifier>>(logger);
    }

    etl.setCallback(this);
  }

  @Override
  public SocketRequestHandle<SourceRoute<Identifier>> openSocket(
      final SourceRoute<Identifier> i,
      final SocketCallback<SourceRoute<Identifier>> deliverSocketToMe,
      @Nullable Map<String, Object> options) {
    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");

    logger.info("openSocket({},{},{})", i, deliverSocketToMe, options);

    // invariants
    if (i.getNumHops() <= 1) {
      throw new IllegalArgumentException("SourceRoute must have more than 1 hop! sr:"+i);
    }

    if (!i.getFirstHop().equals(etl.getLocalIdentifier())) {
      throw new IllegalArgumentException("SourceRoute must start with self! sr:"+i+" self:"+etl.getLocalIdentifier());
    }

    logger.debug("openSocket({})", i);

    final SocketRequestHandleImpl<SourceRoute<Identifier>> handle = new SocketRequestHandleImpl<SourceRoute<Identifier>>(i, options, logger);

    SimpleOutputBuffer sob = new SimpleOutputBuffer(i.getSerializedLength());
    try {
      i.serialize(sob);
    } catch (IOException ioe) {
      deliverSocketToMe.receiveException(handle, ioe);
      return handle;
    }
    final ByteBuffer b = ByteBuffer.wrap(sob.getBytes());

    handle.setSubCancellable(etl.openSocket(i.getHop(1), new SocketCallback<Identifier>(){
      @Override
      @SuppressWarnings("ReferenceEquality") // the same instance is meant
      public void receiveResult(
          SocketRequestHandle<Identifier> c,
          final P2PSocket<Identifier> result) {
        if (handle.getSubCancellable() != null && c != handle.getSubCancellable()) throw new RuntimeException("c != handle.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+handle.getSubCancellable());

        handle.setSubCancellable(() -> {
          result.close();
          return true;
        });

        logger.trace("openSocket({}):receiveResult({})", i, result);
        result.register(false, true, new P2PSocketReceiver<Identifier>() {
          @Override
          public void receiveSelectResult(P2PSocket<Identifier> socket,
              boolean canRead, boolean canWrite) throws IOException {
            if (canRead || !canWrite) throw new IOException("Expected to write! "+canRead+","+canWrite);

            // do the work
            socket.write(b);

            // keep working or pass up the new socket
            if (b.hasRemaining()) {
              socket.register(false, true, this);
            } else {
              openSocketHelper(deliverSocketToMe, handle, socket, i);
            }
          }

          @Override
          public void receiveException(P2PSocket<Identifier> socket,
              Exception e) {
            deliverSocketToMe.receiveException(handle, e);
          }
        });
      }
      @Override
      public void receiveException(SocketRequestHandle<Identifier> c, Exception exception) {
        deliverSocketToMe.receiveException(handle, exception);
      }
    }, options));

    return handle;
  }

  protected void openSocketHelper(
      SocketCallback<SourceRoute<Identifier>> deliverSocketToMe,
      SocketRequestHandleImpl<SourceRoute<Identifier>> handle,
      P2PSocket<Identifier> socket,
      SourceRoute<Identifier> i) {

    deliverSocketToMe.receiveResult(handle, new SocketWrapperSocket<SourceRoute<Identifier>, Identifier>(i, socket, logger, errorHandler, socket.getOptions()));
  }

  /**
   * To override this behavior if needed.
   */
  protected void incomingSocketHelper(P2PSocket<Identifier> socket, SourceRoute<Identifier> sr) throws IOException {
    callback.incomingSocket(new SocketWrapperSocket<SourceRoute<Identifier>, Identifier>(sr, socket, logger, errorHandler, socket.getOptions()));
  }

  @Override
  public void incomingSocket(final P2PSocket<Identifier> socka) throws IOException {
    // read the header
    logger.debug("incomingSocket({})", socka);

    final SocketInputBuffer sib = new SocketInputBuffer(socka);
    socka.register(true, false, new P2PSocketReceiver<Identifier>() {

      @Override
      public void receiveSelectResult(P2PSocket<Identifier> socket,
          boolean canRead, boolean canWrite) throws IOException {
        logger.trace("incomingSocket({}):receiveSelectResult()", socket);
        if (canWrite || !canRead) throw new IOException("Expected to read! "+canRead+","+canWrite);
        try {
          final SourceRoute<Identifier> sr = srFactory.build(sib, etl.getLocalIdentifier(), socka.getIdentifier());

          logger.trace("Read socket {}", sr);
          if (sr.getLastHop().equals(etl.getLocalIdentifier())) {
            // last hop
            incomingSocketHelper(socket, srFactory.reverse(sr));
          } else {
            // sr hop
            int hopNum = sr.getHop(etl.getLocalIdentifier());

            if (hopNum < 1) {
              // error, this is back to me!
              sib.reset();
              byte[] dump = new byte[sib.size()];
              sib.read(dump);
              errorHandler.receivedUnexpectedData(sr, dump, 0, null);
              socka.close();
              return;
            }

            sib.reset();
            byte[] srbytes = new byte[sib.size()];
            sib.read(srbytes);
            final ByteBuffer b = ByteBuffer.wrap(srbytes);

            logger.trace("I'm hop {} in {}", hopNum, sr);
            // we're an intermediate node, open next socket
            Identifier nextHop = sr.getHop(hopNum+1);
            if (forwardSourceRouteStrategy.forward(nextHop, sr, true, socka.getOptions())) {
              logger.trace("Attempting to open next hop {} <{}> in {}", nextHop, hopNum, sr);

              etl.openSocket(nextHop, new SocketCallback<Identifier>() {

                @Override
                public void receiveResult(SocketRequestHandle<Identifier> cancellable, final P2PSocket<Identifier> sockb) {
                  sockb.register(false, true, new P2PSocketReceiver<Identifier>() {

                    @Override
                    public void receiveSelectResult(P2PSocket<Identifier> socket,
                        boolean canRead, boolean canWrite) throws IOException {
                      if (canRead || !canWrite) throw new IOException("Expected to write! "+canRead+","+canWrite);

                      // do the work
                      socket.write(b);

                      // keep working or pass up the new socket
                      if (b.hasRemaining()) {
                        socket.register(false, true, this);
                      } else {
                        for (SourceRouteTap<Identifier> tap : taps) {
                          tap.socketOpened(sr, socka, sockb);
                        }
                        new Forwarder<Identifier>(sr, socka, sockb, logger, taps);
                      }
                    }

                    @Override
                    public void receiveException(P2PSocket<Identifier> socket,
                        Exception e) {
                      errorHandler.receivedException(sr, e);
                      socka.close();
                      sockb.close();
                    }
                  });

                }

                /**
                 * Couldn't open the socket, the next hop was dead.
                 */
                @Override
                public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
                  // may be nice to send some kind of error message to the opener
                  socka.close();
                }
              }, null);
            } else {
              // don't even bother opening to the next hop
              logger.info("Rejecting opening next hop {} <{}> in {}", nextHop, hopNum, sr);
              socka.close();
            }
          }

        } catch (InsufficientBytesException ibe) {
          socket.register(true, false, this);
        } catch (IOException e) {
          if (logger.isInfoEnabled()) errorHandler.receivedException(srFactory.getSourceRoute(etl.getLocalIdentifier(), socket.getIdentifier()), e);
          socka.close();
        }
      }

      @Override
      public void receiveException(P2PSocket<Identifier> socket,Exception e) {
        errorHandler.receivedException(srFactory.getSourceRoute(etl.getLocalIdentifier(), socket.getIdentifier()), e);
      }
    });

  }


  @Override
  public @Nullable MessageRequestHandle<SourceRoute<Identifier>, ByteBuffer> sendMessage(final SourceRoute<Identifier> i, final ByteBuffer m,
      final @Nullable MessageCallback<SourceRoute<Identifier>, ByteBuffer> deliverAckToMe,
      @Nullable Map<String, Object> options) {
    logger.debug("sendMessage({},{})", i, m);

    // invariants
    if (i.getNumHops() <= 1) {
      throw new IllegalArgumentException("SourceRoute must have more than 1 hop! sr:"+i);
    }

    if (!i.getFirstHop().equals(etl.getLocalIdentifier())) {
      throw new IllegalArgumentException("SourceRoute must start with self! sr:"+i+" self:"+etl.getLocalIdentifier());
    }

    final ByteBuffer buf;

    final MessageRequestHandleImpl<SourceRoute<Identifier>, ByteBuffer> handle
      = new MessageRequestHandleImpl<SourceRoute<Identifier>, ByteBuffer>(i, m, options);


    SimpleOutputBuffer sob = new SimpleOutputBuffer(m.remaining() + i.getSerializedLength());
    try {
      i.serialize(sob);
      sob.write(ByteBuffers.array(m), ByteBuffers.arrayPosition(m), m.remaining());
    } catch (IOException ioe) {
      if (deliverAckToMe == null) {
        errorHandler.receivedException(i, ioe);
      } else {
        deliverAckToMe.sendFailed(handle, ioe);
      }
      return null;
    }
    buf = ByteBuffer.wrap(sob.getBytes());

    handle.setSubCancellable(etl.sendMessage(
        i.getHop(1),
        buf,
        new MessageCallback<Identifier, ByteBuffer>() {
          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != handle.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe != null) deliverAckToMe.ack(handle);
          }
          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception ex) {
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != handle.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe == null) {
              errorHandler.receivedException(i, ex);
            } else {
              deliverAckToMe.sendFailed(handle, ex);
            }
          }
        },
        options));
    return handle;
  }

  @Override
  public void messageReceived(Identifier i, ByteBuffer m, @Nullable Map<String, Object> options) throws IOException {
    assert environment.getSelectorManager().isSelectorThread() : "transport callbacks run on the selector thread";
    if (!m.hasRemaining()) {
      errorHandler.receivedUnexpectedData(srFactory.getSourceRoute(etl.getLocalIdentifier(), i), ByteBuffers.arrayForRead(m), ByteBuffers.arrayPosition(m), null);
    }

    byte[] array = ByteBuffers.arrayForRead(m);
    int pos = m.position(); // need to reset to this spot if we forward the message
    SimpleInputBuffer sib = new SimpleInputBuffer(array, ByteBuffers.arrayPosition(m));

    SourceRoute<Identifier> tempSr;
    try {
      tempSr = srFactory.build(sib, etl.getLocalIdentifier(), i);
    } catch (Exception e) {
      // Got NegativeArrayException from serialized message
      errorHandler.receivedException(srFactory.getSourceRoute(etl.getLocalIdentifier(), i), e);
      return;
    }

    final SourceRoute<Identifier> sr = tempSr;

    // advance m properly
    ByteBuffers.setArrayPosition(m, array.length - sib.bytesRemaining());

    if (sr.getLastHop().equals(etl.getLocalIdentifier())) {
      // last hop
      callback.messageReceived(srFactory.reverse(sr), m, options);
    } else {
      // sr hop
      int hopNum = sr.getHop(etl.getLocalIdentifier());
      if (hopNum < 1) {
        errorHandler.receivedUnexpectedData(sr, array, ByteBuffers.arrayStart(m) + pos, null);
        return;
      }
      logger.trace("I'm hop {} in {}", hopNum, sr);
      // notify taps
      for (SourceRouteTap<Identifier> tap : taps) {
        byte[] retArr = Arrays.copyOfRange(array, ByteBuffers.arrayStart(m), ByteBuffers.arrayStart(m) + m.capacity());
        ByteBuffer ret = ByteBuffer.wrap(retArr);
        ret.position(m.position());
        tap.receivedMessage(ret, sr);
      }

      // forward
      m.position(pos);
      etl.sendMessage(
          sr.getHop(hopNum+1),
          m,
          null, // think about this
          null); // it's important to the rendezvous layer to _not_ reuse the options
    }
  }

  // ************************** Getters/Setters *****************
  @Override
  public void setCallback(
      TransportLayerCallback<SourceRoute<Identifier>, ByteBuffer> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<SourceRoute<Identifier>> errorHandler) {
    this.errorHandler = errorHandler;
  }

  @Override
  public void acceptMessages(boolean b) {
    etl.acceptMessages(b);
  }

  @Override
  public void acceptSockets(boolean b) {
    etl.acceptSockets(b);
  }

  @Override
  public SourceRoute<Identifier> getLocalIdentifier() {
    return localIdentifier;
  }

  @Override
  public void destroy() {
    etl.destroy();
  }

  @Override
  public void addSourceRouteTap(SourceRouteTap<Identifier> tap) {
    taps.add(tap);
  }

  @Override
  public boolean removeSourceRouteTap(SourceRouteTap<Identifier> tap) {
    return taps.remove(tap);
  }

}
