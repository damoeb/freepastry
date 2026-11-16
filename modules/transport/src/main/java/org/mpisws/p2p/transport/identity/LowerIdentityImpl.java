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
package org.mpisws.p2p.transport.identity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.InsufficientBytesException;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.mpisws.p2p.transport.util.SocketInputBuffer;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.mpisws.p2p.transport.identity.IdentityImpl.DONT_VERIFY;
import static org.mpisws.p2p.transport.identity.IdentityImpl.FAILURE;
import static org.mpisws.p2p.transport.identity.IdentityImpl.INCORRECT_IDENTITY;
import static org.mpisws.p2p.transport.identity.IdentityImpl.NODE_HANDLE_FROM_INDEX;
import static org.mpisws.p2p.transport.identity.IdentityImpl.NORMAL;
import static org.mpisws.p2p.transport.identity.IdentityImpl.NO_ID;
import static org.mpisws.p2p.transport.identity.IdentityImpl.SUCCESS;

class LowerIdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> implements LowerIdentity<LowerIdentifier, ByteBuffer>, TransportLayerCallback<LowerIdentifier, ByteBuffer> {
  private final IdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> identity;
  private final Environment environment;
  private final byte[] localIdentifier;
  private final IdentitySerializer<UpperIdentifier, MiddleIdentifier, LowerIdentifier> serializer;
  private final Map<MiddleIdentifier, UpperIdentifier> bindings;
  private final BindStrategy<UpperIdentifier, LowerIdentifier> bindStrategy;

  TransportLayer<LowerIdentifier, ByteBuffer> tl;
  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  TransportLayerCallback<LowerIdentifier, ByteBuffer> callback;
  ErrorHandler<LowerIdentifier> errorHandler;
  Logger logger;

  LowerIdentityImpl(
      IdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> identity,
      TransportLayer<LowerIdentifier, ByteBuffer> tl,
      ErrorHandler<LowerIdentifier> handler) {
    this.identity = identity;
    this.environment = identity.environment;
    this.localIdentifier = identity.localIdentifier;
    this.serializer = identity.serializer;
    this.bindings = identity.bindings;
    this.bindStrategy = identity.bindStrategy;
    this.tl = tl;
    logger = environment.getLogManager().getLogger(IdentityImpl.class, "lower");
    if (handler != null) {
      this.errorHandler = handler;
    } else {
      this.errorHandler = new DefaultErrorHandler<LowerIdentifier>(logger);
    }

    tl.setCallback(this);
  }

  @Override
  public SocketRequestHandle<LowerIdentifier> openSocket(
      final LowerIdentifier i,
      final SocketCallback<LowerIdentifier> deliverSocketToMe,
      final @Nullable Map<String, Object> options) {
    logger.info("openSocket({},{},{})", i, deliverSocketToMe, options);

    // what happens if they cancel after the socket has been received by a lower layer, but is still reading the header?
    // May need to re-think this SRHI at all the layers
    final SocketRequestHandleImpl<LowerIdentifier> ret = new SocketRequestHandleImpl<LowerIdentifier>(i, options, logger);
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      if ((options == null) || (options.containsKey(DONT_VERIFY) && ((Boolean)options.get(DONT_VERIFY)).booleanValue())) {
        // don't send TO
        sob.writeByte(0);
      } else {
        // send TO
        sob.writeByte(1);
        final UpperIdentifier dest = identity.getIntendedDest(options);

        if (dest == null) {
          deliverSocketToMe.receiveException(ret, new MemoryExpiredException("No record of the upper identifier for "+i+" index="+options.get(NODE_HANDLE_FROM_INDEX)));
          return ret;
        }

        if (identity.addBinding(dest, i, options)) {
          // no problem, sending message
        } else {
          deliverSocketToMe.receiveException(ret, new NodeIsFaultyException(i));
          return ret;
        }

        serializer.serialize(sob, dest);
      }

      // write FROM
      sob.write(localIdentifier);
    } catch (IOException ioe) {
      deliverSocketToMe.receiveException(ret, ioe);
      return ret;
    }
    final ByteBuffer buf;
    buf = sob.getByteBuffer();
    ret.setSubCancellable(tl.openSocket(i,
        new SocketCallback<LowerIdentifier>(){

          @Override
          public void receiveResult(SocketRequestHandle<LowerIdentifier> cancellable, final P2PSocket<LowerIdentifier> sock) {
            ret.setSubCancellable(() -> {
              sock.close();
              return true;
            });
            try {
              new P2PSocketReceiver<LowerIdentifier>() {

                @Override
                public void receiveSelectResult(P2PSocket<LowerIdentifier> socket, boolean canRead, boolean canWrite) throws IOException {
                  if (canRead) throw new IOException("Never asked to read!");
                  if (!canWrite) throw new IOException("Can't write!");
                  if (socket.write(buf) < 0)  {
                    deliverSocketToMe.receiveException(ret, new ClosedChannelException("Remote node closed socket while opening.  Try again."));
                    return;
                  }
                  if (buf.hasRemaining()) {
                    socket.register(false, true, this);
                  } else {
                    // wait for the response
                    socket.register(true, false, new P2PSocketReceiver<LowerIdentifier>() {
                      ByteBuffer responseBuffer = ByteBuffer.allocate(1);

                      @Override
                      public void receiveException(P2PSocket<LowerIdentifier> socket, Exception ioe) {
                        deliverSocketToMe.receiveException(ret, ioe);
                      }

                      @Override
                      public void receiveSelectResult(final P2PSocket<LowerIdentifier> socket, boolean canRead, boolean canWrite) throws IOException {
                        if (!canRead) throw new IOException("Can't read!");
                        if (canWrite) throw new IOException("Never asked to write!");
                        if (socket.read(responseBuffer) == -1) {
                          // socket unexpectedly closed
                          socket.close();
                          deliverSocketToMe.receiveException(ret, new ClosedChannelException("Remote node closed socket while opening.  Try again."));
                          return;
                        }

                        if (responseBuffer.remaining() > 0) {
                          socket.register(true, false, this);
                        } else {
                          byte answer = responseBuffer.array()[0];
                          if (answer == FAILURE) {
                            // wrong address, read more
                            logger.info("openSocket({},{}) answer = FAILURE", i, deliverSocketToMe);
                            UpperIdentifier newDest = serializer.deserialize(new SocketInputBuffer(socket, localIdentifier.length), i);

                            // the socket's options: they may carry the peer's certificate, which vouches for newDest
                            if (identity.addBinding(newDest, i, socket.getOptions())) {
                            } else {
                              // This is really bad if we get here, because someone responded to us who is dead, maybe a big network lag could cause this?
                              socket.close();
                              deliverSocketToMe.receiveException(ret, new NodeIsFaultyException(i));
                            }

                            // need to do this so the bootstrapper knows the proper identity
                            // done in addBinding now
                            deliverSocketToMe.receiveException(ret, new NodeIsFaultyException(i));
                          } else {
                            ret.setSubCancellable(() -> {
                              throw new IllegalStateException("Can't cancel, already delivered. ret:"+ret+" sock:"+socket);
                            }); // can't cancel any more
                            if (bindStrategy != null && options != null && !(options.containsKey(DONT_VERIFY) && ((Boolean)options.get(DONT_VERIFY)).booleanValue())) {
                              // the handshake is done, so the socket's options may now carry the peer's certificate
                              // subject: let the strategy check the destination against it (an outgoing connection
                              // authenticates the peer just as an incoming one does)
                              UpperIdentifier dest = identity.getIntendedDest(options);
                              if (dest != null && !bindStrategy.accept(dest, i, socket.getOptions())) {
                                socket.close();
                                deliverSocketToMe.receiveException(ret, new NodeIsFaultyException(i));
                                return;
                              }
                            }
                            deliverSocketToMe.receiveResult(ret, socket);
                          }
                        }
                      }
                    });
                  }
                }

                @Override
                public void receiveException(P2PSocket<LowerIdentifier> socket, Exception ioe) {
                  deliverSocketToMe.receiveException(ret, ioe);
                }
              }.receiveSelectResult(sock, false, true);
            } catch (IOException ioe) {
              deliverSocketToMe.receiveException(ret, ioe);
            }
          }

          @Override
          public void receiveException(SocketRequestHandle<LowerIdentifier> s, Exception ex) {
            deliverSocketToMe.receiveException(ret, ex);
          }

        },
        options));
    return ret;
  }

  @Override
  public void incomingSocket(P2PSocket<LowerIdentifier> s) throws IOException {
    logger.debug("incomingSocket({})", s);

    // see if it wants to verify us
    new P2PSocketReceiver<LowerIdentifier>() {
      ByteBuffer buf = ByteBuffer.allocate(1);

      @Override
      public void receiveException(P2PSocket<LowerIdentifier> socket, Exception ioe) {
        errorHandler.receivedException(socket.getIdentifier(), ioe);
      }

      @Override
      public void receiveSelectResult(P2PSocket<LowerIdentifier> socket, boolean canRead, boolean canWrite) throws IOException {
        if (socket.read(buf) < 0) {
          // socket closed
          if (logger.isInfoEnabled()) errorHandler.receivedException(socket.getIdentifier(), new IOException("Socket closed while incoming."));
          return;
        }
        if (buf.hasRemaining()) {
          // need to read more
          socket.register(true, false, this);
          return;
        }

        buf.clear();
        byte wantsToVerifyB = buf.get();
        final boolean wantsToVerify = (wantsToVerifyB == (byte)1);

        new P2PSocketReceiver<LowerIdentifier>() {
          ByteBuffer buf = ByteBuffer.allocate(localIdentifier.length);

          @Override
          public void receiveException(P2PSocket<LowerIdentifier> socket, Exception ioe) {
            errorHandler.receivedException(socket.getIdentifier(), ioe);
          }

          @Override
          public void receiveSelectResult(final P2PSocket<LowerIdentifier> socket, boolean canRead, boolean canWrite) throws IOException {
            if (canWrite) throw new IOException("Never asked to write!");
            if (!canRead) throw new IOException("Can't read!");
            if (wantsToVerify) {
                // the node is sending who he thinks he is talking to

              // read the TO field
              if (socket.read(buf) < 0) {
                // socket closed
                if (logger.isInfoEnabled()) errorHandler.receivedException(socket.getIdentifier(), new IOException("Socket closed while incoming."));
                return;
              }

              if (buf.hasRemaining()) {
                // need to read more
                socket.register(true, false, this);
                return;
              }

              if (!Arrays.equals(buf.array(), localIdentifier)) {
                logger.atInfo().log(() -> "incomingSocket() FAILURE expected "+
                      Arrays.toString(buf.array())+" me:"+
                      Arrays.toString(localIdentifier));

                // not expecting me, send failure
                byte[] result = new byte[1+localIdentifier.length];
                result[0] = FAILURE;
                System.arraycopy(localIdentifier, 0, result, 1, localIdentifier.length);
                final ByteBuffer writeMe = ByteBuffer.wrap(result);
                new P2PSocketReceiver<LowerIdentifier>() {
                  @Override
                  public void receiveException(P2PSocket<LowerIdentifier> socket, Exception ioe) {
                    errorHandler.receivedException(socket.getIdentifier(), ioe);
                  }

                  @Override
                  public void receiveSelectResult(P2PSocket<LowerIdentifier> socket, boolean canRead, boolean canWrite) throws IOException {
                    if (canRead) throw new IOException("Not expecting to read.");
                    if (!canWrite) throw new IOException("Expecting to write.");

                    if (socket.write(writeMe) == -1) {
                      // socket closed
                      if (logger.isInfoEnabled()) errorHandler.receivedException(socket.getIdentifier(), new ClosedChannelException("Error on incoming socket."));
                      return;
                    }

                    if (buf.hasRemaining()) {
                      // need to read more
                      socket.register(false, true, this);
                      return;
                    }
                  }
                }.receiveSelectResult(socket, false, true); // write failure
                return;
              }
            } else {
              // doesn't want to verify
              logger.info("Connection from {} didn't want to verify our identity.", socket.getIdentifier());
            }
            // either he doesn't want to verify, or the TO was me,
            // now read the FROM, and add the proper index into the options
            final SocketInputBuffer sib = new SocketInputBuffer(socket, 1024);
            new P2PSocketReceiver<LowerIdentifier>() {
              @Override
              public void receiveException(P2PSocket<LowerIdentifier> socket, Exception ioe) {
                errorHandler.receivedException(socket.getIdentifier(), ioe);
              }

              @Override
              public void receiveSelectResult(P2PSocket<LowerIdentifier> socket, boolean canRead, boolean canWrite) throws IOException {
                if (canWrite) throw new IOException("Never asked to write!");
                if (!canRead) throw new IOException("Can't read!");

                final @Nullable Map<String, Object> newOptions = OptionsFactory.copyOptions(socket.getOptions());

                UpperIdentifier from;
                try {
                  // add to intendedDest, add option index
                  from = serializer.deserialize(sib, socket.getIdentifier());
                  newOptions.put(NODE_HANDLE_FROM_INDEX, from);
                } catch (InsufficientBytesException ibe) {
                  socket.register(true, false, this);
                  return;
                }

                // once we are here, we have succeeded in deserializing the NodeHanlde, and added it to the new options
                if (identity.addBinding(from, socket.getIdentifier(), socket.getOptions())) {
                  // no problem, sending message
                } else {
                  // this is bad, a previous instance is trying to open a socket
                  logger.atWarn().log(() -> "Serious error.  There was an attempt to open a socket from a supposedly stale identifier:"+
                        from+". Current identifier is "+bindings.get(serializer.translateUp(socket.getIdentifier()))+" lower:"+socket.getIdentifier());
                  socket.close();
                  return;
                }


                // now write Success
                byte[] result = {SUCCESS};
                final ByteBuffer writeMe = ByteBuffer.wrap(result);
                new P2PSocketReceiver<LowerIdentifier>() {
                  @Override
                  public void receiveException(P2PSocket<LowerIdentifier> socket, Exception ioe) {
                    if (logger.isInfoEnabled()) errorHandler.receivedException(socket.getIdentifier(), ioe);
                  }

                  @Override
                  public void receiveSelectResult(P2PSocket<LowerIdentifier> socket, boolean canRead, boolean canWrite) throws IOException {
                    if (canRead) throw new IOException("Not expecting to read.");
                    if (!canWrite) throw new IOException("Expecting to write.");

                    if (socket.write(writeMe) == -1) {
                      // socket closed
                      if (logger.isInfoEnabled()) errorHandler.receivedException(socket.getIdentifier(), new ClosedChannelException("Error on incoming socket."));
                      return;
                    }

                    if (writeMe.hasRemaining()) {
                      // need to read more
                      socket.register(false, true, this);
                      return;
                    }

                    // done writing pass up socket with the newOptions
                    final P2PSocket<LowerIdentifier> returnMe = new SocketWrapperSocket<LowerIdentifier, LowerIdentifier>(
                        socket.getIdentifier(), socket, logger, errorHandler, newOptions);
                    callback.incomingSocket(returnMe);
                  }
                }.receiveSelectResult(socket, false, true); // write SUCCESS

              }
            }.receiveSelectResult(socket, true, false);  // read the FROM
          }
        }.receiveSelectResult(socket, true, false); // read the TO (if wantsToVerify)
      }
    }.receiveSelectResult(s, true, false); // read wantsToVerify
  }

  /**
   * Head the message with the expected identifier
   */
  @Override
  public MessageRequestHandle<LowerIdentifier, ByteBuffer> sendMessage(
      final LowerIdentifier i,
      ByteBuffer m,
      final @Nullable MessageCallback<LowerIdentifier, ByteBuffer> deliverAckToMe,
      final @Nullable Map<String, Object> options) {

    if (logger.isTraceEnabled()) {
      byte[] b = ByteBuffers.peekRemaining(m);
      logger.trace("sendMessage("+i+","+m+")"+Arrays.toString(b));
    } else {
      logger.debug("sendMessage({},{})", i, m);
    }

    // what happens if they cancel after the socket has been received by a lower layer, but is still reading the header?
    // May need to re-think this SRHI at all the layers
    final MessageRequestHandleImpl<LowerIdentifier, ByteBuffer> ret =
      new MessageRequestHandleImpl<LowerIdentifier, ByteBuffer>(i, m, options);

    byte[] msgWithHeader;
    if (options != null && options.containsKey(NODE_HANDLE_FROM_INDEX)) {
      // don't include an id
      msgWithHeader = new byte[1+localIdentifier.length+m.remaining()];
      msgWithHeader[0] = NO_ID;

      System.arraycopy(localIdentifier, 0, msgWithHeader, 1, localIdentifier.length); // write the FROM
      m.get(msgWithHeader, 1+localIdentifier.length, m.remaining()); // write the message

    } else {
      // don't include an id
      UpperIdentifier dest = identity.getIntendedDest(options);

      if (dest == null) {
        if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, new MemoryExpiredException("No record of the upper identifier for "+i+" index="+IdentityImpl.index(options)));
        return ret;
      }

      if (identity.addBinding(dest, i, options)) {
        // no problem, sending message
      } else {
        if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, new NodeIsFaultyException(i, m));
        return ret;
      }

      byte[] destBytes;
      try {
        SimpleOutputBuffer sob = new SimpleOutputBuffer((int)(localIdentifier.length*2.5)); // good estimate
        serializer.serialize(sob, dest);
        destBytes = sob.getBytes();
      } catch (IOException ioe) {
        if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, ioe);
        return ret;
      }

      // build a new ByteBuffer with the header
      msgWithHeader = new byte[1+destBytes.length+localIdentifier.length+m.remaining()];
      msgWithHeader[0] = NORMAL; // write the TYPE
      System.arraycopy(destBytes, 0, msgWithHeader, 1, destBytes.length); // write the TO
      System.arraycopy(localIdentifier, 0, msgWithHeader, 1+destBytes.length, localIdentifier.length); // write the FROM
      m.get(msgWithHeader, 1+destBytes.length+localIdentifier.length, m.remaining()); // write the message
    }


    final ByteBuffer buf = ByteBuffer.wrap(msgWithHeader);

    ret.setSubCancellable(tl.sendMessage(i, buf,
        new MessageCallback<LowerIdentifier, ByteBuffer>(){

          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void ack(MessageRequestHandle<LowerIdentifier, ByteBuffer> msg) {
            if (ret.getSubCancellable() != null && msg != ret.getSubCancellable())
              throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+
                  msg+" sub:"+ret.getSubCancellable());
            if (deliverAckToMe != null) deliverAckToMe.ack(ret);
          }

          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void sendFailed(MessageRequestHandle<LowerIdentifier, ByteBuffer> msg, Exception ex) {
            if (ret.getSubCancellable() != null && msg != ret.getSubCancellable())
              throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+
                  msg+" sub:"+ret.getSubCancellable());
            if (deliverAckToMe == null) {
              errorHandler.receivedException(i, ex);
            } else {
              deliverAckToMe.sendFailed(ret, ex);
            }
          }

        }, options));
    return ret;
  }

  @Override
  public void messageReceived(LowerIdentifier i, ByteBuffer m, @Nullable Map<String, Object> options) throws IOException {
    @Nullable Map<String, Object> newOptions = OptionsFactory.copyOptions(options);

    if (!m.hasRemaining()) {
      throw new IOException("Empty message from " + i);
    }
    byte msgType = m.get();
    logger.debug("messageReceived({},{}):{}", i, m, msgType);
    switch(msgType) {
      case NORMAL:
        // this is a normal message, make sure it's for me
        if (m.remaining() < localIdentifier.length) {
          throw new IOException("Message from " + i + " is too short for the destination identity: " + m.remaining() + " bytes");
        }
        byte[] dest = new byte[localIdentifier.length];
        m.get(dest);
        if (!Arrays.equals(dest, localIdentifier)) {
          // send back an error
          logger.atInfo().log(() -> "received message for wrong node from:"+i+
              " intended:"+Arrays.toString(dest)+
              " me:"+Arrays.toString(localIdentifier));

          byte[] errorMessage = new byte[1+localIdentifier.length];
          errorMessage[0] = INCORRECT_IDENTITY;
          System.arraycopy(localIdentifier, 0, errorMessage, 1, localIdentifier.length);
          ByteBuffer buf = ByteBuffer.wrap(errorMessage);
          tl.sendMessage(i, buf, null, options);   // it's important to the rendezvous layer to reuse the options
          return;
        }
        // fall through: a message for me is read like one without an id

      case NO_ID:
        byte[] array = ByteBuffers.arrayForRead(m);
        SimpleInputBuffer sib = new SimpleInputBuffer(array, ByteBuffers.arrayPosition(m));
        UpperIdentifier from = serializer.deserialize(sib, i);
        ByteBuffers.setArrayPosition(m, array.length - sib.bytesRemaining());

        if (identity.addBinding(from, i, options)) {

          // need to do this so the bootstrapper knows the proper identity
        } else {
          logger.atWarn().log(() -> "Warning.  Received message from stale identifier:"+
              from+". Current identifier is "+bindings.get(serializer.translateUp(i))+" lower:"+i+" Probably a delayed message, dropping.");
          errorHandler.receivedUnexpectedData(i, array, ByteBuffers.arrayPosition(m), newOptions);
          return;
        }

        newOptions.put(NODE_HANDLE_FROM_INDEX, from);
        // continue to read the rest of the message

        // it's for me, no problem
        if (logger.isTraceEnabled()) {
          byte[] b = ByteBuffers.peekRemaining(m);
          logger.trace("received message for me from:"+from+"("+from+"("+i+")) "+Arrays.toString(b));
        }
        callback.messageReceived(i, m, newOptions);
        break;
      case INCORRECT_IDENTITY:
        // it's an error, read it in
        UpperIdentifier oldDest = bindings.get(serializer.translateUp(i));

        UpperIdentifier newDest = serializer.deserialize(new SimpleInputBuffer(ByteBuffers.arrayForRead(m), ByteBuffers.arrayPosition(m)), i);
        logger.info("received INCORRECT_IDENTITY:{} old:{} new:{}", i, oldDest, newDest);

        if (identity.addBinding(newDest, i, options)) { // should call setDeadForever

          // need to do this so the bootstrapper knows the proper identity
        }
    }
  }

  @Override
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  @Override
  public void acceptSockets(boolean b) {
    tl.acceptMessages(b);
  }

  @Override
  public LowerIdentifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  @Override
  public void setCallback(TransportLayerCallback<LowerIdentifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<LowerIdentifier> handler) {
    this.errorHandler = handler;
  }

  @Override
  public void destroy() {
    logger.info("destroy()");
    tl.destroy();
  }
}
