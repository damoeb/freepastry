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
package org.mpisws.p2p.transport.commonapi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.priority.QueueOverflowException;
import org.mpisws.p2p.transport.util.ByteBuffers;
import org.mpisws.p2p.transport.util.DefaultCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

public class CommonAPITransportLayerImpl<Identifier extends NodeHandle> implements
    CommonAPITransportLayer<Identifier>,
    TransportLayerCallback<Identifier, ByteBuffer> /*,
    LivenessListener<Identifier>,
    ProximityListener<Identifier> */{

  TransportLayer<Identifier, ByteBuffer> tl;
  TransportLayerCallback<Identifier, RawMessage> callback;
  ErrorHandler<Identifier> errorHandler;
  RawMessageDeserializer deserializer;
  Logger logger;

  public static final String MSG_CLASS = "commonapi_msg_class";
  public static final String MSG_STRING = "commonapi_msg_string";
  public static final String MSG_TYPE = "commonapi_msg_type";
  public static final String MSG_ADDR = "commonapi_msg_addr";

  private OptionsAdder optionsAdder;

  public CommonAPITransportLayerImpl(
      TransportLayer<Identifier, ByteBuffer> tl,
      IdFactory idFactory,
      RawMessageDeserializer deserializer,
      OptionsAdder optionsAdder,
      ErrorHandler<Identifier> errorHandler,
      Environment env) {

    this.logger = env.getLogManager().getLogger(CommonAPITransportLayerImpl.class, null);
    this.tl = tl;
    this.deserializer = deserializer;
    this.optionsAdder = optionsAdder;
    if (this.optionsAdder == null) {
      this.optionsAdder = (options, m) -> OptionsFactory.addOption(options, MSG_STRING, m.toString(), MSG_TYPE, m.getType(), MSG_CLASS, m.getClass().getName());
    }

    if (tl == null) throw new IllegalArgumentException("tl must be non-null");
    if (idFactory == null) throw new IllegalArgumentException("idFactory must be non-null");
    if (deserializer == null) throw new IllegalArgumentException("deserializer must be non-null");

    this.errorHandler = errorHandler;

    if (this.callback == null) {
      this.callback = new DefaultCallback<Identifier, RawMessage>(env);
    }
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    }


    tl.setCallback(this);

  }

  @Override
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  @Override
  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  @Override
  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  @Override
  public @Nullable MessageRequestHandle<Identifier, RawMessage> sendMessage(
      final Identifier i,
      final RawMessage m,
      final @Nullable MessageCallback<Identifier, RawMessage> deliverAckToMe,
      @Nullable Map<String, Object> options) {
    if (destroyed) return null;

    logger.debug("sendMessage({},{})", i, m);

    final MessageRequestHandleImpl<Identifier, RawMessage> handle
      = new MessageRequestHandleImpl<Identifier, RawMessage>(i, m, options);
    final ByteBuffer buf;

    // we only serialize the Id, we assume the underlying layer got the address of the NodeHandle correct
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      // TODO: maybe we should write my entire address to be compatible with the lower levels, why do we need to do this at all?
      // Is the contract that the lower level's identifier is proper?
      // What about the fact that the priorityTL will establish end to end connectivity?
      // Perhaps this is wasteful.
      // cant put the PriorityTL above this because this is where we serialize

      deserializer.serialize(m, sob);
    } catch (IOException ioe) {
      if (ioe instanceof NodeIsFaultyException) {
        ioe = new NodeIsFaultyException(i,m, ioe);
      }
      if (deliverAckToMe == null) {
        errorHandler.receivedException(i, ioe);
      } else {
        deliverAckToMe.sendFailed(handle, ioe);
      }
      return handle;
    }

    buf = ByteBuffer.wrap(sob.getBytes());
    logger.trace("sendMessage({},{}) serialized:{}", i, m, buf);

    handle.setSubCancellable(tl.sendMessage(
        i,
        buf,
        new MessageCallback<Identifier, ByteBuffer>() {

          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
            logger.trace("sendMessage({},{}).ack()", i, m);
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe != null) deliverAckToMe.ack(handle);
          }

          @Override
          @SuppressWarnings("ReferenceEquality") // the same instance is meant
          public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception ex) {
            if (ex instanceof NodeIsFaultyException) {
              ex = new NodeIsFaultyException(i, m, ex);
            }
            if (ex instanceof QueueOverflowException) {
              ex = new QueueOverflowException(i, m, ex);
            }
            logger.info("sendFailed({},{})", i, m, ex);
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe == null) {
              errorHandler.receivedException(i, ex);
            } else {
              deliverAckToMe.sendFailed(handle, ex);
            }
          }
        },
        optionsAdder.addOptions(options,m)));
    return handle;
  }

  @Override
  public void messageReceived(Identifier i, ByteBuffer m, @Nullable Map<String, Object> options) throws IOException {
    SimpleInputBuffer buf = new SimpleInputBuffer(ByteBuffers.arrayForRead(m), ByteBuffers.arrayPosition(m));
    RawMessage ret = deserializer.deserialize(buf, i);
    logger.debug("messageReceived({},{})", i, ret);
    callback.messageReceived(i, ret, options);
  }

  @Override
  public void setCallback(
      TransportLayerCallback<Identifier, RawMessage> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  private boolean destroyed = false;
  @Override
  public void destroy() {
    destroyed = true;
    tl.destroy();
  }




  @Override
  public @Nullable SocketRequestHandle<Identifier> openSocket(
      final Identifier i,
      final SocketCallback<Identifier> deliverSocketToMe,
      @Nullable Map<String, Object> options) {
    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");

    logger.debug("openSocket({})", i);
    return tl.openSocket(i, deliverSocketToMe, options);

    // we only serialize the Id, we assume the underlying layer got the address of the NodeHandle correct
  }

  @Override
  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    logger.debug("incomingSocket({})", s);
    callback.incomingSocket(s);
  }
}
