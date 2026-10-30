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
package org.mpisws.p2p.transport.peerreview.replay.record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
import org.mpisws.p2p.transport.peerreview.history.stub.NullHashProvider;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.Serializer;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.params.Parameters;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.processing.Processor;
import rice.environment.processing.sim.SimProcessor;
import rice.environment.random.RandomSource;
import rice.environment.time.TimeSource;
import rice.environment.time.simple.SimpleTimeSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.selector.SelectorManager;

import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_RECV;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SEND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_EXCEPTION;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_OPENED_OUTGOING;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_OPEN_INCOMING;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_OPEN_OUTGOING;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EX_TYPE_ClosedChannel;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EX_TYPE_IO;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EX_TYPE_Unknown;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SEQS_PER_MILLI;

public class RecordLayer<Identifier> implements TransportLayer<Identifier, ByteBuffer>,
  TransportLayerCallback<Identifier, ByteBuffer> {

  /**
   * The relevant length of the message.  Default:all (-1)
   */
  public static final String PR_RELEVANT_LEN = "pr_relevant_len";
  /**
   * If the message is relevant.  Default:true (1)
   */
  public static final String PR_RELEVANT_MSG = "pr_relevant_msg";


  Environment environment;
  TransportLayer<Identifier, ByteBuffer> tl;
  TransportLayerCallback<Identifier, ByteBuffer> callback;
  Serializer<Identifier> identifierSerializer;
  ErrorHandler<Identifier> handler;

  SecureHistory history;
  Logger logger;

  long lastLogEntry;

  int socketCtr = Integer.MIN_VALUE;

  public RecordLayer(TransportLayer<Identifier, ByteBuffer> tl, String name, Serializer<Identifier> serializer, Environment env) throws IOException {
    this.logger = env.getLogManager().getLogger(RecordLayer.class, null);
    this.identifierSerializer = serializer;

    NullHashProvider nhp = new NullHashProvider();
    SecureHistoryFactoryImpl shf = new SecureHistoryFactoryImpl(nhp, env);

    this.tl = tl;
    this.tl.setCallback(this);
    this.history = shf.create(name, 0, nhp.EMPTY_HASH);

    this.environment = env;
    this.lastLogEntry = -1;

    this.handler = new DefaultErrorHandler<Identifier>(logger);

    // no env.addDestructable(this): the TransportLayer already destroys this layer

  }

  /**
   * PeerReview only updates its internal clock when it returns to the main loop, but not
   * in between (e.g. while it is handling messages). When the clock needs to be
   * updated, this function is called.
   */
  public void updateLogTime() {
    long now = environment.getTimeSource().currentTimeMillis();

    if (now > lastLogEntry) {
      if (!history.setNextSeq(now * SEQS_PER_MILLI))
        throw new RuntimeException("PeerReview: Cannot roll back history sequence number from "+history.getLastSeq()+" to "+now*SEQS_PER_MILLI+"; did you change the local time?");

      lastLogEntry = now;
    }
  }

  /* Called by applications to log some application-specific event, such as PAST_GET. */

  public void logEvent(short type, ByteBuffer ... entry) throws IOException {
    if (history == null) return;
    if (logger.isTraceEnabled()) logger.trace("logging #{} t:{}", history.getNumEntries(), type, new Exception("Stack Trace"));
    updateLogTime();
    history.appendEntry(type, true, entry);
  }


  @Override
  public SocketRequestHandle<Identifier> openSocket(final Identifier i, final SocketCallback<Identifier> deliverSocketToMe, final Map<String, Object> options) {
    final int socketId = socketCtr++;
    final ByteBuffer socketIdBuffer = ByteBuffer.wrap(MathUtils.intToByteArray(socketId));
    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      identifierSerializer.serialize(i, sob);
      logEvent(EVT_SOCKET_OPEN_OUTGOING, socketIdBuffer, sob.getByteBuffer());
    } catch (IOException ioe) {
      logger.warn("openSocket({})", i, ioe);
    }

    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(i, options, logger);

    ret.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>(){
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> sock) {
        socketIdBuffer.clear();
        try {
          logEvent(EVT_SOCKET_OPENED_OUTGOING, socketIdBuffer);
        } catch (IOException ioe) {
          logger.warn("error logging in openSocket({})", i, ioe);
        }
        socketIdBuffer.clear();
        deliverSocketToMe.receiveResult(ret, new RecordSocket<Identifier>(i, sock, logger, options, socketId, socketIdBuffer, RecordLayer.this));
      }
      @Override
      public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
        socketIdBuffer.clear();
        try {
          logSocketException(socketIdBuffer, ex);
        } catch (IOException ioe) {
          logger.warn("openSocket({})@{}", i, socketId, ioe);
        }
        deliverSocketToMe.receiveException(ret, ex);
      }
    }, options));

    return ret;
  }

  @Override
  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    final int socketId = socketCtr++;
    final ByteBuffer socketIdBuffer = ByteBuffer.wrap(MathUtils.intToByteArray(socketId));
    try {
      socketIdBuffer.clear();
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      identifierSerializer.serialize(s.getIdentifier(), sob);
      logEvent(EVT_SOCKET_OPEN_INCOMING, socketIdBuffer, sob.getByteBuffer());
    } catch (IOException ioe) {
      logger.warn("incomingSocket({})", s.getIdentifier(), ioe);
    }

    callback.incomingSocket(new RecordSocket<Identifier>(s.getIdentifier(), s, logger, s.getOptions(), socketId, socketIdBuffer, RecordLayer.this));
  }

  @Override
  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(Identifier i, ByteBuffer m, MessageCallback<Identifier, ByteBuffer> deliverAckToMe, Map<String, Object> options) {
    if (logger.isTraceEnabled()) {
      logger.trace("sendMessage("+i+","+m+"):"+MathUtils.toHex(Arrays.copyOfRange(m.array(), m.arrayOffset()+m.position(), m.arrayOffset()+m.limit())), new Exception("Stack Trace"));
    } else logger.debug("sendMessage({},{})", i, m);
    // If the 'RELEVANT_MSG' flag is set to false, the message is passed through to the transport
    // layer. This is used e.g. for liveness/proximity pings in Pastry.
    if (options == null || !options.containsKey(PR_RELEVANT_MSG) || ((Integer)options.get(PR_RELEVANT_MSG)).intValue() != 0) {
      int position = m.position(); // mark the current position

      int relevantLen = m.remaining();

      // If someone sets relevantLen=-1, it means the whole message is relevant.
      if (options != null && options.containsKey(PR_RELEVANT_LEN) && ((Integer)options.get(PR_RELEVANT_LEN)).intValue() >= 0) {
        relevantLen = ((Integer)options.get(PR_RELEVANT_LEN)).intValue();
      }

      try {
        SimpleOutputBuffer sob = new SimpleOutputBuffer();
        identifierSerializer.serialize(i, sob);
        logEvent(EVT_SEND, sob.getByteBuffer(), m);
      } catch (IOException ioe) {
        logger.warn("sendMessage({},{})", i, m, ioe);
      }

      m.position(position); // set the incoming position
    }

    return tl.sendMessage(i, m, deliverAckToMe, options);

    // Pass the message to the Commitment protocol


  }

  @Override
  public void messageReceived(Identifier i, ByteBuffer m, Map<String, Object> options) throws IOException {
    try {
      if (identifierSerializer == null) {
        // just drop this event, it's while we're booting,
        // don't forward it or you will mess up the state of the state machine vs the log
        logger.warn("Dropping messageReceived({},{}) while booting", i, m);
        return;
      }
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      identifierSerializer.serialize(i, sob);
      logEvent(EVT_RECV, sob.getByteBuffer(), m);
    } catch (IOException ioe) {
      logger.warn("messageReceived({},{})", i, m, ioe);
    }

    callback.messageReceived(i, m, options);
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
  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.handler = handler;
  }

  @Override
  public void destroy() {
    try {
      if (history != null) history.close();
      history = null;
    } catch (IOException ioe) {
      logger.warn("Error destroying.", ioe);
    }
    tl.destroy();
  }

  public static Environment generateEnvironment() {
    return generateEnvironment(null);
  }

  public static Environment generateEnvironment(RandomSource rs) {
    Parameters params = new SimpleParameters(Environment.defaultParamFileArray,null);
    DirectTimeSource dts = new DirectTimeSource(System.currentTimeMillis());
    LogManager lm = Environment.generateDefaultLogManager();
    dts.setLogManager(lm);
    SelectorManager selector = new RecordSM("Default", new SimpleTimeSource(), dts,lm,rs);
    dts.setSelectorManager(selector);
    Processor proc = new SimProcessor(selector);
    Environment ret = new Environment(selector,proc,rs,dts,lm,
        params, Environment.generateDefaultExceptionStrategy(lm)) {

      @Override
      public Environment cloneEnvironment(String prefix, boolean cloneSelector, boolean cloneProcessor) {
        // new logManager
        DirectTimeSource dts = new DirectTimeSource(getTimeSource().currentTimeMillis());
        LogManager lman = getLogManager();
        if (lman instanceof Slf4jLogManager slm) {
          lman = slm.clone(prefix);
        }
        dts.setLogManager(lman);

        // new random source
        RandomSource rand = cloneRandomSource(lman);

        // new selector
        SelectorManager sman = cloneSelectorManager(prefix, dts, rand, lman, cloneSelector);

        // new processor
        Processor proc = cloneProcessor(prefix, lman, cloneProcessor);

        // build the environment
        Environment ret = new Environment(sman, proc, rand, dts, lman,
            getParameters(), getExceptionStrategy());

        // gain shared fate with the rootEnvironment
        addDestructable(ret);

        return ret;
      }



      // get the new environment to be separate from the old one
      @Override
      public Environment cloneEnvironment(String prefix) {
        return cloneEnvironment(prefix, true, true);
      }

      @Override
      protected SelectorManager cloneSelectorManager(String prefix, TimeSource ts, RandomSource rs, LogManager lman, boolean cloneSelector) {
        SelectorManager sman = getSelectorManager();
        if (cloneSelector) {
          sman = new RecordSM(prefix + " Selector", new SimpleTimeSource(), (DirectTimeSource)ts,lman,rs);
        }
        return sman;
      }

      @Override
      protected TimeSource cloneTimeSource(LogManager manager) {
        throw new RuntimeException("Operation not allowed.  Use the overridden clone method.");
      }
    };
    return ret;
  }

  public void logSocketException(ByteBuffer socketId, Exception ioe) throws IOException {
    logger.info("logSocketException({})", ioe, ioe);
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    String className = ioe.getClass().getName();
    if (className.endsWith("ClosedChannelException")) {
      sob.writeShort(EX_TYPE_ClosedChannel);
      sob.writeUTF(ioe.getMessage());
    } else if (className.equals("java.io.IOException")) {
      sob.writeShort(EX_TYPE_IO);
      sob.writeUTF(ioe.getMessage());
    } else {
      sob.writeShort(EX_TYPE_Unknown);
      sob.writeUTF(className);
      sob.writeUTF(ioe.getMessage());
    }

    ByteBuffer ioeBuffer = ByteBuffer.wrap(sob.getBytes());
    ioeBuffer.limit(sob.getWritten());
    logEvent(EVT_SOCKET_EXCEPTION, socketId, ioeBuffer);
  }
}
