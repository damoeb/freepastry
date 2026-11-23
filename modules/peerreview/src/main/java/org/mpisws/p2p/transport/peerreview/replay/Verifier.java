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
package org.mpisws.p2p.transport.peerreview.replay;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.replay.playback.ReplaySelectorManager;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.rawserialization.WireLengths;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_ACK;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_CHECKPOINT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_INIT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_RECV;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SEND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SENDSIGN;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SIGN;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_CAN_READ;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_CAN_RW;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_CAN_WRITE;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_CLOSE;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_CLOSED;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_EXCEPTION;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_OPENED_OUTGOING;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_OPEN_INCOMING;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_OPEN_OUTGOING;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_READ;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_SHUTDOWN_OUTPUT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SOCKET_WRITE;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_VRF;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EX_TYPE_ClosedChannel;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EX_TYPE_IO;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EX_TYPE_Unknown;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SEQS_PER_MILLI;
import org.mpisws.p2p.transport.TransportLayer;

public class Verifier<Handle extends RawSerializable, Identifier extends RawSerializable>
    implements TransportLayer<Handle, ByteBuffer> {
  /** Event types are one unsigned byte. */
  private static final int NUM_EVENT_TYPES = 256;

  /**
   * Maps an event type (EVT_*) to its EventCallback
   */
  Map<Short, EventCallback> eventCallback = new HashMap<Short, EventCallback>();

  /**
   * So we can call back when we get an ack.
   */
  Map<Long, VerifierMessageRequestHandle<Handle>> callbacks = new HashMap<Long, VerifierMessageRequestHandle<Handle>>();

  private final Handle localHandle;
  private final SecureHistory history;
  PeerReviewCallback<Handle, Identifier> app;
  boolean foundFault;

  /**
   * The seq of the first entry replayed (the starting CHECKPOINT or INIT). SENDs logged before it are
   * outside the replay window.
   */
  private final long windowStartSeq;

  long nextEventIndex;
  IndexEntry next;
  InputBuffer nextEvent;

  boolean initialized;
  int[] eventToCallback = new int[NUM_EVENT_TYPES];
  private final Logger logger;
  private final IdentityTransport<Handle, Identifier> transport;

  PeerReview<Handle, Identifier> peerreview;

  Environment environment;


  public Verifier(
      PeerReview<Handle, Identifier> peerreview,
      Environment env,
      SecureHistory history,
      Handle localHandle,
      long firstEntryToReplay,
      Object extInfo) throws IOException {
    if (!(env.getSelectorManager() instanceof ReplaySelectorManager)) {
      throw new IllegalArgumentException("Environment.getSelectorManager() must be a ReplaySelectorManager, was a "+env.getSelectorManager().getClass());
    }
    this.environment = env;
    this.logger = environment.getLogManager().getLogger(Verifier.class, localHandle.toString());
    this.history = history;
    this.app = null;
    this.transport = peerreview;
    this.peerreview = peerreview;
    this.localHandle = localHandle;
    this.foundFault = false;
    this.nextEventIndex = firstEntryToReplay-1;
    this.initialized = false;

    for (int i=0; i<NUM_EVENT_TYPES; i++)
      eventToCallback[i] = -1;

    fetchNextEvent();
    if (next == null) {
      foundFault = true;
    }
    this.windowStartSeq = next == null ? Long.MIN_VALUE : next.getSeq();
  }

  /**
   * Fetch the next log entry, or set the EOF flag
   */
  protected void fetchNextEvent() {
    next = null;
    nextEventIndex++;

    try {
      next = history.statEntry(nextEventIndex);
    } catch (IOException ioe) {
      logger.warn("Error fetching log entry #{}", nextEventIndex, ioe);
      foundFault = true;
      return;
    }

    logger.debug("fetchNextEvent():{}", next);

    if (next == null) {
      return;
    }

    if (next.isHashed()) {
      // make the nextEvent only the content hash

      logger.debug("Fetched log entry #{} (type {}, hashed, seq={})", nextEventIndex, next.getType(), next.getSeq());
    } else {
      // load the nextEvent from the file

      try {
        nextEvent = new SimpleInputBuffer(history.getEntry(nextEventIndex, next.getSizeInFile()));
      } catch (IOException ioe) {
        logger.warn("Error fetching log entry #{} (type {}, size {} bytes, seq={})", nextEventIndex, next.getType(), next.getSizeInFile(), next.getSeq(), ioe);
        foundFault = true;
        return;
      }
      logger.debug("Fetched log entry #{} (type {}, size {} bytes, seq={})", nextEventIndex, next.getType(), next.getSizeInFile(), next.getSeq());
    }
  }

  public boolean isVerificationSuccessful() {
    return !foundFault;
  };


  @SuppressWarnings("unchecked")
  public void setApplication(PeerReviewCallback<Handle, ?> app) {
    this.app = (PeerReviewCallback<Handle, Identifier>)app;
  }

  /**
   * This is called by the Audit protocol to make another replay step; it returns true
   * if further calls are necessary, and false if the replay has finished. The idea
   * is that we can stop calling this if there is more important work to do, e.g.
   * handle foreground requests
   */
  public boolean makeProgress() {
    logger.debug("makeProgress()");
    if (foundFault || next == null)
      return false;

    if (!initialized && (next.getType() != EVT_CHECKPOINT) && (next.getType() != EVT_INIT)) {
      logger.warn("Replay: No INIT or CHECKPOINT found at the beginning of the log; marking as invalid {}", next);
      foundFault = true;
      return false;
    }

    /**
     * Handle any pending timers. Note that we have to be sure to call them in the exact same
     * order as in the main code; otherwise there can be subtle bugs and side-effects.
     */
    // This code is the job of the SelectorLoop, it's done in the super class of ReplaySelectorManager

    if (next == null)
      return false;

    /* Sanity checks */

    logger.trace("Replaying event #{} (type {}, seq={})", nextEventIndex, next.getType(), next.getSeq());

    if (next.isHashed() && (next.getType() != EVT_CHECKPOINT) && (next.getType() != EVT_INIT)) {
      logger.warn("Replay: Trying to replay hashed event {}", next.getType());
      foundFault = true;
      return false;
    }

    /* Replay the next event */

    try {
      switch (next.getType()) {
      case EVT_VRF: {/* VRF events should have been handled by PRNG::checker */
        fetchNextEvent();
        break;
      }

      case EVT_SEND : /* SEND events should have been handled by Verifier::send() */
        logger.warn("Replay: Encountered EVT_SEND evt #{}; marking as invalid", nextEventIndex, new Exception("Stack Trace"));
        foundFault = true;
        return false;
      case EVT_SOCKET_READ: {
        logger.warn("Replay: Encountered EVT_SOCKET_READ evt #{}; marking as invalid", nextEventIndex, new Exception("Stack Trace"));
        foundFault = true;
        return false;
      }
      case EVT_SOCKET_CLOSE: {
        logger.warn("Replay: Encountered EVT_SOCKET_CLOSE evt #{}; marking as invalid", nextEventIndex, new Exception("Stack Trace"));
        foundFault = true;
        return false;
      }
      case EVT_SOCKET_SHUTDOWN_OUTPUT: {
        logger.warn("Replay: Encountered EVT_SOCKET_SHUTDOWN_OUTPUT evt #{}; marking as invalid", nextEventIndex, new Exception("Stack Trace"));
        foundFault = true;
        return false;
      }

      case EVT_RECV : /* Incoming message; feed it to the state machine */
        Handle sender = peerreview.getHandleSerializer().deserialize(nextEvent);
        nextEvent.readLong(); // the sender's seq
        nextEvent.readBoolean(); // whether the entry is partly hashed

        int msgLen = nextEvent.bytesRemaining();

        byte[] msgBytes = WireLengths.readBytes(nextEvent, msgLen);
        ByteBuffer msgBuf = ByteBuffer.wrap(msgBytes);

        /* The next event is going to be a SIGN; skip it, since it's irrelevant here */

        fetchNextEvent();
        if (next == null || (next.getType() != EVT_SIGN) || (next.getSizeInFile() != (int)(transport.getHashSizeBytes()+transport.getSignatureSizeBytes()))) {
          logger.warn("Replay: RECV event not followed by SIGN; marking as invalid");
          foundFault = true;
          return false;
        }

        fetchNextEvent();

        /* Deliver the message to the state machine */

        app.messageReceived(sender, msgBuf, null);
        break;
      case EVT_SIGN : /* SIGN events should have been handled by the preceding RECV */
        logger.warn("Replay: Spurious SIGN event; marking as invalid");
        foundFault = true;
        return false;
      case EVT_ACK : /* Acknowledge the SEND replayed with the acked seq */
        peerreview.getIdSerializer().deserialize(nextEvent); // the acking node
        long ackedSeq = nextEvent.readLong();
        VerifierMessageRequestHandle<Handle> foo = callbacks.remove(ackedSeq);
        if (foo != null) {
          foo.ack();
        } else if (ackedSeq < windowStartSeq) {
          /* The SEND was logged before the replay window, so the replay never saw it */
          logger.debug("Replay: ACK for seq:{} before the replay window at seq:{}; skipping", ackedSeq, windowStartSeq);
        } else {
          logger.warn("Replay: no message to be acked for seq:{} fail.", ackedSeq);
          foundFault = true;
          return false;
        }
        fetchNextEvent();
        break;
      case EVT_SENDSIGN : /* Skip SENDSIGN events; they are not relevant during replay */
        fetchNextEvent();
        break;
      case EVT_CHECKPOINT : /* Verify CHECKPOINTs */
        if (!initialized) {
          if (!next.isHashed()) {

            /* If the state machine hasn't been initialized yet, we can use this checkpoint */

            initialized = true;

            if (!app.loadCheckpoint(nextEvent)) {
              logger.warn("Cannot load checkpoint");
              foundFault = true;
            }
          } else {
            logger.warn("Replay: Initial checkpoint is hashed; marking as invalid");
            foundFault = true;
          }
        } else {

          /* Ask the state machine to do a checkpoint now ... */

          SimpleOutputBuffer buf = new SimpleOutputBuffer();
          app.storeCheckpoint(buf);
          int actualCheckpointSize = buf.getWritten();

          /* ... and compare it to the contents of the CHECKPOINT entry */

          if (!next.isHashed()) {
            if (actualCheckpointSize != next.getSizeInFile()) {
              logger.warn("Replay: Checkpoint has different size (expected {} bytes, but got {}); marking as invalid", next.getSizeInFile(), actualCheckpointSize);
              logger.debug("Expected:{}", nextEvent);
              logger.debug("Found:{}", actualCheckpointSize);
              foundFault = true;
              return false;
            }


            // compare the checkpoints
            byte[] bar = WireLengths.readBytes(nextEvent, actualCheckpointSize);

            if (!Arrays.equals(bar, buf.getBytes())) {
              logger.warn("Replay: Checkpoint does not match");
              logger.debug("Expected:{}", next.getSizeInFile());
              logger.debug("Found:{}", buf.getWritten());

              foundFault = true;
              return false;
            }
          } else {

            byte[] checkpointHash = transport.hash(buf.getByteBuffer());
            if (!Arrays.equals(checkpointHash, next.getContentHash())) {
              logger.atWarn().log(() -> "Replay: Checkpoint is hashed, but does not match hash value in the log\n"+
                  Arrays.toString(checkpointHash)+"\n"+Arrays.toString(next.getContentHash()));
              foundFault = true;
              return false;
            }

            logger.trace("Hashed checkpoint is OK");
            history.upgradeHashedEntry((int)nextEventIndex, buf.getByteBuffer());
          }
        }

        fetchNextEvent();
        break;
      case EVT_INIT: /* State machine is reinitialized; issue upcall */
        initialized = true;
        app.init();
        fetchNextEvent();
        break;
      case EVT_SOCKET_OPEN_INCOMING: {
        int socketId = nextEvent.readInt();
        Handle opener = peerreview.getHandleSerializer().deserialize(nextEvent);
        fetchNextEvent();
        incomingSocket(opener, socketId);
        break;
      }
      case EVT_SOCKET_OPENED_OUTGOING: {
        int socketId = nextEvent.readInt();
        fetchNextEvent();
        socketOpened(socketId);
        break;
      }
      case EVT_SOCKET_CAN_READ: {
        int socketId = nextEvent.readInt();
        fetchNextEvent();
        socketIO(socketId, true, false);
        break;
      }
      case EVT_SOCKET_CAN_WRITE: {
        int socketId = nextEvent.readInt();
        fetchNextEvent();
        socketIO(socketId, false, true);
        break;
      }
      case EVT_SOCKET_CAN_RW: {
        int socketId = nextEvent.readInt();
        fetchNextEvent();
        socketIO(socketId, true, true);
        break;
      }
      case EVT_SOCKET_EXCEPTION: {
        int socketId = nextEvent.readInt();
        IOException ex = deserializeException(nextEvent);
        logger.info("deserializeException("+ex+")");
        fetchNextEvent();
        socketException(socketId, ex);
        break;
      }
      default:
        if (!eventCallback.containsKey(next.getType())) {
          logger.warn("Replay({}): Unregistered event #{}; marking as invalid", nextEventIndex, next.getType());
          foundFault = true;
          return false;
        }

        IndexEntry temp = next;
        InputBuffer tempEvent = nextEvent;
        fetchNextEvent();
        eventCallback.get(temp.getType()).replayEvent(temp.getType(), tempEvent);
        break;
      }// switch
    } catch (IOException ioe) {
      logger.warn("Exception handling event #{} {}", nextEventIndex, next, ioe);
      foundFault = true;
      return false;
    }

    return true;
  }

  /**
   * Called by the state machine when it wants to send a message
   */
  @Override
  public MessageRequestHandle<Handle, ByteBuffer> sendMessage(
      Handle target, ByteBuffer message, MessageCallback<Handle, ByteBuffer> deliverAckToMe,
      Map<String, Object> options) {
    try {

    int msgLen = message.remaining();
    int pos = message.position();

    int relevantLen = message.remaining();
    if (options != null && options.containsKey(PeerReview.RELEVANT_LENGTH)) {
      relevantLen = (Integer)options.get(PeerReview.RELEVANT_LENGTH);
    }

    logger.debug("Verifier::send({}, {}/{} bytes)", target, relevantLen, message.remaining());

    // Sanity checks

    if (next == null) {
      logger.warn("Replay: Send event after end of segment; marking as invalid");
      foundFault = true;
      return null;
    }

    if (next.getType() == EVT_INIT) {
      logger.trace("Skipped; next event is an INIT");
      return null;
    }

    if (next.getType() != EVT_SEND) {
      logger.warn("Replay({}): SEND event during replay, but next event in log is #{}; marking as invalid", nextEventIndex, next.getType());
      foundFault = true;
      return null;
    }

    long sendSeq = next.getSeq();
    VerifierMessageRequestHandle<Handle> ret = new VerifierMessageRequestHandle<Handle>(target,message,deliverAckToMe,options);
    callbacks.put(sendSeq,ret);

    // If the SEND is hashed, simply compare it to the predicted entry

    if (next.isHashed()) {
      SimpleOutputBuffer buf = new SimpleOutputBuffer();
      Identifier targetId = peerreview.getIdentifierExtractor().extractIdentifier(target);
      targetId.serialize(buf);
      buf.writeBoolean(relevantLen<msgLen);
      buf.write(message.array(), message.arrayOffset()+message.position(), relevantLen);

      if (relevantLen<msgLen) {
        // the irrelevant part is logged as its hash
        ByteBuffer irrelevant = message.duplicate();
        irrelevant.position(pos+relevantLen);
        byte[] hash = transport.hash(irrelevant);
        buf.write(hash, 0, hash.length);
      }
      byte[] cHash = transport.hash(buf.getByteBuffer());
      if (!Arrays.equals(cHash,next.getContentHash())) {
        logger.warn("Replay: SEND is hashed, but hash of predicted SEND entry does not match hash in the log");
        foundFault = true;
        return null;
      }

      fetchNextEvent();
      assert(next.getType() == EVT_SENDSIGN);

      fetchNextEvent();
      return ret;
    }

    // Are we sending to the same destination?
    Handle logReceiver;
     logReceiver = peerreview.getHandleSerializer().deserialize(nextEvent);
    if (!logReceiver.equals(target)) {
      logger.warn("Replay({}): SEND to {} during replay, but log shows SEND to {}; marking as invalid", nextEventIndex, target, logReceiver);
      // reset nextEvent so next time we parse this event it isn't nonsense
      nextEvent = new SimpleInputBuffer(history.getEntry(next, next.getSizeInFile()));
      foundFault = true;
      return ret;
    }

    // Check the message against the message in the log
    boolean logIsHashed = nextEvent.readBoolean();

    if (logIsHashed) {
      if (relevantLen >= msgLen) {
        logger.warn("Replay: Message sent during replay is entirely relevant, but log entry is partly hashed; marking as invalid");
        foundFault = true;
        return null;
      }

      int logRelevantLen = nextEvent.bytesRemaining() - transport.getHashSizeBytes();
      assert(logRelevantLen >= 0);

      if (relevantLen != logRelevantLen) {
        logger.warn("Replay: Message sent during replay has {} relevant bytes, but log entry has {}; marking as invalid", relevantLen, logRelevantLen);
        foundFault = true;
        return null;
      }

      byte[] loggedMsg = WireLengths.readBytes(nextEvent, logRelevantLen);
      ByteBuffer loggedMsgBB = ByteBuffer.wrap(loggedMsg);
      ByteBuffer relevant = message.duplicate();
      relevant.limit(pos+relevantLen);
      if ((relevantLen > 0) && !relevant.equals(loggedMsgBB)) {
        logger.warn("Replay: Relevant part of partly hashed message differs");
        logger.debug("Expected: [{}]", loggedMsgBB);
        logger.debug("Actual:   [{}]", relevant);
        foundFault = true;
        return null;
      }

      byte[] logHash = WireLengths.readBytes(nextEvent, transport.getHashSizeBytes());
      ByteBuffer irrelevant = message.duplicate();
      irrelevant.position(pos+relevantLen);
      byte[] msgHash = transport.hash(irrelevant);
      if (!Arrays.equals(msgHash, logHash)) {
        logger.warn("Replay: Hashed part of partly hashed message differs");
        logger.debug("Expected: [{}]", logHash);
        logger.debug("Actual:   [{}]", msgHash);
        foundFault = true;
        return null;
      }
    } else {
      if (relevantLen < msgLen) {
        logger.warn("Replay: Message sent during replay is only partly relevant, but log entry is not hashed; marking as invalid");
        foundFault = true;
        return null;
      }

      int logMsglen = nextEvent.bytesRemaining();
      if (msgLen != logMsglen) {
        logger.warn("Replay: Message sent during replay has {} bytes, but log entry has {}; marking as invalid", msgLen, logMsglen);
        foundFault = true;
        return null;
      }

      byte[] loggedMsg = WireLengths.readBytes(nextEvent, nextEvent.bytesRemaining());
      byte[] sentMsg = new byte[message.remaining()];
      message.get(sentMsg);

      if (loggedMsg.length != sentMsg.length) {
        logger.warn("Replay: Message sent during replay differs from message in the log by length log:{} sent:{}", loggedMsg.length, sentMsg.length);
        foundFault = true;
        return null;
      }

      if ((msgLen > 0) && !Arrays.equals(loggedMsg, sentMsg)) {
        logger.warn("Replay: Message sent during replay differs from message in the log");
        foundFault = true;
        return null;
      }
    }

      fetchNextEvent();
      assert(next.getType() == EVT_SENDSIGN);
    fetchNextEvent();
    if (next == null) {
      logger.info("next event is null");
    }
    return ret;
    } catch (IOException ioe) {
      logger.warn("Error calculating hash", ioe);
      foundFault = true;
      return null;
    }
  }

  public long getNextEventTime() {
    if (next == null) return -1;
    return next.getSeq()/SEQS_PER_MILLI;
  }

  /**
   * Whether the next event comes before the timers of the state machine that are due in the same millisecond.
   * A CHECKPOINT is logged by PeerReview's checkpoint task, which only the record has. PeerReviewImpl schedules
   * that task before the state machine's init(), and it keeps its place when it repeats, so it runs before every
   * timer of the state machine due at the same time.
   */
  public boolean isNextEventBeforeTimersAtItsTime() {
    return next != null && next.getType() == EVT_CHECKPOINT;
  }

  public boolean isSuccess() {
    if (initialized && isVerificationSuccessful()) {
      if (next == null) return true;
    }
    return false;
  }


  @SuppressWarnings("unchecked")
  protected IOException deserializeException(InputBuffer nextEvent) throws IOException {
    short exType = nextEvent.readShort();
    switch (exType) {
    case EX_TYPE_IO:
      return new IOException(nextEvent.readUTF());
    case EX_TYPE_ClosedChannel:
      return new ClosedChannelException(nextEvent.readUTF());
    case EX_TYPE_Unknown:
      String className = nextEvent.readUTF();
      String message = nextEvent.readUTF();
      Class<? extends IOException> exceptionClass;
      try {
        // don't initialize the class before we know it is an IOException
        Class<?> loaded = Class.forName(className, false, Verifier.class.getClassLoader());
        if (!IOException.class.isAssignableFrom(loaded)) {
          throw new RuntimeException("Logged exception class "+className+" is not an IOException: "+message);
        }
        exceptionClass = loaded.asSubclass(IOException.class);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException("Couldn't find class "+className+" "+message);
      }

      try {
        return exceptionClass.getConstructor(String.class).newInstance(message);
      } catch (Exception e) {
        try {
          // no String constructor, fall back to the no-arg constructor (the message is lost)
          return exceptionClass.getConstructor().newInstance();
        } catch (Exception exception) {
          throw new RuntimeException("Couldn't find constructor for "+className+" "+message);
        }
      }
    default: throw new RuntimeException("Unknown EX_TYPE:"+exType);
    }
  }


  // ********************* Socket Stuff *************************

  Map<Integer, ReplaySocket<Handle>> sockets = new HashMap<Integer, ReplaySocket<Handle>>();

  @Override
  public SocketRequestHandle<Handle> openSocket(final Handle handle, SocketCallback<Handle> deliverSocketToMe, final Map<String, Object> options) {
    try {
      int socketId = openSocket(handle);
      ReplaySocket<Handle> socket = new ReplaySocket<Handle>(handle,socketId,this,options);
      socket.setDeliverSocketToMe(deliverSocketToMe);
      sockets.put(socketId, socket);
      return socket;
    } catch (IOException ioe) {
      SocketRequestHandle<Handle> ret = new SocketRequestHandle<Handle>(){

        @Override
        public Handle getIdentifier() {
          return handle;
        }

        @Override
        public Map<String, Object> getOptions() {
          return options;
        }

        @Override
        public boolean cancel() {
          return true;
        }
      };

      deliverSocketToMe.receiveException(ret, ioe);
      return ret;
    }
  }

  protected void socketIO(int socketId, boolean canRead, boolean canWrite) throws IOException {
    sockets.get(socketId).notifyIO(canRead, canWrite);
  }

  protected void incomingSocket(Handle from, int socketId) throws IOException {
    ReplaySocket<Handle> socket = new ReplaySocket<Handle>(from, socketId, this, null);
    sockets.put(socketId, socket);
    app.incomingSocket(socket);
  }

  protected void socketOpened(int socketId) throws IOException {
    sockets.get(socketId).socketOpened();
  }

  protected void socketException(int socketId, IOException ioe) throws IOException {
    sockets.get(socketId).receiveException(ioe);

  }

  /**
   * Return the new socketId
   * @return the new socketId, Integer.MIN_VALUE for an error
   */
  public int openSocket(Handle target) throws IOException {
    if (next == null) {
      logger.warn("Replay: OpenSocket event after end of segment; marking as invalid");
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    if (next.getType() != EVT_SOCKET_OPEN_OUTGOING) {
      logger.warn("Replay: SOCKET_OPEN_OUTGOING event during replay, but next event in log is #{}; marking as invalid", next.getType());
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    int ret = nextEvent.readInt();

    Handle logReceiver;
    logReceiver = peerreview.getHandleSerializer().deserialize(nextEvent);
    if (!logReceiver.equals(target)) {
      logger.warn("Replay: SOCKET_OPEN_OUTGOING to {} during replay, but log shows SOCKET_OPEN_OUTGOING to {}; marking as invalid", target, logReceiver);
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    fetchNextEvent();

    return ret;
  }

  /**
   * Return the bytes read.
   *
   * @return number of bytes read
   */
  public int readSocket(int socketId, ByteBuffer dst) throws IOException {
    if (next == null) {
      logger.warn("Replay: ReadSocket event after end of segment; marking as invalid");
      foundFault = true;
      return 0;
    }

    if (next.getType() == EVT_SOCKET_CLOSED) {
      fetchNextEvent();
      return -1;
    }

    if (next.getType() != EVT_SOCKET_READ) {
      logger.warn("Replay ({}): SOCKET_READ event during replay, but next event in log is #{}; marking as invalid", nextEventIndex, next.getType(), new Exception("Stack Trace"));
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    int loggedSocket = nextEvent.readInt();
    if (loggedSocket != socketId) {
      logger.warn("Replay: SOCKET_READ on socket {} during replay, but log shows SOCKET_READ to {}; marking as invalid", socketId, loggedSocket);
      foundFault = true;
      return 0;
    }

    // TODO: Change this when we make multiple reads a single event
    int ret = nextEvent.bytesRemaining();
    if (dst.remaining() < ret) {
      logger.warn("Replay: SOCKET_READ reading a maximum of {} on socket {} during replay, but log shows SOCKET_READ reading {} bytes; marking as invalid", dst.remaining(), socketId, ret);
      foundFault = true;
      return 0;
    }

    dst.put(WireLengths.readBytes(nextEvent, ret));
    fetchNextEvent();
    return ret;
  }

  /**
   * Return the bytes written.
   *
   * @return number of bytes written
   */
  public int writeSocket(int socketId, ByteBuffer src) throws IOException {
    if (next == null) {
      logger.warn("Replay: WriteSocket event after end of segment; marking as invalid");
      foundFault = true;
      return 0;
    }

    if (next.getType() == EVT_SOCKET_CLOSED) {
      fetchNextEvent();
      return -1;
    }

    if (next.getType() != EVT_SOCKET_WRITE) {
      logger.warn("Replay: SOCKET_WRITE event during replay, but next event in log is #{}; marking as invalid", next.getType());
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    int loggedSocket = nextEvent.readInt();
    if (loggedSocket != socketId) {
      logger.warn("Replay: SOCKET_WRITE on socket {} during replay, but log shows SOCKET_WRITE to {}; marking as invalid", socketId, loggedSocket);
      foundFault = true;
      return 0;
    }

    // TODO: Change this when we make multiple reads a single event
    int ret = nextEvent.bytesRemaining();
    if (src.remaining() < ret) {
      logger.warn("Replay: SOCKET_WRITE writing a maximum of {} on socket {} during replay, but log shows SOCKET_WRITE writing {} bytes; marking as invalid", src.remaining(), socketId, ret);
      foundFault = true;
      return 0;
    }

    byte[] loggedMsg = WireLengths.readBytes(nextEvent, ret);
    byte[] sentMsg = new byte[ret];
    src.get(sentMsg);

    if (!Arrays.equals(loggedMsg, sentMsg)) {
      logger.warn("Replay: Message wrote during replay differs from message in the log");
      foundFault = true;
      return 0;
    }

    fetchNextEvent();
    return ret;
  }

  public void close(int socketId) {
    if (next == null) {
      logger.warn("Replay({}): SOCKET_CLOSE event after end of segment; marking as invalid", nextEventIndex);
      foundFault = true;
      return;
    }

    if (next.getType() != EVT_SOCKET_CLOSE) {
      logger.warn("Replay({}): SOCKET_CLOSE event during replay, but next event in log is #{}; marking as invalid", nextEventIndex, next.getType());
      foundFault = true;
      return;
    }

    int loggedSocket;
    try {
      loggedSocket = nextEvent.readInt();
    } catch (IOException ioe) {
      logger.warn("Replay: Error deserializing event {}", next);
      foundFault = true;
      return;
    }

    if (loggedSocket != socketId) {
      logger.warn("Replay: SOCKET_CLOSE on socket {} during replay, but log shows SOCKET_CLOSE to {}; marking as invalid", socketId, loggedSocket);
      foundFault = true;
      return;
    }

    fetchNextEvent();
  }

  public void shutdownOutput(int socketId) {
    if (next == null) {
      logger.warn("Replay: EVT_SOCKET_SHUTDOWN_OUTPUT event after end of segment; marking as invalid");
      foundFault = true;
      return;
    }

    if (next.getType() != EVT_SOCKET_SHUTDOWN_OUTPUT) {
      logger.warn("Replay: EVT_SOCKET_SHUTDOWN_OUTPUT event during replay, but next event in log is #{}; marking as invalid", next.getType());
      foundFault = true;
      return;
    }

    int loggedSocket;
    try {
      loggedSocket = nextEvent.readInt();
    } catch (IOException ioe) {
      logger.warn("Replay: Error deserializing event {}", next);
      foundFault = true;
      return;
    }

    if (loggedSocket != socketId) {
      logger.warn("Replay: EVT_SOCKET_SHUTDOWN_OUTPUT on socket {} during replay, but log shows EVT_SOCKET_SHUTDOWN_OUTPUT to {}; marking as invalid", socketId, loggedSocket);
      foundFault = true;
      return;
    }

    fetchNextEvent();
  }

  public Environment getEnvironment() {
    return environment;
  }

  @Override
  public void acceptMessages(boolean enabled) {
    throw new RuntimeException("implement");
  }

  @Override
  public void acceptSockets(boolean enabled) {
    throw new RuntimeException("implement");
  }

  @Override
  public Handle getLocalIdentifier() {
    return localHandle;
  }

  @Override
  public void setCallback(TransportLayerCallback<Handle, ByteBuffer> callback) {
    this.app = (PeerReviewCallback<Handle, Identifier>)callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Handle> handler) {
    throw new RuntimeException("implement");
  }

  @Override
  public void destroy() {
    throw new RuntimeException("implement");
  }

}
