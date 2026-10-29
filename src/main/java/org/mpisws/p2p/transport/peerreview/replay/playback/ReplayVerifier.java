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
package org.mpisws.p2p.transport.peerreview.replay.playback;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.replay.EventCallback;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;
import org.mpisws.p2p.transport.util.Serializer;
import org.slf4j.Logger;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

public abstract class ReplayVerifier<Identifier> implements Verifier<Identifier>,  PeerReviewConstants {

  protected Identifier localHandle;
  protected SecureHistory history;
  PeerReviewCallback<Identifier, ?> app;
  int numEventCallbacks;
  boolean foundFault;

  boolean haveNextEvent;
  long nextEventIndex;
  IndexEntry next;
  boolean nextEventIsHashed;
  InputBuffer nextEvent;

  boolean initialized;
  short signatureSizeBytes;
  short hashSizeBytes;
  int numTimers;
  int[] eventToCallback = new int[256];
  protected Logger logger;
  protected Serializer<Identifier> serializer; // was transport in c++ impl
  protected HashProvider hashProv;

  // these are shortcuts in the Java impl, they would all be true in the c++ impl, but in some cases it's more efficient if we can turn them off
  boolean useSendSign = false;  // true if we're sending the signature after the message
  boolean useSenderSeq = false;
  boolean useLogHashFlag = false;

  public ReplayVerifier(
      Serializer<Identifier> serializer,
      HashProvider hashProv,
      SecureHistory history,
      Identifier localHandle,
      short signatureSizeBytes,
      short hashSizeBytes,
      int firstEntryToReplay,
      Logger logger) throws IOException {
    this.logger = logger;
    this.history = history;
    this.numEventCallbacks = 0;
    this.serializer = serializer;
    this.hashProv = hashProv;
    this.localHandle = localHandle;
    this.foundFault = false;
    this.haveNextEvent = false;
    this.nextEventIndex = firstEntryToReplay-1;
    this.initialized = false;
    this.signatureSizeBytes = signatureSizeBytes;
    this.hashSizeBytes = hashSizeBytes;
    this.numTimers = 0;

    for (int i=0; i<256; i++)
      eventToCallback[i] = -1;

    fetchNextEvent();
    if (!haveNextEvent)
      foundFault = true;
  }

  public boolean verifiedOK() {
    return !foundFault;
  };


  public IndexEntry getNextEvent() {
    return next;
  }

  /**
   * Callback when a message has arrived.
   *
   * @param from
   * @param msg
   * @throws IOException
   */
  protected abstract void receive(Identifier from, ByteBuffer msg) throws IOException;

  /**
   * Callback when a socket comes in from a remote node.
   *
   * @param from
   * @param socketId
   * @throws IOException
   */
  protected abstract void incomingSocket(Identifier from, int socketId) throws IOException;

  /**
   * Callback when a socket is ready to read/write.
   *
   * Note that the simulated node should have already registered for this event, even though it is
   * not logged.  If the node is not registered for the event, it is an error.
   *
   * @param socketId
   * @param canRead
   * @param canWrite
   * @throws IOException
   */
  protected abstract void socketIO(int socketId, boolean canRead, boolean canWrite) throws IOException;

  protected abstract void socketOpened(int socketId) throws IOException;

  protected abstract void socketException(int socketId, IOException ioe) throws IOException;

  public void setApplication(PeerReviewCallback<Identifier, ?> app) {
    this.app = app;
  }

  /**
   * Fetch the next log entry, or set the EOF flag
   */
  protected void fetchNextEvent() {
    haveNextEvent = false;
    nextEventIndex++;

    try {
      next = history.statEntry(nextEventIndex);
    } catch (IOException ioe) {
      logger.warn("Error fetching log entry #{}", nextEventIndex, ioe);
      foundFault = true;
      return;
    }

    logger.debug("fetchNextEvent():{}", next);

    if (next == null)
      return;

    if (next.getSizeInFile()<0) {
      // make the nextEvent only the content hash

      nextEventIsHashed = true;
      nextEvent = new SimpleInputBuffer(next.getContentHash());
      logger.debug("Fetched log entry #{} (type {}, hashed, seq={})", nextEventIndex, next.getType(), next.getSeq());
    } else {
      // load the nextEvent from the file

      nextEventIsHashed = false;
      try {
        nextEvent = new SimpleInputBuffer(history.getEntry(nextEventIndex, next.getSizeInFile()));
      } catch (IOException ioe) {
        logger.warn("Error fetching log entry #{} (type {}, size {} bytes, seq={})", nextEventIndex, next.getType(), next.getSizeInFile(), next.getSeq(), ioe);
        foundFault = true;
        return;
      }
      logger.debug("Fetched log entry #{} (type {}, size {} bytes, seq={})", nextEventIndex, next.getType(), next.getSizeInFile(), next.getSeq());
    }

    haveNextEvent = true;
  }

  /**
   * Called by the state machine when it wants to send a message
   */
  protected void send(Identifier target, ByteBuffer message, int relevantLen) throws IOException {
    int msgLen = message.remaining();
    int pos = message.position();
    int lim = message.limit();

    if (relevantLen < 0)
      relevantLen = message.remaining();

    logger.debug("Verifier::send({}, {}/{} bytes)", target, relevantLen, message.remaining());

    // Sanity checks

    if (!haveNextEvent) {
      logger.warn("Replay: Send event after end of segment; marking as invalid");
      foundFault = true;
      return;
    }

    if (next.getType() == EVT_INIT) {
      logger.trace("Skipped; next event is an INIT");
      return;
    }

    if (next.getType() != EVT_SEND) {
      logger.warn("Replay({}): SEND event during replay, but next event in log is #{}; marking as invalid", nextEventIndex, next.getType());
      foundFault = true;
      return;
    }

    // If the SEND is hashed, simply compare it to the predicted entry

    if (nextEventIsHashed) {
      SimpleOutputBuffer buf = new SimpleOutputBuffer();
      serializer.serialize(target, buf);
      if (useLogHashFlag) buf.writeBoolean(relevantLen<msgLen);
      buf.write(message.array(), message.position(), relevantLen);

      if (relevantLen<msgLen) {
        message.position(pos);
        message.limit(lim);
        byte[] hash = hashProv.hash(message);
      }
      byte[] cHash = hashProv.hash(ByteBuffer.wrap(buf.getBytes()));
      if (!Arrays.equals(cHash, next.getContentHash())) {
        logger.warn("Replay: SEND is hashed, but hash of predicted SEND entry does not match hash in the log");
        foundFault = true;
        return;
      }

      if (useSendSign) {
        fetchNextEvent();
        assert(next.getType() == EVT_SENDSIGN);
      }
      fetchNextEvent();
      return;
    }

    // Are we sending to the same destination?
    Identifier logReceiver;
     logReceiver = serializer.deserialize(nextEvent);
    if (!logReceiver.equals(target)) {
      logger.warn("Replay({}): SEND to {} during replay, but log shows SEND to {}; marking as invalid", nextEventIndex, target, logReceiver);
      // reset nextEvent so next time we parse this event it isn't nonsense
      nextEvent = new SimpleInputBuffer(history.getEntry(next, next.getSizeInFile()));
      foundFault = true;
      return;
    }

    // Check the message against the message in the log
    boolean logIsHashed = false;
    if (useLogHashFlag) logIsHashed = nextEvent.readBoolean();

    if (logIsHashed) {
      if (relevantLen >= msgLen) {
        logger.warn("Replay: Message sent during replay is entirely relevant, but log entry is partly hashed; marking as invalid");
        foundFault = true;
        return;
      }

      int logRelevantLen = nextEvent.bytesRemaining() - hashSizeBytes;
      assert(logRelevantLen >= 0);

      if (relevantLen != logRelevantLen) {
        logger.warn("Replay: Message sent during replay has {} relevant bytes, but log entry has {}; marking as invalid", relevantLen, logRelevantLen);
        foundFault = true;
        return;
      }

      byte[] loggedMsg = new byte[logRelevantLen];
      nextEvent.read(loggedMsg);
      ByteBuffer loggedMsgBB = ByteBuffer.wrap(loggedMsg);
      if ((relevantLen > 0) && message.equals(loggedMsgBB)) {
        logger.warn("Replay: Relevant part of partly hashed message differs");
        logger.debug("Expected: [{}]", loggedMsgBB);
        logger.debug("Actual:   [{}]", message);
        foundFault = true;
        return;
      }

      byte[] logHash = new byte[hashSizeBytes];
      nextEvent.read(logHash);
      byte[] msgHashBytes = message.array();
      byte[] msgHash = new byte[hashSizeBytes];
      System.arraycopy(msgHashBytes, msgHashBytes.length-hashSizeBytes, msgHash, 0, hashSizeBytes);
      assert(msgLen == (relevantLen + hashSizeBytes));
      if (!msgHash.equals(logHash)) {
        logger.warn("Replay: Hashed part of partly hashed message differs");
        logger.debug("Expected: [{}]", logHash);
        logger.debug("Actual:   [{}]", msgHash);
        foundFault = true;
        return;
      }
    } else {
      if (relevantLen < msgLen) {
        logger.warn("Replay: Message sent during replay is only partly relevant, but log entry is not hashed; marking as invalid");
        foundFault = true;
        return;
      }

      int logMsglen = nextEvent.bytesRemaining();
      if (msgLen != logMsglen) {
        logger.warn("Replay: Message sent during replay has {} bytes, but log entry has {}; marking as invalid", msgLen, logMsglen);
        foundFault = true;
        return;
      }

      byte[] loggedMsg = new byte[nextEvent.bytesRemaining()];
      nextEvent.read(loggedMsg);
      byte[] sentMsg = new byte[message.remaining()];
      message.get(sentMsg);

      if (loggedMsg.length != sentMsg.length) {
        logger.warn("Replay: Message sent during replay differs from message in the log by length log:{} sent:{}", loggedMsg.length, sentMsg.length);
        foundFault = true;
        return;
      }

      if ((msgLen > 0) && !Arrays.equals(loggedMsg, sentMsg)) {
        logger.warn("Replay: Message sent during replay differs from message in the log");
        foundFault = true;
        return;
      }
    }

    if (useSendSign) {
      fetchNextEvent();
      assert(next.getType() == EVT_SENDSIGN);
    }
    fetchNextEvent();
  }

  /**
   * Return the new socketId
   * @param target
   * @return the new socketId, Integer.MIN_VALUE for an error
   */
  public int openSocket(Identifier target) throws IOException {
    if (!haveNextEvent) {
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

    Identifier logReceiver;
    logReceiver = serializer.deserialize(nextEvent);
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
   * @param socketId
   * @return number of bytes read
   */
  public int readSocket(int socketId, ByteBuffer dst) throws IOException {
    if (!haveNextEvent) {
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

    nextEvent.read(dst.array(), dst.position(), ret);
    dst.position(dst.position()+ret);
    fetchNextEvent();
    return ret;
  }

  public void generatedSocketException(int socketId, IOException ioe) {
    if (!haveNextEvent) {
      logger.warn("Replay: WriteSocket event after end of segment; marking as invalid");
      foundFault = true;
      return;
    }

    if (next.getType() != EVT_SOCKET_EXCEPTION) {
      logger.warn("Replay: EVT_SOCKET_EXCEPTION event during replay, but next event in log is #{}; marking as invalid", next.getType());
      foundFault = true;
      return;
    }

    try {
      int loggedSocket = nextEvent.readInt();
      if (loggedSocket != socketId) {
        logger.warn("Replay: EVT_SOCKET_EXCEPTION on socket {} during replay, but log shows EVT_SOCKET_EXCEPTION to {}; marking as invalid", socketId, loggedSocket);
        foundFault = true;
        return;
      }
    } catch (IOException ioe2) {
      logger.warn("Replay: Error reading log", ioe2);
    }

    // all good
    fetchNextEvent();
    return;

  }

  /**
   * Return the bytes written.
   *
   * @param socketId
   * @return number of bytes written
   */
  public int writeSocket(int socketId, ByteBuffer src) throws IOException {
    if (!haveNextEvent) {
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

    byte[] loggedMsg = new byte[ret];
    byte[] sentMsg = new byte[ret];
    nextEvent.read(loggedMsg);
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
    if (!haveNextEvent) {
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
    if (!haveNextEvent) {
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


  /**
   * Maps EVT_XXX -> EventCallback
   */
  Map<Short, EventCallback> eventCallback = new HashMap<Short, EventCallback>();

  /**
   * This binds specific event types to one of the handlers
   */
  public void registerEvent(EventCallback callback, short... eventType) {
    for (short s : eventType) {
      registerEvent(callback, s);
    }
  }

  public void registerEvent(EventCallback callback, short eventType) {
    if (eventCallback.containsKey(eventType)) {
      if (callback != eventCallback.get(eventType)) throw new IllegalStateException("Event #"+eventType+" registered twice");
    }
    eventCallback.put(eventType,callback);
  }

  /**
   * This is called by the Audit protocol to make another replay step; it returns true
   * if further calls are necessary, and false if the replay has finished. The idea
   * is that we can stop calling this if there is more important work to do, e.g.
   * handle foreground requests
   */
  public boolean makeProgress() {
    logger.debug("makeProgress()");
    if (foundFault || !haveNextEvent)
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
    // This code is the job of the SelectorManager, it's done in the super class of ReplaySM

    if (!haveNextEvent)
      return false;

    /* Sanity checks */

    logger.debug("Replaying event #{} (type {}, seq={})", nextEventIndex, next.getType(), next.getSeq());

    if (nextEventIsHashed && (next.getType() != EVT_CHECKPOINT) && (next.getType() != EVT_INIT)) {
      logger.warn("Replay: Trying to replay hashed event");
      foundFault = true;
      return false;
    }

    /* Replay the next event */

    try {
      switch (next.getType()) {
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
        Identifier sender = serializer.deserialize(nextEvent);

        long senderSeq = 0;
        if (useSenderSeq) senderSeq = nextEvent.readLong();
        boolean hashed = false;
        if (useLogHashFlag) hashed = nextEvent.readBoolean();

        int msgLen = nextEvent.bytesRemaining();
        int relevantLen = hashed ? (msgLen-hashSizeBytes) : msgLen;

        byte[] msgBytes = new byte[msgLen];
        nextEvent.read(msgBytes);
        ByteBuffer msgBuf = ByteBuffer.wrap(msgBytes);

        /* The next event is going to be a SIGN; skip it, since it's irrelevant here */

        if (useSendSign) {
          fetchNextEvent();
          if (!haveNextEvent || (next.getType() != EVT_SIGN) || (next.getSizeInFile() != (int)(hashSizeBytes+signatureSizeBytes))) {
            logger.warn("Replay: RECV event not followed by SIGN; marking as invalid");
            foundFault = true;
            return false;
          }
        }

        fetchNextEvent();

        /* Deliver the message to the state machine */

        receive(sender, msgBuf);
        break;
      case EVT_SIGN : /* SIGN events should have been handled by the preceding RECV */
        logger.warn("Replay: Spurious SIGN event; marking as invalid");
        foundFault = true;
        return false;
      case EVT_ACK : /* Skip ACKs */
  // warning there should be an upcall here
        fetchNextEvent();
        break;
      case EVT_SENDSIGN : /* Skip SENDSIGN events; they are not relevant during replay */
        fetchNextEvent();
        break;
      case EVT_CHECKPOINT : /* Verify CHECKPOINTs */
      case EVT_INIT: /* State machine is reinitialized; issue upcall */
        initialized = true;
        fetchNextEvent();
        break;
      case EVT_SOCKET_OPEN_INCOMING: {
        int socketId = nextEvent.readInt();
        Identifier opener = serializer.deserialize(nextEvent);
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

  public long getNextEventTime() {
    return next.getSeq()/1000000;
  }

  public boolean isSuccess() {
    if (initialized && verifiedOK()) {
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
      Class<?> c;
      try {
        c = Class.forName(className);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException("Couldn't find class "+className+" "+message);
      }

      Class<?>[] parameterTypes = new Class<?>[1];
      parameterTypes[0] = String.class;
      try {
        Constructor<?> ctor = c.getConstructor(parameterTypes);
        IOException ioe = (IOException)ctor.newInstance(message);
        return ioe;
      } catch (Exception e) {
        try {
          // no String constructor, fall back to the no-arg constructor (the message is lost)
          Constructor<?> ctor = c.getConstructor();
          IOException ioe = (IOException)ctor.newInstance();
          return ioe;
        } catch (Exception e2) {
          throw new RuntimeException("Couldn't find constructor for "+className+" "+message);
        }
      }
      // TODO: check up front that className is an IOException; today a ClassCastException surfaces as "Couldn't find constructor"
    default: throw new RuntimeException("Unknown EX_TYPE:"+exType);
    }
  }
}
