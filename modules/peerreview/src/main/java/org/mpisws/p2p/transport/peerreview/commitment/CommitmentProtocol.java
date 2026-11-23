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
package org.mpisws.p2p.transport.peerreview.commitment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.history.HashAndSequence;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtAck;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtRecv;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSend;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSign;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.OutgoingUserDataMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.slf4j.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.tuples.Tuple;
import rice.selector.TimerTask;

import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_ACK;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_RECV;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SEND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SENDSIGN;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SIGN;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_EXPOSED;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_SUSPECTED;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_TRUSTED;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SEQS_PER_MILLI;
import org.mpisws.p2p.transport.peerreview.infostore.StatusChangeListener;

/**
 * This protocol attaches signatures to outgoing messages and acknowledges
 * incoming messages. It also has transmit and receive queues where messages can
 * be held while acknowledgments are pending, and it can retransmit messages a
 * few times when an acknowledgment is not received.
 */
public class CommitmentProtocol<Handle extends RawSerializable, Identifier extends RawSerializable>
    implements StatusChangeListener<Identifier> {
  private final int INITIAL_TIMEOUT_MILLIS = 1000;
  private final int RETRANSMIT_TIMEOUT_MILLIS = 1000;
  private final int RECEIVE_CACHE_SIZE = 100;
  private final int MAX_RETRANSMISSIONS = 2;
  private final int PROGRESS_INTERVAL_MILLIS = 1000;
  private final int MAX_ENTRIES_PER_MS = SEQS_PER_MILLI;      /* Max number of entries per millisecond */

  /**
   * We need to keep some state for each peer, including separate transmit and
   * receive queues
   */
  Map<Identifier, PeerCommitmentState<Handle>> peer = new HashMap<Identifier, PeerCommitmentState<Handle>>();

  /**
   * We cache a few recently received messages, so we can recognize duplicates.
   * We also remember the location of the corresponding RECV entry in the log,
   * so we can reproduce the matching acknowledgment
   */
  /** the key of the receive cache: a sender and its sequence number */
  record ReceiveKey<I>(I id, long seq) {}

  Map<ReceiveKey<Identifier>, ReceivedMessageRecord<Identifier>> receiveCache;

  SecureHistory history;
  PeerReview<Handle, Identifier> peerreview;
  PeerInfoStore<Handle, Identifier> infoStore;
  IdentityTransport<Handle, Identifier> transport;

  /**
   * If the time is more different than this from a peer, we discard the message
   */
  long timeToleranceMillis;

  TimerTask makeProgressTask;
  Logger logger;

  public CommitmentProtocol(PeerReview<Handle,Identifier> peerreview,
      IdentityTransport<Handle, Identifier> transport,
      PeerInfoStore<Handle, Identifier> infoStore, AuthenticatorStore<Identifier> authStore,
      SecureHistory history,
      long timeToleranceMillis) throws IOException {
    this.peerreview = peerreview;
    this.transport = transport;
    this.infoStore = infoStore;
    this.history = history;
    this.timeToleranceMillis = timeToleranceMillis;

    this.logger = peerreview.getEnvironment().getLogManager().getLogger(CommitmentProtocol.class, null);

    initReceiveCache();
    makeProgressTask = new TimerTask(){
      @Override
      public void run() {
        makeProgressAllPeers();
      }
    };

    peerreview.getEnvironment().getSelectorManager().schedule(makeProgressTask, PROGRESS_INTERVAL_MILLIS, PROGRESS_INTERVAL_MILLIS);
  }

  /**
   * Load the last events from the history into the cache
   */
  protected void initReceiveCache() throws IOException {
    receiveCache = new LinkedHashMap<ReceiveKey<Identifier>, ReceivedMessageRecord<Identifier>>(RECEIVE_CACHE_SIZE, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<ReceiveKey<Identifier>, ReceivedMessageRecord<Identifier>> eldest) {
        return size() > RECEIVE_CACHE_SIZE;
      }
    };

    for (long i=history.getNumEntries()-1; (i>=1) && (receiveCache.size() < RECEIVE_CACHE_SIZE); i--) {
      IndexEntry hIndex = history.statEntry(i);
      if (hIndex.getType() == PeerReviewConstants.EVT_RECV) {
        // NOTE: this could be more efficient, because we don't need the whole thing
        SimpleInputBuffer sib = new SimpleInputBuffer(history.getEntry(hIndex, hIndex.getSizeInFile()));
        Identifier thisSender = peerreview.getIdSerializer().deserialize(sib);

        // NOTE: the message better start with the sender seq, but this is done within this protocol
        addToReceiveCache(thisSender, sib.readLong(), i);
      }
    }
  }

  protected void addToReceiveCache(Identifier id, long senderSeq, long indexInLocalHistory) {
    receiveCache.put(new ReceiveKey<Identifier>(id,senderSeq), new ReceivedMessageRecord<Identifier>(id, senderSeq, indexInLocalHistory));
  }

  protected PeerCommitmentState<Handle> lookupPeer(Handle handle) {
    PeerCommitmentState<Handle> ret = peer.get(peerreview.getIdentifierExtractor().extractIdentifier(handle));
    if (ret != null) return ret;

    ret = new PeerCommitmentState<Handle>(handle);
    peer.put(peerreview.getIdentifierExtractor().extractIdentifier(handle), ret);
    return ret;
  }

  public void notifyCertificateAvailable(Identifier id) {
    makeProgress(id);
  }


  /**
   * Checks whether an incoming message is already in the log (which can happen with duplicates).
   * If not, it adds the message to the log.
   * @return The ack message and whether it was already logged.
   */
  public Tuple<AckMessage<Identifier>,Boolean> logMessageIfNew(UserDataMessage<Handle> udm) {
    try {
      boolean loggedPreviously; // part of the return statement
      long seqOfRecvEntry;
      byte[] myHashTop;
      byte[] myHashTopMinusOne;

      /* Check whether the log contains a matching RECV entry, i.e. one with a message
      from the same node and with the same send sequence number */

      long indexOfRecvEntry = findRecvEntry(peerreview.getIdentifierExtractor().extractIdentifier(udm.getSenderHandle()), udm.getTopSeq());

      /* If there is no such RECV entry, we append one */

      if (indexOfRecvEntry < 0L) {
        /* Construct the RECV entry and append it to the log */

        myHashTopMinusOne = history.getTopLevelEntry().getHash();
        EvtRecv<Handle> recv = udm.getReceiveEvent(transport);
        history.appendEntry(EVT_RECV, true, recv.serialize());

        HashAndSequence foo = history.getTopLevelEntry();
        myHashTop = foo.getHash();
        seqOfRecvEntry = foo.getSeq();

        addToReceiveCache(peerreview.getIdentifierExtractor().extractIdentifier(udm.getSenderHandle()),
            udm.getTopSeq(), history.getNumEntries() - 1);
        logger.debug("New message logged as seq#{}", seqOfRecvEntry);

        /* Construct the SIGN entry and append it to the log */

        history.appendEntry(EVT_SIGN, true, new EvtSign(udm.getPreviousTopHash(),udm.getSignature()).serialize());
        loggedPreviously = false;
      } else {
        loggedPreviously = true;

        /* If the RECV entry already exists, retrieve it */

        IndexEntry indexEntry = history.statEntry(indexOfRecvEntry);
        IndexEntry i1 = history.statEntry(indexOfRecvEntry-1);
        assert(i1 != null && indexEntry != null && indexEntry.getType() == EVT_RECV) : "i1:"+i1+" i2:"+indexEntry;
        seqOfRecvEntry = indexEntry.getSeq();
        myHashTop = indexEntry.getNodeHash();
        myHashTopMinusOne = i1.getNodeHash();
        logger.debug("This message has already been logged as seq#{}", seqOfRecvEntry);
      }

      /* Generate ACK = (MSG_ACK, myID, remoteSeq, localSeq, myTopMinusOne, signature) */

      byte[] hToSign = transport.hash(ByteBuffer.wrap(MathUtils.longToByteArray(seqOfRecvEntry)), ByteBuffer.wrap(myHashTop));

      AckMessage<Identifier> ack = new AckMessage<Identifier>(
          peerreview.getLocalId(),
          udm.getTopSeq(),
          seqOfRecvEntry,
          myHashTopMinusOne,
          transport.sign(hToSign));

      return new Tuple<AckMessage<Identifier>,Boolean>(ack, loggedPreviously);
    } catch (IOException ioe) {
      RuntimeException throwMe = new RuntimeException("Unexpect error logging message :"+udm);
      throwMe.initCause(ioe);
      throw throwMe;
    }
  }

  @Override
  public void notifyStatusChange(Identifier id, int newStatus) {
    makeProgressAllPeers();
  }

  protected void makeProgressAllPeers() {
    for (Identifier identifier : peer.keySet()) {
      makeProgress(identifier);
    }
  }

  /**
   * Tries to make progress on the message queue of the specified peer, e.g. after that peer
   * has become TRUSTED, or after it has sent us an acknowledgment
   */
  protected void makeProgress(Identifier identifier) {
    PeerCommitmentState<Handle> info = peer.get(identifier);
    if (info == null || (info.xmitQueue.isEmpty() && info.recvQueue.isEmpty())) {
      return;
    }

    /* Get the public key. If we don't have it (yet), ask the peer to send it */

    if (!transport.hasCertificate(identifier)) {
      peerreview.requestCertificate(info.handle, identifier);
      return;
    }

    /* Transmit queue: If the peer is suspected, challenge it; otherwise, send the next message
       or retransmit the one currently in flight */

    if (!info.xmitQueue.isEmpty()) {
      int status = infoStore.getStatus(identifier);
      switch (status) {
        case STATUS_EXPOSED: /* Node is already exposed; no point in sending it any further messages */
          logger.warn("Releasing messages sent to exposed node {}", identifier);
          info.clearXmitQueue();
          return;
        case STATUS_SUSPECTED: /* Node is suspected; send the first unanswered challenge */
          if (info.lastChallenge < (peerreview.getTime() - info.currentChallengeInterval)) {
            logger.warn("Pending message for SUSPECTED node {}; challenging node (interval={})", info.getHandle(), info.currentChallengeInterval);
            info.lastChallenge = peerreview.getTime();
            info.currentChallengeInterval *= 2;
            peerreview.challengeSuspectedNode(info.handle);
          }
          return;
        case STATUS_TRUSTED: /* Node is trusted; continue below */
          info.lastChallenge = -1;
          info.currentChallengeInterval = PeerCommitmentState.INITIAL_CHALLENGE_INTERVAL_MICROS;
          break;
      }

      /* If there are no unacknowledged packets to that node, transmit the next packet */

      if (info.numOutstandingPackets == 0) {
        info.numOutstandingPackets++;
        info.lastTransmit = peerreview.getTime();
        info.currentTimeout = INITIAL_TIMEOUT_MILLIS;
        info.retransmitsSoFar = 0;
        OutgoingUserDataMessage<Handle> oudm = info.xmitQueue.getFirst();
          peerreview.transmit(info.getHandle(), oudm, null, oudm.getOptions());
      } else if (peerreview.getTime() > (info.lastTransmit + info.currentTimeout)) {

        /* Otherwise, retransmit the current packet a few times, up to the specified limit */

        if (info.retransmitsSoFar < MAX_RETRANSMISSIONS) {
          logger.atWarn().log(() -> "Retransmitting a "+info.xmitQueue.getFirst().getPayload().remaining()+"-byte message to "+info.getHandle()+
              " (lastxmit="+info.lastTransmit+", timeout="+info.currentTimeout+", type="+
              info.xmitQueue.getFirst().getType()+")");
          info.retransmitsSoFar++;
          info.currentTimeout = RETRANSMIT_TIMEOUT_MILLIS;
          info.lastTransmit = peerreview.getTime();
          OutgoingUserDataMessage<Handle> oudm = info.xmitQueue.getFirst();
            peerreview.transmit(info.handle, oudm, null, oudm.getOptions());
        } else {

          /* If the peer still won't acknowledge the message, file a SEND challenge with its witnesses */

          logger.warn("{} has not acknowledged our message after {} retransmissions; filing as evidence", info.handle, info.retransmitsSoFar);
          OutgoingUserDataMessage<Handle> challenge = info.xmitQueue.removeFirst();
          challenge.sendFailed(new IOException("Peer Review Giving Up sending message to "+identifier));

          long evidenceSeq = peerreview.allocateEvidenceSeq();

          try {
            infoStore.addEvidence(peerreview.getLocalId(),
                peerreview.getIdentifierExtractor().extractIdentifier(info.handle),
                evidenceSeq, challenge, null);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          peerreview.sendEvidenceToWitnesses(peerreview.getIdentifierExtractor().extractIdentifier(info.handle),
              evidenceSeq, challenge);

          info.numOutstandingPackets --;
        }
      }
    }

    /* Receive queue */

    if (!info.recvQueue.isEmpty() && !info.isReceiving) {

      /* The challenge protocol admits only messages from TRUSTED nodes, but the status can change while a
         message waits here, e.g. for the sender's certificate */

      int status = infoStore.getStatus(identifier);
      if (status == STATUS_EXPOSED) {
        logger.warn("Discarding {} messages from exposed node {}", info.recvQueue.size(), identifier);
        info.recvQueue.clear();
        return;
      }
      if (status != STATUS_TRUSTED) {
        return; /* SUSPECTED: keep the messages until the node is trusted again */
      }

      info.isReceiving = true;

      /* Dequeue the packet. After this point, we must either deliver it or discard it */

      Tuple<UserDataMessage<Handle>, Map<String, Object>> pair = info.recvQueue.removeFirst();
      UserDataMessage<Handle> udm = pair.first();

      /* Extract the authenticator */
      Authenticator authenticator;
      byte[] innerHash = udm.getInnerHash(peerreview.getLocalId(), transport);

      authenticator = peerreview.extractAuthenticator(
          peerreview.getIdentifierExtractor().extractIdentifier(udm.getSenderHandle()),
          udm.getTopSeq(),
          EVT_SEND,
          innerHash, udm.getPreviousTopHash(), udm.getSignature());
      if (authenticator != null) {

        /* At this point, we are convinced that:
              - The remote node is TRUSTED
              - The message has an acceptable sequence number
              - The message is properly signed
           Now we must check our log for an existing RECV entry:
              - If we already have such an entry, we generate the ACK from there
              - If we do not yet have the entry, we log the message and deliver it  */

        Tuple<AckMessage<Identifier>, Boolean> ret = logMessageIfNew(udm);

        /* Since the message is not yet in the log, deliver it to the application */

        if (!ret.second()) {
          logger.atDebug().log(() -> "Delivering message from "+udm.getSenderHandle()+" via "+info.handle+" ("+
              udm.getPayloadLength()+" bytes; "+udm.getRelevantLength()+"/"+udm.getPayloadLength()+" relevant)");
          try {
            peerreview.getApp().messageReceived(udm.getSenderHandle(), udm.getPayload(), pair.second());
          } catch (IOException ioe) {
            logger.warn("Error handling {}", udm, ioe);
          }
        } else {
          logger.debug("Message from {} via {} was previously logged; not delivered", udm.getSenderHandle(), info.getHandle());
        }

        /* Send the ACK */

        logger.debug("Returning ACK to{}", info.getHandle());
          peerreview.transmit(info.handle, ret.first(), null, pair.second());
      } else {
        logger.warn("Cannot verify signature on message {} from {}; discarding", udm.getTopSeq(), info.getHandle());
      }

      /* Release the message */

      info.isReceiving = false;
      makeProgress(identifier);
    }
  }

  protected long findRecvEntry(Identifier id, long seq) {
    ReceivedMessageRecord<Identifier> ret = receiveCache.get(new ReceiveKey<Identifier>(id,seq));
    if (ret == null) return -1;
    return ret.indexInLocalHistory;
  }

  protected long findAckEntry(Identifier id, long seq) {
    return -1;
  }

  /**
   * Handle an incoming USERDATA message
   */
  public void handleIncomingMessage(Handle source, UserDataMessage<Handle> msg, Map<String, Object> options) throws IOException {

    /* Check whether the timestamp (in the sequence number) is close enough to our local time.
       If not, the node may be trying to roll forward its clock, so we discard the message. */
    long txmit = (msg.getTopSeq() / MAX_ENTRIES_PER_MS);

    if ((txmit < (peerreview.getTime()-timeToleranceMillis)) || (txmit > (peerreview.getTime()+timeToleranceMillis))) {
      logger.atWarn().log(() -> "Invalid sequence no #"+msg.getTopSeq()+" on incoming message (dt="+(txmit-peerreview.getTime())+"); discarding");
      return;
    }

    /**
     * Append a copy of the message to our receive queue. If the node is
     * trusted, the message is going to be delivered directly by makeProgress();
     * otherwise a challenge is sent.
     */
    lookupPeer(source).recvQueue.addLast(new Tuple<UserDataMessage<Handle>, Map<String,Object>>(msg,options));

    makeProgress(peerreview.getIdentifierExtractor().extractIdentifier(source));
  }

  public MessageRequestHandle<Handle, ByteBuffer> handleOutgoingMessage(
      final Handle target, final ByteBuffer message,
      MessageCallback<Handle, ByteBuffer> deliverAckToMe,
      final Map<String, Object> options) {
    int relevantlen = message.remaining();
    if (options != null && options.containsKey(PeerReview.RELEVANT_LENGTH)) {
      Number number = (Number)options.get(PeerReview.RELEVANT_LENGTH);
      relevantlen = number.intValue();
    }
    assert(relevantlen >= 0);

    /* Append a SEND entry to our local log */

    byte[] hTopMinusOne, hToSign;
    hTopMinusOne = history.getTopLevelEntry().getHash();
    EvtSend<Identifier> evtSend;
    if (relevantlen < message.remaining()) {
      evtSend = new EvtSend<Identifier>(peerreview.getIdentifierExtractor().extractIdentifier(target),message,relevantlen,transport);
    } else {
      evtSend = new EvtSend<Identifier>(peerreview.getIdentifierExtractor().extractIdentifier(target),message);
    }


    try {
      history.appendEntry(evtSend.getType(), true, evtSend.serialize());
    } catch (IOException ioe) {
      MessageRequestHandle<Handle, ByteBuffer> ret = new MessageRequestHandleImpl<Handle, ByteBuffer>(target,message,options);
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, ioe);
      return ret;
    }

    //  hTop, &topSeq
    HashAndSequence top = history.getTopLevelEntry();

    /* Sign the authenticator */
    hToSign = transport.hash(ByteBuffer.wrap(MathUtils.longToByteArray(top.getSeq())), ByteBuffer.wrap(top.getHash()));

    byte[] signature = transport.sign(hToSign);

    /* Append a SENDSIGN entry */

    ByteBuffer relevantMsg = message;
    if (relevantlen < message.remaining()) {
      relevantMsg = ByteBuffer.wrap(message.array(), message.arrayOffset()+message.position(), relevantlen);
    } else {
      relevantMsg = ByteBuffer.wrap(message.array(), message.arrayOffset()+message.position(), message.remaining());
    }
    try {
      history.appendEntry(EVT_SENDSIGN, true, relevantMsg, ByteBuffer.wrap(signature));
    } catch (IOException ioe) {
      MessageRequestHandle<Handle, ByteBuffer> ret = new MessageRequestHandleImpl<Handle, ByteBuffer>(target,message,options);
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, ioe);
      return ret;
    }

    /* Construct a USERDATA message... */

    assert((relevantlen == message.remaining()) || (relevantlen < Byte.toUnsignedInt(UserDataMessage.RELEVANT_LEN_ALL)));

    PeerCommitmentState<Handle> peerCommitmentState = lookupPeer(target);

    OutgoingUserDataMessage<Handle> udm = new OutgoingUserDataMessage<Handle>(top.getSeq(), peerreview.getLocalHandle(), hTopMinusOne, signature, message, relevantlen, options, peerCommitmentState, deliverAckToMe);

    /* ... and put it into the send queue. If the node is trusted and does not have any
       unacknowledged messages, makeProgress() will simply send it out. */
    peerCommitmentState.xmitQueue.addLast(udm);
    makeProgress(peerreview.getIdentifierExtractor().extractIdentifier(target));

    return udm;
  }
  /* This is called if we receive an acknowledgment from another node */

  public void handleIncomingAck(Handle source, AckMessage<Identifier> ackMessage, Map<String, Object> options) throws IOException {

    /* Acknowledgment: Log it (if we don't have it already) and send the next message, if any */

    logger.debug("Received an ACK from {}", source);
    /* The receiver sends its ACK itself. An ACK naming another node would let the receiver avoid
       logging the RECV, and our log would show an ACK from a node we never sent to. */
    if (!ackMessage.getNodeId().equals(peerreview.getIdentifierExtractor().extractIdentifier(source))) {
      logger.warn("ACK from {} claims to come from <{}>; discarding", source, ackMessage.getNodeId());
      return;
    }

    if (transport.hasCertificate(ackMessage.getNodeId())) {
      PeerCommitmentState<Handle> peerCommitmentState = lookupPeer(source);

      boolean checkAck = true;
      OutgoingUserDataMessage<Handle> udm = null;
      if (peerCommitmentState.xmitQueue.isEmpty()) {
        checkAck = false;  // don't know why this happens, but maybe the ACK gets duplicated somehow
      } else {
        udm = peerCommitmentState.xmitQueue.getFirst();
      }

      /* The ACK must acknowledge the sequence number of the packet that is currently
         at the head of the send queue */

      if (checkAck && ackMessage.getSendEntrySeq() == udm.getTopSeq()) {

        /* Now we're ready to check the signature */

        /* The peer will have logged a RECV entry, and the signature is calculated over that
        entry. To verify the signature, we must reconstruct that RECV entry locally */

        byte[] innerHash = udm.getInnerHash(transport);


        Authenticator authenticator = peerreview.extractAuthenticator(
            ackMessage.getNodeId(), ackMessage.getRecvEntrySeq(), EVT_RECV, innerHash,
            ackMessage.getPreviousTopHash(), ackMessage.getSignature());
        if (authenticator != null) {

          /* Signature is okay... append an ACK entry to the log */

          logger.debug("ACK is okay; logging {}", ackMessage);

          EvtAck<Identifier> evtAck = new EvtAck<Identifier>(ackMessage.getNodeId(), ackMessage.getSendEntrySeq(), ackMessage.getRecvEntrySeq(), ackMessage.getPreviousTopHash(), ackMessage.getSignature());
          history.appendEntry(EVT_ACK, true, evtAck.serialize());
          udm.sendComplete();

          /* Remove the message from the xmit queue */

          peerCommitmentState.xmitQueue.removeFirst();
          peerCommitmentState.numOutstandingPackets--;

          /* Make progress (e.g. by sending the next message) */

          makeProgress(peerreview.getIdentifierExtractor().extractIdentifier(peerCommitmentState.getHandle()));
        } else {
          logger.warn("Invalid ACK from <{}>; discarding", ackMessage.getNodeId());
        }
      } else {
        if (findAckEntry(ackMessage.getNodeId(), ackMessage.getSendEntrySeq()) < 0) {
          logger.warn("<{}> has ACKed something we haven't sent ({}); discarding", ackMessage.getNodeId(), ackMessage.getSendEntrySeq());
        } else {
          logger.warn("Duplicate ACK from <{}>; discarding", ackMessage.getNodeId());
        }
      }
    } else {
      logger.warn("We got an ACK from <{}>, but we don't have the certificate; discarding", ackMessage.getNodeId());
    }
  }
}
