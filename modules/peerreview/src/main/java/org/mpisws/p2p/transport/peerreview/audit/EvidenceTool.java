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
package org.mpisws.p2p.transport.peerreview.audit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.commitment.CommitmentProtocol;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtAck;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtInit;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtRecv;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSend;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSendSign;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSign;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.util.Serializer;
import org.slf4j.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.p2p.util.tuples.Tuple;

import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.CERT_MISSING;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_ACK;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_CHECKPOINT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_CHOOSE_Q;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_CHOOSE_RAND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_INIT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_MAX_RESERVED;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_RECV;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SEND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SENDSIGN;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SIGN;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_VRF;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.FLAG_FULL_MESSAGES_SENDER;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.FLAG_INCLUDE_CHECKPOINT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.INVALID;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SIGNATURE_BAD;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SIGNATURE_OK;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.VALID;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SEQS_PER_MILLI;

public class EvidenceTool<Handle extends RawSerializable, Identifier extends RawSerializable> {

  Logger logger;
  Serializer<Identifier> idSerializer;
  Serializer<Handle> handleSerializer;

  int hashSize;
  int signatureSize;
  private PeerReview<Handle, Identifier> peerreview;

  public EvidenceTool(PeerReview<Handle, Identifier> peerreview, Serializer<Handle> handleSerializer, Serializer<Identifier> idSerializer, int hashSize, int signatureSize) {
    this.peerreview = peerreview;
    this.logger = peerreview.getEnvironment().getLogManager().getLogger(EvidenceTool.class, null);
    this.handleSerializer = handleSerializer;
    this.idSerializer = idSerializer;
    this.hashSize = hashSize;
    this.signatureSize = signatureSize;
  }

  /**
   * 1) is the log snippet well-formed, i.e. are the entries of the correct length, and do they have the correct format?
   * 2) do we locally have the node certificate for each signature that occurs in the snippet?
   *
   * if the former doesn't hold, it returns INVALID
   * if the latter doesn't hold, it returns CERT_MISSING, and returns the nodeID of the node whose certificate we need to request
   */
  public Tuple<Integer, Identifier> checkSnippet(LogSnippet snippet) {
    for (SnippetEntry entry : snippet.getEntries()) {
      if (entry.isHash() && entry.getType() != EVT_CHECKPOINT && entry.getType() != EVT_SENDSIGN && entry.getType() != EVT_SEND) {
        logger.warn("Malformed statement: Entry of type #{} is hashed", entry.getType());
        return new Tuple<Integer, Identifier>(INVALID,null);
      }

      /* Further processing depends on the entry type */
      logger.atTrace().log(() -> "Entry type "+entry.getType()+", size="+entry.getContent().length+" "+(entry.isHash() ? " (hashed)" : ""));

      try {
        SimpleInputBuffer sib = new SimpleInputBuffer(entry.getContent());
        switch (entry.getType()) {
          case EVT_SEND : /* No certificates needed; just do syntax checking */
            if (!entry.isHash()) {
              new EvtSend<Identifier>(sib, idSerializer, hashSize);
            }
            break;
          case EVT_RECV : {/* We may need the certificate for the sender */
            EvtRecv<Handle> recv = new EvtRecv<Handle>(sib,handleSerializer,hashSize);
            Identifier id = peerreview.getIdentifierExtractor().extractIdentifier(recv.getSenderHandle());
            if (!peerreview.hasCertificate(id)) {
              logger.debug("AUDIT RESPONSE contains RECV from {}; certificate needed", id);
              return new Tuple<Integer, Identifier>(CERT_MISSING, id);
            }
            break;
          }
          case EVT_SIGN : /* No certificates needed */
            new EvtSign(sib, hashSize, signatureSize);
            break;
          case EVT_ACK : /* We may need the certificate for the sender */
            EvtAck<Identifier> evtAck = new EvtAck<Identifier>(sib,idSerializer,hashSize,signatureSize);
            Identifier id = evtAck.getRemoteId();
            if (!peerreview.hasCertificate(id)) {
              logger.debug("AUDIT RESPONSE contains RECV from {}; certificate needed", id);
              return new Tuple<Integer, Identifier>(CERT_MISSING, id);
            }
            break;
          case EVT_CHECKPOINT : /* No certificates needed */
          case EVT_VRF :
          case EVT_CHOOSE_Q :
          case EVT_CHOOSE_RAND :
            break;
          case EVT_INIT: {/* No certificates needed */
            new EvtInit<Handle>(sib,handleSerializer);
            break;
          }
          case EVT_SENDSIGN : /* No certificates needed */
            break;
          default : /* No certificates needed */
            assert(entry.getType() > EVT_MAX_RESERVED);
            break;
        }
      } catch (IOException ioe) {
        logger.warn("Malformed entry:{}", entry, ioe);
        return new Tuple<Integer, Identifier>(INVALID,null);
      }
    }

    return new Tuple<Integer, Identifier>(VALID,null);
  }

  static class SendEntryRecord {
    ByteBuffer hashedPlusPayload;
    public SendEntryRecord(long seq, ByteBuffer hashedPlusPayload) {
      this.hashedPlusPayload = hashedPlusPayload;
    }
  }


  /**
   * The following method does several things: It verifies all the signatures in
   * a log snippet, it extracts all the authenticators for later forwarding to
   * the corresponding witnesses, and it delivers any new messages to the local
   * node that may be in the snippet (e.g. after an investigation)
   */
  public boolean checkSnippetSignatures(LogSnippet snippet,
      Handle subjectHandle, AuthenticatorStore<Identifier> authStoreOrNull,
      byte flags, CommitmentProtocol<Handle, Identifier> commitmentProtocol,
      byte[] keyNodeHash, long keyNodeMaxSeq) {
    boolean startWithCheckpoint = (flags&FLAG_INCLUDE_CHECKPOINT) == FLAG_INCLUDE_CHECKPOINT;
    byte[] currentNodeHash= snippet.baseHash;
    byte[] prevNodeHash = null;
    byte[] prevPrevNodeHash = null;

    boolean firstEntry = true;
    boolean keyNodeFound = false;

    SnippetEntry lastEntry = null;


    /* If FLAG_FULL_MESSAGES_SENDER is set, we must get a list of all the messages we have received
       from this node, so we can later check whether it has sent any other ones */

    HashSet<Long> seqBuf = new HashSet<Long>();
    try {
      if ((commitmentProtocol != null) && (flags & FLAG_FULL_MESSAGES_SENDER) == FLAG_FULL_MESSAGES_SENDER) {
        assert(peerreview != null);
        SecureHistory history = peerreview.getHistory();
        for (long i=history.getNumEntries()-1; i>=1; i--) {
          IndexEntry entry = history.statEntry(i);
          short type = entry.getType();
          if (entry.getSeq() < (snippet.getFirstSeq() - peerreview.getTimeToleranceMillis()*SEQS_PER_MILLI))
            break;
          if (type == EVT_RECV) {

            byte[] evtBytes = history.getEntry(entry,entry.getSizeInFile());
            EvtRecv<Handle> evtRecv = new EvtRecv<Handle>(new SimpleInputBuffer(evtBytes),handleSerializer,hashSize);
            Handle thisSender = evtRecv.getSenderHandle();
            if (thisSender.equals(subjectHandle)) {
              seqBuf.add(evtRecv.getSenderSeq());
            }
          }
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    /* We keep a cache of recent SEND entries. This is necessary because, when checking
       an ACK entry, we need to compute the message hash from the corresponding SEND entry */

    Map<Long, SendEntryRecord> secache = new HashMap<Long, SendEntryRecord>();


    logger.debug("Checking snippet (flags={})", flags);
    // FLAG_FULL is not checked here; the snippet is verified against the two authenticators whatever the flags say. (Open question inherited from the C++ PeerReview port.)

    /* We read the snippet entry by entry and check the following:
           - Does the computed top-level hash value match the second authenticator in the challenge?
           - Do all the signatures in RECV/SIGN and ACK entries check out?
       If extractAuthsFromResponse is set, we also add all authenticators to the authOut store.
       Note that we do not do conformance/consistency checking here; all we care about is whether
       the snippet matches the two authenticators. */

    for (SnippetEntry entry : snippet.getEntries()) {

      if (!firstEntry) {
        if (entry.getSeq() <= lastEntry.getSeq()) {
          logger.warn("Log snippet attempts to roll back the sequence number from {} to {}; flagging invalid", lastEntry.getSeq(), entry.getSeq());
          return false;
        }

        if (keyNodeHash != null && !keyNodeFound && (entry.getSeq() > keyNodeMaxSeq)) {
          logger.warn("Hash of keyNode does not appear in [{},{}]; flagging invalid", snippet.getFirstSeq(), keyNodeMaxSeq);
          return false;
        }
      }

      logger.atTrace().log(() -> "Entry type "+entry.getType()+", size="+entry.getContent().length+", seq="+entry.getSeq()+" "+(entry.isHash() ? " (hashed)" : ""));

      if (lastEntry != null && (lastEntry.getType() == EVT_RECV) && ((entry.getType() != EVT_SIGN) || (entry.getSeq() != (lastEntry.getSeq()+1)))) {
        logger.warn("Log snippet omits the mandatory EVT_SIGN after an EVT_RECV; flagging invalid");
        return false;
      }

      prevPrevNodeHash = prevNodeHash;
      prevNodeHash = currentNodeHash;

      /* Update the current node hash */

      byte[] contentHash;;
      if (entry.isHash()) {
        contentHash = entry.getContent();
      } else {
        contentHash = peerreview.hash(ByteBuffer.wrap(entry.getContent()));
      }

      currentNodeHash = peerreview.hash(entry.getSeq(), entry.getType(), currentNodeHash, contentHash);

      /* Check signatures */

      try {
      SimpleInputBuffer sib = new SimpleInputBuffer(entry.getContent());
      switch (entry.getType()) {
        case EVT_SEND :
          if (!entry.isHash()) {
            idSerializer.deserialize(sib);
            /* Add an entry to the send-entry cache */
            secache.put(entry.getSeq(), new SendEntryRecord(entry.getSeq(), ByteBuffer.wrap(entry.getContent(), entry.getContent().length-sib.bytesRemaining(), sib.bytesRemaining())));
          }
          break;
        case EVT_RECV :

          /* Do nothing. The processing for RECV will be done when we find the SIGN entry */

          break;
        case EVT_SENDSIGN :
          if (!entry.isHash()) {
            assert(entry.getContent().length >= signatureSize);
            if (!firstEntry) {
              if (lastEntry.getType() != EVT_SEND) {
                logger.warn("Spurious EVT_SENDSIGN in snippet; flagging invalid");
                return false;
              }
              EvtSendSign evtSendSign = new EvtSendSign(sib,signatureSize);
              boolean sendWasHashed = lastEntry.isHash();
              if (sendWasHashed) {
                byte[] actualHash = peerreview.hash(ByteBuffer.wrap(evtSendSign.getRestOfMessage()));
                if (!Arrays.equals(lastEntry.getContent(),actualHash)) {
                  logger.warn("EVT_SENDSIGN content does not match hash in EVT_SEND");
                  return false;
                }
              } else {
                if (entry.getContent().length > signatureSize) {
                  logger.warn("EVT_SENDSIGN contains extra bytes, but preceding EVT_SEND was not hashed");
                  return false;
                }
              }

              SimpleOutputBuffer authPrefix = new SimpleOutputBuffer();
              authPrefix.writeLong(lastEntry.getSeq());
              authPrefix.write(prevNodeHash);

              byte[] signedHash = peerreview.hash(authPrefix.getByteBuffer());

              int verifyResult = peerreview.verify(peerreview.getIdentifierExtractor().extractIdentifier(subjectHandle), signedHash, evtSendSign.getSignature());
              assert((verifyResult == SIGNATURE_OK) || (verifyResult == SIGNATURE_BAD));
              if (verifyResult != SIGNATURE_OK) {
                logger.warn("Signature in EVT_SENDSIGN does not match node hash of EVT_SEND");
                return false;
              }

              /* If the message in the SEND entry was for the local node, we check whether we
                 have previously delivered that message. If not, we deliver it now. */

              if (commitmentProtocol != null && (flags & FLAG_FULL_MESSAGES_SENDER) == FLAG_FULL_MESSAGES_SENDER) {
                assert !sendWasHashed;
                EvtSend<Identifier> evtSend = new EvtSend<Identifier>(new SimpleInputBuffer(lastEntry.getContent()),idSerializer,hashSize);
                Identifier destination = evtSend.getReceiverId();
                if (destination.equals(peerreview.getLocalId())) {
                  boolean previouslyReceived = false;
                  if (seqBuf.contains(lastEntry.getSeq())) {
                    previouslyReceived = true;
                  }

                  if (!previouslyReceived) {
                    logger.trace("accepting new message {}", lastEntry.getSeq());

                    /* We're cheating a little bit here... essentially we reconstruct the original
                       USERDATA message as feed it to the commitment protocol, pretending that
                       it has just arrived from the network. This causes all the right things to happen. */

                    SimpleOutputBuffer msg = new SimpleOutputBuffer();
                    msg.write(evtSend.getPayload().array(),evtSend.getPayload().position(),evtSend.getPayload().remaining());
                    msg.write(evtSendSign.getRestOfMessage());
                    UserDataMessage<Handle> message = new UserDataMessage<Handle>(lastEntry.getSeq(),subjectHandle,prevPrevNodeHash,evtSendSign.getSignature(),msg.getByteBuffer(),evtSend.getPayload().remaining());

                    commitmentProtocol.handleIncomingMessage(subjectHandle, message, null);
                  }
                }
              }
            }
          }
          break;
        case EVT_SIGN :
          assert(entry.getContent().length == (hashSize + signatureSize));
          if (!firstEntry) {

            /* RECV entries must ALWAYS be followed by a SIGN */

            if (lastEntry.getType() != EVT_RECV) {
              logger.warn("Spurious EVT_SIGN in snippet; flagging invalid");
              return false;
            }

            /* Decode all the values */

            EvtSign evtSign = new EvtSign(sib,hashSize,signatureSize);
            EvtRecv<Handle> evtRecv = new EvtRecv<Handle>(new SimpleInputBuffer(lastEntry.getContent()),handleSerializer,hashSize);

            Handle senderHandle = evtRecv.getSenderHandle();
            long senderSeq = evtRecv.getSenderSeq();

            SimpleOutputBuffer sob = new SimpleOutputBuffer();
            Identifier senderId = peerreview.getIdentifierExtractor().extractIdentifier(senderHandle);

            Identifier subjectId = peerreview.getIdentifierExtractor().extractIdentifier(subjectHandle);
            subjectId.serialize(sob);

            sob.writeBoolean(evtRecv.getHash() != null);
            sob.write(evtRecv.getPayload());
            if (evtRecv.getHash() != null) sob.write(evtRecv.getHash());
            /* Extract the authenticator and check it */
            byte[] senderContentHash = peerreview.hash(sob.getByteBuffer());
            Authenticator senderAuth = peerreview.extractAuthenticator(senderSeq, EVT_SEND, senderContentHash, evtSign.getHTopMinusOne(), evtSign.getSignature());

            boolean isGoodAuth = peerreview.addAuthenticatorIfValid(authStoreOrNull, senderId, senderAuth);
            if (!isGoodAuth) {
              logger.warn("Snippet contains a RECV from {} whose signature does not match; flagging invalid", senderHandle);
              return false;
            }
          }

          break;
        case EVT_ACK :
        {
          /* Decode all the values */

          EvtAck<Identifier> evtAck = new EvtAck<Identifier>(sib,idSerializer,hashSize,signatureSize);

          /* Look up the entry in the send-entry cache */

          SendEntryRecord seidx = secache.get(evtAck.getAckedSeq());

          if (seidx == null) {
            /* No check possible, and none needed: the SEND may lie before the start of the snippet, or be
               hashed, so its content is not here. The ACK still counts in the snippet's hash chain. */
          } else {
            /* Extract the authenticator and check it */

            secache.remove(evtAck.getAckedSeq());

            SimpleOutputBuffer sob = new SimpleOutputBuffer();
            subjectHandle.serialize(sob);
            sob.writeLong(evtAck.getAckedSeq());
            byte[] receiverContentHash = peerreview.hash(sob.getByteBuffer(),seidx.hashedPlusPayload);

            byte[] bytes = peerreview.hash(evtAck.getPeerSeq(), EVT_RECV, evtAck.getHTopMinusOne(), receiverContentHash);
            Authenticator receiverAuth = new Authenticator(evtAck.getPeerSeq(),bytes,evtAck.getSignature());

            boolean isGoodAuth = peerreview.addAuthenticatorIfValid(authStoreOrNull, evtAck.getRemoteId(), receiverAuth);
            if (!isGoodAuth) {
              logger.warn("Snippet contains an ACK from {} whose signature does not match; flagging invalid", evtAck.getRemoteId());
              return false;
            }
          }
          break;
        }
        case EVT_CHECKPOINT :
        case EVT_VRF :
        case EVT_INIT :
        case EVT_CHOOSE_Q :
        case EVT_CHOOSE_RAND :
          break;
        default :
          assert(entry.getType() > EVT_MAX_RESERVED);
          break;
      }
      } catch (IOException ioe) {
        logger.warn("Error verifying SnippitEntry {}", entry, ioe);
        return false;
      }

      /* Remember where the last RECV entry was */

      lastEntry = entry;

      /* If this is the first entry, its hash must match the first authenticator in the challenge */

      if (keyNodeHash != null && !keyNodeFound) {
        if (Arrays.equals(currentNodeHash, keyNodeHash)) {
          keyNodeFound = true;
        }
      }

      if (firstEntry) {
        if (startWithCheckpoint) {
          if ((entry.getType() != EVT_CHECKPOINT) && (entry.getType() != EVT_INIT)) {
            logger.warn("Previous checkpoint requested, but not included; flagging invalid");
            return false;
          }
          if (entry.isHash()) {
            logger.warn("Previous checkpoint requested, but only hash is included; flagging invalid");
            return false;
          }
        } else {
        }

        firstEntry = false;
      }

      /* Skip ahead to the next entry in the snippet */


    }

    if (lastEntry.getType() == EVT_RECV) {
      logger.warn("Log snippet ends with a RECV event; missing mandatory SIGN");
      return false;
    }

    if (lastEntry.getType() == EVT_SEND) {
      logger.warn("Log snippet ends with a SEND event; missing mandatory SENDSIGN");
      return false;
    }

    if (keyNodeHash != null && !keyNodeFound) {
      logger.warn("Key node not found in log snippet; flagging invalid");
      return false;
    }

    return true;
  }
}
