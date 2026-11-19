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
package org.mpisws.p2p.transport.peerreview.evidence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.EvidenceRecord;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AccusationMessage;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessage;
import org.slf4j.Logger;
import rice.Continuation;
import rice.p2p.commonapi.rawserialization.RawSerializable;

import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_EXPOSED;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_TRUSTED;

/**
 *  This protocol transfers evidence to the witnesses
 */

public class EvidenceTransferProtocolImpl<Handle extends RawSerializable, Identifier extends RawSerializable>
    implements EvidenceTransferProtocol<Handle, Identifier> {
  static final int WITNESS_SET_VALID_MICROS = 300*1000000;

  PeerReview<Handle, Identifier> peerreview;
  PeerInfoStore<Handle, Identifier> infoStore;

  Map<Identifier, CacheInfo> witnessCache = new HashMap<Identifier, CacheInfo>();
  Map<Identifier, List<MessageInfo<Handle, Identifier>>> pendingMessage = new HashMap<Identifier, List<MessageInfo<Handle, Identifier>>>();
  Collection<QueryInfo> pendingQuery = new HashSet<QueryInfo>();

  Logger logger;

  public EvidenceTransferProtocolImpl(PeerReview<Handle, Identifier> peerreview, IdentityTransport<Handle, Identifier> transport, PeerInfoStore<Handle, Identifier> infoStore) {
    this.peerreview = peerreview;
    this.infoStore = infoStore;

    logger = peerreview.getEnvironment().getLogManager().getLogger(EvidenceTransferProtocolImpl.class, null);
  }

  /**
   *  Called when the local node learns about the members of another node's witness set.
   *
   *  1) Send any pending messages to the witnesses
   *
   *  2) Try to fill in any pending queries, (there is usually only going to be 1 or zero)
   */
  @Override
  public void notifyWitnessSet(Identifier subject, Collection<Handle> witnesses) {

    /* Create a cache entry, if necessary */

    CacheInfo cacheInfo = witnessCache.get(subject);
    if (cacheInfo == null) {
      cacheInfo = new CacheInfo(subject);
      witnessCache.put(subject,cacheInfo);
    }
    cacheInfo.updateWitnesses(witnesses);

    Collection<MessageInfo<Handle, Identifier>> sendMe = pendingMessage.remove(subject);
    if (sendMe != null) {
      for (MessageInfo<Handle, Identifier> msg : sendMe) {
        doSendMessageToWitnesses(cacheInfo.witness,msg);
      }
    }
    /**
     *  If this witness set was part of a query, we update the results and answer if possible
     */
    for (QueryInfo queryInfo : pendingQuery) {
      queryInfo.updateWitnesses(cacheInfo);
    }
  }

  void doSendMessageToWitnesses(Collection<Handle> witnesses, MessageInfo<Handle, Identifier> msg) {

    for (Handle handle : witnesses) {
      peerreview.transmit(handle,msg.message,msg.deliverAckToMe,msg.options);
    }
  }

  /**
   * Send a message to all the members of the target node's witness set. If the
   * witness set is not known, we need to make an upcall to the application
   * first
   */

  @Override
  public void sendMessageToWitnesses(Identifier subject, PeerReviewMessage message,
      MessageCallback<Handle, ByteBuffer> deliverAckToMe,
      Map<String, Object> options) {
    MessageInfo<Handle, Identifier> msg = new MessageInfo<>(subject, message, deliverAckToMe, options);
    Collection<Handle> witnesses = getWitnesses(subject);
    if (witnesses == null) {
      List<MessageInfo<Handle, Identifier>> foo = pendingMessage.get(subject);
      if (foo == null) {
        foo = new ArrayList<MessageInfo<Handle, Identifier>>();
        pendingMessage.put(subject, foo);
      }
      foo.add(msg);
      requestWitnesses(subject);
    } else {
      doSendMessageToWitnesses(witnesses, msg);
    }
  }

  /* Requests witness sets for several nodes */
  @Override
  public void requestWitnesses(Collection<Identifier> subjectList, Continuation<Map<Identifier,Collection<Handle>>, Exception> continuation) {
    new QueryInfo(subjectList,continuation);
  }

  /**
   * Either returns the valid cached value, or returns null
   *
   * Often requestWitnesses() is called next
   *
   * If it returns null, then notifyWitnessSet() will be called later
   */
  protected Collection<Handle> getWitnesses(Identifier subject) {
    CacheInfo foo = witnessCache.get(subject);
    if (foo == null || foo.witness == null || foo.witness.isEmpty() || !foo.isValid()) {
      return null;
    }
    return foo.witness;
  }

  /**
   * Only call me if there isn't already a valid witness set
   */
  protected void requestWitnesses(Identifier subject) {
    CacheInfo foo = witnessCache.get(subject);
    if (foo != null && foo.witnessesRequested) return;

    foo = new CacheInfo(subject);
    witnessCache.put(subject, foo);
    peerreview.getApp().getWitnesses(subject,this);
  }

  /**
   * Since applications like ePOST must communicate to determine the current
   * witness set of a node (which is expensive), we cache witness sets for a
   * while. An entry in this cache can also be half-complete, meaning that an
   * upcall has been made to the application to determine the witness set, but
   * the result has not been received yet.
   */
  public class CacheInfo {
    Identifier subject;
    boolean witnessesRequested;
    Collection<Handle> witness;
    private long validUntil;

    public CacheInfo(Identifier subject) {
      this.subject = subject;
      validUntil = 0;
      witnessesRequested = true;
    }

    public void updateWitnesses(Collection<Handle> witnesses) {
      /* Store the witnesses in the cache entry and make it valid */
      witnessesRequested = false;
      this.witness = witnesses;
      validUntil = peerreview.getTime() + WITNESS_SET_VALID_MICROS;
    }

    public boolean isValid() {
      return validUntil > peerreview.getTime();
    }
  }



  /**
   * When we have a message for a node whose witness set is not (yet) known, it
   * is queued in here
   */
  public static class MessageInfo<Handle, Identifier> {
    private final PeerReviewMessage message;
    private final MessageCallback<Handle, ByteBuffer> deliverAckToMe;
    private final Map<String, Object> options;

    public MessageInfo(Identifier subject, PeerReviewMessage message,
        MessageCallback<Handle, ByteBuffer> deliverAckToMe,
        Map<String, Object> options) {
      this.message = message;
      this.deliverAckToMe = deliverAckToMe;
      this.options = options;
    }
  }

  /**
   * Here we remember all the questions we've asked the application about
   * witness sets
   */

  public class QueryInfo {
    Map<Identifier, Collection<Handle>> subjectList;
    Continuation<Map<Identifier, Collection<Handle>>, Exception> c;
    int numWitnessesWaitingFor = 0;
    boolean done = false;

    public QueryInfo(Collection<Identifier> subjects, Continuation<Map<Identifier, Collection<Handle>>, Exception> continuation) {
      subjectList = new HashMap<Identifier, Collection<Handle>>();
      numWitnessesWaitingFor = subjects.size();
      this.c = continuation;

      pendingQuery.add(this);

      for (Identifier identifier : subjects) {
        Collection<Handle> foo = getWitnesses(identifier);
        subjectList.put(identifier, foo);
        if (foo == null) {
          requestWitnesses(identifier);
        } else {
          numWitnessesWaitingFor--;
        }
      }

      if (numWitnessesWaitingFor == 0) {
        done();
      }
    }

    public void updateWitnesses(CacheInfo cacheInfo) {
      if (subjectList.containsKey(cacheInfo.subject)) {
        Collection<Handle> ret = subjectList.put(cacheInfo.subject, cacheInfo.witness);
        if (ret == null || ret.isEmpty()) {
          numWitnessesWaitingFor--;
          if (numWitnessesWaitingFor == 0) {
            done();
          }
        }
      }
    }

    public void done() {
      if (done) return;
      done = true;
      pendingQuery.remove(this);
      c.receiveResult(subjectList);
    }
  }

  /**
   * When this is called, some node has asked us for evidence about another
   * node, and we have found either (a) an unanswered challenge, or (b) a proof
   * of misbehavior. We send back an ACCUSATION message that contains the
   * evidence.
   */
  @Override
  public void sendEvidence(Handle target, Identifier subject) {
    logger.trace("sendEvidence({}, subject={})", target, subject);

    int status = infoStore.getStatus(subject);
    assert (status != STATUS_TRUSTED);

    /* Get the evidence */

    EvidenceRecord<Handle, Identifier> evidenceRecord = (status == STATUS_EXPOSED)
      ? infoStore.statProof(subject)
      : infoStore.statFirstUnansweredChallenge(subject);

    assert (evidenceRecord != null);

    try {
      /* Put together an ACCUSATION message */
      Evidence evidence = infoStore.getEvidence(evidenceRecord.getOriginator(),subject, evidenceRecord.getTimeStamp());
      AccusationMessage<Identifier> accusation =
        new AccusationMessage<Identifier>(subject, evidenceRecord, evidence);

      /* ... and send it */

      logger.atTrace().log(() -> "Sending accusation about "+subject+" ("+evidenceRecord.getOriginator()+", "+evidenceRecord.getTimeStamp()+") as a "+((status == STATUS_EXPOSED) ? "proof" : "challenge")+" to "+target);
      peerreview.transmit(target, accusation, null, null);
    } catch (IOException ioe) {
      logger.warn("Error sending evidence.");
    }
  }
}
