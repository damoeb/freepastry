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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.evidence.AuditResponse;
import org.mpisws.p2p.transport.peerreview.evidence.ChallengeAudit;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocol;
import org.mpisws.p2p.transport.peerreview.evidence.ProofInconsistent;
import org.mpisws.p2p.transport.peerreview.evidence.ProofNonconformant;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AuthRequest;
import org.mpisws.p2p.transport.peerreview.message.AuthResponse;
import org.mpisws.p2p.transport.peerreview.message.ChallengeMessage;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessage;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;
import org.mpisws.p2p.transport.peerreview.replay.playback.ReplaySM;
import org.slf4j.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.selector.TimerTask;

import static org.mpisws.p2p.transport.peerreview.Basics.renderStatus;

public class AuditProtocolImpl<Handle extends RawSerializable, Identifier extends RawSerializable> implements AuditProtocol<Handle, Identifier> {

  PeerReview<Handle, Identifier> peerreview;
  SecureHistory history;
  PeerInfoStore<Handle, Identifier> infoStore;
  AuthenticatorStore<Identifier> authInStore;
  IdentityTransport<Handle, Identifier> transport;
  AuthenticatorStore<Identifier> authOutStore;
  EvidenceTransferProtocol<Handle, Identifier> evidenceTransferProtocol;
  AuthenticatorStore<Identifier> authCacheStore;

  /**
   * Here we remember calls to investigate() that have not been resolved yet
   */
  Map<Identifier,ActiveInvestigationInfo<Handle>> activeInvestigation = new HashMap<Identifier, ActiveInvestigationInfo<Handle>>();

  /**
   * Here we remember calls to investigate() that have not been resolved yet
   */
  Map<Identifier,ActiveAuditInfo<Handle, Identifier>> activeAudit = new HashMap<Identifier, ActiveAuditInfo<Handle,Identifier>>();

  int logDownloadTimeout;
  boolean replayEnabled;
  long lastAuditStarted;
  long auditIntervalMillis;

  /**
   * Null if there is no current timer.
   */
  protected TimerTask auditTimer;
  protected TimerTask progressTimer;

  Logger logger;

  public AuditProtocolImpl(PeerReview<Handle, Identifier> peerreview,
      SecureHistory history, PeerInfoStore<Handle, Identifier> infoStore,
      AuthenticatorStore<Identifier> authInStore,
      IdentityTransport<Handle, Identifier> transport,
      AuthenticatorStore<Identifier> authOutStore,
      EvidenceTransferProtocol<Handle, Identifier> evidenceTransferProtocol,
      AuthenticatorStore<Identifier> authCacheStore) {
    this.logger = peerreview.getEnvironment().getLogManager().getLogger(AuditProtocolImpl.class, null);
    this.peerreview = peerreview;
    this.history = history;
    this.infoStore = infoStore;
    this.authInStore = authInStore;
    this.transport = transport;
    this.authOutStore = authOutStore;
    this.evidenceTransferProtocol = evidenceTransferProtocol;
    this.authCacheStore = authCacheStore;

    this.progressTimer = null;
    this.logDownloadTimeout = DEFAULT_LOG_DOWNLOAD_TIMEOUT;
    this.replayEnabled = true;
    this.lastAuditStarted = peerreview.getTime();
    this.auditIntervalMillis = DEFAULT_AUDIT_INTERVAL_MILLIS;

    auditTimer = peerreview.getEnvironment().getSelectorManager().schedule(new TimerTask() {
      @Override
      public void run() {
        auditsTimerExpired();
      }},auditIntervalMillis);
  }


  public void setLogDownloadTimeout(int timeoutMicros) {
    this.logDownloadTimeout = timeoutMicros;
  }

  public void disableReplay() {
    this.replayEnabled = false;
  }

  /**
   *  Starts to audit a node
   */
  void beginAudit(Handle target, Authenticator authFrom, Authenticator authTo, byte needPrevCheckpoint, boolean replayAnswer) {
    long evidenceSeq = peerreview.getEvidenceSeq();

    /* Put together an AUDIT challenge */
    ChallengeAudit audit = new ChallengeAudit(needPrevCheckpoint,authFrom,authTo);
    ChallengeMessage<Identifier> auditRequest = new ChallengeMessage<Identifier>(peerreview.getLocalId(),evidenceSeq,audit);

    /* Create an entry in our audit buffer */

    ActiveAuditInfo<Handle, Identifier> aai = new ActiveAuditInfo<Handle, Identifier>(
        target,replayAnswer,false,peerreview.getTime()+logDownloadTimeout,auditRequest,evidenceSeq,null);
    activeAudit.put(peerreview.getIdentifierExtractor().extractIdentifier(target),aai);

    /* Send the AUDIT challenge to the target node */

    logger.debug("Sending AUDIT request to {} (range={}-{},eseq={})", aai.target, authFrom.getSeq(), authTo.getSeq(), evidenceSeq);
    peerreview.transmit(target, auditRequest, null, null);
  }

  public void startAudits() {
    /* Find out which nodes we are currently witnessing */

    Collection<Handle> buffer = peerreview.getApp().getMyWitnessedNodes();

    /* For each of these nodes ... */

    for (Handle h : buffer) {
      Identifier i = peerreview.getIdentifierExtractor().extractIdentifier(h);
      /* If the node is not trusted, we don't audit */

      int status = infoStore.getStatus(i);
      if (status != STATUS_TRUSTED) {
        logger.atDebug().log(() -> "Node "+h+" is "+renderStatus(status)+"; skipping audit");
        continue;
      }

      /* If a previous audit of this node is still in progress, we skip this round */

      if (!activeAudit.containsKey(h)) {

        /* Retrieve the node's newest and last-checked authenticators. Note that we need
           at least two distinct authenticators to be able to audit a node. */

        Authenticator authFrom;
        Authenticator authTo;
        boolean haveEnoughAuthenticators = true;
        byte needPrevCheckpoint = 0;

        logger.info("Starting to audit {}", h);

        if ((authFrom = infoStore.getLastCheckedAuth(i)) == null) {
          if ((authFrom = authInStore.getOldestAuthenticator(i)) != null) {
            logger.debug("We haven't audited this node before; using oldest authenticator");
            needPrevCheckpoint = 1;
          } else {
            logger.debug("We don't have any authenticators for this node; skipping this audit");
            haveEnoughAuthenticators = false;
          }
        }

        if ((authTo = authInStore.getMostRecentAuthenticator(i)) == null) {
          logger.debug("No recent authenticator; skipping this audit");
          haveEnoughAuthenticators = false;
        }

        if (haveEnoughAuthenticators && authFrom.getSeq()>authTo.getSeq()) {
          logger.debug("authFrom>authTo; skipping this audit");
          haveEnoughAuthenticators = false;
        }

        /* Add an entry to our table of ongoing audits. This entry will periodically
           be checked by cleanupAudits() */

        if (haveEnoughAuthenticators) {
          beginAudit(h, authFrom, authTo, needPrevCheckpoint, true);
        }
      } else {
        logger.warn("Node {} is already being audited; skipping", h);
      }
    }

    /* Make sure that makeProgressOnAudits() is called regularly */
    scheduleProgressTimer();
  }

  protected void scheduleProgressTimer() {
    if (progressTimer == null) {
      progressTimer = peerreview.getEnvironment().getSelectorManager().schedule(new TimerTask() {

        @Override
        public void run() {
          makeProgressTimerExpired();
        }}, PROGRESS_INTERVAL_MILLIS);
    }
  }

  /**
   * Called periodically to check if all audits have finished. An audit may not
   * finish if either (a) the target does not respond, or (b) the target's
   * response is discarded because it is malformed. In either case, we report
   * the failed AUDIT challenge to the witnesses.
   */
  void cleanupAudits() {
    long now = peerreview.getTime();

    for (ActiveAuditInfo<Handle, Identifier> foo : activeAudit.values()) {
      if ((now >= foo.currentTimeout) && !foo.isReplaying) {
        Identifier i = peerreview.getIdentifierExtractor().extractIdentifier(foo.target);

        logger.warn("No response to AUDIT request; filing as evidence {}", foo.evidenceSeq);
        try {
          infoStore.addEvidence(peerreview.getLocalId(), i, foo.evidenceSeq,
              foo.request.challenge);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        peerreview.sendEvidenceToWitnesses(i, foo.evidenceSeq, foo.request.challenge);

        terminateAudit(foo.target);
      }
    }
  }

  void terminateAudit(Handle h) {
    activeAudit.remove(h);
  }

  void terminateInvestigation(Handle h) {
    activeInvestigation.remove(h);
  }

  void makeProgressOnInvestigations() {
    long now = peerreview.getTime();

    for (ActiveInvestigationInfo<Handle> aii : activeInvestigation.values()) {
      if (aii.currentTimeout < now) {
        long authFromSeq = aii.authFrom!=null ? aii.authFrom.getSeq() : -1;
        long authToSeq = aii.authTo!=null ? aii.authTo.getSeq() : -1;
        if ((0<=authFromSeq) && (authFromSeq <= aii.since) && (aii.since < authToSeq)) {
          logger.debug("Investigation of {} (since {}) is proceeding with an audit", aii.target, aii.since);

          if (authToSeq > authFromSeq) {
            logger.trace("Authenticators: {}-{}", authFromSeq, authToSeq);
            beginAudit(aii.target, aii.authFrom, aii.authTo, FLAG_FULL_MESSAGES_SENDER, false);
            terminateInvestigation(aii.target);
          } else {
            logger.warn("Cannot start investigation; authTo<authFrom ?!? (since={}, authFrom={}, authTo={})", aii.since, authFromSeq, authToSeq);
            terminateInvestigation(aii.target);
          }
        } else {
          logger.debug("Retransmitting investigation requests for {} at {}", aii.target, aii.since);
          sendInvestigation(aii.target);
          aii.currentTimeout += INVESTIGATION_INTERVAL_MILLIS;
        }
      }
    }
  }

  private void sendInvestigation(Handle target) {

  }


  void setAuditInterval(long newIntervalMillis) {
    auditIntervalMillis = newIntervalMillis;
    auditTimer.cancel();
    startAuditTimer();
  }


  /**
   * A periodic timer to audit all nodes for which we are a witness
   */
  protected void auditsTimerExpired() {
    startAudits();
    lastAuditStarted = peerreview.getTime();
    startAuditTimer();
  }

  protected void startAuditTimer() {
    long now = peerreview.getTime();
    long nextTimeout = lastAuditStarted + (long)((500+(peerreview.getRandomSource().nextInt(1000)))*0.001*auditIntervalMillis);
    if (nextTimeout <= now) {
      nextTimeout = now + 1;
    }

    auditTimer = peerreview.getEnvironment().getSelectorManager().schedule(new TimerTask() {

      @Override
      public void run() {
        auditsTimerExpired();
      }

    },nextTimeout-now);
  }

  /**
   * While some audits haven't finished, we must call makeProgress() regularly
   */
  protected void makeProgressTimerExpired() {
    progressTimer.cancel();
    cleanupAudits();
    makeProgressOnInvestigations();
    if (progressTimer == null && ((activeAudit.size() > 0) || (activeInvestigation.size() > 0))) {
      progressTimer = peerreview.getEnvironment().getSelectorManager().schedule(new TimerTask() {
        @Override
        public void run() {
          makeProgressTimerExpired();
        }
      }, PROGRESS_INTERVAL_MILLIS);
    }
  }

  /**
   * Called by the challenge/response protocol if we have received a response to
   * an AUDIT challenge. At this point, we already know that we have all the
   * necessary certificates (because of the statement protocol).
   */
  public void processAuditResponse(Identifier subject, long timestamp, AuditResponse<Handle> response) throws IOException {
    LogSnippet snippet = response.getLogSnippet();
    ActiveAuditInfo<Handle, Identifier> aai = findOngoingAudit(subject, timestamp);

    /* Read the header of the log snippet */

    ChallengeAudit challengeAudit = (ChallengeAudit)aai.request.challenge;
    long fromSeq = challengeAudit.from.getSeq();
    Authenticator toAuthenticator = challengeAudit.to;
    long toSeq = toAuthenticator.getSeq();
    byte[] currentNodeHash = snippet.baseHash;
    byte[] initialNodeHash = snippet.baseHash;
    Handle subjectHandle = response.getLogOwner();
    long currentSeq = snippet.getFirstSeq();
    long initialCurrentSeq = currentSeq;

    /* Retrieve all the authenticators we have for this node */

    List<Authenticator> auths = authInStore.getAuthenticators(subject, fromSeq, toSeq);

    /* We have to keep a small fraction of the authenticators around so we can later
       answer AUTHREQ messages from other nodes. */

    logger.debug("Checking of AUDIT response against {} authenticators [{}-{}]", auths.size(), fromSeq, toSeq);

    long mostRecentAuthInCache = -1;
    Authenticator mrauth = authCacheStore.getMostRecentAuthenticator(subject);
    if (mrauth != null) {
      mostRecentAuthInCache = mrauth.getSeq();
    }

    for (int i=auths.size()-1; i>=0; i--) {
      Authenticator thisAuth = auths.get(i);
      long thisSeq = thisAuth.getSeq();
      if (thisSeq > (mostRecentAuthInCache + (AUTH_CACHE_INTERVAL*1000L))) {
        logger.trace("Caching auth {} for {}", thisSeq, subject);
        authCacheStore.addAuthenticator(subject, thisAuth);
        mostRecentAuthInCache = thisSeq;
      }
    }

    /* We read the entries one by one, calculating the node hashes as we go, and we compare
       the node hashes to our (sorted) list of authenticators. If there is any divergence,
       we have proof that the node is misbehaving. */

    int authPtr = auths.size() - 1;
    Authenticator nextAuth = (authPtr<0) ? null : auths.get(authPtr);
    long nextAuthSeq = (authPtr<0) ? -1 : nextAuth.getSeq();
    logger.trace("  NA #{} {}", authPtr, nextAuthSeq);

    for (SnippetEntry sEntry : snippet.entries) {
      currentSeq = sEntry.seq;
      if (logger.isTraceEnabled()) logger.trace("[2] Entry type "+sEntry.type+", size="+sEntry.content.length+
          ", seq="+currentSeq+(sEntry.isHash ? " (hashed)" : ""));

      byte[] entry = sEntry.content;

      /* Calculate the node hash from the entry */

      byte[] contentHash;
      if (sEntry.isHash) {
        contentHash = sEntry.content;
        assert(contentHash.length == peerreview.getHashSizeInBytes());
      } else {
        contentHash = transport.hash(ByteBuffer.wrap(entry));
      }

      currentNodeHash = transport.hash(currentSeq, sEntry.type, currentNodeHash, contentHash);
      if (logger.isTraceEnabled()) logger.trace("NH ["+MathUtils.toBase64(currentNodeHash)+"]");

      /* If we have an authenticator for this entry (matched by sequence number), the hash in the
         authenticator must match the node hash. If not, we have a proof of misbehavior. */

      if (authPtr >= 0) {
        boolean foundMisbehavior = false;

        if (currentSeq == nextAuthSeq) {
          if (!Arrays.equals(currentNodeHash, nextAuth.getHash())) {
            logger.warn("Found a divergence for node <{}>'s authenticator #{}", subject, currentSeq);
            foundMisbehavior = true;
          } else {
            logger.trace("Authenticator verified OK");

            authPtr--;
            nextAuth = (authPtr<0) ? null : auths.get(authPtr);
            nextAuthSeq = (authPtr<0) ? -1 : nextAuth.getSeq();
            logger.trace("NA #{} {}", authPtr, nextAuthSeq);
          }
        } else if (currentSeq > nextAuthSeq) {
          logger.warn("Node {} is trying to hide authenticator #{}", subject, nextAuthSeq);
          foundMisbehavior = true;
        }

        if (foundMisbehavior) {
          logger.debug("Extracting proof of misbehavior from audit response");
          ProofInconsistent proof = new ProofInconsistent(toAuthenticator,nextAuth,snippet);

          long evidenceSeq = peerreview.getEvidenceSeq();
          logger.debug("Filing proof against {} under evidence sequence number #{}", subject, evidenceSeq);
          infoStore.addEvidence(peerreview.getLocalId(), subject, evidenceSeq, proof);
          peerreview.sendEvidenceToWitnesses(subject, evidenceSeq, proof);

          terminateAudit(aai.target);
          return;

        }
      }
    }

    /* All these authenticators for this segment checked out okay. We don't need them any more,
       so we can remove them from our local store. */
//
    logger.debug("All authenticators in range [{},{}] check out OK; flushing", fromSeq, toSeq);
    // TODO: check the old authenticators and flush only from fromSeq instead of Long.MIN_VALUE
    authInStore.flushAuthenticatorsFor(subject, Long.MIN_VALUE, toSeq);

    /* Find out if the log snippet is 'useful', i.e. if we can append it to our local history */

    String namebuf = infoStore.getHistoryName(subject);

    logger.debug("opening history for {}", namebuf);
    SecureHistory subjectHistory = peerreview.getHistoryFactory().open(namebuf, "w");

    boolean logCanBeAppended = false;
    long topSeq = 0;
    if (subjectHistory != null) {
      topSeq = subjectHistory.getTopLevelEntry().getSeq();
      if (topSeq >= fromSeq)
        logCanBeAppended = true;
    } else {
      logCanBeAppended = true;
    }

    /* If it should not be replayed (e.g. because it was retrieved during an investigation),
       we stop here */

    if (!aai.shouldBeReplayed/* && !logCanBeAppended*/) {
      logger.debug("This audit response does not need to be replayed; discarding");
      terminateAudit(aai.target);
      return;
    }

    /* Add entries to our local copy of the subject's history */

    logger.debug("Adding entries in snippet to log '{}'", namebuf);
    if (!logCanBeAppended)
      throw new RuntimeException("Cannot append snippet to local copy of node's history; there appears to be a gap ("+topSeq+"-"+fromSeq+")!");

    if (subjectHistory == null) {
      subjectHistory = peerreview.getHistoryFactory().create(namebuf, initialCurrentSeq-1, initialNodeHash);
      if (subjectHistory == null)
        throw new RuntimeException("Cannot create subject history: '"+namebuf+"'");
    }

    long firstNewSeq = currentSeq - (currentSeq%1000000) + 1000000;
    subjectHistory.appendSnippetToHistory(snippet);
    // TODO: verify older authenticators against the appended history

    if (replayEnabled) {

      /* We need to replay the log segment, so let's find the last checkpoint */

      short[] markerTypes = new short[]{ EVT_CHECKPOINT };
      long lastCheckpointIdx = subjectHistory.findLastEntry(markerTypes, fromSeq);

      logger.trace("LastCheckpointIdx={} (up to {})", lastCheckpointIdx, fromSeq);

      if (lastCheckpointIdx < 0) {
        logger.warn("Cannot find last checkpoint in subject history {}", namebuf);

        terminateAudit(subjectHandle);
        return;
      }

      /* Create a Verifier instance and get a Replay instance from the application */

      Verifier<Handle> verifier = peerreview.getVerifierFactory().getVerifier(subjectHistory, subjectHandle, lastCheckpointIdx, fromSeq/1000000, snippet.getExtInfo());
      PeerReviewCallback<Handle, Identifier> replayApp = peerreview.getApp().getReplayInstance(verifier);
      if (replayApp == null) throw new RuntimeException("Application returned NULL when getReplayInstance() was called");

      verifier.setApplication(replayApp);

      aai.verifier = verifier;
      aai.isReplaying = true;

      logger.info("REPLAY ============================================");
      logger.debug("Node being replayed: {}", subjectHandle);
      logger.debug("Range in log       : {}-{}", fromSeq, toSeq);

      /* Do the replay */

      ReplaySM sm = ((ReplaySM)verifier.getEnvironment().getSelectorManager());
      while(sm.makeProgress());

      boolean verifiedOK = verifier.verifiedOK();
      logger.atInfo().log(() -> "END OF REPLAY: "+(verifiedOK ? "VERIFIED OK" : "VERIFICATION FAILED")+" =================");

      /* If there was a divergence, we have a proof of misbehavior */

      if (!verifiedOK) {
        snippet = subjectHistory.serializeRange(lastCheckpointIdx, subjectHistory.getNumEntries()-1, null);
        if (snippet == null) throw new RuntimeException("Cannot serialize range for PROOF "+subject);

        long lastCheckpointSeq;
        IndexEntry foo = subjectHistory.statEntry(lastCheckpointIdx);
        if (foo == null) throw new RuntimeException("Cannot stat checkpoint entry");
        lastCheckpointSeq = foo.getSeq();

        logger.warn("Audit revealed a protocol violation; filing evidence (snippet from {})", lastCheckpointSeq);

        ProofNonconformant<Handle> proof = new ProofNonconformant<Handle>(toAuthenticator,subjectHandle,snippet);
        long evidenceSeq = peerreview.getEvidenceSeq();
        infoStore.addEvidence(peerreview.getLocalId(), subject, evidenceSeq, proof);
        peerreview.sendEvidenceToWitnesses(subject, evidenceSeq, proof);
      }
    }

    /* Terminate the audit, and remember the last authenticator for further reference */

    logger.debug("Audit completed; terminating");
    infoStore.setLastCheckedAuth(peerreview.getIdentifierExtractor().extractIdentifier(aai.target), ((ChallengeAudit)aai.request.challenge).to);
    terminateAudit(aai.target);
  }

  public ActiveAuditInfo<Handle, Identifier> findOngoingAudit(Identifier subject, long evidenceSeq) {
    ActiveAuditInfo<Handle, Identifier> ret = activeAudit.get(subject);
    if (ret == null) return null;
    if (!ret.isReplaying && ret.evidenceSeq == evidenceSeq) {
        return ret;
    }
    return null;
  }

  public Evidence statOngoingAudit(Identifier subject, long evidenceSeq) {
    ActiveAuditInfo<Handle, Identifier> aii = findOngoingAudit(subject, evidenceSeq);
    if (aii == null) return null;
    return aii.request.challenge;
  }

  /* Handle an incoming datagram, which could be either a request for authenticators, or a response to such a request */

  public void handleIncomingDatagram(Handle handle, PeerReviewMessage message) {

    switch (message.getType()) {
      case MSG_AUTHREQ: { /* Request for authenticators */
        AuthRequest<Identifier> request = (AuthRequest<Identifier>)message;

          /* The request contains a timestamp T; we're being asked to return two authenticators,
             one has to be older than T, and the other must be as recent as possible */

          logger.info("Received authenticator request for {} (since {})", request.subject, request.timestamp);

          /* There are several places we might find 'old' authenticators. Check all of them,
             and pick the authenticator that is closest to T */

          Authenticator a1 = authInStore.getLastAuthenticatorBefore(request.subject, request.timestamp);
          Authenticator a2 = authCacheStore.getLastAuthenticatorBefore(request.subject, request.timestamp);
          Authenticator a3 = authInStore.getOldestAuthenticator(request.subject);

          Authenticator best = null;

          if (a1 != null && (best == null || (!(best.getSeq()<request.timestamp) && (a1.getSeq()<request.timestamp)) || ((best.getSeq()<request.timestamp) && (a1.getSeq()<request.timestamp) && (best.getSeq()<a1.getSeq())))) {
            best = a1;
          }

          if (a2 != null && (best == null || (!(best.getSeq()<request.timestamp) && (a2.getSeq()<request.timestamp)) || ((best.getSeq()<request.timestamp) && (a2.getSeq()<request.timestamp) && (best.getSeq()<a2.getSeq())))) {
            best = a2;
          }

          if (a3 != null && (best == null || (!(best.getSeq()<request.timestamp) && (a3.getSeq()<request.timestamp)) || ((best.getSeq()<request.timestamp) && (a3.getSeq()<request.timestamp) && (best.getSeq()<a3.getSeq())))) {
            best = a3;
          }

          /* Recent authenticators can be found in two places; check both */

          Authenticator foo = authInStore.getMostRecentAuthenticator(request.subject);
          if (foo == null) {
            foo = authCacheStore.getMostRecentAuthenticator(request.subject);
          }

          if (best != null && foo != null) {
            AuthResponse<Identifier> response = new AuthResponse<Identifier>(request.subject,best,foo);
            peerreview.transmit(handle, response, null, null);
          } else {
            logger.warn("Cannot respond to this request; we don't have any authenticators for {}", request.subject);
          }
        }

        break;
      case MSG_AUTHRESP: { /* Response to a request for authenticators */
        AuthResponse<Identifier> response = (AuthResponse<Identifier>)message;
          logger.debug("Received AUTHRESP(<{}>, {}..{}) from {}", response.subject, response.authFrom, response.authTo, handle);

          // TODO: check the signatures of response.authFrom and response.authTo
          int idx = -1;
          ActiveInvestigationInfo<Handle> investigation = activeInvestigation.get(response.subject);
          if (idx >= 0) {
            if (investigation.authFrom == null || (response.authFrom.getSeq()<investigation.authFrom.getSeq())) {
              investigation.authFrom =  response.authFrom;
            }

            if (investigation.authTo == null || (response.authTo.getSeq()>investigation.authTo.getSeq())) {
              investigation.authTo = response.authTo;
            }
          } else {
            logger.warn("AUTH response does not match any ongoing investigations; ignoring");
          }
        }

        break;
      default:
        throw new RuntimeException("AuditProtocol cannot handle incoming datagram type #"+message.getType());
    }
  }

  /**
   * Starts an investigation by sending authenticator requests to all the
   * witnesses. Most of them are (hopefully) going to respond; we're going to
   * pick the authenticators that work best for us.
   */
  protected void sendInvestigation(ActiveInvestigationInfo<Handle> investigation) {
    Identifier id = peerreview.getIdentifierExtractor().extractIdentifier(investigation.target);
    AuthRequest<Identifier> request = new AuthRequest<Identifier>(investigation.since,id);

    evidenceTransferProtocol.sendMessageToWitnesses(id, request, null, null);
  }

  /**
   * Start an investigation
   * since is in millis
   */
  public void investigate(Handle target, long since) {
    Identifier id = peerreview.getIdentifierExtractor().extractIdentifier(
        target);
    ActiveInvestigationInfo<Handle> investigation = activeInvestigation.get(id);
    if (investigation != null) {
      if (since * 1000000 > investigation.since) {
        logger.debug("Skipping investigation request for {} at {}, since an investigation at {} is already ongoing", target, since, investigation.since);
        return;
      }

      if (logger.isDebugEnabled()) logger.debug("Extending existing investigation from "+ investigation.since + " to " + since * 1000000);
      investigation.since = since * 1000000; /* log granularity */
    } else {
      investigation = new ActiveInvestigationInfo<Handle>(target, since * 1000000,
          peerreview.getTime() + INVESTIGATION_INTERVAL_MILLIS, null, null);
      activeInvestigation.put(id, investigation);
    }
    sendInvestigation(investigation);
    scheduleProgressTimer();
  }
}
