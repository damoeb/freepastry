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
package org.mpisws.p2p.transport.peerreview.statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.audit.LogSnippet;
import org.mpisws.p2p.transport.peerreview.challenge.ChallengeResponseProtocol;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.evidence.AuditResponse;
import org.mpisws.p2p.transport.peerreview.evidence.ChallengeAudit;
import org.mpisws.p2p.transport.peerreview.evidence.ProofInconsistent;
import org.mpisws.p2p.transport.peerreview.evidence.ProofNonconformant;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AccusationMessage;
import org.mpisws.p2p.transport.peerreview.message.ResponseMessage;
import org.mpisws.p2p.transport.peerreview.message.Statement;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;
import org.slf4j.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.p2p.util.tuples.Tuple;

import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.CERT_MISSING;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.CHAL_AUDIT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.CHAL_SEND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SEND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.FLAG_INCLUDE_CHECKPOINT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.INVALID;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.PROOF_INCONSISTENT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.PROOF_NONCONFORMANT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SEQS_PER_MILLI;

/**
 * The purpose of this protocol is to make sure that we have all the nodeID
 * certificates we need. For each incoming message, it checks whether any
 * additional certificates are needed, e.g. to check evidence about another
 * node. If yes, it temporarily queues the message and requests the certificate
 * from the sender.
 */
public class StatementProtocol<Handle extends RawSerializable, Identifier extends RawSerializable> {
  static final int STATEMENT_COMPLETION_TIMEOUT_MILLIS = 1000;


  private final Logger logger;

  private final Map<Identifier, List<IncompleteStatement<Handle, Identifier>>> incompleteStatement =
    new HashMap<Identifier, List<IncompleteStatement<Handle, Identifier>>>();

  private final ChallengeResponseProtocol<Handle, Identifier> challengeProtocol;
  private final IdentityTransport<Handle, Identifier> transport;
  private final PeerReview<Handle, Identifier> peerreview;

  public StatementProtocol(
      PeerReview<Handle, Identifier> peerreview,
      ChallengeResponseProtocol<Handle, Identifier> challengeProtocol,
      PeerInfoStore<Handle, Identifier> infoStore,
      IdentityTransport<Handle, Identifier> transport) {
    this.peerreview = peerreview;
    this.challengeProtocol = challengeProtocol;
    this.transport = transport;

    this.logger = peerreview.getEnvironment().getLogManager().getLogger(StatementProtocol.class, null);
  }

  /**
   * Called if we have received a certificate for a new nodeID. If any messages
   * in our queue were waiting for this certificate, we may be able to forward
   * them now.
   */
  public void notifyCertificateAvailable(Identifier id) {
   Collection<IncompleteStatement<Handle, Identifier>> foo = incompleteStatement.get(id);
   if (foo != null) {
     for (IncompleteStatement<Handle, Identifier> is : foo) {
       if (!is.isFinished() && is.isMissingCertificate()) {
         makeProgressOnStatement(is);
       }
     }
   }
  }

  /**
   *  If a message hangs around in our queue for too long, we discard it
   */
  public void cleanupIncompleteStatements() {
    long now = peerreview.getTime();
    Iterator<List<IncompleteStatement<Handle, Identifier>>> fooIter = incompleteStatement.values().iterator();
    while(fooIter.hasNext()) {
      List<IncompleteStatement<Handle, Identifier>> foo = fooIter.next();
      Iterator<IncompleteStatement<Handle, Identifier>> barIter = foo.iterator();
      while(barIter.hasNext()) {
        IncompleteStatement<Handle, Identifier> bar = barIter.next();
        if (bar.isFinished() || (bar.getCurrentTimeout() < now)) {
          if (!bar.isFinished()) {
            logger.warn("Statement by {} is incomplete after the timeout; discarding {}", bar.getSender(), bar);
          }
          barIter.remove();
        }
      }
      if (foo.isEmpty()) {
        fooIter.remove();
      }
    }
  }

  /**
   * Incoming ACCUSTION and RESPONSE messages come here first. We check whether
   * we have all the necessary nodeID certificates; if not, we request them from
   * the sender.
   */
  public void handleIncomingStatement(Handle source, Statement<Identifier> statement, Map<String, Object> options) {
    assert((statement instanceof AccusationMessage) || (statement instanceof ResponseMessage));

    logger.atInfo().log(() -> "Incoming "+((statement instanceof AccusationMessage) ? "accusation" : "response")+" from "+source);

    /* Find an empty spot in our message buffer and store the message there */
    IncompleteStatement<Handle, Identifier> idx = new IncompleteStatement<Handle, Identifier>(false,source,peerreview.getTime() + STATEMENT_COMPLETION_TIMEOUT_MILLIS,statement,false,null,options);
    List<IncompleteStatement<Handle, Identifier>> foo = incompleteStatement.get(statement.getSubject());
    if (foo == null) {
      foo = new ArrayList<IncompleteStatement<Handle,Identifier>>();
      incompleteStatement.put(statement.getSubject(), foo);
    }
    foo.add(idx);

    /* Then call our central do-something-useful function */

    makeProgressOnStatement(idx);
    cleanupIncompleteStatements();
  }

  /**
   * This reads a log snippet entry by entry, looks up all the nodeIDs that are
   * mentioned, and requests the first nodeID certificate we don't have locally.
   * Also, it checks if the snippet is malformed in any way. That way, the other
   * protocols won't have to do all these sanity checks later.
   */
  int checkSnippetAndRequestCertificates(LogSnippet snippet, IncompleteStatement<Handle, Identifier> idx) {
    Tuple<Integer,Identifier> ret = peerreview.getEvidenceTool().checkSnippet(snippet);
    int code = ret.first();
    final Identifier missingCertID = ret.second();

    if (code == CERT_MISSING) {
      assert(missingCertID != null);

      idx.markMissingCertificate(missingCertID);
      logger.debug("AUDIT RESPONSE requires certificate for {}; requesting", missingCertID);
      peerreview.requestCertificate(idx.getSender(), missingCertID);

    }

    return code;
  }

  /* Tries to make progress on some waiting message. We try to either (a) request a certificate
  we need but don't have, (b) forward the message to another protocol, or (c) discard
  the message because it's malformed. */

  @SuppressWarnings("unchecked")
  public void makeProgressOnStatement(IncompleteStatement<Handle, Identifier> idx) {
    assert !idx.isFinished();

    /* Fetch the statement and reset all the 'missing' fields */

    Statement<Identifier> statement = idx.getStatement();
    idx.clearMissingCertificate();

    Identifier subject = statement.getSubject();
    Evidence payload = statement.getEvidence();

    /* Retrieve the subject's public key if we don't have it already */

    if (!transport.hasCertificate(subject)) {
      logger.debug("Need subject's certificate to verify statement; asking source for it");
      idx.markMissingCertificate(subject);
      peerreview.requestCertificate(idx.getSender(), subject);
      return;
    }

   /* Further checking depends on the type of the statement. At this point, we may still request
      additional material from the sender, if it becomes necessary. */

   if (statement instanceof AccusationMessage) {

     /* === CHECK ACCUSATIONS === */

     switch (payload.getEvidenceType()) {
       case CHAL_AUDIT: {
         ChallengeAudit auditEvidence = (ChallengeAudit)payload;

         if (!peerreview.verify(subject, auditEvidence.from())) {
           logger.warn("AUDIT challenge's first authenticator has an invalid signature");
           idx.finish();
           return;
         }

         if (!peerreview.verify(subject, auditEvidence.to())) {
           logger.warn("AUDIT challenge's second authenticator has an invalid signature");
           idx.finish();
           return;
         }

         break;
       }
       case CHAL_SEND: {
         UserDataMessage<Handle> udm = (UserDataMessage<Handle>)payload;

         byte[] innerHash = udm.getInnerHash(subject, transport);

         // NOTE: This call puts the authenticator in our authInStore. This is intentional! Otherwise the bad guys
         // could fork their logs and send forked messages only as CHAL_SENDs.
         Authenticator authenticator = peerreview.extractAuthenticator(peerreview.getIdentifierExtractor().extractIdentifier(udm.getSenderHandle()), udm.getTopSeq(), EVT_SEND, innerHash, udm.getPreviousTopHash(), udm.getSignature());
         if (authenticator == null) {
           logger.warn("Message in SEND challenge is not properly signed; discarding");
           idx.finish();
           return;
         }
         break;
       }
       case PROOF_INCONSISTENT: {
         ProofInconsistent proofInconsistent = (ProofInconsistent)payload;

         if (!peerreview.verify(subject, proofInconsistent.getFirstAuthenticator())) {
           logger.warn("INCONSISTENT proof's first authenticator has an invalid signature");
           idx.finish();
           return;
         }

         if (!peerreview.verify(subject, proofInconsistent.getSecondAuthenticator())) {
           logger.warn("INCONSISTENT proof's second authenticator has an invalid signature");
           idx.finish();
           return;
         }

         long seq1 = proofInconsistent.getFirstAuthenticator().getSeq();
         long seq2 = proofInconsistent.getSecondAuthenticator().getSeq();
         byte[] hash1 = proofInconsistent.getFirstAuthenticator().getHash();
         byte[] hash2 = proofInconsistent.getSecondAuthenticator().getHash();

         if(proofInconsistent.getSnippet() == null) {
           if (seq1 != seq2) {
             logger.warn("INCONSISTENT-0 proof's authenticators don't have matching sequence numbers (seq1={}, seq2={}) -- discarding", seq1, seq2);
             idx.finish();
             return;
           }

           if (Arrays.equals(hash1, hash2)) {
             logger.atWarn().log(() -> "INCONSISTENT-0 proof's authenticators have the same hash (seq1="+seq1+", seq2="+seq2+") hash: "+MathUtils.toBase64(hash1)+" -- discarding");
             idx.finish();
             return;
           }
         } else {

           long firstSeq = proofInconsistent.getSnippet().getFirstSeq();

           if (!((firstSeq<seq2) && (seq2<seq1))) {
             logger.warn("INCONSISTENT-1 proof's sequence numbers do not make sense (first={}, seq2={}, seq1={}) -- discarding", firstSeq, seq2, seq1);
             idx.finish();
             return;
           }

           if (!proofInconsistent.getSnippet().checkHashChainContains(hash1, seq1, transport, logger)) {
             logger.warn("Snippet in INCONSISTENT-1 proof cannot be authenticated using first authenticator (#{}) -- discarding", seq1);
             idx.finish();
             return;
           }

           if (proofInconsistent.getSnippet().checkHashChainContains(hash2, seq2, transport, logger)) {
             logger.warn("INCONSISTENT-1 proof claims that authenticator 2 (#{}) is not in the snippet, but it is -- discarding", seq2);
             idx.finish();
             return;
           }
         }

         break;
       }
       case PROOF_NONCONFORMANT: {
         ProofNonconformant<Handle> pn = (ProofNonconformant<Handle>)payload;

         /* Is the authenticator properly signed? */

         if (!peerreview.verify(subject, pn.getTo())) {
           logger.warn("NONCONFORMANT proof's authenticator has an invalid signature");
           idx.finish();
           return;
         }

         /* Is the snippet well-formed, and do we have all the certificates? */

         switch (checkSnippetAndRequestCertificates(pn.getSnippet(), idx)) {
           case INVALID:
             logger.warn("PROOF NONCONFORMANT is not well-formed; discarding");
             idx.finish();
             return;
           case CERT_MISSING:
             return;
           default:
             break;
         }

         /* Are the signatures in the snippet okay, and does int contain the authenticated node? */

         if (!peerreview.getEvidenceTool().checkSnippetSignatures(
             pn.getSnippet(),pn.getMyHandle(),null, FLAG_INCLUDE_CHECKPOINT,null,pn.getTo().getHash(),pn.getTo().getSeq())) {
           logger.warn("PROOF NONCONFORMANT cannot be validated (signatures or authenticator)");
           return;
         }

         /* Now we are convinced that
              - the authenticator is valid
              - the snippet is well-formed, and we have all the certificates
              - the snippet starts with a checkpoint and contains the authenticated node
            We must now replay the log; if the proof is valid, it won't check out. */
         try {
           SecureHistory subjectHistory = peerreview.getHistoryFactory().createTemp(pn.getSnippet().getFirstSeq()-1, pn.getSnippet().getBaseHash());
           subjectHistory.appendSnippetToHistory(pn.getSnippet());

           // TODO: pass the extInfo of the proof to the verifier (null for now; inherited from the C++ PeerReview port)
           Verifier<Handle, Identifier> verifier = peerreview.getVerifierFactory().getVerifier(subjectHistory, pn.getMyHandle(), 1, pn.getSnippet().getFirstSeq()/SEQS_PER_MILLI, null);
           PeerReviewCallback<Handle, Identifier> replayApp = peerreview.getApp().getReplayInstance(verifier);
           verifier.setApplication(replayApp);

           logger.info("REPLAY ============================================");
           logger.debug("Node being replayed: {}", pn.getMyHandle());
           logger.debug("Range in log       : {}-?", pn.getSnippet().getFirstSeq());

           while (verifier.makeProgress());
           boolean verifiedOK = verifier.isVerificationSuccessful();

           if (verifiedOK) {
             logger.warn("PROOF NONCONFORMANT contains a log snippet that actually is conformant; discarding");
             return;
           }
         } catch (IOException ioe) {
           logger.warn("Couldn't replay!!! {}", pn, ioe);
         }
         break;
       }
       default: {
         logger.warn("Unknown payload type #{} in accusation; discarding", payload.getEvidenceType());
         idx.finish();
         return;
       }
     }
   } else {

     /* === CHECK RESPONSES === */

     switch (payload.getEvidenceType()) {

       /* To check an AUDIT RESPONSE, we need to verify that:
             - it is well-formed
             - we have certificates for all senders occurring in RECV entries
          We do NOT check signatures, sequence numbers, or whether the content makes any
          sense at all. We also do NOT check whether this is a valid response to some
          specific challenge.
       */

       case CHAL_AUDIT: {

         logger.debug("Checking AUDIT RESPONSE statement");

         AuditResponse<Handle> auditResponse = (AuditResponse<Handle>)payload;

         switch (checkSnippetAndRequestCertificates(auditResponse.logSnippet(), idx)) {
           case INVALID:
             logger.warn("AUDIT RESPONSE is not well-formed; discarding");
             idx.finish();
             return;
           case CERT_MISSING:
             return;
           default:
             break;
         }

         break;
       }

       case CHAL_SEND:
       {
         // the AckMessage payload will be handled in challengeProtocol.handleStatement()
         break;
       }

       default :
       {
         logger.warn("Unknown payload type #{} in response; discarding", payload.getEvidenceType());
         idx.finish();
         return;
       }
     }
   }

   /* At this point, we are convinced that the statement is valid, and we have all the
      necessary supplemental material to be able to check it and any responses to it */

    try {
      challengeProtocol.handleStatement(idx.getSender(), statement, idx.getOptions());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    idx.finish();
  }

}
