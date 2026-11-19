package org.mpisws.p2p.transport.peerreview;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import org.mpisws.p2p.transport.peerreview.audit.LogSnippet;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.evidence.AuditResponse;
import org.mpisws.p2p.transport.peerreview.evidence.ChallengeAudit;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceSerializerImpl;
import org.mpisws.p2p.transport.peerreview.evidence.ProofInconsistent;
import org.mpisws.p2p.transport.peerreview.evidence.ProofNonconformant;
import org.mpisws.p2p.transport.peerreview.message.AccusationMessage;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.AuthPushMessage;
import org.mpisws.p2p.transport.peerreview.message.AuthRequest;
import org.mpisws.p2p.transport.peerreview.message.AuthResponse;
import org.mpisws.p2p.transport.peerreview.message.ChallengeMessage;
import org.mpisws.p2p.transport.peerreview.message.ResponseMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.util.Serializer;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.testing.fuzz.FuzzSupport;

/**
 * Fuzzes the PeerReview wire messages: authenticators, challenges, accusations, evidence, user data and acknowledgements,
 * and the log snippets an audit returns. The invariant: a value or an {@link IOException}, nothing else.
 */
class PeerReviewFuzzTest {
  private static final int HASH_SIZE = 20;
  private static final int SIGNATURE_SIZE = 64;

  /** A node identifier of 8 bytes, which is all the layers above need of it. */
  record TestId(long value) implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeLong(value);
    }
  }

  private static final Serializer<TestId> ID_SERIALIZER = new Serializer<>() {
    @Override
    public void serialize(TestId testId, OutputBuffer buf) throws IOException {
      testId.serialize(buf);
    }

    @Override
    public TestId deserialize(InputBuffer buf) throws IOException {
      return new TestId(buf.readLong());
    }
  };

  private static final AuthenticatorSerializer AUTHENTICATORS = new AuthenticatorSerializer(HASH_SIZE,
      SIGNATURE_SIZE);
  private static final EvidenceSerializerImpl<TestId, TestId> EVIDENCE = new EvidenceSerializerImpl<>(ID_SERIALIZER,
      ID_SERIALIZER, HASH_SIZE, SIGNATURE_SIZE);

  @FuzzTest(maxDuration = "30s")
  void authenticator(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new Authenticator(buf, HASH_SIZE, SIGNATURE_SIZE));
  }

  @FuzzTest(maxDuration = "30s")
  void authenticatorSerializer(FuzzedDataProvider data) {
    FuzzSupport.read(data, AUTHENTICATORS::deserialize);
  }

  @FuzzTest(maxDuration = "30s")
  void authPush(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> AuthPushMessage.build(buf, ID_SERIALIZER, AUTHENTICATORS));
  }

  @FuzzTest(maxDuration = "30s")
  void authRequest(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new AuthRequest<>(buf, ID_SERIALIZER));
  }

  @FuzzTest(maxDuration = "30s")
  void authResponse(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new AuthResponse<>(buf, ID_SERIALIZER, HASH_SIZE, SIGNATURE_SIZE));
  }

  @FuzzTest(maxDuration = "30s")
  void ack(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> AckMessage.build(buf, ID_SERIALIZER, HASH_SIZE, SIGNATURE_SIZE));
  }

  @FuzzTest(maxDuration = "30s")
  void userData(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> UserDataMessage.build(buf, ID_SERIALIZER, HASH_SIZE, SIGNATURE_SIZE));
  }

  @FuzzTest(maxDuration = "30s")
  void challenge(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new ChallengeMessage<>(buf, ID_SERIALIZER, EVIDENCE));
  }

  @FuzzTest(maxDuration = "30s")
  void accusation(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new AccusationMessage<>(buf, ID_SERIALIZER, EVIDENCE));
  }

  @FuzzTest(maxDuration = "30s")
  void response(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new ResponseMessage<>(buf, ID_SERIALIZER, EVIDENCE));
  }

  @FuzzTest(maxDuration = "30s")
  void evidence(FuzzedDataProvider data) {
    byte type = data.consumeBoolean() ? data.consumeByte((byte) 0, (byte) 6) : data.consumeByte();
    boolean response = data.consumeBoolean();
    FuzzSupport.read(data, buf -> EVIDENCE.deserialize(buf, type, response));
  }

  @FuzzTest(maxDuration = "30s")
  void challengeAudit(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new ChallengeAudit(buf, HASH_SIZE, SIGNATURE_SIZE));
  }

  @FuzzTest(maxDuration = "30s")
  void auditResponse(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new AuditResponse<>(buf, ID_SERIALIZER, HASH_SIZE));
  }

  @FuzzTest(maxDuration = "30s")
  void proofInconsistent(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new ProofInconsistent(buf, AUTHENTICATORS, HASH_SIZE));
  }

  @FuzzTest(maxDuration = "30s")
  void proofNonconformant(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new ProofNonconformant<>(buf, ID_SERIALIZER, HASH_SIZE, SIGNATURE_SIZE));
  }

  @FuzzTest(maxDuration = "30s")
  void logSnippet(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new LogSnippet(buf, HASH_SIZE));
  }
}
