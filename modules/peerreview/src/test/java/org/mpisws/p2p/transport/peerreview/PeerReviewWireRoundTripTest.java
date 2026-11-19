package org.mpisws.p2p.transport.peerreview;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mpisws.p2p.transport.peerreview.audit.LogSnippet;
import org.mpisws.p2p.transport.peerreview.audit.SnippetEntry;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.evidence.AuditResponse;
import org.mpisws.p2p.transport.peerreview.evidence.ChallengeAudit;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceSerializerImpl;
import org.mpisws.p2p.transport.peerreview.evidence.ProofInconsistent;
import org.mpisws.p2p.transport.peerreview.evidence.ProofNonconformant;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtAck;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtInit;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtRecv;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSend;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSendSign;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSign;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.message.AccusationMessage;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.AuthPushMessage;
import org.mpisws.p2p.transport.peerreview.message.AuthRequest;
import org.mpisws.p2p.transport.peerreview.message.AuthResponse;
import org.mpisws.p2p.transport.peerreview.message.ChallengeMessage;
import org.mpisws.p2p.transport.peerreview.message.ResponseMessage;
import org.mpisws.p2p.transport.peerreview.message.Statement;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.util.Serializer;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Writes every PeerReview wire type to bytes and reads it back the way a hop does: the reader must consume exactly the
 * bytes serialize() wrote, and the copy must serialize to the same bytes again (a message read at one node and sent
 * on). The PeerReview module does not depend on Pastry, so this carries its own small version of the Pastry test
 * module's WireRoundTrip.
 */
class PeerReviewWireRoundTripTest {
  private static final int HASH = 32;
  private static final int SIG = 64;

  record TestId(long value) implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeLong(value);
    }
  }

  record EmptyId() implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) {
    }
  }

  private static final Serializer<TestId> IDS = new Serializer<>() {
    @Override
    public void serialize(TestId testId, OutputBuffer buf) throws IOException {
      testId.serialize(buf);
    }

    @Override
    public TestId deserialize(InputBuffer buf) throws IOException {
      return new TestId(buf.readLong());
    }
  };

  private static final AuthenticatorSerializer AUTHENTICATORS = new AuthenticatorSerializer(HASH, SIG);
  private static final EvidenceSerializerImpl<TestId, TestId> EVIDENCE = new EvidenceSerializerImpl<>(IDS, IDS, HASH,
      SIG);
  private static final HashProvider HASHER = new SHA256HashProvider();

  @FunctionalInterface
  interface Reader<T> {
    T read(InputBuffer buf) throws IOException;
  }

  @FunctionalInterface
  interface Writer<T> {
    void write(T value, OutputBuffer buf) throws IOException;
  }

  private static <T> byte[] bytes(T value, Writer<? super T> writer) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    writer.write(value, out);
    return out.getBytes();
  }

  private static <T> T readAll(byte[] bytes, Reader<T> reader, Object value) throws IOException {
    SimpleInputBuffer input = new SimpleInputBuffer(bytes);
    T read = reader.read(input);
    assertNotNull(read, () -> "read " + value);
    assertEquals(0, input.bytesRemaining(), () -> "bytes left after reading " + value);
    return read;
  }

  /** Serialize, read, serialize the copy, assert the same bytes, read again; returns the second copy. */
  private static <T> T roundTrip(T value, Writer<? super T> writer, Reader<? extends T> reader) throws IOException {
    byte[] first = bytes(value, writer);
    T copy = readAll(first, reader::read, value);
    byte[] second = bytes(copy, writer);
    assertArrayEquals(first, second, () -> "the copy of " + value + " serializes to the same bytes");
    return readAll(second, reader::read, value);
  }

  private static <T extends RawSerializable> T roundTrip(T value, Reader<? extends T> reader) throws IOException {
    return roundTrip(value, RawSerializable::serialize, reader);
  }

  private static byte[] fill(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (seed + i * 7);
    }
    return bytes;
  }

  private static Authenticator auth(long seq) {
    return new Authenticator(seq, fill(HASH, (int) seq), fill(SIG, (int) seq + 1));
  }

  private static TestId id(long v) {
    return new TestId(v);
  }

  // ---- commitment

  @ParameterizedTest
  @ValueSource(longs = {0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE})
  @DisplayName("Authenticator round-trips at the extreme sequence numbers")
  void authenticator(long seq) throws IOException {
    Authenticator authenticator = auth(seq);

    assertEquals(authenticator, roundTrip(authenticator, Authenticator::serialize, buf -> new Authenticator(buf, HASH, SIG)));
    assertEquals(authenticator, roundTrip(authenticator, Authenticator::serialize, AUTHENTICATORS::deserialize));
  }

  // ---- message.*

  @Test
  void ack() throws IOException {
    AckMessage<TestId> ackMessage = new AckMessage<>(id(9), 5, Long.MAX_VALUE, fill(HASH, 1), fill(SIG, 2));

    AckMessage<TestId> read = roundTrip(ackMessage, buf -> AckMessage.build(buf, IDS, HASH, SIG));

    assertEquals(ackMessage.getNodeId(), read.getNodeId());
    assertEquals(5, read.getSendEntrySeq());
    assertEquals(Long.MAX_VALUE, read.getRecvEntrySeq());
    assertArrayEquals(ackMessage.getPreviousTopHash(), read.getPreviousTopHash());
    assertArrayEquals(ackMessage.getSignature(), read.getSignature());
  }

  @Test
  @DisplayName("AuthPush with zero ids")
  void authPushEmpty() throws IOException {
    AuthPushMessage<TestId> msg = new AuthPushMessage<>(new LinkedHashMap<>());

    assertEquals(msg, roundTrip(msg, buf -> AuthPushMessage.build(buf, IDS, AUTHENTICATORS)));
  }

  @Test
  @DisplayName("AuthPush with ids that have zero authenticators, one, and several")
  void authPush() throws IOException {
    Map<TestId, List<Authenticator>> map = new LinkedHashMap<>();
    map.put(id(1), List.of());
    map.put(id(2), List.of(auth(7)));
    map.put(id(3), List.of(auth(8), auth(9), auth(10)));
    map.put(id(4), List.of());
    AuthPushMessage<TestId> msg = new AuthPushMessage<>(map);

    assertEquals(msg, roundTrip(msg, buf -> AuthPushMessage.build(buf, IDS, AUTHENTICATORS)));
  }

  @Test
  @DisplayName("AuthPush with the most identifiers the reader accepts, none with authenticators")
  void authPushMaxIdentifiers() throws IOException {
    Map<TestId, List<Authenticator>> map = new LinkedHashMap<>();
    for (int i = 0; i < AuthPushMessage.MAX_IDENTIFIERS; i++) {
      map.put(id(i), List.of());
    }
    AuthPushMessage<TestId> msg = new AuthPushMessage<>(map);

    assertEquals(msg, roundTrip(msg, buf -> AuthPushMessage.build(buf, IDS, AUTHENTICATORS)));
  }

  @Test
  @DisplayName("AuthPush with the most authenticators the reader accepts for one id")
  void authPushMaxAuthenticators() throws IOException {
    List<Authenticator> list = new ArrayList<>();
    for (int i = 0; i < AuthPushMessage.MAX_AUTHENTICATORS_PER_ID; i++) {
      list.add(auth(i));
    }
    AuthPushMessage<TestId> msg = new AuthPushMessage<>(Map.of(id(1), list));

    assertEquals(msg, roundTrip(msg, buf -> AuthPushMessage.build(buf, IDS, AUTHENTICATORS)));
  }

  @Test
  @DisplayName("AuthPush whose identifiers take no bytes on the wire")
  void authPushZeroByteIdentifiers() throws IOException {
    Serializer<EmptyId> empty = new Serializer<>() {
      @Override
      public void serialize(EmptyId emptyId, OutputBuffer buf) {
      }

      @Override
      public EmptyId deserialize(InputBuffer buf) {
        return new EmptyId();
      }
    };
    AuthPushMessage<EmptyId> msg = new AuthPushMessage<>(Map.of(new EmptyId(), List.of()));

    assertEquals(msg, roundTrip(msg, buf -> AuthPushMessage.build(buf, empty, AUTHENTICATORS)));
  }

  @Test
  void authRequest() throws IOException {
    AuthRequest<TestId> authRequest = new AuthRequest<>(Long.MIN_VALUE, id(3));

    assertEquals(authRequest, roundTrip(authRequest, buf -> new AuthRequest<>(buf, IDS)));
  }

  @Test
  void authResponse() throws IOException {
    AuthResponse<TestId> authResponse = new AuthResponse<>(id(3), auth(1), auth(2));

    assertEquals(authResponse, roundTrip(authResponse, buf -> new AuthResponse<>(buf, IDS, HASH, SIG)));
  }

  @Test
  void userDataFullyRelevant() throws IOException {
    UserDataMessage<TestId> msg = userData(fill(40, 3), 40);

    assertSameUserData(msg, roundTrip(msg, buf -> UserDataMessage.build(buf, IDS, HASH, SIG)));
  }

  @Test
  void userDataEmptyPayload() throws IOException {
    UserDataMessage<TestId> msg = userData(new byte[0], 0);

    assertSameUserData(msg, roundTrip(msg, buf -> UserDataMessage.build(buf, IDS, HASH, SIG)));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 39, 100, 254})
  @DisplayName("UserData with only the first bytes relevant, up to the largest length the one-byte code can carry")
  void userDataPartlyRelevant(int relevant) throws IOException {
    UserDataMessage<TestId> msg = userData(fill(300, 3), relevant);

    UserDataMessage<TestId> read = roundTrip(msg, buf -> UserDataMessage.build(buf, IDS, HASH, SIG));

    assertSameUserData(msg, read);
    assertEquals(relevant, read.getRelevantLength());
  }

  @Test
  @DisplayName("UserData whose payload is exactly as long as the full-relevance code")
  void userDataPayload255() throws IOException {
    UserDataMessage<TestId> msg = userData(fill(255, 3), 255);

    assertSameUserData(msg, roundTrip(msg, buf -> UserDataMessage.build(buf, IDS, HASH, SIG)));
  }

  private static UserDataMessage<TestId> userData(byte[] payload, int relevant) {
    return new UserDataMessage<>(77, id(5), fill(HASH, 1), fill(SIG, 2), ByteBuffer.wrap(payload), relevant);
  }

  private static void assertSameUserData(UserDataMessage<TestId> expected, UserDataMessage<TestId> actual) {
    assertEquals(expected.getTopSeq(), actual.getTopSeq());
    assertEquals(expected.getSenderHandle(), actual.getSenderHandle());
    assertArrayEquals(expected.getPreviousTopHash(), actual.getPreviousTopHash());
    assertArrayEquals(expected.getSignature(), actual.getSignature());
    assertEquals(expected.getRelevantLength(), actual.getRelevantLength());
    assertEquals(expected.getPayload(), actual.getPayload());
  }

  @Test
  void challengeWithAuditChallenge() throws IOException {
    ChallengeMessage<TestId> msg = new ChallengeMessage<>(id(1), 12, new ChallengeAudit((byte) 5, auth(1), auth(2)));

    ChallengeMessage<TestId> read = roundTrip(msg, buf -> new ChallengeMessage<>(buf, IDS, EVIDENCE));

    assertEquals(msg.originator(), read.originator());
    assertEquals(12, read.evidenceSeq());
    assertEquals(msg.challenge(), read.challenge());
  }

  @Test
  void challengeWithSendChallenge() throws IOException {
    UserDataMessage<TestId> udm = userData(fill(10, 1), 4);
    ChallengeMessage<TestId> msg = new ChallengeMessage<>(id(1), 12, udm);

    ChallengeMessage<TestId> read = roundTrip(msg, buf -> new ChallengeMessage<>(buf, IDS, EVIDENCE));

    assertEquals(PeerReviewConstants.CHAL_SEND, read.getChallengeType());
    @SuppressWarnings("unchecked")
    UserDataMessage<TestId> readUdm = assertInstanceOf(UserDataMessage.class, read.challenge());
    assertSameUserData(udm, readUdm);
  }

  private static ProofInconsistent proofInconsistent(boolean withSnippet) {
    return withSnippet ? new ProofInconsistent(auth(1), auth(2), snippet(3, false))
        : new ProofInconsistent(auth(1), auth(2));
  }

  private static LogSnippet snippet(int entries, boolean hashed) {
    List<SnippetEntry> list = new ArrayList<>();
    for (int i = 0; i < entries; i++) {
      list.add(hashed && i % 2 == 1 ? new SnippetEntry((byte) 3, 100 + i, true, fill(HASH, i))
          : new SnippetEntry((byte) 3, 100 + i, false, fill(5 + i, i)));
    }
    return new LogSnippet(fill(HASH, 99), list);
  }

  private static ProofNonconformant<TestId> nonconformantProof() {
    return new ProofNonconformant<>(auth(4), id(8), snippet(2, true));
  }

  @Test
  void accusationCarriesEveryAccusationEvidence() throws IOException {
    List<Evidence> evidence = List.of(new ChallengeAudit((byte) 0, auth(1), auth(2)), userData(fill(9, 1), 9),
        proofInconsistent(false), proofInconsistent(true), nonconformantProof());

    for (Evidence evidenceItem : evidence) {
      AccusationMessage<TestId> msg = new AccusationMessage<>(id(1), id(2), 55, evidenceItem);

      AccusationMessage<TestId> read = roundTrip(msg, buf -> new AccusationMessage<>(buf, IDS, EVIDENCE));

      assertStatement(msg, read);
      assertEquals(evidenceItem.getEvidenceType(), read.getEvidence().getEvidenceType());
      assertEquals(evidenceItem.getClass(), read.getEvidence().getClass());
    }
  }

  @Test
  void responseCarriesEveryResponseEvidence() throws IOException {
    List<Evidence> evidence = List.of(new AuditResponse<>(id(6), snippet(1, false)),
        new AuditResponse<>(id(6), snippet(4, true)), new AckMessage<>(id(9), 5, 6, fill(HASH, 1), fill(SIG, 2)));

    for (Evidence evidenceItem : evidence) {
      ResponseMessage<TestId> msg = new ResponseMessage<>(id(1), id(2), 55, evidenceItem);

      ResponseMessage<TestId> read = roundTrip(msg, buf -> new ResponseMessage<>(buf, IDS, EVIDENCE));

      assertStatement(msg, read);
      assertEquals(evidenceItem.getEvidenceType(), read.getEvidence().getEvidenceType());
      assertEquals(evidenceItem.getClass(), read.getEvidence().getClass());
    }
  }

  private static void assertStatement(Statement<TestId> expected, Statement<TestId> actual) {
    assertEquals(expected.getOriginator(), actual.getOriginator());
    assertEquals(expected.getSubject(), actual.getSubject());
    assertEquals(expected.getEvidenceSeq(), actual.getEvidenceSeq());
  }

  // ---- evidence.*

  @Test
  void challengeAudit() throws IOException {
    ChallengeAudit challengeAudit = new ChallengeAudit(PeerReviewConstants.FLAG_INCLUDE_CHECKPOINT, auth(1), auth(2));

    ChallengeAudit read = roundTrip(challengeAudit, Evidence::serialize, buf -> new ChallengeAudit(buf, HASH, SIG));

    assertEquals(challengeAudit, read);
    assertTrue(read.isIncludePrevCheckpoint());
  }

  @Test
  void auditResponse() throws IOException {
    AuditResponse<TestId> auditResponse = new AuditResponse<>(id(4), snippet(3, true));

    AuditResponse<TestId> read = roundTrip(auditResponse, Evidence::serialize, buf -> new AuditResponse<>(buf, IDS, HASH));

    assertEquals(auditResponse, read);
    assertEquals(100, read.getFirstSeq());
  }

  @Test
  void proofInconsistentWithAndWithoutSnippet() throws IOException {
    for (boolean withSnippet : new boolean[] {false, true}) {
      ProofInconsistent proof = proofInconsistent(withSnippet);

      ProofInconsistent read = roundTrip(proof, Evidence::serialize, buf -> new ProofInconsistent(buf, AUTHENTICATORS, HASH));

      assertEquals(proof.getFirstAuthenticator(), read.getFirstAuthenticator());
      assertEquals(proof.getSecondAuthenticator(), read.getSecondAuthenticator());
      if (withSnippet) {
        assertEquals(proof.getSnippet(), read.getSnippet());
      } else {
        assertNull(read.getSnippet());
      }
    }
  }

  @Test
  void proofNonconformant() throws IOException {
    ProofNonconformant<TestId> proofNonconformant = nonconformantProof();

    ProofNonconformant<TestId> read = roundTrip(proofNonconformant, Evidence::serialize, buf -> new ProofNonconformant<>(buf, IDS, HASH, SIG));

    assertEquals(proofNonconformant.getTo(), read.getTo());
    assertEquals(proofNonconformant.getMyHandle(), read.getMyHandle());
    assertEquals(proofNonconformant.getSnippet(), read.getSnippet());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @DisplayName("Every evidence type reads through the EvidenceSerializer by its type byte")
  void evidenceSerializer(boolean response) throws IOException {
    List<Evidence> evidence = response
        ? List.of(new AuditResponse<>(id(6), snippet(2, false)),
            new AckMessage<>(id(9), 5, 6, fill(HASH, 1), fill(SIG, 2)), proofInconsistent(true), nonconformantProof())
        : List.of(new ChallengeAudit((byte) 0, auth(1), auth(2)), userData(fill(9, 1), 9), proofInconsistent(false),
            nonconformantProof());
    for (Evidence evidenceItem : evidence) {
      Evidence read = roundTrip(evidenceItem, Evidence::serialize,
          buf -> EVIDENCE.deserialize(buf, (byte) evidenceItem.getEvidenceType(), response));

      assertEquals(evidenceItem.getClass(), read.getClass());
    }
  }

  // ---- audit.*

  @Test
  void logSnippetSingleEntry() throws IOException {
    LogSnippet logSnippet = snippet(1, false);

    assertEquals(logSnippet, roundTrip(logSnippet, LogSnippet::serialize, buf -> new LogSnippet(buf, HASH)));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 252, 253, 254, 255, 256, 1000, 65534, 65535, 65536, 70000})
  @DisplayName("SnippetEntry content lengths around the one-byte, two-byte and four-byte size codes")
  void snippetContentLengths(int length) throws IOException {
    LogSnippet logSnippet = new LogSnippet(fill(HASH, 1), List.of(new SnippetEntry((byte) 7, 10, false, fill(length, 3)),
        new SnippetEntry((byte) 8, 11, false, fill(length, 4))));

    assertEquals(logSnippet, roundTrip(logSnippet, LogSnippet::serialize, buf -> new LogSnippet(buf, HASH)));
  }

  @Test
  @DisplayName("LogSnippet sequence numbers: consecutive, millisecond multiples, absolute jumps and the extremes")
  void logSnippetSequenceCodes() throws IOException {
    long millis = PeerReviewConstants.SEQS_PER_MILLI;
    long[] seqs = {0, 1, 2, 5 * millis, 5 * millis + 1, 6 * millis, 6 * millis + 7, 7 * millis, 7 * millis + millis / 2, 8 * millis, 8 * millis + 254 * millis,
        1_000_000_000_000_000L, Long.MAX_VALUE};
    List<SnippetEntry> entries = new ArrayList<>();
    for (int i = 0; i < seqs.length; i++) {
      entries.add(new SnippetEntry((byte) 1, seqs[i], i % 3 == 0, i % 3 == 0 ? fill(HASH, i) : fill(3, i)));
    }
    LogSnippet logSnippet = new LogSnippet(fill(HASH, 1), entries);

    LogSnippet read = roundTrip(logSnippet, LogSnippet::serialize, buf -> new LogSnippet(buf, HASH));

    assertEquals(logSnippet, read);
    for (int i = 0; i < seqs.length; i++) {
      assertEquals(seqs[i], read.getEntries().get(i).getSeq(), "entry " + i);
    }
  }

  @Test
  void snippetEntryHashedAndPlain() throws IOException {
    SnippetEntry hashed = new SnippetEntry((byte) 2, 5, true, fill(HASH, 1));
    SnippetEntry plain = new SnippetEntry((byte) 3, 6, false, fill(4, 1));

    SnippetEntry readPlain = roundTrip(plain, (e, buf) -> e.serialize(buf, hashed),
        buf -> new SnippetEntry(buf, HASH, hashed));
    SnippetEntry readHashed = roundTrip(hashed, (e, buf) -> e.serialize(buf, plain),
        buf -> new SnippetEntry(buf, HASH, plain));

    assertEquals(plain, readPlain);
    assertEquals(hashed, readHashed);
    assertTrue(readHashed.isHash());
  }

  // ---- history.*

  @Test
  void indexEntry() throws IOException {
    SecureHistoryFactory factory = new SecureHistoryFactory(HASHER, null);
    for (int size : new int[] {-1, 0, 1, Integer.MAX_VALUE}) {
      IndexEntry indexEntry = new IndexEntry(Long.MAX_VALUE, 1234567890123L, (short) -2, size, fill(HASH, 1), fill(HASH, 2));

      IndexEntry read = roundTrip(indexEntry, IndexEntry::serialize, factory::build);

      assertEquals(indexEntry.getSeq(), read.getSeq());
      assertEquals(indexEntry.getFileIndex(), read.getFileIndex());
      assertEquals(size, read.getSizeInFile());
      assertEquals(indexEntry.getType(), read.getType());
      assertArrayEquals(indexEntry.getContentHash(), read.getContentHash());
      assertArrayEquals(indexEntry.getNodeHash(), read.getNodeHash());
      assertEquals(factory.getSerializedSize(), bytes(indexEntry, IndexEntry::serialize).length);
    }
  }

  @Test
  void evtAck() throws IOException {
    EvtAck<TestId> evtAck = new EvtAck<>(id(3), 4, Long.MAX_VALUE, fill(HASH, 1), fill(SIG, 2));

    EvtAck<TestId> read = roundTrip(evtAck, EvtAck::serialize, buf -> new EvtAck<>(buf, IDS, HASH, SIG));

    assertEquals(evtAck.getRemoteId(), read.getRemoteId());
    assertEquals(4, read.getAckedSeq());
    assertEquals(Long.MAX_VALUE, read.getPeerSeq());
    assertArrayEquals(evtAck.getHTopMinusOne(), read.getHTopMinusOne());
    assertArrayEquals(evtAck.getSignature(), read.getSignature());
  }

  @Test
  @DisplayName("EvtInit is its handle's bytes and nothing else")
  void evtInit() throws IOException {
    byte[] handleBytes = bytes(id(42), TestId::serialize);
    EvtInit<TestId> first = new EvtInit<>(new SimpleInputBuffer(handleBytes), IDS);

    EvtInit<TestId> read = roundTrip(first, EvtInit::serialize, buf -> new EvtInit<>(buf, IDS));

    assertArrayEquals(handleBytes, bytes(read, EvtInit::serialize));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 8, 300})
  @DisplayName("EvtRecv with the whole payload, with an empty payload, and with only a relevant prefix plus hash")
  void evtRecv(int length) throws IOException {
    byte[] payload = fill(length, 5);
    List<EvtRecv<TestId>> events = new ArrayList<>();
    events.add(new EvtRecv<>(id(1), 8, ByteBuffer.wrap(payload)));
    events.add(new EvtRecv<>(id(1), 8, ByteBuffer.wrap(payload), length / 2, HASHER));
    events.add(new EvtRecv<>(id(1), 8, ByteBuffer.wrap(payload), 0, HASHER));
    events.add(new EvtRecv<>(id(1), 8, ByteBuffer.wrap(payload), length, HASHER));
    for (EvtRecv<TestId> evtRecv : events) {
      EvtRecv<TestId> read = roundTrip(evtRecv, EvtRecv::serialize, buf -> new EvtRecv<>(buf, IDS, HASH));

      assertEquals(evtRecv.getSenderHandle(), read.getSenderHandle());
      assertEquals(8, read.getSenderSeq());
      assertArrayEquals(evtRecv.getPayload(), read.getPayload());
      assertArrayEquals(evtRecv.getHash(), read.getHash());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 8, 300})
  void evtSend(int length) throws IOException {
    byte[] payload = fill(length, 5);
    List<EvtSend<TestId>> events = new ArrayList<>();
    events.add(new EvtSend<>(id(1), ByteBuffer.wrap(payload)));
    events.add(new EvtSend<>(id(1), ByteBuffer.wrap(payload), length / 2, HASHER));
    events.add(new EvtSend<>(id(1), ByteBuffer.wrap(payload), 0, HASHER));
    events.add(new EvtSend<>(id(1), ByteBuffer.wrap(payload), length, HASHER));
    for (EvtSend<TestId> evtSend : events) {
      EvtSend<TestId> read = roundTrip(evtSend, EvtSend::serialize, buf -> new EvtSend<>(buf, IDS, HASH));

      assertEquals(evtSend.getReceiverId(), read.getReceiverId());
      assertEquals(evtSend.getPayload(), read.getPayload());
    }
  }

  @Test
  void evtSign() throws IOException {
    EvtSign evtSign = new EvtSign(fill(HASH, 1), fill(SIG, 2));

    EvtSign read = roundTrip(evtSign, EvtSign::serialize, buf -> new EvtSign(buf, HASH, SIG));

    assertArrayEquals(evtSign.getHTopMinusOne(), read.getHTopMinusOne());
    assertArrayEquals(evtSign.getSignature(), read.getSignature());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 100})
  @DisplayName("EvtSendSign has a reader but no writer: the signature is followed by the rest of the message")
  void evtSendSign(int rest) throws IOException {
    byte[] signature = fill(SIG, 2);
    byte[] restOfMessage = fill(rest, 9);
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.write(signature, 0, signature.length);
    out.write(restOfMessage, 0, restOfMessage.length);

    EvtSendSign read = readAll(out.getBytes(), buf -> new EvtSendSign(buf, SIG), "EvtSendSign");

    assertArrayEquals(signature, read.getSignature());
    assertArrayEquals(restOfMessage, read.getRestOfMessage());
  }
}
