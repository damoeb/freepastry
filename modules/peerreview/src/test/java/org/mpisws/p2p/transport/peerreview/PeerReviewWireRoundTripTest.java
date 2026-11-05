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
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializerImpl;
import org.mpisws.p2p.transport.peerreview.evidence.AuditResponse;
import org.mpisws.p2p.transport.peerreview.evidence.ChallengeAudit;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceSerializerImpl;
import org.mpisws.p2p.transport.peerreview.evidence.ProofInconsistent;
import org.mpisws.p2p.transport.peerreview.evidence.ProofNonconformant;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
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
    public void serialize(TestId i, OutputBuffer buf) throws IOException {
      i.serialize(buf);
    }

    @Override
    public TestId deserialize(InputBuffer buf) throws IOException {
      return new TestId(buf.readLong());
    }
  };

  private static final AuthenticatorSerializerImpl AUTHENTICATORS = new AuthenticatorSerializerImpl(HASH, SIG);
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
    SimpleInputBuffer in = new SimpleInputBuffer(bytes);
    T read = reader.read(in);
    assertNotNull(read, () -> "read " + value);
    assertEquals(0, in.bytesRemaining(), () -> "bytes left after reading " + value);
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
    byte[] b = new byte[length];
    for (int i = 0; i < length; i++) {
      b[i] = (byte) (seed + i * 7);
    }
    return b;
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
    Authenticator a = auth(seq);

    assertEquals(a, roundTrip(a, Authenticator::serialize, buf -> new Authenticator(buf, HASH, SIG)));
    assertEquals(a, roundTrip(a, Authenticator::serialize, AUTHENTICATORS::deserialize));
  }

  // ---- message.*

  @Test
  void ack() throws IOException {
    AckMessage<TestId> a = new AckMessage<>(id(9), 5, Long.MAX_VALUE, fill(HASH, 1), fill(SIG, 2));

    AckMessage<TestId> read = roundTrip(a, buf -> AckMessage.build(buf, IDS, HASH, SIG));

    assertEquals(a.getNodeId(), read.getNodeId());
    assertEquals(5, read.getSendEntrySeq());
    assertEquals(Long.MAX_VALUE, read.getRecvEntrySeq());
    assertArrayEquals(a.getHashTopMinusOne(), read.getHashTopMinusOne());
    assertArrayEquals(a.getSignature(), read.getSignature());
  }

  @Test
  @DisplayName("AuthPush with zero ids")
  void authPushEmpty() throws IOException {
    AuthPushMessage<TestId> m = new AuthPushMessage<>(new LinkedHashMap<>());

    assertEquals(m, roundTrip(m, buf -> AuthPushMessage.build(buf, IDS, AUTHENTICATORS)));
  }

  @Test
  @DisplayName("AuthPush with ids that have zero authenticators, one, and several")
  void authPush() throws IOException {
    Map<TestId, List<Authenticator>> map = new LinkedHashMap<>();
    map.put(id(1), List.of());
    map.put(id(2), List.of(auth(7)));
    map.put(id(3), List.of(auth(8), auth(9), auth(10)));
    map.put(id(4), List.of());
    AuthPushMessage<TestId> m = new AuthPushMessage<>(map);

    assertEquals(m, roundTrip(m, buf -> AuthPushMessage.build(buf, IDS, AUTHENTICATORS)));
  }

  @Test
  @DisplayName("AuthPush with the most identifiers the reader accepts, none with authenticators")
  void authPushMaxIdentifiers() throws IOException {
    Map<TestId, List<Authenticator>> map = new LinkedHashMap<>();
    for (int i = 0; i < AuthPushMessage.MAX_IDENTIFIERS; i++) {
      map.put(id(i), List.of());
    }
    AuthPushMessage<TestId> m = new AuthPushMessage<>(map);

    assertEquals(m, roundTrip(m, buf -> AuthPushMessage.build(buf, IDS, AUTHENTICATORS)));
  }

  @Test
  @DisplayName("AuthPush with the most authenticators the reader accepts for one id")
  void authPushMaxAuthenticators() throws IOException {
    List<Authenticator> list = new ArrayList<>();
    for (int i = 0; i < AuthPushMessage.MAX_AUTHENTICATORS_PER_ID; i++) {
      list.add(auth(i));
    }
    AuthPushMessage<TestId> m = new AuthPushMessage<>(Map.of(id(1), list));

    assertEquals(m, roundTrip(m, buf -> AuthPushMessage.build(buf, IDS, AUTHENTICATORS)));
  }

  @Test
  @DisplayName("AuthPush whose identifiers take no bytes on the wire")
  void authPushZeroByteIdentifiers() throws IOException {
    Serializer<EmptyId> empty = new Serializer<>() {
      @Override
      public void serialize(EmptyId i, OutputBuffer buf) {
      }

      @Override
      public EmptyId deserialize(InputBuffer buf) {
        return new EmptyId();
      }
    };
    AuthPushMessage<EmptyId> m = new AuthPushMessage<>(Map.of(new EmptyId(), List.of()));

    assertEquals(m, roundTrip(m, buf -> AuthPushMessage.build(buf, empty, AUTHENTICATORS)));
  }

  @Test
  void authRequest() throws IOException {
    AuthRequest<TestId> m = new AuthRequest<>(Long.MIN_VALUE, id(3));

    assertEquals(m, roundTrip(m, buf -> new AuthRequest<>(buf, IDS)));
  }

  @Test
  void authResponse() throws IOException {
    AuthResponse<TestId> m = new AuthResponse<>(id(3), auth(1), auth(2));

    assertEquals(m, roundTrip(m, buf -> new AuthResponse<>(buf, IDS, HASH, SIG)));
  }

  @Test
  void userDataFullyRelevant() throws IOException {
    UserDataMessage<TestId> m = userData(fill(40, 3), 40);

    assertSameUserData(m, roundTrip(m, buf -> UserDataMessage.build(buf, IDS, HASH, SIG)));
  }

  @Test
  void userDataEmptyPayload() throws IOException {
    UserDataMessage<TestId> m = userData(new byte[0], 0);

    assertSameUserData(m, roundTrip(m, buf -> UserDataMessage.build(buf, IDS, HASH, SIG)));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 39, 100, 254})
  @DisplayName("UserData with only the first bytes relevant, up to the largest length the one-byte code can carry")
  void userDataPartlyRelevant(int relevant) throws IOException {
    UserDataMessage<TestId> m = userData(fill(300, 3), relevant);

    UserDataMessage<TestId> read = roundTrip(m, buf -> UserDataMessage.build(buf, IDS, HASH, SIG));

    assertSameUserData(m, read);
    assertEquals(relevant, read.getRelevantLen());
  }

  @Test
  @DisplayName("UserData whose payload is exactly as long as the full-relevance code")
  void userDataPayload255() throws IOException {
    UserDataMessage<TestId> m = userData(fill(255, 3), 255);

    assertSameUserData(m, roundTrip(m, buf -> UserDataMessage.build(buf, IDS, HASH, SIG)));
  }

  private static UserDataMessage<TestId> userData(byte[] payload, int relevant) {
    return new UserDataMessage<>(77, id(5), fill(HASH, 1), fill(SIG, 2), ByteBuffer.wrap(payload), relevant);
  }

  private static void assertSameUserData(UserDataMessage<TestId> expected, UserDataMessage<TestId> actual) {
    assertEquals(expected.getTopSeq(), actual.getTopSeq());
    assertEquals(expected.getSenderHandle(), actual.getSenderHandle());
    assertArrayEquals(expected.getHTopMinusOne(), actual.getHTopMinusOne());
    assertArrayEquals(expected.getSignature(), actual.getSignature());
    assertEquals(expected.getRelevantLen(), actual.getRelevantLen());
    assertEquals(expected.getPayload(), actual.getPayload());
  }

  @Test
  void challengeWithAuditChallenge() throws IOException {
    ChallengeMessage<TestId> m = new ChallengeMessage<>(id(1), 12, new ChallengeAudit((byte) 5, auth(1), auth(2)));

    ChallengeMessage<TestId> read = roundTrip(m, buf -> new ChallengeMessage<>(buf, IDS, EVIDENCE));

    assertEquals(m.originator(), read.originator());
    assertEquals(12, read.evidenceSeq());
    assertEquals(m.challenge(), read.challenge());
  }

  @Test
  void challengeWithSendChallenge() throws IOException {
    UserDataMessage<TestId> udm = userData(fill(10, 1), 4);
    ChallengeMessage<TestId> m = new ChallengeMessage<>(id(1), 12, udm);

    ChallengeMessage<TestId> read = roundTrip(m, buf -> new ChallengeMessage<>(buf, IDS, EVIDENCE));

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

    for (Evidence e : evidence) {
      AccusationMessage<TestId> m = new AccusationMessage<>(id(1), id(2), 55, e);

      AccusationMessage<TestId> read = roundTrip(m, buf -> new AccusationMessage<>(buf, IDS, EVIDENCE));

      assertStatement(m, read);
      assertEquals(e.getEvidenceType(), read.getEvidence().getEvidenceType());
      assertEquals(e.getClass(), read.getEvidence().getClass());
    }
  }

  @Test
  void responseCarriesEveryResponseEvidence() throws IOException {
    List<Evidence> evidence = List.of(new AuditResponse<>(id(6), snippet(1, false)),
        new AuditResponse<>(id(6), snippet(4, true)), new AckMessage<>(id(9), 5, 6, fill(HASH, 1), fill(SIG, 2)));

    for (Evidence e : evidence) {
      ResponseMessage<TestId> m = new ResponseMessage<>(id(1), id(2), 55, e);

      ResponseMessage<TestId> read = roundTrip(m, buf -> new ResponseMessage<>(buf, IDS, EVIDENCE));

      assertStatement(m, read);
      assertEquals(e.getEvidenceType(), read.getEvidence().getEvidenceType());
      assertEquals(e.getClass(), read.getEvidence().getClass());
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
    ChallengeAudit c = new ChallengeAudit(PeerReviewConstants.FLAG_INCLUDE_CHECKPOINT, auth(1), auth(2));

    ChallengeAudit read = roundTrip(c, Evidence::serialize, buf -> new ChallengeAudit(buf, HASH, SIG));

    assertEquals(c, read);
    assertTrue(read.isIncludePrevCheckpoint());
  }

  @Test
  void auditResponse() throws IOException {
    AuditResponse<TestId> r = new AuditResponse<>(id(4), snippet(3, true));

    AuditResponse<TestId> read = roundTrip(r, Evidence::serialize, buf -> new AuditResponse<>(buf, IDS, HASH));

    assertEquals(r, read);
    assertEquals(100, read.getFirstSeq());
  }

  @Test
  void proofInconsistentWithAndWithoutSnippet() throws IOException {
    for (boolean withSnippet : new boolean[] {false, true}) {
      ProofInconsistent p = proofInconsistent(withSnippet);

      ProofInconsistent read = roundTrip(p, Evidence::serialize, buf -> new ProofInconsistent(buf, AUTHENTICATORS, HASH));

      assertEquals(p.getAuth1(), read.getAuth1());
      assertEquals(p.getAuth2(), read.getAuth2());
      if (withSnippet) {
        assertEquals(p.getSnippet(), read.getSnippet());
      } else {
        assertNull(read.getSnippet());
      }
    }
  }

  @Test
  void proofNonconformant() throws IOException {
    ProofNonconformant<TestId> p = nonconformantProof();

    ProofNonconformant<TestId> read = roundTrip(p, Evidence::serialize, buf -> new ProofNonconformant<>(buf, IDS, HASH, SIG));

    assertEquals(p.getTo(), read.getTo());
    assertEquals(p.getMyHandle(), read.getMyHandle());
    assertEquals(p.getSnippet(), read.getSnippet());
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
    for (Evidence e : evidence) {
      Evidence read = roundTrip(e, Evidence::serialize,
          buf -> EVIDENCE.deserialize(buf, (byte) e.getEvidenceType(), response));

      assertEquals(e.getClass(), read.getClass());
    }
  }

  // ---- audit.*

  @Test
  void logSnippetSingleEntry() throws IOException {
    LogSnippet s = snippet(1, false);

    assertEquals(s, roundTrip(s, LogSnippet::serialize, buf -> new LogSnippet(buf, HASH)));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 252, 253, 254, 255, 256, 1000, 65534, 65535, 65536, 70000})
  @DisplayName("SnippetEntry content lengths around the one-byte, two-byte and four-byte size codes")
  void snippetContentLengths(int length) throws IOException {
    LogSnippet s = new LogSnippet(fill(HASH, 1), List.of(new SnippetEntry((byte) 7, 10, false, fill(length, 3)),
        new SnippetEntry((byte) 8, 11, false, fill(length, 4))));

    assertEquals(s, roundTrip(s, LogSnippet::serialize, buf -> new LogSnippet(buf, HASH)));
  }

  @Test
  @DisplayName("LogSnippet sequence numbers: consecutive, millisecond multiples, absolute jumps and the extremes")
  void logSnippetSequenceCodes() throws IOException {
    long ms = PeerReviewConstants.SEQS_PER_MILLI;
    long[] seqs = {0, 1, 2, 5 * ms, 5 * ms + 1, 6 * ms, 6 * ms + 7, 7 * ms, 7 * ms + ms / 2, 8 * ms, 8 * ms + 254 * ms,
        1_000_000_000_000_000L, Long.MAX_VALUE};
    List<SnippetEntry> entries = new ArrayList<>();
    for (int i = 0; i < seqs.length; i++) {
      entries.add(new SnippetEntry((byte) 1, seqs[i], i % 3 == 0, i % 3 == 0 ? fill(HASH, i) : fill(3, i)));
    }
    LogSnippet s = new LogSnippet(fill(HASH, 1), entries);

    LogSnippet read = roundTrip(s, LogSnippet::serialize, buf -> new LogSnippet(buf, HASH));

    assertEquals(s, read);
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
    SecureHistoryFactoryImpl factory = new SecureHistoryFactoryImpl(HASHER, null);
    for (int size : new int[] {-1, 0, 1, Integer.MAX_VALUE}) {
      IndexEntry e = new IndexEntry(Long.MAX_VALUE, 1234567890123L, (short) -2, size, fill(HASH, 1), fill(HASH, 2));

      IndexEntry read = roundTrip(e, IndexEntry::serialize, factory::build);

      assertEquals(e.getSeq(), read.getSeq());
      assertEquals(e.getFileIndex(), read.getFileIndex());
      assertEquals(size, read.getSizeInFile());
      assertEquals(e.getType(), read.getType());
      assertArrayEquals(e.getContentHash(), read.getContentHash());
      assertArrayEquals(e.getNodeHash(), read.getNodeHash());
      assertEquals(factory.getSerializedSize(), bytes(e, IndexEntry::serialize).length);
    }
  }

  @Test
  void evtAck() throws IOException {
    EvtAck<TestId> e = new EvtAck<>(id(3), 4, Long.MAX_VALUE, fill(HASH, 1), fill(SIG, 2));

    EvtAck<TestId> read = roundTrip(e, EvtAck::serialize, buf -> new EvtAck<>(buf, IDS, HASH, SIG));

    assertEquals(e.getRemoteId(), read.getRemoteId());
    assertEquals(4, read.getAckedSeq());
    assertEquals(Long.MAX_VALUE, read.getHisSeq());
    assertArrayEquals(e.getHTopMinusOne(), read.getHTopMinusOne());
    assertArrayEquals(e.getSignature(), read.getSignature());
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
    for (EvtRecv<TestId> e : events) {
      EvtRecv<TestId> read = roundTrip(e, EvtRecv::serialize, buf -> new EvtRecv<>(buf, IDS, HASH));

      assertEquals(e.getSenderHandle(), read.getSenderHandle());
      assertEquals(8, read.getSenderSeq());
      assertArrayEquals(e.getPayload(), read.getPayload());
      assertArrayEquals(e.getHash(), read.getHash());
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
    for (EvtSend<TestId> e : events) {
      EvtSend<TestId> read = roundTrip(e, EvtSend::serialize, buf -> new EvtSend<>(buf, IDS, HASH));

      assertEquals(e.getReceiverId(), read.getReceiverId());
      assertEquals(e.getPayload(), read.getPayload());
    }
  }

  @Test
  void evtSign() throws IOException {
    EvtSign e = new EvtSign(fill(HASH, 1), fill(SIG, 2));

    EvtSign read = roundTrip(e, EvtSign::serialize, buf -> new EvtSign(buf, HASH, SIG));

    assertArrayEquals(e.getHTopMinusOne(), read.getHTopMinusOne());
    assertArrayEquals(e.getSignature(), read.getSignature());
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
