package org.mpisws.p2p.transport.peerreview.replay;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.transport.peerreview.IdentifierExtractor;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import org.mpisws.p2p.transport.util.Serializer;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SEND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SENDSIGN;

/**
 * Replays a SEND against the SEND entry in the log, using the production {@link VerifierImpl}.
 */
public class VerifierImplSendTest {

  record Node(int id) implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(id);
    }
  }

  static final Node LOCAL = new Node(1);
  static final Node TARGET = new Node(2);
  static final byte[] MESSAGE = {10, 11, 12, 13, 14, 15, 16, 17};
  static final int RELEVANT = 3;
  static final long FIRST_SEND_ENTRY = 1;

  static final Serializer<Node> SERIALIZER = new Serializer<Node>() {
    @Override
    public void serialize(Node n, OutputBuffer buf) throws IOException {
      n.serialize(buf);
    }

    @Override
    public Node deserialize(InputBuffer buf) throws IOException {
      return new Node(buf.readInt());
    }
  };

  @TempDir
  File dir;

  Environment env;
  Environment logEnv;
  HashProvider hashProv;
  SecureHistory history;
  PeerReview<Node, Node> peerreview;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    logEnv = new Environment();
    env = VerifierFactoryImpl.generateEnvironment("test", 0, 0, null);
    hashProv = new SHA256HashProvider();
    history = new SecureHistoryFactoryImpl(hashProv, logEnv).create(new File(dir, "log").getPath(), 0, hashProv.getEmptyHash());
    IdentifierExtractor<Node, Node> extractor = h -> h;
    peerreview = (PeerReview<Node, Node>) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {PeerReview.class}, (proxy, method, args) -> switch (method.getName()) {
          case "getIdentifierExtractor" -> extractor;
          case "getHandleSerializer" -> SERIALIZER;
          case "getHashSizeBytes" -> hashProv.getHashSizeBytes();
          case "hash" -> args.length == 1 ? hashProv.hash((ByteBuffer[]) args[0])
              : hashProv.hash((Long) args[0], (Short) args[1], (byte[]) args[2], (byte[]) args[3]);
          default -> throw new UnsupportedOperationException(method.getName());
        });
  }

  @AfterEach
  public void tearDown() throws IOException {
    history.close();
    logEnv.destroy();
    env.destroy();
  }

  /** The SEND entry: the receiver, the partly-relevant flag, the relevant part, then the hash of the rest. */
  byte[] sendEntry(Node target, byte[] message, int relevantLen) throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    target.serialize(buf);
    boolean partly = relevantLen < message.length;
    buf.writeBoolean(partly);
    buf.write(message, 0, relevantLen);
    if (partly) {
      byte[] irrelevantHash = hashProv.hash(ByteBuffer.wrap(message, relevantLen, message.length - relevantLen));
      buf.write(irrelevantHash, 0, irrelevantHash.length);
    }
    return buf.getBytes();
  }

  /** Every SEND is followed by a SENDSIGN. */
  void appendHashedSend(byte[] entry) throws IOException {
    history.appendHash(EVT_SEND, hashProv.hash(ByteBuffer.wrap(entry)));
    history.appendEntry(EVT_SENDSIGN, true, ByteBuffer.wrap(new byte[] {0}));
  }

  void appendLoggedSend(byte[] entry) throws IOException {
    history.appendEntry(EVT_SEND, true, ByteBuffer.wrap(entry));
    history.appendEntry(EVT_SENDSIGN, true, ByteBuffer.wrap(new byte[] {0}));
  }

  /** The history starts with an INIT entry, which would make the verifier skip the SEND; replay from the entry after it. */
  VerifierImpl<Node, Node> verifier() throws IOException {
    return new VerifierImpl<>(peerreview, env, history, LOCAL, FIRST_SEND_ENTRY, null);
  }

  void send(VerifierImpl<Node, Node> verifier, ByteBuffer message, int relevantLen) {
    Map<String, Object> options = relevantLen < 0 ? null : Map.of(PeerReview.RELEVANT_LENGTH, relevantLen);
    verifier.sendMessage(TARGET, message, null, options);
  }

  /** The message as a view into a larger array: arrayOffset() == 2, position() == 0. */
  static ByteBuffer sliced(byte[] message) {
    byte[] padded = new byte[message.length + 2];
    padded[0] = 99;
    padded[1] = 98;
    System.arraycopy(message, 0, padded, 2, message.length);
    return ByteBuffer.wrap(padded, 2, message.length).slice();
  }

  static byte[] withLastByteChanged() {
    byte[] other = MESSAGE.clone();
    other[other.length - 1]++;
    return other;
  }

  static byte[] withFirstByteChanged() {
    byte[] other = MESSAGE.clone();
    other[0]++;
    return other;
  }

  @Test
  public void hashedSendMatchesTheSameMessage() throws IOException {
    appendHashedSend(sendEntry(TARGET, MESSAGE, MESSAGE.length));
    VerifierImpl<Node, Node> verifier = verifier();

    send(verifier, ByteBuffer.wrap(MESSAGE), -1);

    assertTrue(verifier.verifiedOK(), "the hash of the replayed SEND equals the hash in the log");
  }

  @Test
  public void hashedSendDetectsADifferentMessage() throws IOException {
    appendHashedSend(sendEntry(TARGET, MESSAGE, MESSAGE.length));
    VerifierImpl<Node, Node> verifier = verifier();

    send(verifier, ByteBuffer.wrap(withFirstByteChanged()), -1);

    assertFalse(verifier.verifiedOK());
  }

  @Test
  public void hashedPartlyRelevantSendMatchesTheSameMessage() throws IOException {
    appendHashedSend(sendEntry(TARGET, MESSAGE, RELEVANT));
    VerifierImpl<Node, Node> verifier = verifier();

    send(verifier, ByteBuffer.wrap(MESSAGE), RELEVANT);

    assertTrue(verifier.verifiedOK());
  }

  @Test
  public void hashedPartlyRelevantSendDetectsADifferentIrrelevantPart() throws IOException {
    appendHashedSend(sendEntry(TARGET, MESSAGE, RELEVANT));
    VerifierImpl<Node, Node> verifier = verifier();

    send(verifier, ByteBuffer.wrap(withLastByteChanged()), RELEVANT);

    assertFalse(verifier.verifiedOK(), "the irrelevant part is covered by its hash");
  }

  @Test
  public void loggedPartlyRelevantSendMatchesTheSameMessage() throws IOException {
    appendLoggedSend(sendEntry(TARGET, MESSAGE, RELEVANT));
    VerifierImpl<Node, Node> verifier = verifier();

    send(verifier, ByteBuffer.wrap(MESSAGE), RELEVANT);

    assertTrue(verifier.verifiedOK());
  }

  @Test
  public void loggedPartlyRelevantSendDetectsADifferentRelevantPart() throws IOException {
    appendLoggedSend(sendEntry(TARGET, MESSAGE, RELEVANT));
    VerifierImpl<Node, Node> verifier = verifier();

    send(verifier, ByteBuffer.wrap(withFirstByteChanged()), RELEVANT);

    assertFalse(verifier.verifiedOK());
  }

  @Test
  public void loggedPartlyRelevantSendDetectsADifferentIrrelevantPart() throws IOException {
    appendLoggedSend(sendEntry(TARGET, MESSAGE, RELEVANT));
    VerifierImpl<Node, Node> verifier = verifier();

    send(verifier, ByteBuffer.wrap(withLastByteChanged()), RELEVANT);

    assertFalse(verifier.verifiedOK());
  }

  @Test
  public void hashedSendMatchesTheSameMessageWithAnArrayOffset() throws IOException {
    appendHashedSend(sendEntry(TARGET, MESSAGE, MESSAGE.length));
    VerifierImpl<Node, Node> verifier = verifier();

    send(verifier, sliced(MESSAGE), -1);

    assertTrue(verifier.verifiedOK());
  }

  @Test
  public void hashedPartlyRelevantSendMatchesTheSameMessageWithAnArrayOffset() throws IOException {
    appendHashedSend(sendEntry(TARGET, MESSAGE, RELEVANT));
    VerifierImpl<Node, Node> verifier = verifier();

    send(verifier, sliced(MESSAGE), RELEVANT);

    assertTrue(verifier.verifiedOK());
  }

  @Test
  public void historyStoresAnEntryWithAnArrayOffset() throws IOException {
    history.appendEntry(EVT_SEND, true, sliced(MESSAGE));

    assertArrayEquals(MESSAGE, history.getEntry(history.getNumEntries() - 1, MESSAGE.length));
  }
}
