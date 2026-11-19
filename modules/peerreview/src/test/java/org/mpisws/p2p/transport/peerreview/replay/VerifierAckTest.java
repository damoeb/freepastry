package org.mpisws.p2p.transport.peerreview.replay;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtAck;
import org.mpisws.p2p.transport.util.Serializer;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_ACK;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_CHECKPOINT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SEND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SENDSIGN;

/**
 * Replays an ACK whose SEND the replay has not seen, using the production {@link Verifier}.
 * The replay starts at a CHECKPOINT; SENDs logged before it are outside the replay window.
 */
public class VerifierAckTest {

  record Node(int id) implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(id);
    }
  }

  static final Node LOCAL = new Node(1);
  static final Node REMOTE = new Node(2);
  static final long SEND_SEQ = 467_000_000L;
  static final long CHECKPOINT_SEQ = 471_000_000L;
  static final long ACK_SEQ = 472_000_000L;

  static final Serializer<Node> SERIALIZER = new Serializer<Node>() {
    @Override
    public void serialize(Node node, OutputBuffer buf) throws IOException {
      node.serialize(buf);
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
  PeerReviewCallback<Node, Node> app;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    logEnv = new Environment();
    env = VerifierFactory.generateEnvironment("test", 0, 0, null);
    hashProv = new SHA256HashProvider();
    history = new SecureHistoryFactory(hashProv, logEnv).create(new File(dir, "log").getPath(), 0, hashProv.getEmptyHash());
    peerreview = (PeerReview<Node, Node>) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {PeerReview.class}, (proxy, method, args) -> switch (method.getName()) {
          case "getHandleSerializer", "getIdSerializer" -> SERIALIZER;
          default -> throw new UnsupportedOperationException(method.getName());
        });
    app = (PeerReviewCallback<Node, Node>) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {PeerReviewCallback.class}, (proxy, method, args) -> switch (method.getName()) {
          case "loadCheckpoint" -> true;
          default -> throw new UnsupportedOperationException(method.getName());
        });
  }

  @AfterEach
  public void tearDown() throws IOException {
    history.close();
    logEnv.destroy();
    env.destroy();
  }

  void appendSendAt(long seq) throws IOException {
    history.setNextSeq(seq);
    history.appendEntry(EVT_SEND, true, ByteBuffer.wrap(new byte[] {0, 0, 0, 2, 0, 42}));
    history.appendEntry(EVT_SENDSIGN, true, ByteBuffer.wrap(new byte[] {0}));
  }

  /** Appends the CHECKPOINT the replay starts from and returns its index. */
  long appendCheckpointAt(long seq) throws IOException {
    long index = history.getNumEntries();
    history.setNextSeq(seq);
    history.appendEntry(EVT_CHECKPOINT, true, ByteBuffer.wrap(new byte[] {7}));
    return index;
  }

  void appendAckAt(long seq, long ackedSeq) throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    new EvtAck<Node>(REMOTE, ackedSeq, 1L, new byte[hashProv.getHashSizeBytes()], new byte[4]).serialize(buf);
    history.setNextSeq(seq);
    history.appendEntry(EVT_ACK, true, buf.getByteBuffer());
  }

  Verifier<Node, Node> replayFrom(long firstEntry) throws IOException {
    Verifier<Node, Node> verifier = new Verifier<>(peerreview, env, history, LOCAL, firstEntry, null);
    verifier.setApplication(app);
    while (verifier.makeProgress()) {
      // replay the whole snippet
    }
    return verifier;
  }

  @Test
  public void ackForASendBeforeTheStartingCheckpointIsNoFault() throws IOException {
    appendSendAt(SEND_SEQ);
    long checkpoint = appendCheckpointAt(CHECKPOINT_SEQ);
    appendAckAt(ACK_SEQ, SEND_SEQ);

    Verifier<Node, Node> verifier = replayFrom(checkpoint);

    assertTrue(verifier.isVerificationSuccessful(), "the SEND lies before the replay window, so the replay cannot have seen it");
    assertTrue(verifier.isSuccess());
  }

  @Test
  public void ackForAnUnsentSeqInsideTheWindowIsAFault() throws IOException {
    long checkpoint = appendCheckpointAt(CHECKPOINT_SEQ);
    appendAckAt(ACK_SEQ, CHECKPOINT_SEQ + 1);

    Verifier<Node, Node> verifier = replayFrom(checkpoint);

    assertFalse(verifier.isVerificationSuccessful(), "no SEND in the window has this seq");
  }

  @Test
  public void ackForTheStartingCheckpointSeqIsAFault() throws IOException {
    long checkpoint = appendCheckpointAt(CHECKPOINT_SEQ);
    appendAckAt(ACK_SEQ, CHECKPOINT_SEQ);

    Verifier<Node, Node> verifier = replayFrom(checkpoint);

    assertFalse(verifier.isVerificationSuccessful(), "the window includes its first entry");
  }

  @Test
  public void ackForASeqAfterTheAckIsAFault() throws IOException {
    long checkpoint = appendCheckpointAt(CHECKPOINT_SEQ);
    appendAckAt(ACK_SEQ, ACK_SEQ + 1_000_000L);

    Verifier<Node, Node> verifier = replayFrom(checkpoint);

    assertFalse(verifier.isVerificationSuccessful(), "an ACK cannot acknowledge a SEND logged after it");
  }
}
