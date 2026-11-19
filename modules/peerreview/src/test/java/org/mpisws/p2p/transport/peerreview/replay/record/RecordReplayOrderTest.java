package org.mpisws.p2p.transport.peerreview.replay.record;

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
import org.mpisws.p2p.transport.peerreview.replay.Verifier;
import org.mpisws.p2p.transport.peerreview.replay.VerifierFactory;
import org.mpisws.p2p.transport.peerreview.replay.playback.ReplaySM;
import org.mpisws.p2p.transport.util.Serializer;
import rice.environment.Environment;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_CHECKPOINT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_INIT;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_RECV;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SEND;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SENDSIGN;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SIGN;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SEQS_PER_MILLI;

/**
 * Records a node with the {@link RecordSM} while a message arrives by {@link SelectorManager#invoke(Runnable)} from
 * another thread as one of the node's timers is due, then replays the log with the production {@link Verifier}
 * and {@link ReplaySM}. The replay runs every timer due at or before a logged event before that event, so the
 * recording must order them the same way, or the auditor exposes a correct node.
 */
public class RecordReplayOrderTest {

  record Node(int id) implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(id);
    }
  }

  static final Node LOCAL = new Node(1);
  static final Node PEER = new Node(2);
  static final long START = 1000;
  static final int SEND_DELAY = 10;
  static final long MESSAGE_ARRIVES = START + SEND_DELAY + 5;
  static final short SIGNATURE_SIZE = 8;
  static final byte TIMER_MESSAGE = 1;
  static final byte REPLY_MESSAGE = 2;

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

  /** Where the state machine sends its messages: the log while recording, the {@link Verifier} during the replay. */
  interface Outbox {
    void send(Node target, ByteBuffer message) throws IOException;
  }

  /**
   * The state machine under audit: it sends one message from a timer {@link #SEND_DELAY} ms after INIT, and
   * replies to every received message from a timer due now. Each message carries the number of messages
   * received so far, so a replay that delivers them in another order sends other messages.
   */
  static class StateMachine {
    final SelectorManager timer;
    final Outbox outbox;
    int received;

    StateMachine(SelectorManager timer, Outbox outbox) {
      this.timer = timer;
      this.outbox = outbox;
    }

    void init() {
      sendFromTimer(PEER, TIMER_MESSAGE, SEND_DELAY);
    }

    void messageReceived(Node from) {
      received++;
      sendFromTimer(from, REPLY_MESSAGE, 0);
    }

    void storeCheckpoint(OutputBuffer buf) throws IOException {
      buf.writeInt(received);
    }

    void sendFromTimer(Node target, byte kind, int delay) {
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            outbox.send(target, ByteBuffer.wrap(new byte[] {kind, (byte) received}));
          } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
          }
        }
      }, delay);
    }
  }

  @TempDir
  File dir;

  Environment logEnv;
  HashProvider hashProv;
  SecureHistory history;
  DirectTimeSource realTime;
  DirectTimeSource simTime;
  RecordSM recordSM;
  StateMachine recorded;
  long lastLoggedMillis = -1;

  @BeforeEach
  public void setUp() throws Exception {
    logEnv = new Environment();
    hashProv = new SHA256HashProvider();
    history = new SecureHistoryFactory(hashProv, logEnv).create(new File(dir, "log").getPath(), 0, hashProv.getEmptyHash());
    realTime = new DirectTimeSource(START);
    simTime = new DirectTimeSource(START);
    // the selector thread is not started: the test drives the loop, other threads only invoke()
    recordSM = new RecordSM("record", realTime, simTime, logEnv.getLogManager(), null);
    recorded = new StateMachine(recordSM, this::logSend);
  }

  @AfterEach
  public void tearDown() throws IOException {
    recordSM.destroy();
    history.close();
    logEnv.destroy();
  }

  /** Like PeerReview: every entry carries the time of the record clock. */
  void log(short type, byte[] entry) throws IOException {
    long now = simTime.currentTimeMillis();
    if (now > lastLoggedMillis) {
      assertTrue(history.setNextSeq(now * SEQS_PER_MILLI));
      lastLoggedMillis = now;
    }
    history.appendEntry(type, true, ByteBuffer.wrap(entry));
  }

  void logSend(Node target, ByteBuffer message) throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    SERIALIZER.serialize(target, buf);
    buf.writeBoolean(false); // not partly hashed
    byte[] bytes = new byte[message.remaining()];
    message.duplicate().get(bytes);
    buf.write(bytes, 0, bytes.length);
    log(EVT_SEND, buf.getBytes());
    log(EVT_SENDSIGN, new byte[] {0});
  }

  /** A message from the network: logged as RECV and SIGN, then handed to the state machine. */
  void receive(Node from, byte[] message) {
    try {
      SimpleOutputBuffer buf = new SimpleOutputBuffer();
      SERIALIZER.serialize(from, buf);
      buf.writeLong(0); // the sender's seq
      buf.writeBoolean(false); // not partly hashed
      buf.write(message, 0, message.length);
      log(EVT_RECV, buf.getBytes());
      log(EVT_SIGN, new byte[hashProv.getHashSizeBytes() + SIGNATURE_SIZE]);
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
    recorded.messageReceived(from);
  }

  void recordInit() throws IOException {
    log(EVT_INIT, new byte[] {0});
    recorded.init();
  }

  /**
   * Like PeerReviewImpl's checkpoint task: a timer that only the record has, and that logs a CHECKPOINT of the
   * state machine. PeerReviewImpl schedules it before callback.init(), so it runs first among the timers due in
   * the same millisecond.
   */
  void scheduleCheckpoint(int delay) {
    recordSM.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          SimpleOutputBuffer buf = new SimpleOutputBuffer();
          recorded.storeCheckpoint(buf);
          log(EVT_CHECKPOINT, buf.getBytes());
        } catch (IOException ioe) {
          throw new IllegalStateException(ioe);
        }
      }
    }, delay);
  }

  /** Another thread delivers a message by invoke(), as an in-memory transport or a processor callback does. */
  void deliverFromAnotherThread(byte[] message) throws InterruptedException {
    Thread network = new Thread(() -> recordSM.invoke(() -> receive(PEER, message)));
    network.start();
    network.join();
  }

  /** The selector thread wakes up at {@code millis}: runs one pass of its loop. */
  void loopAt(long millis) {
    realTime.setTime(millis);
    recordSM.executeDueTasks();
  }

  @SuppressWarnings("unchecked")
  Verifier<Node, Node> replay() throws IOException {
    PeerReview<Node, Node> peerreview = (PeerReview<Node, Node>) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {PeerReview.class}, (proxy, method, args) -> switch (method.getName()) {
          case "getHandleSerializer" -> SERIALIZER;
          case "getHashSizeBytes" -> hashProv.getHashSizeBytes();
          case "getSignatureSizeBytes" -> SIGNATURE_SIZE;
          default -> throw new UnsupportedOperationException(method.getName());
        });
    Environment env = VerifierFactory.generateEnvironment("replay", 0, 0, null);
    try {
      Verifier<Node, Node> verifier = new Verifier<>(peerreview, env, history, LOCAL, 0, null);
      ReplaySM replaySM = (ReplaySM) env.getSelectorManager();
      replaySM.setVerifier(verifier);
      StateMachine replayed = new StateMachine(replaySM, (target, message) -> verifier.sendMessage(target, message, null, null));
      verifier.setApplication((PeerReviewCallback<Node, ?>) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class<?>[] {PeerReviewCallback.class}, (proxy, method, args) -> switch (method.getName()) {
            case "init" -> {
              replayed.init();
              yield null;
            }
            case "storeCheckpoint" -> {
              replayed.storeCheckpoint((OutputBuffer) args[0]);
              yield null;
            }
            case "messageReceived" -> {
              replayed.messageReceived((Node) args[0]);
              yield null;
            }
            default -> throw new UnsupportedOperationException(method.getName());
          }));

      while (replaySM.makeProgress());
      return verifier;
    } finally {
      env.destroy();
    }
  }

  void assertReplayAgrees() throws IOException {
    Verifier<Node, Node> verifier = replay();
    assertTrue(verifier.isVerificationSuccessful(), "the replay of a correct node diverges from its log");
    assertTrue(verifier.isSuccess(), "the replay consumed the whole log");
  }

  @Test
  public void messagePendingWhileATimerIsDueReplays() throws Exception {
    recordInit();
    deliverFromAnotherThread(new byte[] {7});

    loopAt(MESSAGE_ARRIVES);
    loopAt(MESSAGE_ARRIVES);

    assertReplayAgrees();
  }

  @Test
  public void twoMessagesPendingWhileATimerIsDueReplay() throws Exception {
    recordInit();
    deliverFromAnotherThread(new byte[] {7});
    deliverFromAnotherThread(new byte[] {8});

    loopAt(MESSAGE_ARRIVES);
    loopAt(MESSAGE_ARRIVES);

    assertReplayAgrees();
  }

  @Test
  public void messageBeforeTheTimerIsDueReplays() throws Exception {
    recordInit();
    deliverFromAnotherThread(new byte[] {7});

    loopAt(START + 5);
    loopAt(MESSAGE_ARRIVES);

    assertReplayAgrees();
  }

  @Test
  public void checkpointDueWithATimerReplays() throws Exception {
    scheduleCheckpoint(SEND_DELAY);
    recordInit();

    loopAt(MESSAGE_ARRIVES);

    assertReplayAgrees();
  }

  @Test
  public void checkpointAfterATimerDueAtTheSameTimeReplays() throws Exception {
    recordInit();
    scheduleCheckpoint(SEND_DELAY);

    loopAt(MESSAGE_ARRIVES);

    assertReplayAgrees();
  }
}
