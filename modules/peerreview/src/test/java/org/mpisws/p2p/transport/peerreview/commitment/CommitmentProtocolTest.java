package org.mpisws.p2p.transport.peerreview.commitment;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.transport.peerreview.IdentifierExtractor;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SEQS_PER_MILLI;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_EXPOSED;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_SUSPECTED;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_TRUSTED;

/**
 * Tests that the commitment protocol delivers messages only from TRUSTED nodes, and accepts an ACK only from
 * the node it claims to come from.
 */
public class CommitmentProtocolTest {

  /** Serves as both the handle and the identifier. */
  record Node(int id) implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(id);
    }
  }

  static final Node LOCAL = new Node(1);
  static final Node REMOTE = new Node(2);
  static final long TIME_TOLERANCE_MILLIS = 60_000;

  @TempDir
  File dir;

  Environment env;
  HashProvider hashProvider;
  SecureHistory history;
  CommitmentProtocol<Node, Node> testObj;

  final Map<Node, Integer> statusById = new HashMap<>();
  boolean hasRemoteCertificate = true;
  final List<Node> deliveredFrom = new ArrayList<>();

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals", "ReferenceEquality"}) // the caller picks the generic interface type; equals() is proxy identity
  <T> T stub(Class<?> type, Object delegate, BiFunction<String, Object[], Object> answer) {
    return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
      if (delegate != null && method.getDeclaringClass().isInstance(delegate)) {
        try {
          return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
          throw e.getCause();
        }
      }
      switch (method.getName()) {
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        default:
          return answer.apply(method.getName(), args);
      }
    });
  }

  @BeforeEach
  public void setUp() throws IOException {
    env = new Environment();
    hashProvider = new SHA256HashProvider();
    history = new SecureHistoryFactory(hashProvider, env).create(new File(dir, "log").getPath(), 0, hashProvider.getEmptyHash());
    PeerReviewCallback<Node, Node> app = stub(PeerReviewCallback.class, null, (name, args) -> {
      if (name.equals("messageReceived")) deliveredFrom.add((Node) args[0]);
      return null;
    });
    PeerReview<Node, Node> peerreview = stub(PeerReview.class, null, (name, args) -> switch (name) {
      case "getEnvironment" -> env;
      case "getTime" -> env.getTimeSource().currentTimeMillis();
      case "getIdentifierExtractor" -> (IdentifierExtractor<Node, Node>) handle -> handle;
      case "getLocalId", "getLocalHandle" -> LOCAL;
      case "getApp" -> app;
      // every signature checks out; these tests are about who may send, not about signatures
      case "extractAuthenticator" -> new Authenticator((Long) args[1], new byte[] {1}, new byte[] {2});
      default -> null;
    });
    IdentityTransport<Node, Node> transport = stub(IdentityTransport.class, hashProvider, (name, args) -> switch (name) {
      case "hasCertificate" -> args[0].equals(LOCAL) || hasRemoteCertificate;
      case "sign" -> new byte[] {3};
      default -> null;
    });
    PeerInfoStore<Node, Node> infoStore = stub(PeerInfoStore.class, null, (name, args) -> switch (name) {
      case "getStatus" -> statusById.getOrDefault((Node) args[0], STATUS_TRUSTED);
      default -> null;
    });
    testObj = new CommitmentProtocol<Node, Node>(peerreview, transport, infoStore, null, history, TIME_TOLERANCE_MILLIS);
  }

  @AfterEach
  public void tearDown() throws IOException {
    history.close();
    env.destroy();
  }

  UserDataMessage<Node> messageFrom(Node sender) {
    long topSeq = env.getTimeSource().currentTimeMillis() * SEQS_PER_MILLI;
    return new UserDataMessage<Node>(topSeq, sender, new byte[hashProvider.getHashSizeBytes()], new byte[] {2}, ByteBuffer.wrap(new byte[] {42}), 1);
  }

  @Test
  public void messageFromATrustedNodeIsDelivered() throws IOException {
    testObj.handleIncomingMessage(REMOTE, messageFrom(REMOTE), null);

    assertEquals(List.of(REMOTE), deliveredFrom);
  }

  @Test
  public void messageQueuedForACertificateIsNotDeliveredOnceTheSenderIsExposed() throws IOException {
    hasRemoteCertificate = false;
    testObj.handleIncomingMessage(REMOTE, messageFrom(REMOTE), null);
    statusById.put(REMOTE, STATUS_EXPOSED);

    hasRemoteCertificate = true;
    testObj.notifyCertificateAvailable(REMOTE);

    assertTrue(deliveredFrom.isEmpty(), "a message from an exposed node must not reach the application");
  }

  @Test
  public void messageQueuedForACertificateWaitsWhileTheSenderIsSuspected() throws IOException {
    hasRemoteCertificate = false;
    testObj.handleIncomingMessage(REMOTE, messageFrom(REMOTE), null);
    statusById.put(REMOTE, STATUS_SUSPECTED);

    hasRemoteCertificate = true;
    testObj.notifyCertificateAvailable(REMOTE);
    assertTrue(deliveredFrom.isEmpty(), "a message from a suspected node must wait until the node is trusted again");

    statusById.put(REMOTE, STATUS_TRUSTED);
    testObj.notifyStatusChange(REMOTE, STATUS_TRUSTED);
    assertEquals(List.of(REMOTE), deliveredFrom);
  }

  /** Sends a message to REMOTE and returns the sequence number an ACK for it must carry. */
  long sendToRemote() {
    testObj.handleOutgoingMessage(REMOTE, ByteBuffer.wrap(new byte[] {7}), null, null);
    return testObj.peer.get(REMOTE).xmitQueue.getFirst().getTopSeq();
  }

  AckMessage<Node> ackClaimingToBeFrom(Node nodeId, long sendEntrySeq) {
    return new AckMessage<Node>(nodeId, sendEntrySeq, 99L, new byte[hashProvider.getHashSizeBytes()], new byte[] {2});
  }

  @Test
  public void ackFromTheReceiverIsLogged() throws IOException {
    long sendEntrySeq = sendToRemote();
    long entriesBefore = history.getNumEntries();

    testObj.handleIncomingAck(REMOTE, ackClaimingToBeFrom(REMOTE, sendEntrySeq), null);

    assertTrue(testObj.peer.get(REMOTE).xmitQueue.isEmpty());
    assertEquals(entriesBefore + 1, history.getNumEntries());
  }

  @Test
  public void ackClaimingAnotherNodeIdThanItsSourceIsDiscarded() throws IOException {
    long sendEntrySeq = sendToRemote();
    long entriesBefore = history.getNumEntries();

    testObj.handleIncomingAck(REMOTE, ackClaimingToBeFrom(new Node(3), sendEntrySeq), null);

    assertEquals(1, testObj.peer.get(REMOTE).xmitQueue.size(), "the message to REMOTE is still unacknowledged");
    assertEquals(entriesBefore, history.getNumEntries(), "no ACK entry is logged");
  }
}
