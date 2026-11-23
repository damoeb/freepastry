package org.mpisws.p2p.transport.peerreview.challenge;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.replay.record.RecordLayer;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_EXPOSED;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_TRUSTED;

/**
 * Tests that what a SUSPECTED node sent is released when the node's status changes, although the queue is
 * filled by handle and the status change names the identifier.
 */
public class ChallengeResponseProtocolQueueTest {

  /** The identifier of a node. */
  record NodeId(int id) implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(id);
    }
  }

  /** A handle: the identifier plus an address, a different type than the identifier as in a real overlay. */
  record NodeHandle(int id, String address) implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(id);
      buf.writeUTF(address);
    }
  }

  static final NodeHandle SUSPECT = new NodeHandle(2, "10.0.0.2:5009");
  static final NodeId SUSPECT_ID = new NodeId(2);

  Environment env;
  ChallengeResponseProtocol<NodeHandle, NodeId> testObj;
  final List<Long> addedEvidenceSeqs = new ArrayList<>();

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() {
    env = RecordLayer.generateEnvironment();
    IdentityTransport<NodeHandle, NodeId> transport = (IdentityTransport<NodeHandle, NodeId>) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {IdentityTransport.class}, (proxy, method, args) -> method.getReturnType() == boolean.class ? false : method.getReturnType() == short.class ? (short) 0 : null);
    PeerReviewImpl<NodeHandle, NodeId> peerreview = new PeerReviewImpl<NodeHandle, NodeId>(transport, env, null, null, handle -> new NodeId(handle.id()), null);
    PeerInfoStore<NodeHandle, NodeId> infoStore = (PeerInfoStore<NodeHandle, NodeId>) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {PeerInfoStore.class}, (proxy, method, args) -> {
          if (method.getName().equals("addEvidence")) addedEvidenceSeqs.add((Long) args[2]);
          return method.getReturnType() == int.class ? 0 : null;
        });
    testObj = new ChallengeResponseProtocol<NodeHandle, NodeId>(peerreview, transport, infoStore, null, null, null, null);
  }

  @AfterEach
  public void tearDown() {
    env.destroy();
  }

  void enqueueAccusationFromSuspect(long evidenceSeq) {
    testObj.copyAndEnqueueTail(SUSPECT, null, true, new NodeId(3), new NodeId(4), evidenceSeq, null);
  }

  @Test
  public void queuedPacketsAreDeliveredWhenTheSenderIsTrustedAgain() {
    enqueueAccusationFromSuspect(7);
    enqueueAccusationFromSuspect(8);

    testObj.notifyStatusChange(SUSPECT_ID, STATUS_TRUSTED);

    assertEquals(List.of(7L, 8L), addedEvidenceSeqs);
    assertTrue(testObj.queue.isEmpty());
  }

  @Test
  public void queuedPacketsAreDroppedWhenTheSenderIsExposed() {
    enqueueAccusationFromSuspect(7);

    testObj.notifyStatusChange(SUSPECT_ID, STATUS_EXPOSED);

    assertTrue(testObj.queue.isEmpty(), "the exposed node's queue is released");
    testObj.notifyStatusChange(SUSPECT_ID, STATUS_TRUSTED);
    assertTrue(addedEvidenceSeqs.isEmpty(), "nothing an exposed node sent is delivered");
  }
}
