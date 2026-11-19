package rice.pastry.socket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.pastry.PastryNode;
import rice.pastry.join.JoinRequest;
import rice.pastry.testing.SocketRing;
import rice.pastry.transport.PastryMessageReceipt;
import rice.pastry.transport.PastryMessageSendListener;

/**
 * Requests a remote peer can send to a node of a plain socket ring, which must cost the peer its request, not the
 * environment: an exception on the selector thread destroys every node of the JVM.
 */
class SocketRingBadInputTest {
  private static final int TIMEOUT_MILLIS = 10_000;
  /** The NetworkInfo layer's header for "which nodes can you reach directly?". */
  private static final byte HEADER_NODES_REQUEST = 4;
  /** Long enough for the selector thread of the receiving node to have handled what arrived. */
  private static final long SETTLE_MILLIS = 1000;

  @Test
  @DisplayName("a NetworkInfo nodes request to a plain socket node is answered with its leaf set's addresses")
  void nodesRequestToAPlainSocketNode() throws Exception {
    try (SocketRing ring = new SocketRing()) {
      ring.boot(2, node -> null);
      SocketNodeHandle target = (SocketNodeHandle) ring.nodes.get(0).getLocalHandle();
      SocketNodeHandle other = (SocketNodeHandle) ring.nodes.get(1).getLocalHandle();

      int count;
      try (Socket peer = new Socket(target.getInetSocketAddress().getAddress(), target.getInetSocketAddress().getPort())) {
        peer.setSoTimeout(TIMEOUT_MILLIS);
        OutputStream out = peer.getOutputStream();
        out.write(SocketPastryNodeFactory.PASTRY_MAGIC_NUMBER);
        out.write(HEADER_NODES_REQUEST);
        out.flush();
        try {
          count = new DataInputStream(peer.getInputStream()).readByte();
        } catch (IOException e) {
          count = -1;
        }
      }

      ring.assertNoExceptions();
      assertEquals(1, count, "the node knows one other node");
      assertTrue(ring.nodes.get(0).getLeafSet().member(other), "the other node is in the leaf set");
    }
  }

  @Test
  @DisplayName("a ready node ignores an accepted join request it receives, which it did not ask for")
  void readyNodeIgnoresAForgedAcceptedJoinRequest() throws Exception {
    try (SocketRing ring = new SocketRing()) {
      ring.boot(2, node -> null);
      PastryNode victim = ring.nodes.get(0);
      PastryNode attacker = ring.nodes.get(1);

      // "your join was accepted by me", with a row of the routing table to broadcast that is missing
      JoinRequest forged = new JoinRequest(victim.getLocalHandle(), (byte) 4, 1L);
      forged.pushRow(null);
      forged.acceptJoin(attacker.getLocalHandle(), attacker.getLeafSet());
      CompletableFuture<Void> sent = new CompletableFuture<>();
      attacker.getEnvironment().getSelectorManager().invoke(() -> attacker.send(victim.getLocalHandle(), forged,
          new PastryMessageSendListener() {
            @Override
            public void sent(PastryMessageReceipt msg) {
              sent.complete(null);
            }

            @Override
            public void sendFailed(PastryMessageReceipt msg, Exception reason) {
              sent.completeExceptionally(reason);
            }
          }, null));
      sent.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      Thread.sleep(SETTLE_MILLIS);

      ring.assertNoExceptions();
      assertTrue(victim.isReady(), "the node is still ready");
    }
  }
}
