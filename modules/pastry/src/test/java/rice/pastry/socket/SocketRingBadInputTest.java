package rice.pastry.socket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.pastry.testing.SocketRing;

/**
 * Requests a remote peer can send to a node of a plain socket ring, which must cost the peer its request, not the
 * environment: an exception on the selector thread destroys every node of the JVM.
 */
class SocketRingBadInputTest {
  private static final int TIMEOUT_MILLIS = 10_000;
  /** The NetworkInfo layer's header for "which nodes can you reach directly?". */
  private static final byte HEADER_NODES_REQUEST = 4;

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
}
