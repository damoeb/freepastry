package rice.pastry.standard;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.leafset.LeafSet;
import rice.pastry.testing.WireRoundTrip.SocketNode;

/**
 * A consistent join message names its sender as the base of the leaf set it carries. The joiner records that it
 * heard from that node, and goes ready once it heard from all its neighbours. In certified mode the connection's
 * certificate proves who sent the message: a message that names another node is dropped, so an insider cannot answer
 * for a joiner's neighbours.
 */
class ConsistentJoinProtocolForgedSenderTest {
  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
  private static final int WAIT_SECONDS = 10;

  private Environment env;
  private SocketNode local;
  private ConsistentJoinProtocol protocol;
  /** the ports the handles of this test name: bound here, so nothing this node sends to them reaches anyone else */
  private final List<AutoCloseable> ports = new ArrayList<>();

  @BeforeEach
  void setUp() throws IOException {
    env = new Environment();
    local = SocketNode.create(env, "4000");
    protocol = new ConsistentJoinProtocol(local.node, local.local(), local.node.getRoutingTable(), local.node.getLeafSet(), null);
  }

  @AfterEach
  void tearDown() throws Exception {
    env.destroy();
    for (AutoCloseable port : ports) {
      port.close();
    }
  }

  @Test
  @DisplayName("certified mode: a join message that names another node as its sender does not count as that node's answer")
  void certifiedForgedSenderIsNotHeardFrom() throws Exception {
    certifiedMode();
    NodeHandle neighbour = handle("8000");
    NodeHandle insider = handle("9000");

    receive(message(neighbour), insider);

    assertFalse(onSelector(() -> protocol.gotResponse.containsKey(neighbour)), "the neighbour counts as heard from");
  }

  @Test
  @DisplayName("certified mode: a join message from the node it names counts as that node's answer")
  void certifiedRealSenderIsHeardFrom() throws Exception {
    certifiedMode();
    NodeHandle neighbour = handle("8000");

    receive(message(neighbour), neighbour);

    assertTrue(onSelector(() -> protocol.gotResponse.containsKey(neighbour)), "the neighbour counts as heard from");
  }

  @Test
  @DisplayName("plain mode: no connection proves the sender, so the sender named in the message is taken as before")
  void plainModeTakesTheNamedSender() throws Exception {
    NodeHandle neighbour = handle("8000");
    NodeHandle other = handle("9000");

    receive(message(neighbour), other);

    assertTrue(onSelector(() -> protocol.gotResponse.containsKey(neighbour)), "the neighbour counts as heard from");
  }

  /** an identity policy, as {@code CertifiedSocketPastryNodeFactory} sets; it admits no one, so the leaf set stays as it is */
  private void certifiedMode() {
    local.node.setIdentityPolicy(handle -> false);
  }

  /** @return the message the node would send: its leaf set, with no other members, no failed nodes, no request */
  private ConsistentJoinMessage message(NodeHandle from) {
    return new ConsistentJoinMessage(new LeafSet(from, local.node.getLeafSet().maxSize(), false), new HashSet<>(), false);
  }

  /** delivers the message as the transport layer would, with the sender the connection authenticated */
  private void receive(ConsistentJoinMessage message, NodeHandle transportSender) throws Exception {
    message.setSender(transportSender);
    onSelector(() -> {
      protocol.receiveMessage(message);
      return null;
    });
  }

  /** @return a handle with this id at a loopback port this test holds (TCP and UDP) */
  private NodeHandle handle(String id) throws IOException {
    ServerSocket serverSocket = new ServerSocket(0, 1, LOOPBACK);
    ports.add(serverSocket);
    ports.add(new DatagramSocket(serverSocket.getLocalPort(), LOOPBACK));
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    new MultiInetSocketAddress(new InetSocketAddress(LOOPBACK, serverSocket.getLocalPort())).serialize(out);
    out.writeLong(1L);
    Id.build(id).serialize(out);
    return local.node.readNodeHandle(new SimpleInputBuffer(out.getBytes()));
  }

  private <T> T onSelector(java.util.concurrent.Callable<T> task) throws Exception {
    CompletableFuture<T> result = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> {
      try {
        result.complete(task.call());
      } catch (Exception | Error e) {
        result.completeExceptionally(e);
      }
    });
    return result.get(WAIT_SECONDS, TimeUnit.SECONDS);
  }
}
