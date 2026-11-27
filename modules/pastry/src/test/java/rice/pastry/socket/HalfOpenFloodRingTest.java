package rice.pastry.socket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.PastryNode;

/**
 * C1 (R1): an outsider that opens TCP connections to a node, sends the magic number and then stalls must not evict the
 * node's established peers. Today the limit-sockets layer counts those half-open sockets with the established ones and
 * closes the least recently used, which are the established peers.
 *
 * <p>A probe layer under the limit-sockets layer of every node records which sockets are established (opened by the
 * node, or declared so by the layer above with {@code handshakeCompleted()}) and which of those the node closes.
 */
class HalfOpenFloodRingTest {
  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
  private static final List<String> IDS = List.of("00", "80", "A0", "60");
  private static final int MAX_OPEN_SOCKETS = 16;
  private static final int HALF_OPEN_SOCKETS = 32;
  private static final long READY_SECONDS = 10;
  /** long enough for the victim to have accepted and handled the whole flood */
  private static final long SETTLE_MILLIS = 2000;

  private final Environment env = new Environment();
  private final List<Socket> attackerSockets = new ArrayList<>();
  /** established sockets of the victim that the victim closed, in order */
  private final List<String> closedEstablishedSockets = Collections.synchronizedList(new ArrayList<>());
  private final List<ProbeSocket> victimSockets = Collections.synchronizedList(new ArrayList<>());

  @AfterEach
  void tearDown() throws IOException {
    for (Socket socket : attackerSockets) socket.close();
    env.destroy();
  }

  @Test
  @DisplayName("R1: half-open sockets from one outsider evict no established peer of a ring node")
  void halfOpenFloodEvictsNoEstablishedPeer() throws Exception {
    env.getParameters().setInt("pastry_socket_scm_max_open_sockets", MAX_OPEN_SOCKETS);
    List<PastryNode> nodes = new ArrayList<>();
    InetSocketAddress boot = null;
    InetSocketAddress victimAddress = null;
    for (int i = 0; i < IDS.size(); i++) {
      int port = freePort();
      String id = IDS.get(i);
      boolean isVictim = i == 0;
      PastryNode node = new ProbedFactory(id, port, isVictim).newNode();
      nodes.add(node);
      if (boot == null) {
        boot = new InetSocketAddress(LOOPBACK, port);
        victimAddress = boot;
      }
      node.boot(boot);
      node.awaitReadyOrJoinFailed(READY_SECONDS, TimeUnit.SECONDS);
      assertTrue(node.isReady(), "node " + id + " is ready");
    }
    Thread.sleep(SETTLE_MILLIS);
    // every node has talked to the victim
    for (PastryNode node : nodes) {
      assertTrue(node.getLeafSet().getUniqueCount() >= IDS.size() - 1, "leaf set of " + node.getNodeId() + ": " + node.getLeafSet());
    }
    int establishedBefore = countEstablishedOpen();
    assertTrue(establishedBefore >= IDS.size() - 1, "the victim has a socket to every peer: " + victimSockets);
    closedEstablishedSockets.clear();

    for (int i = 0; i < HALF_OPEN_SOCKETS; i++) {
      Socket attacker = new Socket(victimAddress.getAddress(), victimAddress.getPort());
      attackerSockets.add(attacker);
      attacker.getOutputStream().write(SocketPastryNodeFactory.PASTRY_MAGIC_NUMBER);
      attacker.getOutputStream().flush();
    }
    Thread.sleep(SETTLE_MILLIS);

    assertEquals(List.of(), closedEstablishedSockets, "established sockets the victim closed during the flood");
  }

  private int countEstablishedOpen() {
    synchronized (victimSockets) {
      int count = 0;
      for (ProbeSocket socket : victimSockets) {
        if (socket.isEstablished && !socket.isClosedByNode) count++;
      }
      return count;
    }
  }

  private static int freePort() throws IOException {
    try (ServerSocket probe = new ServerSocket(0, 1, LOOPBACK)) {
      return probe.getLocalPort();
    }
  }

  private final class ProbedFactory extends SocketPastryNodeFactory {
    private final boolean isVictim;

    ProbedFactory(String id, int port, boolean isVictim) throws IOException {
      super(() -> Id.build(id), LOOPBACK, port, env);
      this.isVictim = isVictim;
    }

    @Override
    protected TransportLayer<InetSocketAddress, ByteBuffer> getLimitSocketsTransportLayer(
        TransportLayer<InetSocketAddress, ByteBuffer> mntl, PastryNode pn) {
      return super.getLimitSocketsTransportLayer(isVictim ? new ProbeLayer(mntl) : mntl, pn);
    }
  }

  /** Passes everything through, wrapping each socket in a {@link ProbeSocket}. */
  private final class ProbeLayer implements TransportLayer<InetSocketAddress, ByteBuffer>, TransportLayerCallback<InetSocketAddress, ByteBuffer> {
    private final TransportLayer<InetSocketAddress, ByteBuffer> lower;
    @SuppressWarnings("NullAway.Init") // set by the layer above in its constructor
    private TransportLayerCallback<InetSocketAddress, ByteBuffer> callback;

    ProbeLayer(TransportLayer<InetSocketAddress, ByteBuffer> lower) {
      this.lower = lower;
      lower.setCallback(this);
    }

    @Override
    public @Nullable SocketRequestHandle<InetSocketAddress> openSocket(InetSocketAddress i, SocketCallback<InetSocketAddress> deliverSocketToMe, @Nullable Map<String, Object> options) {
      return lower.openSocket(i, new SocketCallback<>() {
        @Override
        public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
          deliverSocketToMe.receiveResult(cancellable, probe(sock, true));
        }

        @Override
        public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception ex) {
          deliverSocketToMe.receiveException(s, ex);
        }
      }, options);
    }

    @Override
    public void incomingSocket(P2PSocket<InetSocketAddress> s) throws IOException {
      callback.incomingSocket(probe(s, false));
    }

    private ProbeSocket probe(P2PSocket<InetSocketAddress> socket, boolean isOutgoing) {
      ProbeSocket probe = new ProbeSocket(socket, isOutgoing);
      victimSockets.add(probe);
      return probe;
    }

    @Override
    public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) throws IOException {
      callback.messageReceived(i, m, options);
    }

    @Override
    public @Nullable MessageRequestHandle<InetSocketAddress, ByteBuffer> sendMessage(InetSocketAddress i, ByteBuffer m, @Nullable MessageCallback<InetSocketAddress, ByteBuffer> deliverAckToMe, @Nullable Map<String, Object> options) {
      return lower.sendMessage(i, m, deliverAckToMe, options);
    }

    @Override
    public InetSocketAddress getLocalIdentifier() {
      return lower.getLocalIdentifier();
    }

    @Override
    public void acceptSockets(boolean enabled) {
      lower.acceptSockets(enabled);
    }

    @Override
    public void acceptMessages(boolean enabled) {
      lower.acceptMessages(enabled);
    }

    @Override
    public void setCallback(TransportLayerCallback<InetSocketAddress, ByteBuffer> callback) {
      this.callback = callback;
    }

    @Override
    public void setErrorHandler(ErrorHandler<InetSocketAddress> handler) {
      lower.setErrorHandler(handler);
    }

    @Override
    public void destroy() {
      lower.destroy();
    }
  }

  /** Records whether the socket is established and whether the node above shut it down or closed it. */
  private final class ProbeSocket extends SocketWrapperSocket<InetSocketAddress, InetSocketAddress> {
    volatile boolean isEstablished;
    volatile boolean isClosedByNode;

    ProbeSocket(P2PSocket<InetSocketAddress> socket, boolean isOutgoing) {
      super(socket.getIdentifier(), socket, env.getLogManager().getLogger(ProbeSocket.class, null),
          new DefaultErrorHandler<>(env.getLogManager().getLogger(ProbeSocket.class, null)), socket.getOptions());
      this.isEstablished = isOutgoing;
    }

    @Override
    public void handshakeCompleted() {
      isEstablished = true;
      super.handshakeCompleted();
    }

    @Override
    public void shutdownOutput() {
      nodeClosed();
      super.shutdownOutput();
    }

    @Override
    public void close() {
      nodeClosed();
      super.close();
    }

    private void nodeClosed() {
      if (isEstablished && !isClosedByNode) closedEstablishedSockets.add(toString());
      isClosedByNode = true;
    }

    @Override
    public String toString() {
      return "socket " + (isEstablished ? "established" : "half-open") + " with " + getIdentifier();
    }
  }
}
