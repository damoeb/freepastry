package org.mpisws.p2p.transport.limitsockets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import rice.environment.Environment;

/**
 * C1 (R1): sockets that never finish their handshake must not push established peers out of the socket limit. Today
 * one pool holds both, so a few addresses with half-open sockets evict every authenticated peer.
 */
class LimitSocketsPoolsTest {
  private static final int MAX_OPEN_SOCKETS = 4;
  private static final int HALF_OPEN_FLOOD = 32;

  private final Environment env = new Environment();
  private final FakeLowerLayer lower = new FakeLowerLayer();
  private final List<P2PSocket<String>> delivered = new ArrayList<>();

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("R1: a flood of sockets that never finish the handshake evicts no established peer")
  void halfOpenFloodEvictsNoEstablishedPeer() throws IOException {
    LimitSocketsTransportLayer<String, ByteBuffer> layer = new LimitSocketsTransportLayer<>(MAX_OPEN_SOCKETS, lower, null, env);
    layer.setCallback(new RecordingCallback());
    List<FakeSocket> established = new ArrayList<>();
    for (int i = 0; i < MAX_OPEN_SOCKETS; i++) {
      FakeSocket honest = new FakeSocket("honest-" + i);
      established.add(honest);
      lower.callback.incomingSocket(honest);
      // the layer above (priority, after its header; over TLS after the TLS handshake) declares it established
      delivered.get(delivered.size() - 1).handshakeCompleted();
    }

    for (int i = 0; i < HALF_OPEN_FLOOD; i++) {
      lower.callback.incomingSocket(new FakeSocket("attacker-" + i));
    }

    for (FakeSocket honest : established) {
      assertFalse(honest.isShutDownOrClosed(), honest + " was closed by the half-open flood");
    }
  }

  @Test
  @DisplayName("an established socket the node opened itself survives a flood of half-open incoming sockets")
  void halfOpenFloodSparesOutgoingSockets() throws IOException {
    LimitSocketsTransportLayer<String, ByteBuffer> layer = new LimitSocketsTransportLayer<>(MAX_OPEN_SOCKETS, lower, null, env);
    layer.setCallback(new RecordingCallback());
    List<P2PSocket<String>> opened = new ArrayList<>();
    layer.openSocket("neighbour", new SocketCallback<>() {
      @Override
      public void receiveResult(SocketRequestHandle<String> cancellable, P2PSocket<String> sock) {
        opened.add(sock);
      }

      @Override
      public void receiveException(SocketRequestHandle<String> s, Exception ex) {
      }
    }, null);
    assertEquals(1, opened.size());

    for (int i = 0; i < HALF_OPEN_FLOOD; i++) {
      lower.callback.incomingSocket(new FakeSocket("attacker-" + i));
    }

    assertFalse(lower.outgoing.get(0).isShutDownOrClosed(), "the outgoing socket was closed by the half-open flood");
  }

  private final class RecordingCallback implements TransportLayerCallback<String, ByteBuffer> {
    @Override
    public void messageReceived(String i, ByteBuffer m, @Nullable Map<String, Object> options) {
    }

    @Override
    public void incomingSocket(P2PSocket<String> s) {
      delivered.add(s);
    }
  }

  /** The layer below: hands up incoming sockets when the test says so, and opens fake sockets at once. */
  static final class FakeLowerLayer implements TransportLayer<String, ByteBuffer> {
    @SuppressWarnings("NullAway.Init") // set by the layer under test in its constructor
    TransportLayerCallback<String, ByteBuffer> callback;
    final List<FakeSocket> outgoing = new ArrayList<>();

    @Override
    public @Nullable SocketRequestHandle<String> openSocket(String i, SocketCallback<String> deliverSocketToMe, @Nullable Map<String, Object> options) {
      FakeSocket socket = new FakeSocket(i);
      outgoing.add(socket);
      deliverSocketToMe.receiveResult(null, socket);
      return null;
    }

    @Override
    public @Nullable MessageRequestHandle<String, ByteBuffer> sendMessage(String i, ByteBuffer m, @Nullable MessageCallback<String, ByteBuffer> deliverAckToMe, @Nullable Map<String, Object> options) {
      return null;
    }

    @Override
    public String getLocalIdentifier() {
      return "local";
    }

    @Override
    public void acceptSockets(boolean enabled) {
    }

    @Override
    public void acceptMessages(boolean enabled) {
    }

    @Override
    public void setCallback(TransportLayerCallback<String, ByteBuffer> callback) {
      this.callback = callback;
    }

    @Override
    public void setErrorHandler(ErrorHandler<String> handler) {
    }

    @Override
    public void destroy() {
    }
  }

  /** Records whether a layer above shut it down or closed it. */
  static final class FakeSocket implements P2PSocket<String> {
    private final String identifier;
    boolean isShutDown;
    boolean isClosed;

    FakeSocket(String identifier) {
      this.identifier = identifier;
    }

    boolean isShutDownOrClosed() {
      return isShutDown || isClosed;
    }

    @Override
    public String getIdentifier() {
      return identifier;
    }

    @Override
    public long read(ByteBuffer dst) {
      return 0;
    }

    @Override
    public long write(ByteBuffer src) {
      int length = src.remaining();
      src.position(src.limit());
      return length;
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<String> receiver) {
    }

    @Override
    public void shutdownOutput() {
      isShutDown = true;
    }

    @Override
    public void close() {
      isClosed = true;
    }

    @Override
    public @Nullable Map<String, Object> getOptions() {
      return null;
    }

    @Override
    public String toString() {
      return "FakeSocket[" + identifier + "]";
    }
  }
}
