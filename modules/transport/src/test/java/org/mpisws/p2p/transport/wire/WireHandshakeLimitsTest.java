package org.mpisws.p2p.transport.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.testing.transportlayer.FreePorts;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;
import rice.environment.Environment;

/**
 * W9: a peer that opens sockets and stalls in the handshake must not hold them forever, and one address must not fill
 * the node with half-open sockets; a UDP flood must not keep the selector thread in one read.
 */
class WireHandshakeLimitsTest {
  private static final byte[] MAGIC_NUMBER = {0x27, 0x40, 0x75, 0x3A};
  private static final int SHORT_HANDSHAKE_TIMEOUT_MILLIS = 300;
  private static final int LONG_TIMEOUT_MILLIS = 60_000;
  private static final int CLIENT_READ_TIMEOUT_MILLIS = 5_000;
  private static final int MAX_HANDSHAKING_SOCKETS = 4;
  private static final int HALF_OPEN_SOCKETS = 10;
  private static final int MAX_DATAGRAMS_PER_READ = 8;
  private static final int FLOOD_DATAGRAMS = 50;
  private static final int DATAGRAM_BURST = 5;

  private final Environment env = new Environment();
  private final InetAddress localhost = InetAddress.getLoopbackAddress();
  private final List<Socket> clients = new ArrayList<>();

  @AfterEach
  void tearDown() throws IOException {
    for (Socket client : clients) {
      client.close();
    }
    env.destroy();
  }

  @Test
  @DisplayName("a socket that sends the magic number and part of a header, then stalls, is closed after the handshake deadline")
  void stalledHeaderIsClosedAfterDeadline() throws Exception {
    env.getParameters().setInt("transport_wire_handshake_timeout_millis", SHORT_HANDSHAKE_TIMEOUT_MILLIS);
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    MagicNumberTransportLayer<InetSocketAddress> magic =
        new MagicNumberTransportLayer<>(wire, env, null, MAGIC_NUMBER, LONG_TIMEOUT_MILLIS);
    magic.setCallback(headerLayerThatWaitsFor(16));

    Socket client = connect(address);
    client.getOutputStream().write(MAGIC_NUMBER);
    client.getOutputStream().write(new byte[] {1, 2});
    client.getOutputStream().flush();

    assertClosedByNode(client);
  }

  @Test
  @DisplayName("an incoming socket carries the address it really connects from up the stack, past the magic number layer")
  void incomingSocketCarriesItsRemoteAddress() throws Exception {
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    MagicNumberTransportLayer<InetSocketAddress> magic =
        new MagicNumberTransportLayer<>(wire, env, null, MAGIC_NUMBER, LONG_TIMEOUT_MILLIS);
    CompletableFuture<@Nullable Map<String, Object>> options = new CompletableFuture<>();
    magic.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> o) {
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> socket) {
        options.complete(socket.getOptions());
      }
    });

    Socket client = connect(address);
    client.getOutputStream().write(MAGIC_NUMBER);
    client.getOutputStream().flush();

    Map<String, Object> actual = options.get(CLIENT_READ_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    assertEquals(client.getLocalSocketAddress(), actual == null ? null : actual.get(WireTransportLayer.OPTION_REMOTE_ADDRESS));
  }

  @Test
  @DisplayName("one address gets at most the configured number of sockets in the handshake")
  void halfOpenSocketsPerAddressAreCapped() throws Exception {
    env.getParameters().setInt("transport_wire_handshake_timeout_millis", LONG_TIMEOUT_MILLIS);
    env.getParameters().setInt("transport_wire_max_handshaking_sockets_per_prefix", MAX_HANDSHAKING_SOCKETS);
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    AtomicInteger delivered = new AtomicInteger();
    wire.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) {
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> socket) {
        delivered.incrementAndGet();
      }
    });

    for (int i = 0; i < HALF_OPEN_SOCKETS; i++) {
      connect(address);
    }
    int closedByNode = 0;
    for (Socket client : clients) {
      if (isClosedByNode(client, 1_000)) closedByNode++;
    }

    assertEquals(MAX_HANDSHAKING_SOCKETS, delivered.get(), "sockets the node accepted into the handshake");
    assertEquals(HALF_OPEN_SOCKETS - MAX_HANDSHAKING_SOCKETS, closedByNode, "sockets the node closed at once");
  }

  @Test
  @DisplayName("a socket whose handshake completed outlives the deadline and frees its place for the next socket")
  void completedHandshakeKeepsSocketAndFreesPlace() throws Exception {
    env.getParameters().setInt("transport_wire_handshake_timeout_millis", SHORT_HANDSHAKE_TIMEOUT_MILLIS);
    env.getParameters().setInt("transport_wire_max_handshaking_sockets_per_prefix", 1);
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    AtomicInteger delivered = new AtomicInteger();
    wire.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) {
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> socket) {
        delivered.incrementAndGet();
        socket.handshakeCompleted();
      }
    });

    Socket first = connect(address);
    assertFalse(isClosedByNode(first, 3 * SHORT_HANDSHAKE_TIMEOUT_MILLIS), "the node closed a socket past its handshake");
    Socket second = connect(address);
    assertFalse(isClosedByNode(second, SHORT_HANDSHAKE_TIMEOUT_MILLIS), "the node refused a socket after the first one finished its handshake");
    assertEquals(2, delivered.get());
  }

  @Test
  @DisplayName("one selection reads a bounded number of datagrams, and the rest is read later")
  void datagramDrainIsBounded() throws Exception {
    env.getParameters().setInt("transport_wire_max_datagrams_per_read", MAX_DATAGRAMS_PER_READ);
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    AtomicInteger received = new AtomicInteger();
    CompletableFuture<Integer> receivedWhenSelectorTaskRan = new CompletableFuture<>();
    CompletableFuture<Void> allReceived = new CompletableFuture<>();
    wire.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) {
        if (received.incrementAndGet() == 1) {
          env.getSelectorManager().invoke(() -> receivedWhenSelectorTaskRan.complete(received.get()));
        }
        if (received.get() == FLOOD_DATAGRAMS) allReceived.complete(null);
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> socket) {
      }
    });

    // queue the whole flood in the kernel before the node reads any of it
    wire.acceptMessages(false);
    awaitSelectorLoop();
    try (DatagramSocket flooder = new DatagramSocket(0, localhost)) {
      for (int i = 0; i < FLOOD_DATAGRAMS; i++) {
        flooder.send(new DatagramPacket(new byte[] {(byte) i}, 1, address));
      }
    }
    Thread.sleep(200);
    wire.acceptMessages(true);

    allReceived.get(10, TimeUnit.SECONDS);
    int receivedBeforeOtherWork = receivedWhenSelectorTaskRan.get(10, TimeUnit.SECONDS);
    assertTrue(receivedBeforeOtherWork <= MAX_DATAGRAMS_PER_READ,
        "the selector ran other work only after " + receivedBeforeOtherWork + " datagrams");
  }

  @Test
  @DisplayName("datagrams beyond the budget of their network prefix are dropped unread")
  void datagramsBeyondPrefixBudgetAreDropped() throws Exception {
    env.getParameters().setDouble("transport_wire_datagram_rate_per_prefix_per_second", 0.001);
    env.getParameters().setInt("transport_wire_datagram_burst_per_prefix", DATAGRAM_BURST);
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    AtomicInteger received = new AtomicInteger();
    wire.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) {
        received.incrementAndGet();
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> socket) {
      }
    });

    try (DatagramSocket flooder = new DatagramSocket(0, localhost)) {
      for (int i = 0; i < FLOOD_DATAGRAMS; i++) {
        flooder.send(new DatagramPacket(new byte[] {(byte) i}, 1, address));
      }
    }
    Thread.sleep(500);
    awaitSelectorLoop();

    assertEquals(DATAGRAM_BURST, received.get(), "datagrams passed up");
  }

  @Test
  @DisplayName("a flood from a neighbour's prefix does not drop the datagrams of the pinned neighbour itself")
  void pinnedSourceBypassesPrefixBudget() throws Exception {
    env.getParameters().setDouble("transport_wire_datagram_rate_per_prefix_per_second", 0.001);
    env.getParameters().setInt("transport_wire_datagram_burst_per_prefix", DATAGRAM_BURST);
    InetSocketAddress address = new InetSocketAddress(localhost, FreePorts.freePort(localhost));
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    AtomicInteger fromNeighbour = new AtomicInteger();
    try (DatagramSocket flooder = new DatagramSocket(0, localhost); DatagramSocket neighbour = new DatagramSocket(0, localhost)) {
      InetSocketAddress neighbourAddress = (InetSocketAddress) neighbour.getLocalSocketAddress();
      wire.setDatagramPinPolicy(() -> neighbourAddress::equals);
      wire.setCallback(new TransportLayerCallback<>() {
        @Override
        public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) {
          if (i.equals(neighbourAddress)) fromNeighbour.incrementAndGet();
        }

        @Override
        public void incomingSocket(P2PSocket<InetSocketAddress> socket) {
        }
      });

      for (int i = 0; i < FLOOD_DATAGRAMS; i++) {
        flooder.send(new DatagramPacket(new byte[] {(byte) i}, 1, address));
      }
      Thread.sleep(300);
      for (int i = 0; i < DATAGRAM_BURST * 2; i++) {
        neighbour.send(new DatagramPacket(new byte[] {(byte) i}, 1, address));
      }
      Thread.sleep(300);
    }
    awaitSelectorLoop();

    assertEquals(DATAGRAM_BURST * 2, fromNeighbour.get(), "datagrams of the pinned neighbour passed up");
  }

  /** Stands for the identity and other header layers: reads until it has the whole header, which never comes. */
  private static TransportLayerCallback<InetSocketAddress, ByteBuffer> headerLayerThatWaitsFor(int headerBytes) {
    return new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) {
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> socket) {
        ByteBuffer header = ByteBuffer.allocate(headerBytes);
        socket.register(true, false, new P2PSocketReceiver<>() {
          @Override
          public void receiveSelectResult(P2PSocket<InetSocketAddress> s, boolean canRead, boolean canWrite) throws IOException {
            if (s.read(header) < 0) {
              s.close();
              return;
            }
            if (header.hasRemaining()) s.register(true, false, this);
          }

          @Override
          public void receiveException(P2PSocket<InetSocketAddress> s, Exception e) {
          }
        });
      }
    };
  }

  private Socket connect(InetSocketAddress address) throws IOException {
    Socket client = new Socket(address.getAddress(), address.getPort());
    clients.add(client);
    return client;
  }

  private static void assertClosedByNode(Socket client) throws IOException {
    if (!isClosedByNode(client, CLIENT_READ_TIMEOUT_MILLIS)) {
      fail("the node still holds the stalled socket open after " + CLIENT_READ_TIMEOUT_MILLIS + " ms");
    }
  }

  private static boolean isClosedByNode(Socket client, int waitMillis) throws IOException {
    client.setSoTimeout(waitMillis);
    InputStream in = client.getInputStream();
    try {
      return in.read() == -1;
    } catch (SocketTimeoutException stillOpen) {
      return false;
    } catch (IOException reset) {
      return true;
    }
  }

  private void awaitSelectorLoop() throws Exception {
    CompletableFuture<Void> looped = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> looped.complete(null));
    looped.get(10, TimeUnit.SECONDS);
  }
}
