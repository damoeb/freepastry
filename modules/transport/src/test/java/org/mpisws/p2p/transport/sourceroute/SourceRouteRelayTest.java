package org.mpisws.p2p.transport.sourceroute;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.sourceroute.factory.WireSourceRouteFactory;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * An intermediate hop must not relay a source route to an arbitrary address that the route names: here a plain UDP or
 * TCP listener that is not a node at all.
 */
class SourceRouteRelayTest {
  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
  private static final int WAIT_MILLIS = 1500;
  private static final byte[] PAYLOAD = {'r', 'e', 'l', 'a', 'y'};

  private Environment env;
  private WireTransportLayer wire;
  private InetSocketAddress hop;
  private DatagramSocket udpTarget;
  private ServerSocket tcpTarget;
  private DatagramSocket attacker;

  @BeforeEach
  void setUp() throws IOException {
    env = new Environment();
    udpTarget = new DatagramSocket(new InetSocketAddress(LOOPBACK, 0));
    udpTarget.setSoTimeout(WAIT_MILLIS);
    tcpTarget = new ServerSocket(0, 1, LOOPBACK);
    tcpTarget.setSoTimeout(WAIT_MILLIS);
    attacker = new DatagramSocket(new InetSocketAddress(LOOPBACK, 0));
    try (DatagramSocket probe = new DatagramSocket(new InetSocketAddress(LOOPBACK, 0))) {
      hop = new InetSocketAddress(LOOPBACK, probe.getLocalPort());
    }
  }

  @AfterEach
  void tearDown() {
    attacker.close();
    udpTarget.close();
    try {
      tcpTarget.close();
    } catch (IOException ignored) {
      // closing
    }
    env.destroy();
  }

  private void startHop(@Nullable SourceRouteForwardStrategy<InetSocketAddress> strategy) throws IOException {
    wire = new WireTransportLayer(hop, env, null);
    SourceRouteTransportLayer<InetSocketAddress> layer =
        new SourceRouteTransportLayer<>(new WireSourceRouteFactory(), wire, strategy, env, null);
    layer.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(SourceRoute<InetSocketAddress> i, ByteBuffer m, @Nullable Map<String, Object> o) {}

      @Override
      public void incomingSocket(P2PSocket<SourceRoute<InetSocketAddress>> s) {}
    });
  }

  private byte[] route(InetSocketAddress target) throws IOException {
    InetSocketAddress source = (InetSocketAddress) attacker.getLocalSocketAddress();
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    new WireSourceRouteFactory().getSourceRoute(List.of(source, hop, target)).serialize(out);
    return java.util.Arrays.copyOf(out.getBytes(), out.getWritten());
  }

  /** Sends a source-routed datagram through the hop and returns what reached the UDP target, or null. */
  private byte @Nullable [] relayDatagram() throws IOException {
    byte[] header = route((InetSocketAddress) udpTarget.getLocalSocketAddress());
    byte[] datagram = new byte[header.length + PAYLOAD.length];
    System.arraycopy(header, 0, datagram, 0, header.length);
    System.arraycopy(PAYLOAD, 0, datagram, header.length, PAYLOAD.length);
    attacker.send(new DatagramPacket(datagram, datagram.length, hop));
    DatagramPacket received = new DatagramPacket(new byte[1024], 1024);
    try {
      udpTarget.receive(received);
    } catch (SocketTimeoutException refused) {
      return null;
    }
    return java.util.Arrays.copyOf(received.getData(), received.getLength());
  }

  /** Opens a source-routed socket through the hop and returns the connection that reached the TCP target, or null. */
  private @Nullable Socket relaySocket() throws IOException {
    try (Socket s = new Socket(LOOPBACK, hop.getPort())) {
      OutputStream out = s.getOutputStream();
      out.write(route((InetSocketAddress) tcpTarget.getLocalSocketAddress()));
      out.flush();
      try {
        return tcpTarget.accept();
      } catch (SocketTimeoutException refused) {
        return null;
      }
    }
  }

  @Test
  void aHopWithTheDefaultStrategyDoesNotRelayADatagramToANonPeer() throws IOException {
    startHop(null);

    assertNull(relayDatagram(), "the hop relayed a datagram to a plain UDP listener");
  }

  @Test
  void aHopWithTheDefaultStrategyDoesNotRelayASocketToANonPeer() throws IOException {
    startHop(null);

    try (Socket relayed = relaySocket()) {
      assertNull(relayed, "the hop opened a relayed connection to a plain TCP listener");
    }
  }

  @Test
  void aHopAsksItsStrategyBeforeRelayingADatagram() throws IOException {
    startHop((nextHop, sr, socket, options) -> false);

    assertNull(relayDatagram(), "the hop relayed a datagram although its strategy refused the next hop");
  }
}
