package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.fuzz.PipeNetwork;
import org.slf4j.LoggerFactory;

/**
 * Sends big messages with a {@link BigMessageSender} and reads them with a {@link BigMessageReader} at the other end of
 * an in-memory socket: the header byte, the 4-byte sizes and the bodies of several messages in a row.
 */
class BigMessageWireRoundTripTest {
  private static final int MAX_MSG_SIZE = 4096;
  private static final int MAX_BIG_MSG_SIZE = 1024 * 1024;

  private static InetSocketAddress addr(int lastOctet) throws IOException {
    return new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, (byte) lastOctet}), 9000 + lastOctet);
  }

  private static byte[] payload(int length, int seed) {
    byte[] b = new byte[length];
    for (int i = 0; i < length; i++) {
      b[i] = (byte) (seed + i * 31);
    }
    return b;
  }

  @Test
  @DisplayName("Big messages of every size class arrive in order, each exactly as sent")
  void bigMessagesArriveInOrder() throws IOException {
    PipeNetwork net = new PipeNetwork();
    InetSocketAddress sender = addr(1);
    InetSocketAddress receiver = addr(2);
    List<byte[]> received = new ArrayList<>();
    List<InetSocketAddress> senders = new ArrayList<>();
    TransportLayerCallback<InetSocketAddress, ByteBuffer> upper = new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, Map<String, Object> options) {
        senders.add(i);
        byte[] bytes = new byte[m.remaining()];
        m.get(bytes);
        received.add(bytes);
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) {
      }
    };
    EntityManagerContext<InetSocketAddress> ctx = new EntityManagerContext<>(null, null, null, null,
        LoggerFactory.getLogger(BigMessageWireRoundTripTest.class), 10, 10, MAX_BIG_MSG_SIZE, List.of(),
        new TransportListeners<>(), () -> null, () -> upper, () -> false, id -> null);
    net.lower(receiver).setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, Map<String, Object> options) {
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(1);
        s.read(header);
        assertEquals(PriorityTransportLayerImpl.BIG_MSG_SOCKET_B, header.get(0));
        s.register(true, false, new BigMessageReader<>(ctx));
      }
    });
    int[] sizes = {1, 2, 255, 256, MAX_MSG_SIZE, MAX_MSG_SIZE + 1, 65_535, 65_536, 300_000, MAX_BIG_MSG_SIZE};
    BigMessageSender<InetSocketAddress> target = new BigMessageSender<>(receiver, net.lower(sender),
        LoggerFactory.getLogger(BigMessageWireRoundTripTest.class), new HashMap<>());
    for (int i = 0; i < sizes.length; i++) {
      target.addMessage(ByteBuffer.wrap(payload(sizes[i], i)), null, null);
    }

    target.start(null);
    net.run();

    assertEquals(List.of(), net.failures());
    assertEquals(sizes.length, received.size());
    for (int i = 0; i < sizes.length; i++) {
      assertArrayEquals(payload(sizes[i], i), received.get(i), "message " + i);
      assertEquals(sender.getAddress(), senders.get(i).getAddress());
    }
  }
}
