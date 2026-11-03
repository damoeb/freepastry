package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.TransportLayerListener;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.slf4j.LoggerFactory;

class BigMessageReaderTest {
  private static final int MAX = 1024;

  private final List<ByteBuffer> delivered = new ArrayList<>();
  private final TransportListeners<String> listeners = new TransportListeners<>();

  private BigMessageReader<String> reader() {
    TransportLayerCallback<String, ByteBuffer> callback = new TransportLayerCallback<>() {
      @Override
      public void messageReceived(String i, ByteBuffer m, Map<String, Object> options) {
        delivered.add(m);
      }

      @Override
      public void incomingSocket(P2PSocket<String> s) {}
    };
    EntityManagerContext<String> ctx = new EntityManagerContext<>(null, null, null, null,
        LoggerFactory.getLogger(BigMessageReaderTest.class), 10, 10, MAX, List.of(), listeners,
        () -> null, () -> callback, () -> false, id -> null);
    return new BigMessageReader<>(ctx);
  }

  private static byte[] size(int size) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(size).array();
  }

  @Test
  void aSizeAboveTheMaximumClosesTheSocketWithoutAllocating() throws Exception {
    BigMessageReader<String> target = reader();
    FakeSocket socket = new FakeSocket(size(Integer.MAX_VALUE));

    target.receiveSelectResult(socket, true, false);

    assertTrue(socket.closed);
    assertNull(target.msgBuf);
  }

  @Test
  void aNegativeSizeClosesTheSocket() throws Exception {
    BigMessageReader<String> target = reader();
    FakeSocket socket = new FakeSocket(size(-1));

    target.receiveSelectResult(socket, true, false);

    assertTrue(socket.closed);
    assertNull(target.msgBuf);
  }

  @Test
  void aSizeOfZeroClosesTheSocket() throws Exception {
    BigMessageReader<String> target = reader();
    FakeSocket socket = new FakeSocket(size(0));

    target.receiveSelectResult(socket, true, false);

    assertTrue(socket.closed);
  }

  @Test
  void aMessageWithinTheMaximumIsDelivered() throws Exception {
    BigMessageReader<String> target = reader();
    byte[] body = {1, 2, 3};
    ByteBuffer wire = ByteBuffer.allocate(Integer.BYTES + body.length).putInt(body.length).put(body);
    FakeSocket socket = new FakeSocket(wire.array());

    target.receiveSelectResult(socket, true, false);

    assertEquals(1, delivered.size());
    assertEquals(3, delivered.get(0).remaining());
    assertFalse(socket.closed);
  }

  @Test
  @DisplayName("a delivered BIG message is reported to the transport layer listeners")
  void aDeliveredMessageNotifiesListeners() throws Exception {
    List<Integer> reads = new ArrayList<>();
    listeners.addTransportLayerListener(new TransportLayerListener<>() {
      @Override
      public void wrote(int bytes, String i, Map<String, Object> options, boolean passthrough, boolean socket) {}

      @Override
      public void read(int bytes, String i, Map<String, Object> options, boolean passthrough, boolean socket) {
        reads.add(bytes);
      }
    });
    BigMessageReader<String> target = reader();
    byte[] body = {1, 2, 3};
    ByteBuffer wire = ByteBuffer.allocate(Integer.BYTES + body.length).putInt(body.length).put(body);

    target.receiveSelectResult(new FakeSocket(wire.array()), true, false);

    assertEquals(List.of(3), reads);
  }
}
