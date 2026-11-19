package org.mpisws.p2p.transport.priority;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.fuzz.FuzzSocket;
import org.slf4j.LoggerFactory;

/**
 * Fuzzes the socket readers of the priority layer: the header that tells what an incoming socket is for, the size
 * prefix and body of a message on the primary socket, and the size and body of a big message on its own socket. The
 * invariant: nothing but an {@link java.io.IOException} escapes, and the socket is the only thing a bad peer can cost.
 */
class PriorityFuzzTest {
  private static final int MAX_MSG_SIZE = 4096;

  private static PeerMessageWriterContext<String> context() {
    TransportLayerCallback<String, ByteBuffer> callback = new TransportLayerCallback<>() {
      @Override
      public void messageReceived(String i, ByteBuffer msg, Map<String, Object> options) {
      }

      @Override
      public void incomingSocket(P2PSocket<String> socket) {
      }
    };
    return new PeerMessageWriterContext<>(null, null, null, null, LoggerFactory.getLogger(PriorityFuzzTest.class), 10, 10,
        MAX_MSG_SIZE, List.of(), new TransportListeners<>(), () -> null, () -> callback, () -> false, id -> null);
  }

  @FuzzTest(maxDuration = "30s")
  void bigMessageReader(FuzzedDataProvider data) {
    FuzzSocket<String> socket = new FuzzSocket<>("peer", data.consumeRemainingAsBytes());
    socket.register(true, false, new BigMessageReader<>(context()));
    socket.drive();
  }

  @FuzzTest(maxDuration = "30s")
  void sizeReader(FuzzedDataProvider data) {
    PeerMessageWriterContext<String> ctx = context();
    FuzzSocket<String> socket = new FuzzSocket<>("peer", data.consumeRemainingAsBytes());
    new SizeReader<>(new PeerMessageWriter<>("peer", ctx), socket);
    socket.drive();
  }

  @FuzzTest(maxDuration = "30s")
  void incomingSocketDispatcher(FuzzedDataProvider data) {
    byte[] bytes = data.consumeRemainingAsBytes();
    if (bytes.length > 0 && (bytes[0] == PriorityTransportLayer.PRIMARY_SOCKET_B
        || bytes[0] == PriorityTransportLayer.BIG_MSG_SOCKET_B)) {
      // these hand the socket to an PeerMessageWriter, which needs the selector thread; the readers are fuzzed on their own
      return;
    }
    FuzzSocket<String> socket = new FuzzSocket<>("peer", bytes);
    socket.register(true, false, new IncomingSocketDispatcher<>(socket, context()));
    socket.drive();
  }
}
