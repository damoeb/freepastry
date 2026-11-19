package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.slf4j.LoggerFactory;

class IncomingSocketDispatcherTest {

  private boolean passedOn;

  private IncomingSocketDispatcher<String> dispatcher(FakeSocket socket) {
    TransportLayerCallback<String, ByteBuffer> callback = new TransportLayerCallback<>() {
      @Override
      public void messageReceived(String i, ByteBuffer msg, Map<String, Object> options) {}

      @Override
      public void incomingSocket(P2PSocket<String> s) {
        passedOn = true;
      }
    };
    PeerMessageWriterContext<String> ctx = new PeerMessageWriterContext<>(null, null, null, null,
        LoggerFactory.getLogger(IncomingSocketDispatcherTest.class), 10, 10, 1024, List.of(),
        new TransportListeners<>(), () -> null, () -> callback, () -> false, id -> null);
    return new IncomingSocketDispatcher<>(socket, ctx);
  }

  @Test
  void anUnknownSocketHeaderClosesTheSocket() throws Exception {
    FakeSocket socket = new FakeSocket((byte) 99);

    dispatcher(socket).receiveSelectResult(socket, true, false);

    assertTrue(socket.closed);
    assertFalse(passedOn);
  }

  @Test
  void aPassthroughHeaderIsHandedToTheLayerAbove() throws Exception {
    FakeSocket socket = new FakeSocket(PriorityTransportLayer.PASSTHROUGH_SOCKET_B);

    dispatcher(socket).receiveSelectResult(socket, true, false);

    assertFalse(socket.closed);
    assertTrue(passedOn);
  }
}
