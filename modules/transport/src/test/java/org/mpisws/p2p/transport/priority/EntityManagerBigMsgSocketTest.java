package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class EntityManagerBigMsgSocketTest {

  @Test
  void aBigMessageSocketThatFailsToReadIsClosed() {
    PeerMessageWriterContext<String> ctx = new PeerMessageWriterContext<>(null, null, null, null,
        LoggerFactory.getLogger(EntityManagerBigMsgSocketTest.class), 10, 10, 1024, List.of(),
        new TransportListeners<>(), () -> null, () -> null, () -> false, id -> null);
    PeerMessageWriter<String> target = new PeerMessageWriter<>("peer", ctx);
    FakeSocket socket = new FakeSocket();
    socket.readFailure = new IOException("connection reset");

    target.handleBigMsgSocket(socket);

    assertTrue(socket.closed);
  }
}
