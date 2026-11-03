package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.LoggerFactory;

class PassthroughSocketOpenerTest {

  /** Accepts the scripted number of bytes per write call, or -1 for a closed channel. */
  private static final class ScriptedSocket implements P2PSocket<String> {
    private final List<Integer> script;
    P2PSocketReceiver<String> registered;
    boolean closed;

    ScriptedSocket(Integer... script) {
      this.script = new ArrayList<>(List.of(script));
    }

    @Override
    public String getIdentifier() {
      return "peer";
    }

    @Override
    public Map<String, Object> getOptions() {
      return Map.of();
    }

    @Override
    public long read(ByteBuffer dst) {
      return 0;
    }

    @Override
    public long write(ByteBuffer src) {
      int n = script.removeFirst();
      if (n > 0) {
        src.position(src.position() + n);
      }
      return n;
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<String> receiver) {
      registered = receiver;
    }

    @Override
    public void shutdownOutput() {}

    @Override
    public void close() {
      closed = true;
    }
  }

  private final List<String> events = new ArrayList<>();

  private PassthroughSocketOpener<String> opener() {
    SocketRequestHandleImpl<String> handle =
        new SocketRequestHandleImpl<>("peer", null, LoggerFactory.getLogger(PassthroughSocketOpenerTest.class));
    return new PassthroughSocketOpener<>(handle, new SocketCallback<>() {
      @Override
      public void receiveResult(SocketRequestHandle<String> cancellable, P2PSocket<String> sock) {
        events.add("result");
      }

      @Override
      public void receiveException(SocketRequestHandle<String> s, Exception ex) {
        events.add("exception");
      }
    });
  }

  @Test
  @DisplayName("a short header write is retried before the socket is handed up")
  void aShortWriteIsRetried() throws IOException {
    ScriptedSocket socket = new ScriptedSocket(0, 1);
    opener().receiveResult(null, socket);

    socket.registered.receiveSelectResult(socket, false, true);
    assertTrue(events.isEmpty());

    socket.registered.receiveSelectResult(socket, false, true);
    assertEquals(List.of("result"), events);
  }

  @Test
  @DisplayName("a closed channel while writing the header fails the open")
  void aClosedChannelFailsTheOpen() throws IOException {
    ScriptedSocket socket = new ScriptedSocket(-1);
    opener().receiveResult(null, socket);

    socket.registered.receiveSelectResult(socket, false, true);

    assertEquals(List.of("exception"), events);
    assertTrue(socket.closed);
  }
}
