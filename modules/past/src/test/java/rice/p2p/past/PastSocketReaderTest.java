package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;

@DisplayName("PastSocketReader")
class PastSocketReaderTest {

  private static final int MAX_BYTES = 1024;

  /** A socket that hands out the queued chunks one per read, then reports the end of the stream. */
  private static final class ChunkedSocket implements AppSocket {
    private final Deque<byte[]> chunks = new ArrayDeque<>();

    ChunkedSocket(byte[]... chunks) {
      for (byte[] chunk : chunks) {
        this.chunks.add(chunk);
      }
    }

    @Override
    public long read(ByteBuffer dst) {
      byte[] chunk = chunks.poll();
      if (chunk == null) {
        return -1;
      }
      dst.put(chunk);
      return chunk.length;
    }

    @Override
    public long write(ByteBuffer src) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, int timeout, AppSocketReceiver receiver) {}

    @Override
    public void shutdownOutput() {}

    @Override
    public void close() {}
  }

  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (byte) values[i];
    }
    return result;
  }

  @Test
  @DisplayName("reads a message whose size header arrives in two pieces")
  void readsAHeaderInTwoPieces() throws IOException {
    ChunkedSocket socket = new ChunkedSocket(bytes(0, 0), bytes(0, 3, 7, 8, 9));
    PastSocketReader target = new PastSocketReader(MAX_BYTES);

    assertNull(target.read(socket));
    assertArrayEquals(bytes(7, 8, 9), target.read(socket));
  }

  @Test
  @DisplayName("reads a message whose header and body arrive together")
  void readsAWholeMessage() throws IOException {
    ChunkedSocket socket = new ChunkedSocket(bytes(0, 0, 0, 2, 5, 6));
    PastSocketReader target = new PastSocketReader(MAX_BYTES);

    assertArrayEquals(bytes(5, 6), target.read(socket));
  }

  @Test
  @DisplayName("rejects a size above the cap before allocating it")
  void rejectsASizeAboveTheCap() {
    ChunkedSocket socket = new ChunkedSocket(bytes(0x7f, 0xff, 0xff, 0xff));
    PastSocketReader target = new PastSocketReader(MAX_BYTES);

    assertThrows(IOException.class, () -> target.read(socket));
  }

  @Test
  @DisplayName("rejects a negative size")
  void rejectsANegativeSize() {
    ChunkedSocket socket = new ChunkedSocket(bytes(0xff, 0xff, 0xff, 0xff));
    PastSocketReader target = new PastSocketReader(MAX_BYTES);

    assertThrows(IOException.class, () -> target.read(socket));
  }

  @Test
  @DisplayName("accepts a size exactly at the cap")
  void acceptsASizeAtTheCap() throws IOException {
    ChunkedSocket socket = new ChunkedSocket(bytes(0, 0, 0x04, 0x00), new byte[MAX_BYTES]);
    PastSocketReader target = new PastSocketReader(MAX_BYTES);

    assertEquals(MAX_BYTES, target.read(socket).length);
  }

  @Test
  @DisplayName("reports the end of the stream")
  void reportsTheEndOfTheStream() {
    PastSocketReader target = new PastSocketReader(MAX_BYTES);

    assertThrows(EOFException.class, () -> target.read(new ChunkedSocket()));
  }
}
