package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;

@DisplayName("PastSocketReader")
class PastSocketReaderTest {

  private static final int MAX_BYTES = 1024;

  /** A socket that hands out the queued chunks, at most what fits per read, then reports the end of the stream. */
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
      int count = Math.min(chunk.length, dst.remaining());
      dst.put(chunk, 0, count);
      if (count < chunk.length) {
        chunks.addFirst(Arrays.copyOfRange(chunk, count, chunk.length));
      }
      return count;
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

  /** A socket that hands out the bytes it was given, then has nothing more to read (it stays open). */
  private static final class IdleSocket implements AppSocket {
    private final ByteBuffer incoming;

    IdleSocket(byte[] incoming) {
      this.incoming = ByteBuffer.wrap(incoming);
    }

    @Override
    public long read(ByteBuffer dst) {
      int count = Math.min(dst.remaining(), incoming.remaining());
      for (int i = 0; i < count; i++) {
        dst.put(incoming.get());
      }
      return count;
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

  @Test
  @DisplayName("C3: sockets that announce the largest message and send nothing do not make the reader allocate it")
  void announcingTheLargestMessageDoesNotAllocateIt() throws IOException {
    int max = 16 * 1024 * 1024;
    PastSocketReader target = new PastSocketReader(max);
    int sockets = 100;
    java.util.List<AppSocket> open = new java.util.ArrayList<>(); // the reader keeps sockets weakly
    for (int n = 0; n < sockets; n++) {
      AppSocket socket = new IdleSocket(ByteBuffer.allocate(Integer.BYTES).putInt(max).array());
      open.add(socket);
      assertNull(target.read(socket));
    }

    assertTrue(target.heldBytes() <= sockets * 8L * 1024, "held " + target.heldBytes() + " bytes for " + open.size() + " idle sockets");
  }

  @Test
  @DisplayName("a message bigger than the first chunk arrives whole, in pieces")
  void aMessageBiggerThanTheFirstChunkArrivesWhole() throws IOException {
    int size = 100_000;
    byte[] payload = new byte[size];
    for (int n = 0; n < size; n++) {
      payload[n] = (byte) n;
    }
    byte[] header = ByteBuffer.allocate(Integer.BYTES).putInt(size).array();
    ChunkedSocket socket = new ChunkedSocket(header, Arrays.copyOfRange(payload, 0, 30_000), Arrays.copyOfRange(payload, 30_000, size));
    PastSocketReader target = new PastSocketReader(size);

    assertNull(target.read(socket));
    assertArrayEquals(payload, target.read(socket));
  }
}
