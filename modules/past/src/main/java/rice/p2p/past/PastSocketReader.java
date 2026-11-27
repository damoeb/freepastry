package rice.p2p.past;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.WeakHashMap;
import org.jspecify.annotations.Nullable;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.util.MathUtils;

/**
 * Reassembles the size-prefixed messages that Past sends over an {@link AppSocket}: a 4-byte size, then that many bytes.
 * Remembers, per socket, how far the current message has arrived. The buffer grows with the bytes that arrive (doubling
 * from {@link #INITIAL_CHUNK_BYTES}), not with the announced size, so sockets that announce big messages and send
 * nothing hold little memory (design 3e).
 */
final class PastSocketReader {
  /** the buffer of a message before any of it has arrived */
  static final int INITIAL_CHUNK_BYTES = 8 * 1024;

  /** The largest message accepted, in bytes. */
  final int maxMessageBytes;

  /** Per socket: the buffer for the size header, then the buffer for the message once the size is known. */
  private static final class Pending {
    final ByteBuffer header = ByteBuffer.allocate(Integer.BYTES);
    /** the announced size of the message */
    int size;
    /** the array behind {@link #body} */
    byte @Nullable [] bytes;
    @Nullable ByteBuffer body;
  }

  private final WeakHashMap<AppSocket, Pending> pendingSocketTransactions = new WeakHashMap<>();

  PastSocketReader(int maxMessageBytes) {
    this.maxMessageBytes = maxMessageBytes;
  }

  /**
   * Reads what the socket has ready, for the message in progress.
   *
   * @return the complete message, or {@code null} if more bytes are still to come
   * @throws EOFException if the peer closed the socket
   * @throws IOException if the announced size is negative or above the cap
   */
  byte @Nullable [] read(AppSocket socket) throws IOException {
    Pending pending = pendingSocketTransactions.computeIfAbsent(socket, key -> new Pending());
    if (pending.body == null) {
      if (socket.read(pending.header) == -1) {
        throw new EOFException();
      }
      if (pending.header.hasRemaining()) {
        return null; // a partial header, the rest follows
      }
      int size = MathUtils.byteArrayToInt(pending.header.array());
      if (size < 0 || size > maxMessageBytes) {
        throw new IOException("Past message of " + size + " bytes announced, at most " + maxMessageBytes + " are accepted");
      }
      pending.size = size;
      pending.bytes = new byte[Math.min(size, INITIAL_CHUNK_BYTES)];
      pending.body = ByteBuffer.wrap(pending.bytes);
    }

    ByteBuffer body = pending.body;
    while (true) {
      if (socket.read(body) == -1) {
        throw new EOFException();
      }
      if (body.hasRemaining()) {
        return null; // more is to come
      }
      if (body.capacity() == pending.size) {
        // make sure to clear things up so we can keep receiving
        pendingSocketTransactions.remove(socket);
        return pending.bytes;
      }
      // the buffer is full and more is announced: grow it
      byte[] biggerBytes = new byte[(int) Math.min((long) pending.size, 2L * body.capacity())];
      ByteBuffer bigger = ByteBuffer.wrap(biggerBytes);
      body.flip();
      bigger.put(body);
      body = bigger;
      pending.bytes = biggerBytes;
      pending.body = bigger;
    }
  }

  /** The bytes the buffers of the messages in progress hold, over all sockets. */
  long heldBytes() {
    long held = 0;
    for (Pending pending : pendingSocketTransactions.values()) {
      ByteBuffer body = pending.body;
      if (body != null) held += body.capacity();
    }
    return held;
  }

  /** Drops what has arrived of the message in progress on the socket. */
  void forget(AppSocket socket) {
    pendingSocketTransactions.remove(socket);
  }
}
