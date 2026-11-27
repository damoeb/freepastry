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
 * Remembers, per socket, how far the current message has arrived.
 */
final class PastSocketReader {

  /** The largest message accepted, in bytes. */
  final int maxMessageBytes;

  /** Per socket: the buffer for the size header, then the buffer for the message once the size is known. */
  private static final class Pending {
    final ByteBuffer header = ByteBuffer.allocate(Integer.BYTES);
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
      pending.bytes = new byte[size];
      pending.body = ByteBuffer.wrap(pending.bytes);
    }

    ByteBuffer body = pending.body;
    byte[] bytes = pending.bytes;
    if (socket.read(body) == -1) {
      throw new EOFException();
    }

    if (body.remaining() == 0) {
      // make sure to clear things up so we can keep receiving
      pendingSocketTransactions.remove(socket);
      return bytes;
    }
    return null;
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
