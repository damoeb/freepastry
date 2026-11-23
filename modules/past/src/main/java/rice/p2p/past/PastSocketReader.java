package rice.p2p.past;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.WeakHashMap;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.util.MathUtils;

/**
 * Reassembles the size-prefixed messages that Past sends over an {@link AppSocket}: a 4-byte size, then that many bytes.
 * Remembers, per socket, how far the current message has arrived.
 */
final class PastSocketReader {

  /** The largest message accepted, in bytes. */
  final int maxMessageBytes;

  private final WeakHashMap<AppSocket, ByteBuffer[]> pendingSocketTransactions = new WeakHashMap<>();

  PastSocketReader(int maxMessageBytes) {
    this.maxMessageBytes = maxMessageBytes;
  }

  /**
   * Reads what the socket has ready, for the message in progress.
   *
   * @return the complete message, or {@code null} if more bytes are still to come
   * @throws EOFException if the peer closed the socket
   */
  byte[] read(AppSocket socket) throws IOException {
    ByteBuffer[] buffers = pendingSocketTransactions.get(socket);
    if (buffers == null) {
      // this is a new message

      // read the size
      buffers = new ByteBuffer[1];
      buffers[0] = ByteBuffer.allocate(Integer.BYTES);
      if (socket.read(buffers[0]) == -1) {
        throw new EOFException();
      }

      // TODO: handle a partial read of the 4-byte size header
      byte[] sizeBytes = buffers[0].array();
      int size = MathUtils.byteArrayToInt(sizeBytes);

      // allocate a buffer to store the object, save it in the pst
      buffers[0] = ByteBuffer.allocate(size);
      pendingSocketTransactions.put(socket, buffers);
    }

    // now we have a bb

    // read some bytes
    if (socket.read(buffers[0]) == -1) {
      throw new EOFException();
    }

    if (buffers[0].remaining() == 0) {
      // make sure to clear things up so we can keep receiving
      pendingSocketTransactions.remove(socket);
      return buffers[0].array();
    }
    return null;
  }

  /** Drops what has arrived of the message in progress on the socket. */
  void forget(AppSocket socket) {
    pendingSocketTransactions.remove(socket);
  }
}
