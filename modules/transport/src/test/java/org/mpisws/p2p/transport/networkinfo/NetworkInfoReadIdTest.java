package org.mpisws.p2p.transport.networkinfo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.util.InsufficientBytesException;
import org.mpisws.p2p.transport.util.SocketInputBuffer;

class NetworkInfoReadIdTest {

  /** A socket that hands out the bytes that have arrived so far. */
  private static final class TrickleSocket implements P2PSocket<InetSocketAddress> {
    private ByteBuffer arrived = ByteBuffer.allocate(0);

    void arrive(byte... bytes) {
      ByteBuffer next = ByteBuffer.allocate(arrived.remaining() + bytes.length);
      next.put(arrived).put(bytes).flip();
      arrived = next;
    }

    @Override
    public InetSocketAddress getIdentifier() {
      return InetSocketAddress.createUnresolved("peer", 1);
    }

    @Override
    public long read(ByteBuffer dst) {
      int n = Math.min(dst.remaining(), arrived.remaining());
      for (int i = 0; i < n; i++) {
        dst.put(arrived.get());
      }
      return n;
    }

    @Override
    public long write(ByteBuffer src) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<InetSocketAddress> receiver) {
    }

    @Override
    public void shutdownOutput() {
    }

    @Override
    public void close() {
    }

    @Override
    public Map<String, Object> getOptions() {
      return Map.of();
    }
  }

  private static byte[] size(int size) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(size).array();
  }

  @Test
  void anIdLargerThanTheMaximumIsRejected() {
    TrickleSocket socket = new TrickleSocket();
    socket.arrive(size(NetworkInfoTransportLayer.MAX_ID_SIZE + 1));

    assertThrows(java.io.IOException.class, () -> NetworkInfoTransportLayer.readId(new SocketInputBuffer(socket)));
  }

  @Test
  void aNegativeIdSizeIsRejected() {
    TrickleSocket socket = new TrickleSocket();
    socket.arrive(size(-1));

    assertThrows(java.io.IOException.class, () -> NetworkInfoTransportLayer.readId(new SocketInputBuffer(socket)));
  }

  @Test
  void anIdThatArrivesInPiecesIsReadWhole() throws Exception {
    TrickleSocket socket = new TrickleSocket();
    SocketInputBuffer sib = new SocketInputBuffer(socket);
    socket.arrive(size(4));
    socket.arrive((byte) 1, (byte) 2);

    assertThrows(InsufficientBytesException.class, () -> NetworkInfoTransportLayer.readId(sib));
    socket.arrive((byte) 3, (byte) 4);

    assertArrayEquals(new byte[] {1, 2, 3, 4}, NetworkInfoTransportLayer.readId(sib));
  }
}
