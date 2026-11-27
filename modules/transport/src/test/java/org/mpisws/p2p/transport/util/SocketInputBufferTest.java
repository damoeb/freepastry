package org.mpisws.p2p.transport.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;

class SocketInputBufferTest {

  /** A socket that hands out the bytes that have arrived so far. */
  static final class TrickleSocket implements P2PSocket<InetSocketAddress> {
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
      int byteCount = Math.min(dst.remaining(), arrived.remaining());
      for (int i = 0; i < byteCount; i++) {
        dst.put(arrived.get());
      }
      return byteCount;
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

  @Test
  void aBulkReadWithTooFewBytesThrowsAndRewinds() throws Exception {
    TrickleSocket socket = new TrickleSocket();
    SocketInputBuffer sib = new SocketInputBuffer(socket);
    socket.arrive((byte) 1, (byte) 2);

    assertThrows(InsufficientBytesException.class, () -> sib.read(new byte[4]));
    socket.arrive((byte) 3, (byte) 4);

    byte[] actual = new byte[4];
    assertEquals(4, sib.read(actual));
    assertArrayEquals(new byte[] {1, 2, 3, 4}, actual);
  }

  @Test
  @Timeout(10)
  void aReadLargerThanTheInitialCacheGrowsIt() throws Exception {
    TrickleSocket socket = new TrickleSocket();
    SocketInputBuffer sib = new SocketInputBuffer(socket, 4);
    byte[] data = new byte[40];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }
    socket.arrive(data);

    byte[] actual = new byte[40];
    assertEquals(40, sib.read(actual));
    assertArrayEquals(data, actual);
  }

  @Test
  void aRangedBulkReadWithTooFewBytesThrows() {
    TrickleSocket socket = new TrickleSocket();
    SocketInputBuffer sib = new SocketInputBuffer(socket);
    socket.arrive((byte) 1);

    assertThrows(InsufficientBytesException.class, () -> sib.read(new byte[4], 1, 3));
  }
}
