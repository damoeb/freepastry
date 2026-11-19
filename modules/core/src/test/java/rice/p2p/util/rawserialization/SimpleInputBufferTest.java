package rice.p2p.util.rawserialization;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SimpleInputBufferTest {

  @Test
  void readsAWrappedBufferFromItsPosition() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
    buffer.position(1);

    SimpleInputBuffer buf = new SimpleInputBuffer(buffer);

    assertEquals(2, buf.readByte());
    assertEquals(2, buf.bytesRemaining());
  }

  @Test
  void readsASlicedBufferFromItsOwnStart() throws IOException {
    ByteBuffer slice = ByteBuffer.wrap(new byte[] {1, 2, 3, 4}, 2, 2).slice();

    SimpleInputBuffer buf = new SimpleInputBuffer(slice);

    assertEquals(3, buf.readByte());
    assertEquals(4, buf.readByte());
    assertEquals(0, buf.bytesRemaining());
  }
}
