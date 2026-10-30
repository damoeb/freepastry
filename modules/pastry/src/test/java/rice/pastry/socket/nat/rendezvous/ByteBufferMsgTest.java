package rice.pastry.socket.nat.rendezvous;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * ByteBufferMsg.serialize() used to read the backing array from the buffer's position without its array offset, so a
 * slice of a larger array wrote the wrong bytes.
 */
class ByteBufferMsgTest {

  private static final byte VERSION = 0;
  private static final int PRIORITY = 0;
  private static final int ADDRESS = 1;

  @Test
  void serializeWritesTheBytesOfAWrappedBuffer() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});

    assertArrayEquals(new byte[] {VERSION, 0, 0, 0, 3, 1, 2, 3}, serialize(buffer));
  }

  @Test
  void serializeWritesTheBytesOfASlice() throws IOException {
    ByteBuffer slice = ByteBuffer.wrap(new byte[] {9, 9, 1, 2, 3}, 2, 3).slice();

    assertArrayEquals(new byte[] {VERSION, 0, 0, 0, 3, 1, 2, 3}, serialize(slice));
  }

  private static byte[] serialize(ByteBuffer buffer) throws IOException {
    ByteBufferMsg msg = new ByteBufferMsg(buffer, new EmptyNodeHandle(), PRIORITY, ADDRESS);
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    msg.serialize(out);
    return Arrays.copyOf(out.getBytes(), out.getWritten());
  }

  /** A sender that writes nothing, so the output holds only the message's own fields. */
  private static final class EmptyNodeHandle extends NodeHandle {
    @Override
    public Id getId() {
      return null;
    }

    @Override
    public boolean checkLiveness() {
      return true;
    }

    @Override
    public void serialize(OutputBuffer buf) {
    }
  }
}
