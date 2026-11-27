package rice.p2p.util.rawserialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import rice.p2p.util.BloomFilter;

class WireLengthsTest {

  private static SimpleInputBuffer buffer(int announced, int payload) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(announced);
    out.write(new byte[payload], 0, payload);
    return new SimpleInputBuffer(out.getBytes());
  }

  @Test
  void acceptsACountThatFits() throws IOException {
    assertEquals(3, WireLengths.readInt(buffer(3, 3), 1));
  }

  @Test
  void rejectsANegativeCount() throws IOException {
    SimpleInputBuffer buf = buffer(-1, 8);
    assertThrows(IOException.class, () -> WireLengths.readInt(buf, 1));
  }

  @Test
  void rejectsACountLargerThanTheBuffer() throws IOException {
    SimpleInputBuffer buf = buffer(Integer.MAX_VALUE, 8);
    assertThrows(IOException.class, () -> WireLengths.readInt(buf, 1));
  }

  @Test
  void accountsForTheElementSize() throws IOException {
    SimpleInputBuffer buf = buffer(3, 8);
    assertThrows(IOException.class, () -> WireLengths.readInt(buf, 4));
  }

  @Test
  void readsAnAnnouncedNumberOfBytes() throws IOException {
    assertEquals(4, WireLengths.readBytes(buffer(4, 6)).length);
  }

  @Test
  void rejectsAByteArrayLongerThanTheBuffer() throws IOException {
    SimpleInputBuffer buf = buffer(1 << 30, 6);
    assertThrows(IOException.class, () -> WireLengths.readBytes(buf));
  }

  @Test
  void bloomFilterRejectsAHugeParameterCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(10); // length
    out.writeInt(Integer.MAX_VALUE); // parameters
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());
    assertThrows(IOException.class, () -> new BloomFilter(buf));
  }

  @Test
  void bloomFilterRejectsMoreBitsThanBytes() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(10); // length
    out.writeInt(0); // parameters
    out.writeInt(Integer.MAX_VALUE); // bits
    out.writeByte((byte) 0);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());
    assertThrows(IOException.class, () -> new BloomFilter(buf));
  }
}
