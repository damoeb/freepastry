package org.mpisws.p2p.transport.peerreview.audit;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

class SnippetEntryTest {
  private static final byte SIZE_CODE_INT = (byte) 0xFE;
  private static final int HASH_SIZE = 20;

  private static SimpleInputBuffer entry(int size) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 1); // type
    out.writeByte(SIZE_CODE_INT);
    out.writeInt(size);
    return new SimpleInputBuffer(out.getBytes());
  }

  @Test
  void rejectsAContentLongerThanTheMessage() throws IOException {
    SimpleInputBuffer buf = entry(Integer.MAX_VALUE);

    assertThrows(IOException.class, () -> new SnippetEntry(buf, 0, HASH_SIZE));
  }

  @Test
  void rejectsANegativeContentLength() throws IOException {
    SimpleInputBuffer buf = entry(-1);

    assertThrows(IOException.class, () -> new SnippetEntry(buf, 0, HASH_SIZE));
  }
}
