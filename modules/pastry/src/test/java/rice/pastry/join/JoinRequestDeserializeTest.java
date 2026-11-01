package rice.pastry.join;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

class JoinRequestDeserializeTest {

  private static SimpleInputBuffer joinRequest(int rtBaseBitLength) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeByte((byte) rtBaseBitLength);
    return new SimpleInputBuffer(out.getBytes());
  }

  @Test
  void rejectsARoutingTableBaseOfZeroBits() throws IOException {
    SimpleInputBuffer buf = joinRequest(0);

    assertThrows(IOException.class, () -> new JoinRequest(buf, null, null, null));
  }

  @Test
  void rejectsARoutingTableBaseWithBillionsOfColumns() throws IOException {
    SimpleInputBuffer buf = joinRequest(30);

    assertThrows(IOException.class, () -> new JoinRequest(buf, null, null, null));
  }

  @Test
  void rejectsANegativeRoutingTableBase() throws IOException {
    SimpleInputBuffer buf = joinRequest(-3);

    assertThrows(IOException.class, () -> new JoinRequest(buf, null, null, null));
  }
}
