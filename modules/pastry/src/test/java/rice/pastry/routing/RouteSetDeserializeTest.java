package rice.pastry.routing;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

class RouteSetDeserializeTest {

  private static SimpleInputBuffer routeSet(int maxSize, int size) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) maxSize);
    out.writeByte((byte) size);
    out.writeByte((byte) 0);
    return new SimpleInputBuffer(out.getBytes());
  }

  @Test
  void rejectsMoreNodesThanTheSetHolds() throws IOException {
    SimpleInputBuffer buf = routeSet(1, 100);

    assertThrows(IOException.class, () -> new RouteSet(buf, null, null));
  }

  @Test
  void rejectsANegativeMaxSize() throws IOException {
    SimpleInputBuffer buf = routeSet(-1, 0);

    assertThrows(IOException.class, () -> new RouteSet(buf, null, null));
  }

  @Test
  void rejectsANegativeSize() throws IOException {
    SimpleInputBuffer buf = routeSet(4, -1);

    assertThrows(IOException.class, () -> new RouteSet(buf, null, null));
  }
}
