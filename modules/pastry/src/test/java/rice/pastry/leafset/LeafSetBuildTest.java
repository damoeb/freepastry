package rice.pastry.leafset;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.PastryNode;
import rice.pastry.testing.StubHandleNodes;

class LeafSetBuildTest {
  private Environment env;
  private PastryNode node;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    node = StubHandleNodes.newNode(env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /** A leafset with one unique handle whose clockwise side holds a single entry pointing at {@code index}. */
  private SimpleInputBuffer leafSet(int size, int numUnique, int cwSize, int index) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) size);
    out.writeByte((byte) numUnique);
    out.writeByte((byte) cwSize);
    out.writeByte((byte) 0);
    out.writeByte((byte) 0); // a node handle
    out.writeByte((byte) 0); // a node handle
    out.writeByte((byte) index);
    return new SimpleInputBuffer(out.getBytes());
  }

  @Test
  void rejectsAHandleIndexBeyondTheTable() throws IOException {
    SimpleInputBuffer buf = leafSet(24, 1, 1, 5);

    assertThrows(IOException.class, () -> LeafSet.build(buf, node));
  }

  @Test
  void rejectsANegativeHandleIndex() throws IOException {
    SimpleInputBuffer buf = leafSet(24, 1, 1, -1);

    assertThrows(IOException.class, () -> LeafSet.build(buf, node));
  }

  @Test
  void rejectsANegativeSize() throws IOException {
    SimpleInputBuffer buf = leafSet(-2, 1, 1, 0);

    assertThrows(IOException.class, () -> LeafSet.build(buf, node));
  }

  @Test
  void rejectsANegativeCount() throws IOException {
    SimpleInputBuffer buf = leafSet(24, -1, 1, 0);

    assertThrows(IOException.class, () -> LeafSet.build(buf, node));
  }
}
