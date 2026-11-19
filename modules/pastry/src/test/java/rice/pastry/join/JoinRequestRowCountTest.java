package rice.pastry.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.routing.RouteSet;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

/**
 * The row count of a join request says how many routing table rows are still to fill: every hop pushes rows below it,
 * and the joiner broadcasts the rows from it on. A count outside the rows of the request indexes past them.
 */
class JoinRequestRowCountTest {
  private static final byte BASE = 4;
  private static final int NUM_ROWS = Id.IdBitLength / BASE;
  /** version, time stamp, base: the bytes before the joiner's handle. */
  private static final int HEADER_BYTES = 1 + Long.BYTES + 1;

  private static Environment env;
  private static SocketNode local;
  private static NodeHandle joiner;

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    local = SocketNode.create(env, "4000");
    joiner = local.handle("1000");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  /** A fresh join request with its row count replaced. */
  private static SimpleInputBuffer withRowCount(int rowCount) throws IOException {
    byte[] bytes = WireRoundTrip.bytes(new JoinRequest(joiner, BASE, 5L));
    int at = HEADER_BYTES + WireRoundTrip.bytes(joiner, NodeHandle::serialize).length + 1; // after "no join handle"
    bytes[at] = (byte) (rowCount >> 8);
    bytes[at + 1] = (byte) rowCount;
    return new SimpleInputBuffer(bytes);
  }

  private static JoinRequest read(SimpleInputBuffer buf) throws IOException {
    return new JoinRequest(buf, local.node, joiner, local.node);
  }

  @ParameterizedTest(name = "row count {0}")
  @ValueSource(ints = {-1, Short.MIN_VALUE, NUM_ROWS + 1, Short.MAX_VALUE})
  @DisplayName("a row count outside 0 to the number of rows is rejected with an IOException")
  void rejectsARowCountOutsideTheRows(int rowCount) throws IOException {
    SimpleInputBuffer buf = withRowCount(rowCount);

    assertThrows(IOException.class, () -> read(buf).pushRow(new RouteSet[1 << BASE]));
  }

  @ParameterizedTest(name = "row count {0}")
  @ValueSource(ints = {0, 1, NUM_ROWS})
  @DisplayName("a row count from 0 to the number of rows is read")
  void readsARowCountInsideTheRows(int rowCount) throws IOException {
    assertEquals(rowCount, read(withRowCount(rowCount)).lastRow());
  }
}
