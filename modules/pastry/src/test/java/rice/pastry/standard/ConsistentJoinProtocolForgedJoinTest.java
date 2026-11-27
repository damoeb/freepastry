package rice.pastry.standard;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.pastry.NodeHandle;
import rice.pastry.join.JoinRequest;
import rice.pastry.testing.WireRoundTrip.SocketNode;

/**
 * The rows of an accepted join request come from the hops on the path, and the joiner broadcasts them: a row that a
 * hop did not have (or a peer left out) is missing.
 */
class ConsistentJoinProtocolForgedJoinTest {
  private Environment env;
  private SocketNode local;

  @BeforeEach
  void setUp() throws IOException {
    env = new Environment();
    local = SocketNode.create(env, "4000");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("broadcasting the rows of a join request skips a missing row")
  void broadcastRowsSkipsAMissingRow() throws IOException {
    NodeHandle acceptor = local.handle("8000");
    ConsistentJoinProtocol protocol = new ConsistentJoinProtocol(local.node, local.local(), local.node.getRoutingTable(),
        local.node.getLeafSet(), null);
    JoinRequest joinRequest = new JoinRequest(local.local(), (byte) 4, 1L);
    joinRequest.pushRow(null);
    joinRequest.acceptJoin(acceptor, local.node.getLeafSet());

    assertDoesNotThrow(() -> protocol.broadcastRows(joinRequest));
  }
}
