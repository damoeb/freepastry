package rice.pastry.standard;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.leafset.LeafSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsistentJoinProtocolTest {

  private static final int LEAF_SET_SIZE = 24;

  private Environment env;
  private PastryNodeFactory factory;
  private PastryNode local;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    NetworkSimulator<DirectNodeHandle, RawMessage> simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env);
    local = factory.newNode();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void whoDoWeNeedAResponseFromCoversTheLargerClockwiseHalf() throws Exception {
    NodeHandle[] cw = { handle(), handle(), handle() };
    NodeHandle[] ccw = { handle() };

    Collection<NodeHandle> actual = protocolFor(cw, ccw).whoDoWeNeedAResponseFrom();

    assertEquals(new HashSet<NodeHandle>(List.of(cw[0], cw[1], cw[2], ccw[0])), new HashSet<NodeHandle>(actual));
  }

  @Test
  void whoDoWeNeedAResponseFromStopsAtTheSmallerClockwiseHalf() throws Exception {
    NodeHandle[] cw = { handle() };
    NodeHandle[] ccw = { handle(), handle(), handle() };

    Collection<NodeHandle> actual = protocolFor(cw, ccw).whoDoWeNeedAResponseFrom();

    assertEquals(new HashSet<NodeHandle>(List.of(cw[0], ccw[0], ccw[1], ccw[2])), new HashSet<NodeHandle>(actual));
  }

  private ConsistentJoinProtocol protocolFor(NodeHandle[] cw, NodeHandle[] ccw) {
    LeafSet leafSet = new LeafSet(local.getLocalHandle(), LEAF_SET_SIZE, false, cw, ccw);
    return new ConsistentJoinProtocol(local, local.getLocalHandle(), local.getRoutingTable(), leafSet, null);
  }

  private NodeHandle handle() throws Exception {
    return factory.newNode().getLocalHandle();
  }
}
