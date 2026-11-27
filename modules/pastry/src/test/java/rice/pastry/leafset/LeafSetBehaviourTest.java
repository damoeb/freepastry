package rice.pastry.leafset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.IdRange;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.NodeHandleFactoryListener;
import rice.pastry.NodeSet;
import rice.pastry.PastryNode;
import rice.pastry.testing.StubHandleNodes;
import rice.pastry.routing.RoutingTable;

/** Pins the replica/neighbor set, range, merge and wire behaviour of {@link LeafSet} on a small deterministic ring. */
class LeafSetBehaviourTest {
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

  private static final int LEAF_SET_SIZE = 8;
  /** Positions on the ring, in steps of 10 (see {@link #idAt}). */
  private static final int[] RING = {10, 30, 50, 70, 90, 110, 130, 150, 170, 190, 210, 230};
  private static final int BASE_POSITION = 110;

  /** A handle that is only an id; it can be written to and read from a buffer. */
  static final class FixedHandle extends NodeHandle {
    private final Id id;

    FixedHandle(Id id) {
      this.id = id;
    }

    @Override
    public Id getNodeId() {
      return id;
    }

    @Override
    public int getLiveness() {
      return LIVENESS_ALIVE;
    }

    @Override
    public boolean ping() {
      return true;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof FixedHandle other && other.id.equals(id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      id.serialize(buf);
    }

    @Override
    public String toString() {
      return id.toString();
    }
  }

  private static final NodeHandleFactory<NodeHandle> FACTORY = new NodeHandleFactory<NodeHandle>() {
    @Override
    public NodeHandle readNodeHandle(InputBuffer buf) throws IOException {
      return new FixedHandle(Id.build(buf));
    }

    @Override
    public NodeHandle coalesce(NodeHandle handle) {
      return handle;
    }

    @Override
    public void addNodeHandleFactoryListener(NodeHandleFactoryListener<? super NodeHandle> listener) {
    }

    @Override
    public void removeNodeHandleFactoryListener(NodeHandleFactoryListener<? super NodeHandle> listener) {
    }
  };

  private static Id idAt(int position) {
    return Id.build(new int[] {0, 0, 0, 0, position << 20});
  }

  private static NodeHandle handleAt(int position) {
    return new FixedHandle(idAt(position));
  }

  /** A leaf set of the given size around {@code basePosition}, filled with every other ring position. */
  private static LeafSet leafSet(int basePosition, int size, int... positions) {
    LeafSet leafSet = new LeafSet(handleAt(basePosition), size, (RoutingTable) null);
    for (int position : positions) {
      leafSet.put(handleAt(position));
    }
    return leafSet;
  }

  /** A routing table that only records nothing: the merge writes every inserted handle to it. */
  private RoutingTable routingTable() throws IOException {
    return new RoutingTable(handleAt(BASE_POSITION), 1, (byte) 4, node) {
      @Override
      public synchronized boolean put(NodeHandle handle) {
        return true;
      }
    };
  }

  private static LeafSet fullRing() {
    return leafSet(BASE_POSITION, LEAF_SET_SIZE, RING);
  }

  private static List<Id> ids(NodeSet set) {
    List<Id> ids = new ArrayList<>();
    for (int i = 0; i < set.size(); i++) {
      ids.add(set.get(i).getNodeId());
    }
    return ids;
  }

  @Test
  void holdsTheNearestNeighboursOnBothSides() {
    LeafSet leafSet = fullRing();

    assertEquals(8, leafSet.size());
    assertEquals(List.of(handleAt(130), handleAt(150), handleAt(170), handleAt(190)),
        List.of(leafSet.get(1), leafSet.get(2), leafSet.get(3), leafSet.get(4)));
    assertEquals(List.of(handleAt(90), handleAt(70), handleAt(50), handleAt(30)),
        List.of(leafSet.get(-1), leafSet.get(-2), leafSet.get(-3), leafSet.get(-4)));
  }

  @Test
  void replicaSetIsOrderedByDistanceToTheKey() {
    LeafSet leafSet = fullRing();
    Id key = idAt(135);

    List<Id> actual = ids(leafSet.replicaSet(key, 4));

    List<Id> expected = new ArrayList<>();
    for (int position : RING) {
      expected.add(idAt(position));
    }
    expected.sort(Comparator.comparing(id -> id.distance(key)));
    assertEquals(expected.subList(0, 4), actual);
  }

  @Test
  void replicaSetOfARootBeyondTheLeafSetIsEmpty() {
    LeafSet leafSet = fullRing();

    assertEquals(0, leafSet.replicaSet(idAt(240), 3).size());
    assertEquals(0, leafSet.replicaSet(idAt(135), 0).size());
  }

  @Test
  void replicaSetOfASmallRingWrapsAroundWithoutDuplicates() {
    LeafSet leafSet = leafSet(10, LEAF_SET_SIZE, 30, 50);

    List<Id> actual = ids(leafSet.replicaSet(idAt(30), 10));

    assertEquals(List.of(idAt(30), idAt(50), idAt(10)), actual);
  }

  @Test
  void neighborSetStartsAtTheLocalNode() {
    LeafSet leafSet = fullRing();

    List<Id> actual = ids(leafSet.neighborSet(3));

    assertEquals(idAt(BASE_POSITION), actual.get(0));
    assertEquals(3, actual.size());
  }

  @Test
  void neighborSetOfAWideRequestHoldsEveryMember() {
    LeafSet leafSet = fullRing();

    Set<Id> actual = new HashSet<>(ids(leafSet.neighborSet(100)));

    Set<Id> expected = new HashSet<>();
    expected.add(idAt(BASE_POSITION));
    for (NodeHandle handle : leafSet) {
      expected.add(handle.getNodeId());
    }
    assertEquals(expected, actual);
  }

  @Test
  void rangeOfTheLocalNodeIsHalfWayToItsNeighbours() {
    LeafSet leafSet = fullRing();

    IdRange range = leafSet.range(handleAt(BASE_POSITION), 0);

    assertEquals(new IdRange(idAt(100), idAt(120)), range);
  }

  @Test
  void rangeOfRankOneReachesOneNodeFurther() {
    LeafSet leafSet = fullRing();

    IdRange range = leafSet.range(handleAt(130), 1);

    assertEquals(new IdRange(idAt(110), idAt(150)), range);
  }

  @Test
  void rangeSplitsIntoAClockwiseAndACounterClockwiseHalf() {
    LeafSet leafSet = fullRing();
    NodeHandle node = handleAt(130);

    IdRange clockwise = leafSet.range(node, 1, true);
    IdRange counterClockwise = leafSet.range(node, 1, false);

    assertEquals(new IdRange(idAt(140), idAt(150)), clockwise);
    assertEquals(new IdRange(idAt(110), idAt(120)), counterClockwise);
  }

  @Test
  void rangeOfAnUnknownNodeCannotBeDetermined() {
    LeafSet leafSet = fullRing();

    assertThrows(LeafSetRangeCannotBeDeterminedException.class, () -> leafSet.range(handleAt(5), 0));
    assertThrows(IllegalArgumentException.class, () -> leafSet.range(handleAt(130), -1));
  }

  @Test
  void rangeBeyondTheKnownNeighboursCannotBeDetermined() {
    LeafSet leafSet = fullRing();

    assertThrows(LeafSetRangeCannotBeDeterminedException.class, () -> leafSet.range(handleAt(190), 2));
  }

  @Test
  void rangeInAnOverlappingLeafSetWrapsAround() {
    LeafSet leafSet = leafSet(10, LEAF_SET_SIZE, 30, 50);

    IdRange wholeRing = leafSet.range(handleAt(30), 2);

    assertEquals(new IdRange(idAt(30), idAt(30)), wholeRing);
  }

  private static LeafSet roundTrip(LeafSet leafSet) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    leafSet.serialize(out);
    return LeafSet.build(new SimpleInputBuffer(out.getBytes()), FACTORY);
  }

  @Test
  void aSerializedLeafSetBuildsAnEqualLeafSet() throws IOException {
    LeafSet leafSet = fullRing();

    LeafSet copy = roundTrip(leafSet);

    assertEquals(leafSet.toString(), copy.toString());
    assertEquals(leafSet.maxSize(), copy.maxSize());
    assertEquals(leafSet.clockwiseSize(), copy.clockwiseSize());
    assertEquals(leafSet.counterClockwiseSize(), copy.counterClockwiseSize());
  }

  @Test
  void aSerializedOverlappingLeafSetKeepsItsDuplicates() throws IOException {
    LeafSet leafSet = leafSet(10, LEAF_SET_SIZE, 30, 50);

    LeafSet copy = roundTrip(leafSet);

    assertTrue(copy.overlaps());
    assertEquals(leafSet.toString(), copy.toString());
    assertEquals(leafSet.asList(), copy.asList());
  }

  @Test
  void mergingARemoteLeafSetInsertsItsNearestNodes() throws IOException {
    LeafSet local = leafSet(BASE_POSITION, LEAF_SET_SIZE);
    LeafSet remote = leafSet(130, LEAF_SET_SIZE, 10, 30, 50, 70, 90, 110, 150, 170, 190, 210, 230);
    Set<NodeHandle> inserted = new HashSet<>();

    boolean changed = local.merge(remote, handleAt(130), routingTable(), false, inserted);

    assertTrue(changed);
    assertEquals(inserted, new HashSet<>(local.getUniqueSet()));
    assertEquals(handleAt(130), local.get(1));
  }

  @Test
  void aTestOnlyMergeLeavesTheLeafSetUntouched() throws IOException {
    LeafSet local = leafSet(BASE_POSITION, LEAF_SET_SIZE);
    LeafSet remote = leafSet(130, LEAF_SET_SIZE, 10, 30, 50, 70, 90, 110, 150, 170, 190, 210, 230);
    Set<NodeHandle> wouldInsert = new HashSet<>();

    boolean changed = local.merge(remote, handleAt(130), routingTable(), true, wouldInsert);

    assertTrue(changed);
    assertEquals(0, local.size());
    assertTrue(wouldInsert.contains(handleAt(130)));
  }
}
