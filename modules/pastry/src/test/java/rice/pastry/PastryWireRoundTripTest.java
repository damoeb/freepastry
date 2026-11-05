package rice.pastry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.environment.Environment;
import rice.pastry.join.JoinRequest;
import rice.pastry.leafset.BroadcastLeafSet;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.RequestLeafSet;
import rice.pastry.pns.PNSApplication;
import rice.pastry.pns.messages.LeafSetRequest;
import rice.pastry.pns.messages.LeafSetResponse;
import rice.pastry.pns.messages.RouteRowRequest;
import rice.pastry.pns.messages.RouteRowResponse;
import rice.pastry.routing.BroadcastRouteRow;
import rice.pastry.routing.RequestRouteRow;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouteSet;
import rice.pastry.standard.ConsistentJoinMsg;
import rice.pastry.standard.StandardAddress;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;

/**
 * Pastry's own messages and the values they carry, through the bytes of a socket ring: read where they arrive, and
 * sent on from there.
 */
class PastryWireRoundTripTest {
  private static final int LEAF_SET_SIZE = 8;
  private static final int ROUTE_SET_SIZE = 3;
  private static final int COLUMNS = 16;

  private static Environment env;
  private static SocketNode local;
  private static NodeHandle a;
  private static NodeHandle b;
  private static NodeHandle c;

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    local = SocketNode.create(env, "4000");
    a = local.handle("1000");
    b = local.handle("8000");
    c = local.handle("C000");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  private static LeafSet leafSet(NodeHandle... members) {
    LeafSet ls = new LeafSet(local.local(), LEAF_SET_SIZE, false);
    for (NodeHandle h : members) {
      ls.put(h);
    }
    return ls;
  }

  private static RouteSet routeSet(int row, int col, NodeHandle... members) {
    RouteSet rs = new RouteSet(ROUTE_SET_SIZE, row, col, local.node);
    for (NodeHandle h : members) {
      rs.put(h);
    }
    return rs;
  }

  private static RouteSet[] row(int rowIndex) {
    RouteSet[] row = new RouteSet[COLUMNS];
    row[1] = routeSet(rowIndex, 1, a);
    row[8] = routeSet(rowIndex, 8, b, c);
    row[12] = routeSet(rowIndex, 12);
    return row;
  }

  @Test
  @DisplayName("a join request with rows and a leaf set is read and sent on, inside a route message too")
  void joinRequest() throws IOException {
    JoinRequest jr = new JoinRequest(a, (byte) 4, 1234L);
    jr.pushRow(row(0));
    jr.pushRow(new RouteSet[COLUMNS]);
    jr.acceptJoin(b, leafSet(a, b, c));

    JoinRequest copy = local.hop(jr, a);

    assertEquals(a, copy.getHandle());
    assertEquals(b, copy.getJoinHandle());
    assertEquals(3, copy.getLeafSet().getUniqueCount() - 1);
    assertEquals(jr.lastRow(), copy.lastRow());

    RouteMessage routed = local.hop(new RouteMessage(Id.build("9000"), jr, (byte) 1), a);
    JoinRequest unwrapped = (JoinRequest) routed.unwrap(local.deserializer(jr.getDestination()));
    assertArrayEquals(WireRoundTrip.bytes(jr), WireRoundTrip.bytes(unwrapped));
    assertArrayEquals(SocketNode.withoutPriority(local.wire(new RouteMessage(Id.build("9000"), jr, (byte) 1))),
        SocketNode.withoutPriority(local.wire(routed)));
  }

  @Test
  @DisplayName("a fresh join request, without rows, join handle or leaf set, is read and sent on")
  void freshJoinRequest() throws IOException {
    JoinRequest copy = local.hop(new JoinRequest(a, (byte) 4, 5L), a);

    assertNull(copy.getJoinHandle());
    assertNull(copy.getLeafSet());
  }

  @ParameterizedTest(name = "base {0} bits")
  @ValueSource(bytes = {1, 2, 3, 4, 5, 6, 7, 8})
  @DisplayName("a join request round-trips for every supported routing table base")
  void joinRequestBases(byte base) throws IOException {
    JoinRequest jr = new JoinRequest(a, base, 5L);
    RouteSet[] row = new RouteSet[1 << base];
    row[0] = routeSet(0, 0, b);
    jr.pushRow(row);

    local.hop(jr, a);
  }

  @Test
  void broadcastLeafSet() throws IOException {
    BroadcastLeafSet copy = local.hop(new BroadcastLeafSet(a, leafSet(a, b, c), BroadcastLeafSet.Update, 77L), a);

    assertEquals(a, copy.from());
    assertEquals(BroadcastLeafSet.Update, copy.type());
    assertEquals(77L, copy.getTimeStamp());
  }

  @Test
  void broadcastEmptyLeafSet() throws IOException {
    local.hop(new BroadcastLeafSet(a, leafSet(), BroadcastLeafSet.JoinInitial, 0L), a);
  }

  @Test
  void requestLeafSet() throws IOException {
    assertEquals(42L, local.hop(new RequestLeafSet(a, 42L), a).getTimeStamp());
  }

  @Test
  void broadcastRouteRow() throws IOException {
    BroadcastRouteRow copy = local.hop(new BroadcastRouteRow(a, row(2)), a);

    assertEquals(COLUMNS, copy.getRow().length);
    assertNull(copy.getRow()[0]);
    assertEquals(2, copy.getRow()[8].size());
  }

  @ParameterizedTest(name = "base {0} bits")
  @ValueSource(ints = {1, 2, 3, 4, 5, 6})
  @DisplayName("a full route row round-trips for every routing table base up to 6 bits")
  void broadcastRouteRowBases(int base) throws IOException {
    hopRouteRow(base);
  }

  @ParameterizedTest(name = "base {0} bits")
  @ValueSource(ints = {7, 8})
  @DisplayName("a full route row round-trips for the bases of 7 and 8 bits a join request accepts")
  void broadcastRouteRowWideBases(int base) throws IOException {
    hopRouteRow(base);
  }

  private static void hopRouteRow(int base) throws IOException {
    RouteSet[] row = new RouteSet[1 << base];
    row[row.length - 1] = routeSet(0, row.length - 1, a);

    assertEquals(row.length, local.hop(new BroadcastRouteRow(a, row), a).getRow().length);
  }

  @Test
  void broadcastEmptyRouteRow() throws IOException {
    assertEquals(0, local.hop(new BroadcastRouteRow(a, new RouteSet[0]), a).getRow().length);
  }

  @Test
  void requestRouteRow() throws IOException {
    assertEquals(3, local.hop(new RequestRouteRow(a, (short) 3), a).getRow());
  }

  @Test
  void consistentJoin() throws IOException {
    Set<NodeHandle> failed = new HashSet<>(List.of(b, c));

    local.hop(new ConsistentJoinMsg(leafSet(a, b), failed, true), a);
    local.hop(new ConsistentJoinMsg(leafSet(), new HashSet<>(), false), a);
  }

  @Test
  void pnsMessages() throws IOException {
    int pns = StandardAddress.getAddress(PNSApplication.class, null, env);

    local.hop(new LeafSetRequest(a, pns), a);
    assertEquals(4, local.hop(new LeafSetResponse(leafSet(a, b, c), pns), a).getLeafset().getUniqueCount());
    assertEquals(5, local.hop(new RouteRowRequest(a, (short) 5, pns), a).getIndex());
    RouteRowResponse copy = local.hop(new RouteRowResponse(a, (short) 1, row(1), pns), a);
    assertEquals(COLUMNS, copy.getRow().length);
    assertEquals(0, local.hop(new RouteRowResponse(a, (short) 1, new RouteSet[0], pns), a).getRow().length);
  }

  @Test
  void values() throws IOException {
    WireRoundTrip.roundTrip(leafSet(a, b, c), LeafSet::serialize, buf -> LeafSet.build(buf, local.node));
    WireRoundTrip.roundTrip(leafSet(), LeafSet::serialize, buf -> LeafSet.build(buf, local.node));
    WireRoundTrip.roundTrip(routeSet(1, 2, a, b, c), RouteSet::serialize, buf -> new RouteSet(buf, local.node, local.node));
    WireRoundTrip.roundTrip(routeSet(1, 2), RouteSet::serialize, buf -> new RouteSet(buf, local.node, local.node));
    NodeSet set = new NodeSet();
    set.put(a);
    set.put(b);
    WireRoundTrip.roundTrip(set, NodeSet::serialize, buf -> new NodeSet(buf, local.node));
    WireRoundTrip.roundTrip(new NodeSet(), NodeSet::serialize, buf -> new NodeSet(buf, local.node));
    WireRoundTrip.roundTrip(new IdRange(Id.build("1000"), Id.build("8000")), IdRange::serialize, IdRange::new);
    WireRoundTrip.roundTrip(new IdRange(), IdRange::serialize, IdRange::new);
    WireRoundTrip.roundTrip(new IdRange(false), IdRange::serialize, IdRange::new);
    WireRoundTrip.roundTrip(Id.build("ABCDEF"), Id::serialize, Id::build);
    assertEquals(a, WireRoundTrip.roundTrip(a, NodeHandle::serialize, local.node::readNodeHandle));
  }
}
