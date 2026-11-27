package rice.pastry.adversary;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.environment.random.simple.SimpleRandomSource;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.standard.RandomNodeIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class OracleTest {

  private Environment env;
  private DirectPastryNodeFactory factory;
  private final Oracle oracle = new Oracle();

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env),
        new EuclideanNetwork<DirectNodeHandle, RawMessage>(env), env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("ids convert to ring positions and back")
  void idsRoundTrip() {
    Id id = RingIds.randomId(new SimpleRandomSource(3, null));

    assertEquals(id, RingIds.toId(RingIds.toInteger(id)));
    assertEquals(RingIds.toId(BigInteger.ZERO), RingIds.toId(RingIds.RING_SIZE));
  }

  @Test
  @DisplayName("the root of a key is the ready node closest to it, across the wrap of the ring")
  void rootIsTheClosestReadyNode() throws Exception {
    Oracle.Member low = readyMember(1);
    Oracle.Member middle = readyMember(1000);
    Oracle.Member high = readyMember(-10);

    assertSame(low, oracle.root(position(400)));
    assertSame(middle, oracle.root(position(600)));
    assertSame(high, oracle.root(position(-6)));
    assertSame(low, oracle.root(position(-3)));
  }

  @Test
  @DisplayName("a departed or not yet ready node is nobody's root")
  void onlyLiveReadyNodesAreRoots() throws Exception {
    Oracle.Member departed = readyMember(10);
    Oracle.Member notReady = member(20, false);
    Oracle.Member far = readyMember(1_000_000);
    oracle.departed(departed);

    assertSame(far, oracle.root(position(15)));
    assertNull(new Oracle().root(position(1)));
    assertEquals(List.of(notReady, far), oracle.live());
  }

  @Test
  @DisplayName("the expected leaf set has up to half the size on each side, without the node itself")
  void expectedLeafSet() throws Exception {
    Oracle.Member[] members = new Oracle.Member[6];
    for (int i = 0; i < members.length; i++) members[i] = readyMember(100L * (i + 1));

    assertEquals(Set.of(members[1].id, members[2].id, members[5].id, members[4].id),
        oracle.expectedLeafSet(members[0], 2));
    assertEquals(Set.of(members[1].id, members[2].id, members[3].id, members[4].id, members[5].id),
        oracle.expectedLeafSet(members[0], 12));
  }

  @Test
  @DisplayName("domain entropy is 1 for an even spread, 0 for one domain, normalised by the possible maximum")
  void domainEntropy() throws Exception {
    NodeHandle a = member(1, true, 0).node.getLocalHandle();
    NodeHandle b = member(2, true, 1).node.getLocalHandle();
    NodeHandle c = member(3, true, 2).node.getLocalHandle();
    NodeHandle d = member(4, true, 3).node.getLocalHandle();
    NodeHandle e = member(5, true, 0).node.getLocalHandle();

    assertEquals(1.0, Metrics.normalisedDomainEntropy(List.of(a, b, c, d), oracle, 16), 1e-9);
    assertEquals(0.5, Metrics.normalisedDomainEntropy(List.of(a, b, e, b), oracle, 16), 0.05);
    assertEquals(0.0, Metrics.normalisedDomainEntropy(List.of(a, e), oracle, 16), 1e-9);
    assertEquals(-1.0, Metrics.normalisedDomainEntropy(List.of(a), oracle, 16), 1e-9);
  }

  @Test
  @DisplayName("the 5th percentile is the nearest rank below")
  void percentile() {
    assertEquals(1.0, Metrics.percentile5(List.of(5.0, 1.0, 3.0)));
    assertEquals(-1.0, Metrics.percentile5(List.of()));
  }

  private static Id position(long value) {
    return RingIds.toId(BigInteger.valueOf(value));
  }

  private Oracle.Member readyMember(long position) throws Exception {
    return member(position, true);
  }

  private Oracle.Member member(long position, boolean isReady) throws Exception {
    return member(position, isReady, 0);
  }

  private Oracle.Member member(long position, boolean isReady, int domain) throws Exception {
    Oracle.Member member = new Oracle.Member(factory.newNode(position(position)), false, domain, false, 0);
    oracle.created(member);
    if (isReady) oracle.ready(member, 0);
    return member;
  }
}
