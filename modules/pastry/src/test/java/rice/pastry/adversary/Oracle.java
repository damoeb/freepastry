package rice.pastry.adversary;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.jspecify.annotations.Nullable;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;

/**
 * Global knowledge of the ring: every node ever created, which of them are live and ready, which are malicious and in
 * which failure domain each sits. It answers the correct root, replica set and leaf set of any key at the current
 * simulated instant. Used on the selector thread only.
 */
final class Oracle {

  /** A node of the run, honest or malicious. */
  static final class Member {
    final PastryNode node;
    final Id id;
    final BigInteger position;
    final boolean isMalicious;
    final int domain;
    final boolean isLateJoiner;
    final long createdAtMillis;
    long readyAtMillis = -1;
    long convergedAtMillis = -1;
    boolean isLive = true;
    @Nullable HarnessApplication application;

    Member(PastryNode node, boolean isMalicious, int domain, boolean isLateJoiner, long createdAtMillis) {
      this.node = node;
      this.id = node.getNodeId();
      this.position = RingIds.toInteger(id);
      this.isMalicious = isMalicious;
      this.domain = domain;
      this.isLateJoiner = isLateJoiner;
      this.createdAtMillis = createdAtMillis;
    }

    boolean isReady() {
      return isLive && readyAtMillis >= 0;
    }
  }

  private final Map<Id, Member> everCreated = new LinkedHashMap<>();
  private final NavigableMap<BigInteger, Member> ready = new TreeMap<>();
  private final List<Member> live = new ArrayList<>();
  private final List<Member> liveMalicious = new ArrayList<>();

  void created(Member member) {
    everCreated.put(member.id, member);
    live.add(member);
    if (member.isMalicious) liveMalicious.add(member);
  }

  void ready(Member member, long nowMillis) {
    if (!member.isLive || member.readyAtMillis >= 0) return;
    member.readyAtMillis = nowMillis;
    ready.put(member.position, member);
  }

  void departed(Member member) {
    member.isLive = false;
    ready.remove(member.position);
    live.remove(member);
    liveMalicious.remove(member);
  }

  @Nullable Member member(Id id) {
    return everCreated.get(id);
  }

  @Nullable Member member(NodeHandle handle) {
    return everCreated.get(handle.getNodeId());
  }

  boolean isMalicious(NodeHandle handle) {
    Member member = member(handle);
    return member != null && member.isMalicious;
  }

  int domain(NodeHandle handle) {
    Member member = member(handle);
    if (member == null) throw new IllegalArgumentException("not a node of this run: " + handle);
    return member.domain;
  }

  /** Every node of the run, live or departed, in creation order. */
  Collection<Member> everCreated() {
    return everCreated.values();
  }

  /** Live nodes in creation order. */
  List<Member> live() {
    return live;
  }

  /** Live malicious nodes in creation order: the colluder registry. */
  List<Member> liveMalicious() {
    return liveMalicious;
  }

  /** Live, ready nodes in id order. */
  Iterable<Member> ready() {
    return ready.values();
  }

  int readyCount() {
    return ready.size();
  }

  /** The live, ready node numerically closest to {@code key}, or {@code null} for an empty ring. */
  @Nullable Member root(Id key) {
    if (ready.isEmpty()) return null;
    BigInteger position = RingIds.toInteger(key);
    Map.Entry<BigInteger, Member> clockwise = ready.ceilingEntry(position);
    if (clockwise == null) clockwise = ready.firstEntry();
    Map.Entry<BigInteger, Member> counterClockwise = ready.floorEntry(position);
    if (counterClockwise == null) counterClockwise = ready.lastEntry();
    BigInteger clockwiseDistance = RingIds.ringDistance(position, clockwise.getKey());
    BigInteger counterClockwiseDistance = RingIds.ringDistance(position, counterClockwise.getKey());
    return clockwiseDistance.compareTo(counterClockwiseDistance) <= 0 ? clockwise.getValue() : counterClockwise.getValue();
  }

  /**
   * The ids the leaf set of {@code member} has when it is correct: up to {@code half} ready nodes on each side, without
   * the node itself.
   */
  Set<Id> expectedLeafSet(Member member, int half) {
    Set<Id> expected = new HashSet<>();
    int othersCount = ready.size() - (ready.containsKey(member.position) ? 1 : 0);
    int perSide = Math.min(half, othersCount);
    BigInteger cursor = member.position;
    for (int i = 0; i < perSide; i++) {
      Map.Entry<BigInteger, Member> next = ready.higherEntry(cursor);
      if (next == null) next = ready.firstEntry();
      if (next.getValue() == member) break;
      expected.add(next.getValue().id);
      cursor = next.getKey();
    }
    cursor = member.position;
    for (int i = 0; i < perSide; i++) {
      Map.Entry<BigInteger, Member> previous = ready.lowerEntry(cursor);
      if (previous == null) previous = ready.lastEntry();
      if (previous.getValue() == member) break;
      expected.add(previous.getValue().id);
      cursor = previous.getKey();
    }
    return expected;
  }

  /** The live, ready colluder closest to {@code key}, or {@code null} if there is none. */
  @Nullable Member closestColluder(Id key) {
    BigInteger position = RingIds.toInteger(key);
    Member best = null;
    BigInteger bestDistance = null;
    for (Member colluder : liveMalicious) {
      if (!colluder.isReady()) continue;
      BigInteger distance = RingIds.ringDistance(position, colluder.position);
      if (bestDistance == null || distance.compareTo(bestDistance) < 0) {
        best = colluder;
        bestDistance = distance;
      }
    }
    return best;
  }
}
