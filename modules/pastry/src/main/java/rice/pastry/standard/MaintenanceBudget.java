package rice.pastry.standard;

import org.mpisws.p2p.transport.util.TokenBucketMap;
import rice.environment.params.Parameters;
import rice.environment.time.TimeSource;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.leafset.LeafSet;
import rice.pastry.routing.RouteSet;
import rice.pastry.routing.RoutingTable;

/**
 * The maintenance work a remote peer can make this node do at will (design 3f): answering route row and leaf set
 * requests, taking in unsolicited route rows, and answering join requests as the root. Each kind has a token bucket per
 * peer (the sender the connection authenticated, in certified mode), driven by the environment's {@code TimeSource};
 * join answers are also capped for the node as a whole. A request beyond its bucket is ignored: the honest requester
 * asks again on its own schedule (route rows and leaf sets periodically, joins with backoff), so a flood degrades only
 * the flooder. Peers in the leaf set or routing table get {@code pastry_budget_pinned_factor} times the rate and burst,
 * so the maintenance bursts of neighbours are never throttled first.
 *
 * <p>Selector thread only, like the protocols that consult it.
 */
public final class MaintenanceBudget {
  /**
   * The rates and bursts of each kind of work.
   *
   * @param routeRowRequestsPerSecond {@code pastry_budget_route_row_requests_per_second}: route row requests a peer
   *        gets answered
   * @param routeRowRequestBurst {@code pastry_budget_route_row_request_burst}
   * @param routeRowBroadcastsPerSecond {@code pastry_budget_route_row_broadcasts_per_second}: route rows a peer sends
   *        this node that it takes in, whether asked for or not
   * @param routeRowBroadcastBurst {@code pastry_budget_route_row_broadcast_burst}
   * @param leafSetRequestsPerSecond {@code pastry_budget_leaf_set_requests_per_second}: leaf set requests (and leases)
   *        a peer gets answered
   * @param leafSetRequestBurst {@code pastry_budget_leaf_set_request_burst}
   * @param joinsPerJoinerPerSecond {@code pastry_budget_joins_per_joiner_per_second}: join requests of one joiner id
   *        that this node answers as the root
   * @param joinBurstPerJoiner {@code pastry_budget_join_burst_per_joiner}
   * @param joinsPerSecond {@code pastry_budget_joins_per_second}: join requests of all joiners together
   * @param joinBurst {@code pastry_budget_join_burst}
   * @param identityProbesPerSecond {@code pastry_budget_identity_probes_per_second}: verification connections
   *        (certified mode) to handles one peer supplied, in a message this node is handling
   * @param identityProbeBurst {@code pastry_budget_identity_probe_burst}
   * @param pinnedFactor {@code pastry_budget_pinned_factor}: the factor of leaf set and routing table members, at
   *        least 1
   * @param maxPeers {@code pastry_budget_max_peers}: peers that have a bucket of a kind; beyond it the least recently
   *        used one is forgotten
   */
  public record Config(
      double routeRowRequestsPerSecond,
      double routeRowRequestBurst,
      double routeRowBroadcastsPerSecond,
      double routeRowBroadcastBurst,
      double leafSetRequestsPerSecond,
      double leafSetRequestBurst,
      double joinsPerJoinerPerSecond,
      double joinBurstPerJoiner,
      double joinsPerSecond,
      double joinBurst,
      double identityProbesPerSecond,
      double identityProbeBurst,
      double pinnedFactor,
      int maxPeers) {

    /** @throws IllegalArgumentException if the pinned factor is below 1 */
    public Config {
      if (!(pinnedFactor >= 1)) throw new IllegalArgumentException("pastry_budget_pinned_factor must be at least 1: " + pinnedFactor);
    }

    /** @throws IllegalArgumentException if a value is out of range */
    public static Config fromParameters(Parameters parameters) {
      return new Config(
          parameters.getDouble("pastry_budget_route_row_requests_per_second"),
          parameters.getDouble("pastry_budget_route_row_request_burst"),
          parameters.getDouble("pastry_budget_route_row_broadcasts_per_second"),
          parameters.getDouble("pastry_budget_route_row_broadcast_burst"),
          parameters.getDouble("pastry_budget_leaf_set_requests_per_second"),
          parameters.getDouble("pastry_budget_leaf_set_request_burst"),
          parameters.getDouble("pastry_budget_joins_per_joiner_per_second"),
          parameters.getDouble("pastry_budget_join_burst_per_joiner"),
          parameters.getDouble("pastry_budget_joins_per_second"),
          parameters.getDouble("pastry_budget_join_burst"),
          parameters.getDouble("pastry_budget_identity_probes_per_second"),
          parameters.getDouble("pastry_budget_identity_probe_burst"),
          parameters.getDouble("pastry_budget_pinned_factor"),
          parameters.getInt("pastry_budget_max_peers"));
    }
  }

  /** One kind of work: a bucket per peer, and a bigger one per pinned peer. */
  private static final class PerPeer<K> {
    private final TokenBucketMap<K> buckets;
    private final TokenBucketMap<K> pinnedBuckets;

    PerPeer(double perSecond, double burst, double pinnedFactor, int maxPeers) {
      buckets = new TokenBucketMap<>(perSecond, burst, maxPeers);
      pinnedBuckets = new TokenBucketMap<>(perSecond * pinnedFactor, burst * pinnedFactor, maxPeers);
    }

    boolean tryTake(K key, boolean isPinned, long nowMillis) {
      return (isPinned ? pinnedBuckets : buckets).tryTake(key, nowMillis);
    }
  }

  private final PastryNode node;
  private final TimeSource timeSource;
  private final PerPeer<Id> routeRowRequests;
  private final PerPeer<Id> routeRowBroadcasts;
  private final PerPeer<Id> leafSetRequests;
  private final PerPeer<Id> joinsPerJoiner;
  private final PerPeer<Id> identityProbes;
  private final TokenBucketMap<Boolean> joins;

  /** @throws IllegalArgumentException if a value of the configuration is out of range */
  public MaintenanceBudget(PastryNode node, Config config) {
    this.node = node;
    this.timeSource = node.getEnvironment().getTimeSource();
    routeRowRequests = new PerPeer<>(config.routeRowRequestsPerSecond(), config.routeRowRequestBurst(), config.pinnedFactor(), config.maxPeers());
    routeRowBroadcasts = new PerPeer<>(config.routeRowBroadcastsPerSecond(), config.routeRowBroadcastBurst(), config.pinnedFactor(), config.maxPeers());
    leafSetRequests = new PerPeer<>(config.leafSetRequestsPerSecond(), config.leafSetRequestBurst(), config.pinnedFactor(), config.maxPeers());
    joinsPerJoiner = new PerPeer<>(config.joinsPerJoinerPerSecond(), config.joinBurstPerJoiner(), 1, config.maxPeers());
    joins = new TokenBucketMap<>(config.joinsPerSecond(), config.joinBurst(), 1);
    identityProbes = new PerPeer<>(config.identityProbesPerSecond(), config.identityProbeBurst(), config.pinnedFactor(), config.maxPeers());
  }

  /** Whether to answer this route row request of the sender. */
  public boolean admitRouteRowRequest(NodeHandle sender) {
    return routeRowRequests.tryTake(sender.getNodeId(), isNeighbour(sender), timeSource.currentTimeMillis());
  }

  /** Whether to take in this route row the sender broadcast (an answer to a request or unsolicited). */
  public boolean admitRouteRowBroadcast(NodeHandle sender) {
    return routeRowBroadcasts.tryTake(sender.getNodeId(), isNeighbour(sender), timeSource.currentTimeMillis());
  }

  /** Whether to answer this leaf set request (and give the lease it asks for) of the sender. */
  public boolean admitLeafSetRequest(NodeHandle sender) {
    return leafSetRequests.tryTake(sender.getNodeId(), isNeighbour(sender), timeSource.currentTimeMillis());
  }

  /** Whether to answer this join request as the root: within the joiner's bucket and the node's. */
  public boolean admitJoin(Id joiner) {
    long now = timeSource.currentTimeMillis();
    return joinsPerJoiner.tryTake(joiner, false, now) && joins.tryTake(Boolean.TRUE, now);
  }

  /**
   * Whether to open a verification connection (certified mode) to a handle that the supplier sent in the message
   * this node is handling. Beyond the budget the handle is not admitted now; it is checked again when it comes back.
   */
  public boolean admitIdentityProbe(NodeHandle supplier) {
    return identityProbes.tryTake(supplier.getNodeId(), isNeighbour(supplier), timeSource.currentTimeMillis());
  }

  /** Whether the peer is in the leaf set or the routing table. */
  private boolean isNeighbour(NodeHandle handle) {
    LeafSet leafSet = node.getLeafSet();
    if (leafSet != null && leafSet.member(handle)) return true;
    RoutingTable routingTable = node.getRoutingTable();
    if (routingTable == null) return false;
    RouteSet routeSet = routingTable.getBestEntry(handle.getNodeId());
    return routeSet != null && routeSet.getIndex(handle) >= 0;
  }
}
