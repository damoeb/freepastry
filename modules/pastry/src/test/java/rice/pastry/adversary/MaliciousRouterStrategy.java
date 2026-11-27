package rice.pastry.adversary;

import java.util.Iterator;
import java.util.List;

import org.jspecify.annotations.Nullable;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.join.JoinAddress;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouterStrategy;

/**
 * {@link Behaviour.Drop} and {@link Behaviour.Misroute}: the next-hop choice of a malicious node. Returning
 * {@code null} drops the message ({@code StandardRouter} then only fails it locally, the upstream hop already has its
 * hop ack). Before the attack start, and for join requests under {@link Behaviour.Selective}, it chooses as the
 * default strategy of {@code StandardRouter} does.
 */
final class MaliciousRouterStrategy implements RouterStrategy {

  private final PastryNode local;
  private final Colluders colluders;
  private final Behaviour.@Nullable Drop drop;
  private final Behaviour.@Nullable Misroute misroute;
  private final boolean isSelective;

  MaliciousRouterStrategy(PastryNode local, Colluders colluders) {
    this.local = local;
    this.colluders = colluders;
    Scenario scenario = colluders.scenario();
    this.drop = scenario.behaviour(Behaviour.Drop.class);
    this.misroute = scenario.behaviour(Behaviour.Misroute.class);
    this.isSelective = scenario.has(Behaviour.Selective.class);
  }

  @Override
  public @Nullable NodeHandle pickNextHop(RouteMessage msg, Iterator<NodeHandle> candidates) {
    if (colluders.isAttacking() && attacks(msg)) {
      Behaviour.Drop dropping = drop;
      if (dropping != null && (dropping.targetRange() == null || dropping.targetRange().containsId(msg.getTarget()))
          && colluders.random().nextDouble() < dropping.probability()) {
        colluders.messageDropped();
        return null;
      }
      Behaviour.Misroute misrouting = misroute;
      if (misrouting != null) {
        Oracle.Member colluder = pickColluder(msg, misrouting.target());
        if (colluder != null) {
          if (!colluder.id.equals(local.getNodeId())) colluders.messageMisrouted();
          return colluder.node.getLocalHandle();
        }
      }
    }
    return pickAlive(msg, candidates);
  }

  private boolean attacks(RouteMessage msg) {
    return !isSelective || msg.getAuxAddress() != JoinAddress.getCode();
  }

  private Oracle.@Nullable Member pickColluder(RouteMessage msg, Behaviour.MisrouteTarget target) {
    Oracle oracle = colluders.oracle();
    return switch (target) {
      case CLOSEST_COLLUDER -> oracle.closestColluder(msg.getTarget());
      case RANDOM_COLLUDER -> {
        List<Oracle.Member> registry = oracle.liveMalicious();
        yield registry.isEmpty() ? null : registry.get(colluders.random().nextInt(registry.size()));
      }
    };
  }

  /** The choice of {@code StandardRouter.AliveRouterStrategy}, which is package-private there. */
  private static @Nullable NodeHandle pickAlive(RouteMessage msg, Iterator<NodeHandle> candidates) {
    NodeHandle first = candidates.next();
    if (first.getLiveness() < NodeHandle.LIVENESS_SUSPECTED) {
      return first;
    }
    while (candidates.hasNext()) {
      NodeHandle nodeHandle = candidates.next();
      if (nodeHandle.getLiveness() < NodeHandle.LIVENESS_SUSPECTED) {
        return nodeHandle;
      }
      if (first.getLiveness() > nodeHandle.getLiveness()) first = nodeHandle;
    }
    if (first.getLiveness() >= NodeHandle.LIVENESS_DEAD) {
      return null;
    }
    msg.getOptions().setRerouteIfSuspected(false);
    return first;
  }
}
