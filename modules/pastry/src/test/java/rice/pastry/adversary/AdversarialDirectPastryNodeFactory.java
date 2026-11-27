package rice.pastry.adversary;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.ReadyStrategy;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.join.JoinProtocol;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.routing.RouteSetProtocol;
import rice.pastry.routing.RouterStrategy;
import rice.pastry.routing.RoutingTable;

/**
 * The direct node factory of the harness: a malicious node gets its behaviours through the factory seams (router
 * strategy, join protocol, route set protocol, leaf set protocol); an honest node is built exactly as by
 * {@link DirectPastryNodeFactory}. {@link Behaviour.FastPing} lives in {@link AdversarialNetwork} instead, and the
 * failure domain in {@link FailureDomains}.
 */
final class AdversarialDirectPastryNodeFactory extends DirectPastryNodeFactory {

  private final Colluders colluders;
  private final AdversarialNetwork network;
  private boolean isNextNodeMalicious;
  private final Map<Id, PoisoningRouteSetProtocol> rowPoisoners = new HashMap<>();
  private final Map<Id, PoisoningLeafSetProtocol> leafSetPoisoners = new HashMap<>();

  AdversarialDirectPastryNodeFactory(NodeIdFactory nodeIdFactory, AdversarialNetwork network, Environment env,
      Colluders colluders) {
    super(nodeIdFactory, network, env);
    this.colluders = colluders;
    this.network = network;
  }

  AdversarialNetwork network() {
    return network;
  }

  /** Builds a node with the given id, malicious or honest. */
  PastryNode newNode(Id id, boolean isMalicious) throws IOException {
    isNextNodeMalicious = isMalicious;
    try {
      return newNode(id);
    } finally {
      isNextNodeMalicious = false;
    }
  }

  @Nullable PoisoningRouteSetProtocol rowPoisoner(Id id) {
    return rowPoisoners.get(id);
  }

  @Nullable PoisoningLeafSetProtocol leafSetPoisoner(Id id) {
    return leafSetPoisoners.get(id);
  }

  @Override
  protected @Nullable RouterStrategy getRouterStrategy(PastryNode pn) {
    Scenario scenario = colluders.scenario();
    if (isNextNodeMalicious && (scenario.has(Behaviour.Drop.class) || scenario.has(Behaviour.Misroute.class))) {
      return new MaliciousRouterStrategy(pn, colluders);
    }
    return super.getRouterStrategy(pn);
  }

  @Override
  protected JoinProtocol getJoinProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable,
      ReadyStrategy readyStrategy) {
    if (isNextNodeMalicious && colluders.scenario().has(Behaviour.LieInJoin.class)) {
      LyingJoinProtocol protocol = new LyingJoinProtocol(pn, routeTable, leafSet, readyStrategy,
          getConsistentJoinConfig(pn), colluders);
      protocol.register();
      return protocol;
    }
    return super.getJoinProtocol(pn, leafSet, routeTable, readyStrategy);
  }

  @Override
  protected RouteSetProtocol getRouteSetProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable) {
    if (isNextNodeMalicious && colluders.scenario().has(Behaviour.PoisonRows.class)) {
      PoisoningRouteSetProtocol protocol = new PoisoningRouteSetProtocol(pn, routeTable, colluders);
      protocol.register();
      rowPoisoners.put(pn.getNodeId(), protocol);
      return protocol;
    }
    return super.getRouteSetProtocol(pn, leafSet, routeTable);
  }

  @Override
  protected LeafSetProtocol getLeafSetProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable) {
    if (isNextNodeMalicious && colluders.scenario().has(Behaviour.PoisonLeafSet.class)) {
      PoisoningLeafSetProtocol protocol = new PoisoningLeafSetProtocol(pn, leafSet, routeTable, colluders);
      protocol.register();
      leafSetPoisoners.put(pn.getNodeId(), protocol);
      return protocol;
    }
    return super.getLeafSetProtocol(pn, leafSet, routeTable);
  }
}
