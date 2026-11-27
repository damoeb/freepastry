package rice.pastry.adversary;

import rice.pastry.PastryNode;
import rice.pastry.ReadyStrategy;
import rice.pastry.join.JoinRequest;
import rice.pastry.leafset.LeafSet;
import rice.pastry.routing.RoutingTable;
import rice.pastry.standard.ConsistentJoinConfig;
import rice.pastry.standard.ConsistentJoinProtocol;

/**
 * {@link Behaviour.LieInJoin}: as the root of a join, the malicious node answers with a leaf set of live colluders
 * only, the ones closest to it, instead of its own leaf set. Everything else of the join protocol stays honest.
 */
final class LyingJoinProtocol extends ConsistentJoinProtocol {

  private final Colluders colluders;
  private final PastryNode local;
  private final int leafSetSize;

  LyingJoinProtocol(PastryNode local, RoutingTable routingTable, LeafSet leafSet, ReadyStrategy nextReadyStrategy,
      ConsistentJoinConfig config, Colluders colluders) {
    super(local, local.getLocalHandle(), routingTable, leafSet, nextReadyStrategy, null, config);
    this.local = local;
    this.colluders = colluders;
    this.leafSetSize = leafSet.maxSize();
  }

  @Override
  protected LeafSet leafSetForJoiner(JoinRequest joinRequest) {
    if (!colluders.isAttacking()) return super.leafSetForJoiner(joinRequest);
    LeafSet lie = new LeafSet(local.getLocalHandle(), leafSetSize, false);
    for (Oracle.Member colluder : colluders.oracle().liveMalicious()) {
      if (colluder.isReady()) lie.put(colluder.node.getLocalHandle());
    }
    return lie;
  }
}
