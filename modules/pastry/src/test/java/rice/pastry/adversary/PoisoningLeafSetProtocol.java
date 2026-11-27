package rice.pastry.adversary;

import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.leafset.BroadcastLeafSet;
import rice.pastry.leafset.LeafSet;
import rice.pastry.routing.RoutingTable;
import rice.pastry.standard.PeriodicLeafSetProtocol;
import rice.selector.TimerTask;

/**
 * {@link Behaviour.PoisonLeafSet}: from the attack start on, the malicious node broadcasts a leaf set of live colluders
 * only (the ones closest to it) to every honest node of its own leaf set, every push interval. The honest node merges
 * what fits; with uniform ids few colluders fit, with clustered or interleaved ids many do.
 */
final class PoisoningLeafSetProtocol extends PeriodicLeafSetProtocol {

  private final PastryNode local;
  private final LeafSet leafSet;
  private final Colluders colluders;
  private @Nullable TimerTask pushTask;

  PoisoningLeafSetProtocol(PastryNode local, LeafSet leafSet, RoutingTable routingTable, Colluders colluders) {
    super(local, local.getLocalHandle(), leafSet, routingTable);
    this.local = local;
    this.leafSet = leafSet;
    this.colluders = colluders;
  }

  /** Starts the broadcasts; called at the attack start. */
  void startPushing(long intervalMillis) {
    long firstDelayMillis = 1L + colluders.random().nextInt((int) Math.max(1, intervalMillis));
    pushTask = new TimerTask() {
      @Override
      public void run() {
        Oracle.Member self = colluders.oracle().member(local.getNodeId());
        if (self == null || !self.isLive) {
          cancel();
          return;
        }
        pushToNeighbours();
      }
    };
    local.getEnvironment().getSelectorManager().getTimer().schedule(pushTask, firstDelayMillis, intervalMillis);
  }

  @Override
  public void destroy() {
    TimerTask task = pushTask;
    if (task != null) task.cancel();
    super.destroy();
  }

  private void pushToNeighbours() {
    LeafSet lie = new LeafSet(local.getLocalHandle(), leafSet.maxSize(), false);
    for (Oracle.Member colluder : colluders.oracle().liveMalicious()) {
      if (colluder.isReady()) lie.put(colluder.node.getLocalHandle());
    }
    List<NodeHandle> honestNeighbours = new ArrayList<>();
    for (NodeHandle neighbour : leafSet.getUniqueSet()) {
      if (!colluders.oracle().isMalicious(neighbour)) honestNeighbours.add(neighbour);
    }
    for (NodeHandle neighbour : honestNeighbours) {
      thePastryNode.send(neighbour, new BroadcastLeafSet(local.getLocalHandle(), lie, BroadcastLeafSet.Update, 0), null,
          options);
    }
  }
}
