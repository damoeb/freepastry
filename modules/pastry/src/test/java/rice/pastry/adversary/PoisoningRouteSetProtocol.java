package rice.pastry.adversary;

import java.util.List;

import org.jspecify.annotations.Nullable;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.messaging.Message;
import rice.pastry.routing.BroadcastRouteRow;
import rice.pastry.routing.RequestRouteRow;
import rice.pastry.routing.RouteSet;
import rice.pastry.routing.RoutingTable;
import rice.pastry.standard.StandardRouteSetProtocol;
import rice.selector.TimerTask;

/**
 * {@link Behaviour.PoisonRows}: from the attack start on, the malicious node answers every route-row request with a
 * row of colluders only, built for the requester's prefix, and pushes such rows unsolicited to a random honest node
 * every push interval. The victim accepts any row today: it puts every live handle of it into its table, where a
 * colluder wins a full slot whenever it is closer in proximity, which {@link Behaviour.FastPing} arranges.
 */
final class PoisoningRouteSetProtocol extends StandardRouteSetProtocol {

  /** Rows pushed per victim, from the top row down: the rows a ring of a few thousand nodes populates. */
  private static final int PUSHED_ROW_COUNT = 4;

  private final PastryNode local;
  private final RoutingTable routingTable;
  private final Colluders colluders;
  private @Nullable TimerTask pushTask;

  PoisoningRouteSetProtocol(PastryNode local, RoutingTable routingTable, Colluders colluders) {
    super(local, routingTable);
    this.local = local;
    this.routingTable = routingTable;
    this.colluders = colluders;
  }

  @Override
  public void messageForApplication(Message msg) {
    if (colluders.isAttacking() && msg instanceof RequestRouteRow request) {
      NodeHandle requester = request.returnHandle();
      thePastryNode.send(requester, new BroadcastRouteRow(local.getLocalHandle(),
          poisonedRow(requester.getNodeId(), request.getRow())), null, options);
      return;
    }
    super.messageForApplication(msg);
  }

  /** Starts the unsolicited pushes; called at the attack start. */
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
        pushToRandomVictim();
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

  private void pushToRandomVictim() {
    List<Oracle.Member> live = colluders.oracle().live();
    if (live.isEmpty()) return;
    Oracle.Member victim = live.get(colluders.random().nextInt(live.size()));
    if (victim.isMalicious || !victim.isReady()) return;
    NodeHandle victimHandle = victim.node.getLocalHandle();
    int topRow = routingTable.numRows() - 1;
    for (int row = topRow; row > topRow - PUSHED_ROW_COUNT && row >= 0; row--) {
      RouteSet[] poisoned = poisonedRow(victim.id, row);
      if (isEmpty(poisoned)) continue;
      thePastryNode.send(victimHandle, new BroadcastRouteRow(local.getLocalHandle(), poisoned), null, options);
    }
  }

  /**
   * Row {@code row} of a routing table owned by {@code owner}, with a live colluder in every column that has one: a
   * colluder whose most significant digit that differs from {@code owner} is digit {@code row}, in the column of its
   * own value of that digit.
   */
  private RouteSet[] poisonedRow(Id owner, int row) {
    int base = routingTable.baseBitLength();
    RouteSet[] result = new RouteSet[routingTable.numColumns()];
    for (Oracle.Member colluder : colluders.oracle().liveMalicious()) {
      if (!colluder.isReady() || colluder.id.equals(owner)) continue;
      if (owner.indexOfMostSignificantDifferingDigit(colluder.id, base) != row) continue;
      int column = colluder.id.getDigit(row, base);
      if (result[column] == null) {
        result[column] = new RouteSet(1, row, column, local, colluder.node.getLocalHandle());
      }
    }
    return result;
  }

  private static boolean isEmpty(RouteSet[] row) {
    for (RouteSet routeSet : row) {
      if (routeSet != null) return false;
    }
    return true;
  }
}
