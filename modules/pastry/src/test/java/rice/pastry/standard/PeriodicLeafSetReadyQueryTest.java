package rice.pastry.standard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeListener;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.routing.RoutingTable;

/**
 * {@link PeriodicLeafSetProtocol#isReady()} is a query: asked off the selector thread when the leases no longer
 * match the committed ready state, it must not change that state there, but leave the change to the selector.
 */
class PeriodicLeafSetReadyQueryTest {

  private static final long TIMEOUT_SECONDS = 10;

  private Environment env;
  private PastryNode node;
  private PeriodicLeafSetProtocol leafSetProtocol;
  private final CompletableFuture<Boolean> nextReadyChange = new CompletableFuture<Boolean>();
  private volatile boolean wasReadyChangeOnTheSelector = false;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    node = new DirectPastryNodeFactory(new RandomNodeIdFactory(env),
        new EuclideanNetwork<DirectNodeHandle, RawMessage>(env), env) {
      @Override
      protected LeafSetProtocol getLeafSetProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable) {
        leafSetProtocol = new PeriodicLeafSetProtocol(pn, pn.getLocalHandle(), leafSet, routeTable);
        leafSetProtocol.register();
        return leafSetProtocol;
      }
    }.newNode();
    CompletableFuture<Void> isBooted = new CompletableFuture<Void>();
    node.addPastryNodeListener(new PastryNodeListener() {
      @Override
      public void pastryNodeReady(PastryNode pastryNode, boolean ready) {
        if (ready) {
          isBooted.complete(null);
        }
      }
    });
    node.boot((Object) null);
    isBooted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private void runOnSelector(Runnable task) throws Exception {
    CompletableFuture<Void> done = new CompletableFuture<Void>();
    env.getSelectorManager().invoke(() -> {
      try {
        task.run();
        done.complete(null);
      } catch (Throwable t) {
        done.completeExceptionally(t);
      }
    });
    done.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("a ready query off the selector thread leaves the state change to the selector")
  void readyQueryOffTheSelectorChangesTheStateOnTheSelector() throws Exception {
    runOnSelector(() -> {
      assertTrue(leafSetProtocol.hasSetStrategy, "the leaf set protocol decides readiness after boot");
      // the leases (none needed with an empty leaf set) say ready, the committed state still says not ready
      leafSetProtocol.ready = false;
      node.addPastryNodeListener(new PastryNodeListener() {
        @Override
        public void pastryNodeReady(PastryNode pastryNode, boolean ready) {
          wasReadyChangeOnTheSelector = env.getSelectorManager().isSelectorThread();
          nextReadyChange.complete(ready);
        }
      });
    });

    boolean actual = node.isReady();

    assertFalse(actual, "off the selector the query answers with the committed state");
    assertTrue(nextReadyChange.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(wasReadyChangeOnTheSelector, "the ready state changed off the selector thread");
    runOnSelector(() -> assertEquals(true, leafSetProtocol.ready));
  }
}
