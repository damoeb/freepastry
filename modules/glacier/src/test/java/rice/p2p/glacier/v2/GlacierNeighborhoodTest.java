package rice.p2p.glacier.v2;

import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.NodeHandleSet;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class GlacierNeighborhoodTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlacierNeighborhoodTest.class);
  private static final long NEIGHBOR_TIMEOUT = 10_000;

  private Environment env;
  private PastryIdFactory factory;
  private Id localId;
  private Id staleNeighbor;
  private Map<Id, Long> storedNeighbors;
  private List<Runnable> pendingUnstores;
  private GlacierNeighborhood testObj;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    localId = factory.buildId("local");
    staleNeighbor = factory.buildId("stale neighbor");
    storedNeighbors = new LinkedHashMap<>();
    pendingUnstores = new ArrayList<>();

    // a neighbor storage that answers reads at once but completes unstore() only when the test says so, as a disk-backed storage does
    var neighborStorage = GlacierFakes.storage((proxy, method, args) -> {
      switch (method.getName()) {
        case "scan": {
          IdSet ids = factory.buildIdSet();
          for (Id id : storedNeighbors.keySet())
            ids.addId(id);
          return ids;
        }
        case "getObject":
          return CompletableFuture.completedFuture(storedNeighbors.get((Id) args[0]));
        case "unstore": {
          CompletableFuture<Boolean> unstored = new CompletableFuture<Boolean>();
          pendingUnstores.add(() -> {
            storedNeighbors.remove((Id) args[0]);
            unstored.complete(true);
          });
          return unstored;
        }
        default:
          return null;
      }
    });
    testObj = new GlacierNeighborhood(neighborStorage, factory, localId, NEIGHBOR_TIMEOUT, env, LOGGER);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void theResponsibleRangeIsRecomputedOnceAnExpiredNeighborHasBeenRemoved() {
    ((DirectTimeSource) env.getTimeSource()).incrementTime((int) (5 * NEIGHBOR_TIMEOUT));
    storedNeighbors.put(staleNeighbor, 0L);
    NodeHandleSet emptyLeafSet = (NodeHandleSet) Proxy.newProxyInstance(NodeHandleSet.class.getClassLoader(), new Class<?>[] {NodeHandleSet.class}, (proxy, method, args) -> method.getName().equals("containsHandleFor") ? Boolean.FALSE : null);

    testObj.expireNeighbors(emptyLeafSet);
    var rangeWhileTheNeighborIsStillStored = testObj.getResponsibleRange();
    assertEquals(1, pendingUnstores.size(), "the stale neighbor should be unstored");

    pendingUnstores.get(0).run();

    var aloneOnTheRing = factory.buildIdRange(localId, localId);
    assertNotEquals(aloneOnTheRing, rangeWhileTheNeighborIsStillStored);
    assertEquals(aloneOnTheRing, testObj.getResponsibleRange(), "the range still accounts for the neighbor that was removed");
  }
}
