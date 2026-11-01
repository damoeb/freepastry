package rice.p2p.aggregation;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdSet;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.StorageManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregateFlusherTest {

  private static final int OBJECT_SIZE = 10;
  private static final int MAX_OBJECTS_IN_AGGREGATE = 2;

  private Environment env;
  private PastryIdFactory idFactory;
  private AggregateFlusher testObj;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    env.getParameters().setInt("p2p_aggregation_maxObjectsInAggregate", MAX_OBJECTS_IN_AGGREGATE);
    env.getParameters().setInt("p2p_aggregation_maxAggregateSize", 1000 * OBJECT_SIZE);
    env.getParameters().setInt("p2p_aggregation_maxAggregatesPerRun", 100);
    idFactory = new PastryIdFactory(env);
    StorageManager waitingList = (StorageManager) Proxy.newProxyInstance(StorageManager.class.getClassLoader(), new Class<?>[] {StorageManager.class}, (proxy, method, args) -> {
      if (method.getName().equals("getMetadata")) {
        return new ObjectDescriptor((Id) args[0], 0, 0, 0, OBJECT_SIZE);
      }
      return null;
    });
    AggregationSettings config = new AggregationSettings(AggregationConfig.fromParameters(env.getParameters()));
    testObj = new AggregateFlusher(waitingList, null, null, null, config, LoggerFactory.getLogger(AggregateFlusherTest.class));
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void packBinsCountsTheObjectCarriedOverIntoTheNextBin() {
    IdSet waiting = idFactory.buildIdSet();
    for (int i = 0; i < 5; i++) {
      waiting.addId(idFactory.buildId("object" + i));
    }

    List<ObjectDescriptor[]> bins = testObj.packBins(waiting, new ArrayList<>());

    assertEquals(List.of(2, 2, 1), bins.stream().map(bin -> bin.length).toList());
  }
}
