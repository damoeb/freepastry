package rice.p2p.aggregation;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.past.Past;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.ExpiringPastContent;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregateTreeTest {

  @TempDir
  Path dir;

  private Environment env;
  private PastryIdFactory idFactory;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    idFactory = new PastryIdFactory(env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /** An aggregate store whose futures are complete when they are returned. */
  private Past synchronousStore(Map<Id, Aggregate> aggregates) {
    return (Past) Proxy.newProxyInstance(Past.class.getClassLoader(), new Class<?>[] {Past.class}, (proxy, method, args) -> {
      switch (method.getName()) {
        case "lookupHandles" -> {
          Id id = (Id) args[0];
          PastContentHandle handle = (PastContentHandle) Proxy.newProxyInstance(Past.class.getClassLoader(), new Class<?>[] {PastContentHandle.class}, (p, m, a) -> m.getName().equals("getId") ? id : null);
          return CompletableFuture.completedFuture(new PastContentHandle[] {handle});
        }
        case "fetch" -> {
          return CompletableFuture.completedFuture(aggregates.get(((PastContentHandle) args[0]).getId()));
        }
        default -> { }
      }
      return null;
    });
  }

  private Aggregate aggregate(Id id, Id... pointers) {
    Aggregate aggregate = new Aggregate(new ExpiringPastContent[] {}, pointers);
    aggregate.setId(id);
    return aggregate;
  }

  @Test
  void rebuildCompletesOnceWhenTheStoreAnswersSynchronously() throws Exception {
    Id root = idFactory.buildId("root");
    Id left = idFactory.buildId("left");
    Id right = idFactory.buildId("right");
    AggregateList aggregateList = new AggregateList(dir.resolve("aggregates").toString(), "test", idFactory, false, "test", env);
    AggregationSettings config = new AggregationSettings(AggregationConfig.fromParameters(env.getParameters()));
    AggregateTree testObj = new AggregateTree(
      synchronousStore(Map.of(root, aggregate(root, left, right), left, aggregate(left), right, aggregate(right))),
      null, aggregateList, null, null, config, env.getTimeSource(), LoggerFactory.getLogger(AggregateTreeTest.class));
    List<Boolean> completedWithAllAggregates = new ArrayList<>();
    List<Exception> failures = new ArrayList<>();

    testObj.setHandle(root, new Continuation<Boolean, Exception>() {
      @Override
      public void receiveResult(Boolean result) {
        completedWithAllAggregates.add(aggregateList.getAggregateDescriptor(root) != null && aggregateList.getAggregateDescriptor(left) != null && aggregateList.getAggregateDescriptor(right) != null);
      }
      @Override
      public void receiveException(Exception e) {
        failures.add(e);
      }
    });

    assertEquals(List.of(), failures);
    assertEquals(List.of(true), completedWithAllAggregates);
  }
}
