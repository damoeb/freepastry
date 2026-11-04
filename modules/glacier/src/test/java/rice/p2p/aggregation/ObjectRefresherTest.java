package rice.p2p.aggregation;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.VersioningPast;
import rice.p2p.past.Past;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ObjectRefresherTest {

  private static final long VERSION = 1;
  private static final long EXPIRATION = 1000;

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

  @Test
  void versionedRefreshReportsAnObjectStoreThatAnswersForFewerIdsThanAsked() throws Exception {
    Id first = idFactory.buildId("first");
    Id second = idFactory.buildId("second");
    AggregateList aggregateList = new AggregateList(dir.resolve("aggregates").toString(), "test", idFactory, false, "test", env);
    aggregateList.addAggregateDescriptor(new AggregateDescriptor(idFactory.buildId("aggregate"), EXPIRATION, new ObjectDescriptor[] {
      new ObjectDescriptor(first, VERSION, 0, 0, 1), new ObjectDescriptor(second, VERSION, 0, 0, 1)
    }, new Id[] {}));
    Past objectStore = (Past) Proxy.newProxyInstance(VersioningPast.class.getClassLoader(), new Class<?>[] {Past.class, VersioningPast.class}, (proxy, method, args) -> {
      if (method.getName().equals("refresh")) {
        @SuppressWarnings("unchecked")
        Continuation<Object[], Exception> command = (Continuation<Object[], Exception>) args[3];
        command.receiveResult(new Object[] {true});
      }
      return null;
    });
    AggregationSettings config = new AggregationSettings(AggregationConfig.fromParameters(env.getParameters()));
    ObjectRefresher testObj = new ObjectRefresher(objectStore, null, aggregateList, null, config, LoggerFactory.getLogger(ObjectRefresherTest.class));
    List<Object[]> results = new ArrayList<>();
    List<Exception> failures = new ArrayList<>();

    testObj.refresh(new Id[] {first, second}, new long[] {VERSION, VERSION}, new long[] {EXPIRATION, EXPIRATION}, new Continuation<Object[], Exception>() {
      @Override
      public void receiveResult(Object[] result) {
        results.add(result);
      }
      @Override
      public void receiveException(Exception e) {
        failures.add(e);
      }
    });

    assertEquals(List.of(), failures);
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals(true, results.get(0)[0]);
    assertInstanceOf(AggregationException.class, results.get(0)[1]);
  }
}
