package rice.p2p.past.gc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.Outcome;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.past.ObjectNotFoundException;
import rice.p2p.past.RefreshFailedException;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class GCRefreshResultsTest {

  private Environment env;
  private Id found;
  private Id missing;
  private Id silent;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    PastryIdFactory factory = new PastryIdFactory(env);
    found = factory.buildId("found");
    missing = factory.buildId("missing");
    silent = factory.buildId("silent");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("reports success, ObjectNotFoundException and RefreshFailedException per id, in input order")
  void reportsPerId() {
    Map<Id, Boolean> outcome = new HashMap<>();
    outcome.put(found, true);
    outcome.put(missing, false);

    List<Outcome<Boolean>> actual = ExpiringPastImpl.collectRefreshResults(new Id[] {missing, found, silent}, outcome);

    assertEquals(3, actual.size());
    assertInstanceOf(ObjectNotFoundException.class, assertInstanceOf(Outcome.Failure.class, actual.get(0)).cause());
    assertEquals(new Outcome.Success<Boolean>(true), actual.get(1));
    assertInstanceOf(RefreshFailedException.class, assertInstanceOf(Outcome.Failure.class, actual.get(2)).cause());
  }
}
