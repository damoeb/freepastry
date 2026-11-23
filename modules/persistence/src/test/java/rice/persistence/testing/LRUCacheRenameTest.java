package rice.persistence.testing;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test: LRUCache.rename() must call back its continuation.
 */
public class LRUCacheRenameTest {

  private Environment env;
  private PastryIdFactory factory;
  private LRUCache cache;

  @BeforeEach
  public void setUp() {
    env = Environment.directEnvironment();
    factory = new PastryIdFactory(env);
    cache = new LRUCache(new MemoryStorage(factory), 100000, env);
  }

  @AfterEach
  public void tearDown() {
    env.destroy();
  }

  @Test
  public void renameCallsBackWithResult() {
    Id oldId = factory.buildId("old");
    Id newId = factory.buildId("new");
    Recorder stored = new Recorder();
    Futures.forward(cache.cache(oldId, null, "value"), stored);
    assertEquals(List.of(Boolean.TRUE), stored.results);

    Recorder renamed = new Recorder();
    Futures.forward(cache.rename(oldId, newId), renamed);

    assertEquals(List.of(Boolean.TRUE), renamed.results);
    assertTrue(cache.exists(newId));
    assertFalse(cache.exists(oldId));
  }

  @Test
  public void renameOfMissingIdCallsBackFalse() {
    Recorder renamed = new Recorder();
    Futures.forward(cache.rename(factory.buildId("missing"), factory.buildId("new")), renamed);

    assertEquals(List.of(Boolean.FALSE), renamed.results);
  }

  private static class Recorder implements Continuation<Boolean, Exception> {
    final List<Object> results = new ArrayList<Object>();

    @Override
    public void receiveResult(Boolean result) {
      results.add(result);
    }

    @Override
    public void receiveException(Exception exception) {
      results.add(exception);
    }
  }
}
