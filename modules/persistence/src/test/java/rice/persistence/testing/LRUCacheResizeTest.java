package rice.persistence.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.environment.Environment;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;

/**
 * Regression test: shrinking an LRUCache whose storage holds nothing the cache tracks must not run off an empty list.
 */
public class LRUCacheResizeTest {

  private Environment env;
  private PastryIdFactory factory;
  private MemoryStorage storage;
  private LRUCache cache;

  @BeforeEach
  public void setUp() {
    env = Environment.directEnvironment();
    factory = new PastryIdFactory(env);
    storage = new MemoryStorage(factory);
    cache = new LRUCache(storage, 100000, env);
  }

  @AfterEach
  public void tearDown() {
    env.destroy();
  }

  @Test
  public void shrinkingBelowUntrackedContentReportsAnExceptionInsteadOfThrowing() {
    // stored behind the cache's back, so the cache has nothing to evict
    storage.store(factory.buildId("untracked"), null, "value", new Recorder());
    Recorder shrunk = new Recorder();

    cache.setMaximumSize(1, shrunk);

    assertEquals(1, shrunk.results.size());
    assertInstanceOf(IllegalStateException.class, shrunk.results.get(0));
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
