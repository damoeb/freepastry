package rice.persistence.testing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;

/**
 * Pins the order in which an LRUCache evicts: the least recently cached, read or renamed object goes first.
 */
public class LRUCacheEvictionOrderTest {

  private static final int OBJECT_SIZE = 10;

  private Environment env;
  private PastryIdFactory factory;
  private LRUCache cache;
  private Id a;
  private Id b;
  private Id c;

  @BeforeEach
  public void setUp() {
    env = Environment.directEnvironment();
    factory = new PastryIdFactory(env);
    // room for two objects, not three
    cache = new LRUCache(new MemoryStorage(factory), 2 * OBJECT_SIZE + 5, env);
    a = factory.buildId("a");
    b = factory.buildId("b");
    c = factory.buildId("c");
    put(a);
    put(b);
  }

  @AfterEach
  public void tearDown() {
    env.destroy();
  }

  @Test
  public void evictsTheOldestInsertWhenNothingWasTouched() {
    put(c);

    assertFalse(cache.exists(a));
    assertTrue(cache.exists(b));
    assertTrue(cache.exists(c));
  }

  @Test
  public void readingAnObjectProtectsItFromEviction() {
    cache.getObject(a, noop());

    put(c);

    assertTrue(cache.exists(a));
    assertFalse(cache.exists(b));
    assertTrue(cache.exists(c));
  }

  @Test
  public void cachingAnExistingObjectAgainProtectsItFromEviction() {
    put(a);

    put(c);

    assertTrue(cache.exists(a));
    assertFalse(cache.exists(b));
  }

  @Test
  public void renamedObjectCountsAsMostRecentlyUsed() {
    Id renamed = factory.buildId("renamed");
    cache.rename(a, renamed, noop());

    put(c);

    assertTrue(cache.exists(renamed));
    assertFalse(cache.exists(b));
  }

  @Test
  public void anObjectLargerThanTheCacheIsNotCachedAndEvictsNothing() {
    boolean[] cached = {true};
    cache.cache(c, null, new byte[3 * OBJECT_SIZE], new Continuation<Boolean, Exception>() {
      @Override
      public void receiveResult(Boolean result) {
        cached[0] = result;
      }

      @Override
      public void receiveException(Exception exception) {
        throw new AssertionError(exception);
      }
    });

    assertFalse(cached[0]);
    assertTrue(cache.exists(a));
    assertTrue(cache.exists(b));
  }

  private void put(Id id) {
    cache.cache(id, null, new byte[OBJECT_SIZE], noop());
  }

  private static <T> Continuation<T, Exception> noop() {
    return new Continuation<T, Exception>() {
      @Override
      public void receiveResult(T result) {}

      @Override
      public void receiveException(Exception exception) {
        throw new AssertionError(exception);
      }
    };
  }
}
