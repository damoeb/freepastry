package rice.persistence.testing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

/**
 * Pins the order in which an LRUCache evicts: the least recently cached, read or renamed object goes first.
 */
public class LRUCacheEvictionOrderTest {

  private static final int OBJECT_SIZE = 10;

  private Environment env;
  private PastryIdFactory factory;
  private LRUCache cache;
  private Id firstId;
  private Id secondId;
  private Id thirdId;

  @BeforeEach
  public void setUp() {
    env = Environment.directEnvironment();
    factory = new PastryIdFactory(env);
    // room for two objects, not three
    cache = new LRUCache(new MemoryStorage(factory), 2 * OBJECT_SIZE + 5, env);
    firstId = factory.buildId("a");
    secondId = factory.buildId("b");
    thirdId = factory.buildId("c");
    put(firstId);
    put(secondId);
  }

  @AfterEach
  public void tearDown() {
    env.destroy();
  }

  @Test
  public void evictsTheOldestInsertWhenNothingWasTouched() {
    put(thirdId);

    assertFalse(cache.exists(firstId));
    assertTrue(cache.exists(secondId));
    assertTrue(cache.exists(thirdId));
  }

  @Test
  public void readingAnObjectProtectsItFromEviction() {
    Futures.forward(cache.getObject(firstId), noop());

    put(thirdId);

    assertTrue(cache.exists(firstId));
    assertFalse(cache.exists(secondId));
    assertTrue(cache.exists(thirdId));
  }

  @Test
  public void cachingAnExistingObjectAgainProtectsItFromEviction() {
    put(firstId);

    put(thirdId);

    assertTrue(cache.exists(firstId));
    assertFalse(cache.exists(secondId));
  }

  @Test
  public void renamedObjectCountsAsMostRecentlyUsed() {
    Id renamed = factory.buildId("renamed");
    Futures.forward(cache.rename(firstId, renamed), noop());

    put(thirdId);

    assertTrue(cache.exists(renamed));
    assertFalse(cache.exists(secondId));
  }

  @Test
  public void anObjectLargerThanTheCacheIsNotCachedAndEvictsNothing() {
    boolean[] cached = {true};
    Futures.forward(cache.cache(thirdId, null, new byte[3 * OBJECT_SIZE]), new Continuation<Boolean, Exception>() {
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
    assertTrue(cache.exists(firstId));
    assertTrue(cache.exists(secondId));
  }

  private void put(Id id) {
    Futures.forward(cache.cache(id, null, new byte[OBJECT_SIZE]), noop());
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
