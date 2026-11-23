package rice.persistence;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.testing.TestStorageSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Pins where and when the persistence implementations deliver: PersistentStorage on the selector thread after the
 * call returned (its IO runs on the blocking IO thread), the in-memory ones synchronously inside the call, and
 * the LockManager grants in FIFO order.
 */
class StorageDeliveryTest {

  static final long WAIT_MILLIS = 10000;

  @TempDir
  Path root;

  private final List<Environment> environments = new ArrayList<Environment>();

  @AfterEach
  void tearDown() {
    for (Environment environment : environments) environment.destroy();
  }

  private Environment environment(boolean simulated) {
    Environment environment = simulated ? Environment.directEnvironment(1) : new Environment();
    environments.add(environment);
    return environment;
  }

  /** The outcome of an operation; whether it came on the selector thread, and before the call returned. */
  record Delivery(Object outcome, boolean onSelector, boolean synchronous) {
  }

  /** Starts {@code operation} on the selector thread and waits for what it delivers. */
  static Delivery deliveryOf(Environment environment, Consumer<Continuation<Object, Exception>> operation) throws Exception {
    CompletableFuture<Delivery> delivered = new CompletableFuture<Delivery>();
    boolean[] returned = {false};
    Continuation<Object, Exception> recorder = new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        delivered.complete(new Delivery(result, environment.getSelectorManager().isSelectorThread(), !returned[0]));
      }

      @Override
      public void receiveException(Exception exception) {
        delivered.complete(new Delivery(exception, environment.getSelectorManager().isSelectorThread(), !returned[0]));
      }
    };
    environment.getSelectorManager().invoke(() -> {
      try {
        operation.accept(recorder);
      } finally {
        returned[0] = true;
      }
    });
    return delivered.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
  }

  private static Delivery onSelectorLater(Object outcome) {
    return new Delivery(outcome, true, false);
  }

  private static Delivery synchronous(Object outcome) {
    return new Delivery(outcome, true, true);
  }

  private void pinPersistentStorage(boolean simulated) throws Exception {
    Environment env = environment(simulated);
    PastryIdFactory factory = new PastryIdFactory(env);
    PersistentStorage storage = new PersistentStorage(factory, new TestStorageSerializer(), root.toString(), 1000000, env);
    Id id = factory.buildId("pinned");
    Id renamed = factory.buildId("renamed");

    assertEquals(onSelectorLater(true), deliveryOf(env, c -> storage.store(id, "meta", "value", c)));
    assertEquals(synchronous(false), deliveryOf(env, c -> storage.store(id, "meta", null, c)), "nothing to store");
    assertEquals(onSelectorLater("value"), deliveryOf(env, c -> storage.getObject(id, c)));
    assertEquals(synchronous(null), deliveryOf(env, c -> storage.getObject(factory.buildId("missing"), c)), "not stored");
    assertEquals(onSelectorLater(true), deliveryOf(env, c -> storage.setMetadata(id, "meta2", c)));
    assertEquals(synchronous(false), deliveryOf(env, c -> storage.setMetadata(factory.buildId("missing"), "meta", c)));
    assertEquals(onSelectorLater(true), deliveryOf(env, c -> storage.rename(id, renamed, c)));
    assertEquals(onSelectorLater(true), deliveryOf(env, c -> storage.unstore(renamed, c)));
    assertEquals(onSelectorLater(true), deliveryOf(env, storage::flush));
  }

  @Test
  void persistentStorageDeliversOnTheSelectorAfterTheCall() throws Exception {
    pinPersistentStorage(false);
  }

  @Test
  void persistentStorageDeliversOnTheSelectorUnderTheSimulator() throws Exception {
    pinPersistentStorage(true);
  }

  @Test
  void blockingIODeliversTheExceptionOfTheWorkOnTheSelector() throws Exception {
    Environment env = environment(false);
    BlockingIO blockingIO = new BlockingIO(env);
    IOException failure = new IOException("disk");
    Delivery delivery = deliveryOf(env, c -> blockingIO.<Object>run(() -> "failing", c, () -> {
      throw failure;
    }));
    assertSame(failure, delivery.outcome());
    assertEquals(onSelectorLater(failure), delivery);
  }

  @Test
  void memoryStorageDeliversInsideTheCall() throws Exception {
    Environment env = environment(false);
    PastryIdFactory factory = new PastryIdFactory(env);
    MemoryStorage storage = new MemoryStorage(factory);
    Id id = factory.buildId("pinned");
    assertEquals(synchronous(true), deliveryOf(env, c -> storage.store(id, "meta", "value", c)));
    assertEquals(synchronous("value"), deliveryOf(env, c -> storage.getObject(id, c)));
    assertEquals(synchronous(true), deliveryOf(env, c -> storage.setMetadata(id, "meta2", c)));
    assertEquals(synchronous(true), deliveryOf(env, c -> storage.rename(id, factory.buildId("renamed"), c)));
    assertEquals(synchronous(false), deliveryOf(env, c -> storage.unstore(id, c)));
    assertEquals(synchronous(true), deliveryOf(env, storage::flush));
  }

  @Test
  void lruCacheDeliversWhenItsStorageDoes() throws Exception {
    Environment env = environment(false);
    PastryIdFactory factory = new PastryIdFactory(env);
    Id id = factory.buildId("pinned");
    LRUCache inMemory = new LRUCache(new MemoryStorage(factory), 1000000, env);
    assertEquals(synchronous(true), deliveryOf(env, c -> inMemory.cache(id, "meta", "value", c)));
    assertEquals(synchronous("value"), deliveryOf(env, c -> inMemory.getObject(id, c)));
    assertEquals(synchronous(true), deliveryOf(env, c -> inMemory.setMaximumSize(1, c)), "evicts synchronously");
    assertEquals(synchronous(null), deliveryOf(env, c -> inMemory.getObject(id, c)), "evicted");

    LRUCache onDisk = new LRUCache(new PersistentStorage(factory, new TestStorageSerializer(), root.toString(), 1000000, env), 1000000, env);
    assertEquals(onSelectorLater(true), deliveryOf(env, c -> onDisk.cache(id, "meta", "value", c)));
    assertEquals(onSelectorLater("value"), deliveryOf(env, c -> onDisk.getObject(id, c)));
    assertEquals(onSelectorLater(true), deliveryOf(env, c -> onDisk.uncache(id, c)));
  }

  @Test
  void cachedStorageFallsBackToTheStorageAndNoOpCacheFailsRename() throws Exception {
    Environment env = environment(false);
    PastryIdFactory factory = new PastryIdFactory(env);
    Id id = factory.buildId("pinned");
    MemoryStorage storage = new MemoryStorage(factory);
    CachedStorageImpl cached = new CachedStorageImpl(factory, storage, new NoOpCache(factory));
    assertEquals(synchronous(true), deliveryOf(env, c -> cached.store(id, "meta", "value", c)));
    assertEquals(synchronous("value"), deliveryOf(env, c -> cached.getObject(id, c)), "missed by the cache, found in the storage");
    Delivery renameOfNoOpCache = deliveryOf(env, c -> cached.rename(id, factory.buildId("renamed"), c));
    assertInstanceOf(IllegalArgumentException.class, renameOfNoOpCache.outcome());
    assertEquals(true, renameOfNoOpCache.synchronous());
  }

  @Test
  void lockManagerGrantsInOrder() throws Exception {
    Environment env = environment(false);
    PastryIdFactory factory = new PastryIdFactory(env);
    Id id = factory.buildId("locked");
    LockManager locks = new LockManager(env);
    List<String> granted = new CopyOnWriteArrayList<String>();
    CompletableFuture<Void> done = new CompletableFuture<Void>();
    env.getSelectorManager().invoke(() -> {
      locks.lock(id, grantTo("first", granted));
      locks.lock(id, grantTo("second", granted));
      locks.lock(id, grantTo("third", granted));
      granted.add("queued");
      locks.unlock(id);
      granted.add("unlocked once");
      locks.unlock(id);
      locks.unlock(id);
      locks.lock(id, grantTo("fourth", granted));
      done.complete(null);
    });
    done.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("first", "queued", "second", "unlocked once", "third", "fourth"), granted);
  }

  private static Continuation<Object, Exception> grantTo(String name, List<String> granted) {
    return new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        granted.add(name);
      }

      @Override
      public void receiveException(Exception exception) {
        granted.add(name + " failed");
      }
    };
  }
}
