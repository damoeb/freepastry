package rice.persistence;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.Futures;
import rice.environment.Environment;
import rice.environment.EnvironmentDestroyedException;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.testing.TestStorageSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Where and when the futures of the persistence implementations complete: PersistentStorage on the selector
 * thread after the call returned (its IO runs on the blocking IO thread, or on the selector under the
 * simulator), the in-memory ones before the call returns, as their continuations were called before.
 */
class StorageFutureTest {

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

  /** The outcome of an operation; whether it completed on the selector thread, and before the call returned. */
  record Completion(Object outcome, boolean onSelector, boolean synchronous) {
  }

  /** Starts {@code operation} on the selector thread and waits for its future. */
  static Completion completionOf(Environment environment, Supplier<CompletableFuture<?>> operation) throws Exception {
    CompletableFuture<Completion> completed = new CompletableFuture<Completion>();
    environment.getSelectorManager().invoke(() -> {
      CompletableFuture<?> future = operation.get();
      boolean synchronous = future.isDone();
      var _ = future.whenComplete((value, throwable) -> completed.complete(new Completion(
          throwable == null ? value : Futures.unwrap(throwable), environment.getSelectorManager().isSelectorThread(), synchronous)));
    });
    return completed.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
  }

  private static Completion later(Object outcome) {
    return new Completion(outcome, true, false);
  }

  private static Completion synchronous(Object outcome) {
    return new Completion(outcome, true, true);
  }

  private void checkPersistentStorage(boolean simulated) throws Exception {
    Environment env = environment(simulated);
    PastryIdFactory factory = new PastryIdFactory(env);
    PersistentStorage storage = new PersistentStorage(factory, new TestStorageSerializer(), root.toString(), 1000000, env);
    Id id = factory.buildId("pinned");
    Id renamed = factory.buildId("renamed");

    assertEquals(later(true), completionOf(env, () -> storage.store(id, "meta", "value")));
    assertEquals(synchronous(false), completionOf(env, () -> storage.store(id, "meta", null)), "nothing to store");
    assertEquals(later("value"), completionOf(env, () -> storage.getObject(id)));
    assertEquals(synchronous(null), completionOf(env, () -> storage.getObject(factory.buildId("missing"))), "not stored");
    assertEquals(later(true), completionOf(env, () -> storage.setMetadata(id, "meta2")));
    assertEquals(synchronous(false), completionOf(env, () -> storage.setMetadata(factory.buildId("missing"), "meta")));
    assertEquals(later(true), completionOf(env, () -> storage.rename(id, renamed)));
    assertEquals(later(true), completionOf(env, () -> storage.unstore(renamed)));
    assertEquals(later(true), completionOf(env, storage::flush));
  }

  @Test
  void persistentStorageCompletesOnTheSelectorAfterTheCall() throws Exception {
    checkPersistentStorage(false);
  }

  @Test
  void persistentStorageCompletesOnTheSelectorUnderTheSimulator() throws Exception {
    checkPersistentStorage(true);
  }

  @Test
  void persistentStorageFailsWithTheExceptionOfTheBlockingIO() throws Exception {
    Environment env = environment(false);
    PastryIdFactory factory = new PastryIdFactory(env);
    PersistentStorage storage = new PersistentStorage(factory, new TestStorageSerializer(), root.toString(), 1000000, env);
    Id id = factory.buildId("unserializable");
    Completion completion = completionOf(env, () -> storage.store(id, null, new Object()));
    assertInstanceOf(IOException.class, completion.outcome());
    assertTrue(completion.onSelector());
  }

  @Test
  void memoryStorageCompletesInsideTheCall() throws Exception {
    Environment env = environment(false);
    PastryIdFactory factory = new PastryIdFactory(env);
    MemoryStorage storage = new MemoryStorage(factory);
    Id id = factory.buildId("pinned");
    assertEquals(synchronous(true), completionOf(env, () -> storage.store(id, "meta", "value")));
    assertEquals(synchronous("value"), completionOf(env, () -> storage.getObject(id)));
    assertEquals(synchronous(true), completionOf(env, () -> storage.setMetadata(id, "meta2")));
    assertEquals(synchronous(true), completionOf(env, () -> storage.rename(id, factory.buildId("renamed"))));
    assertEquals(synchronous(false), completionOf(env, () -> storage.unstore(id)));
    assertEquals(synchronous(true), completionOf(env, storage::flush));
  }

  @Test
  void lruCacheCompletesWhenItsStorageDoes() throws Exception {
    Environment env = environment(false);
    PastryIdFactory factory = new PastryIdFactory(env);
    Id id = factory.buildId("pinned");
    LRUCache inMemory = new LRUCache(new MemoryStorage(factory), 1000000, env);
    assertEquals(synchronous(true), completionOf(env, () -> inMemory.cache(id, "meta", "value")));
    assertEquals(synchronous("value"), completionOf(env, () -> inMemory.getObject(id)));
    assertEquals(synchronous(true), completionOf(env, () -> inMemory.setMaximumSize(1)), "evicts synchronously");
    assertEquals(synchronous(null), completionOf(env, () -> inMemory.getObject(id)), "evicted");

    LRUCache onDisk = new LRUCache(new PersistentStorage(factory, new TestStorageSerializer(), root.toString(), 1000000, env), 1000000, env);
    assertEquals(later(true), completionOf(env, () -> onDisk.cache(id, "meta", "value")));
    assertEquals(later("value"), completionOf(env, () -> onDisk.getObject(id)));
    assertEquals(later(true), completionOf(env, () -> onDisk.uncache(id)));
  }

  @Test
  void cachedStorageFallsBackToTheStorageAndNoOpCacheFailsRename() throws Exception {
    Environment env = environment(false);
    PastryIdFactory factory = new PastryIdFactory(env);
    Id id = factory.buildId("pinned");
    CachedStorageImpl cached = new CachedStorageImpl(factory, new MemoryStorage(factory), new NoOpCache(factory));
    assertEquals(synchronous(true), completionOf(env, () -> cached.store(id, "meta", "value")));
    assertEquals(synchronous("value"), completionOf(env, () -> cached.getObject(id)), "missed by the cache, found in the storage");
    Completion renameOfNoOpCache = completionOf(env, () -> cached.rename(id, factory.buildId("renamed")));
    assertInstanceOf(IllegalArgumentException.class, renameOfNoOpCache.outcome());
    assertTrue(renameOfNoOpCache.synchronous());
  }

  @Test
  void aPendingOperationFailsWhenTheEnvironmentIsDestroyed() throws Exception {
    Environment env = Environment.directEnvironment(1);
    PastryIdFactory factory = new PastryIdFactory(env);
    PersistentStorage storage = new PersistentStorage(factory, new TestStorageSerializer(), root.toString(), 1000000, env);
    CompletableFuture<CompletableFuture<Boolean>> started = new CompletableFuture<CompletableFuture<Boolean>>();
    env.getSelectorManager().invoke(() -> {
      started.complete(storage.store(factory.buildId("pending"), null, "value"));
      env.destroy();
    });
    CompletableFuture<Boolean> pending = started.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    Throwable failure = pending.handle((value, throwable) -> throwable).get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    // the simulator's blocking IO was still queued on the selector when destroy() failed the tracked future
    assertInstanceOf(EnvironmentDestroyedException.class, failure);
  }
}
