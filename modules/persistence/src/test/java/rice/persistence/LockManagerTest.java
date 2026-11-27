package rice.persistence;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.environment.EnvironmentDestroyedException;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@link LockManager} grants a lock at once when it is free, and otherwise in FIFO order on unlock, on the
 * thread that unlocks.
 */
class LockManagerTest {

  static final long WAIT_MILLIS = 10000;

  private Environment env;

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private static void grantTo(String name, CompletableFuture<Void> grant, List<String> granted, Environment env) {
    var _ = grant.thenRun(() -> granted.add(name + (env.getSelectorManager().isSelectorThread() ? "" : " off the selector")));
  }

  @Test
  void lockManagerGrantsInOrder() throws Exception {
    env = new Environment();
    Id id = new PastryIdFactory(env).buildId("locked");
    LockManager locks = new LockManager(env);
    List<String> granted = new CopyOnWriteArrayList<String>();
    CompletableFuture<Void> done = new CompletableFuture<Void>();
    env.getSelectorManager().invoke(() -> {
      grantTo("first", locks.lock(id), granted, env);
      grantTo("second", locks.lock(id), granted, env);
      grantTo("third", locks.lock(id), granted, env);
      granted.add("queued");
      locks.unlock(id);
      granted.add("unlocked once");
      locks.unlock(id);
      locks.unlock(id);
      grantTo("fourth", locks.lock(id), granted, env);
      done.complete(null);
    });
    done.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("first", "queued", "second", "unlocked once", "third", "fourth"), granted);
  }

  @Test
  void aCancelledRequestIsSkipped() throws Exception {
    env = new Environment();
    Id id = new PastryIdFactory(env).buildId("locked");
    LockManager locks = new LockManager(env);
    List<String> granted = new CopyOnWriteArrayList<String>();
    CompletableFuture<Void> done = new CompletableFuture<Void>();
    env.getSelectorManager().invoke(() -> {
      grantTo("holder", locks.lock(id), granted, env);
      CompletableFuture<Void> impatient = locks.lock(id);
      grantTo("waiter", locks.lock(id), granted, env);
      impatient.cancel(false);
      locks.unlock(id);
      done.complete(null);
    });
    done.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("holder", "waiter"), granted);
  }

  @Test
  void aQueuedRequestFailsWhenTheEnvironmentIsDestroyed() throws Exception {
    env = new Environment();
    Id id = new PastryIdFactory(env).buildId("locked");
    LockManager locks = new LockManager(env);
    CompletableFuture<CompletableFuture<Void>> queued = new CompletableFuture<CompletableFuture<Void>>();
    env.getSelectorManager().invoke(() -> {
      assertTrue(locks.lock(id).isDone(), "free, granted at once");
      queued.complete(locks.lock(id));
    });
    CompletableFuture<Void> waiting = queued.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    env.destroy();
    Throwable failure = waiting.handle((value, throwable) -> throwable).get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertInstanceOf(EnvironmentDestroyedException.class, failure);
  }
}
