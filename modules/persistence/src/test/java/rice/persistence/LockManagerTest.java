package rice.persistence;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The {@link LockManager} grants a lock at once when it is free, and otherwise in FIFO order on unlock.
 */
class LockManagerTest {

  static final long WAIT_MILLIS = 10000;

  private Environment env;

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private Environment environment(boolean simulated) {
    env = simulated ? Environment.directEnvironment(1) : new Environment();
    return env;
  }

  @Test
  void lockManagerGrantsInOrder() throws Exception {
    environment(false);
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
