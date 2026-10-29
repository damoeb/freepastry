package rice.persistence.testing;

import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import rice.environment.Environment;

/**
 * Runs the step-based storage test programs (MemoryStorageTest, PersistentStorageTest, LRUCacheTest), which fail
 * through {@link Test#await(long)}. (Test is the step-based base class here, hence the qualified Jupiter annotation.)
 */
public class StorageProgramsTest {

  private static final long TIMEOUT_MILLIS = 60000;

  private Environment env;

  @AfterEach
  public void destroyEnvironment() {
    if (env != null) env.destroy();
  }

  @org.junit.jupiter.api.Test
  public void memoryStorage() throws Exception {
    env = new Environment();
    runSteps(new MemoryStorageTest(true, env));
  }

  @org.junit.jupiter.api.Test
  public void persistentStorage(@TempDir Path dir) throws Exception {
    env = new Environment();
    runSteps(new PersistentStorageTest(true, env, dir.toString()));
  }

  @org.junit.jupiter.api.Test
  public void lruCache() throws Exception {
    env = new Environment();
    runSteps(new LRUCacheTest(env));
  }

  private static void runSteps(Test test) throws InterruptedException {
    test.start();
    test.await(TIMEOUT_MILLIS);
  }
}
