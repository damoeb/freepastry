package rice.persistence;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.Continuation;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.testing.TestStorageSerializer;

/**
 * Flushing a persistent storage that has metadata cache files.
 */
class FlushTest {

  @TempDir
  Path root;

  private Environment env;
  private PastryIdFactory factory;
  private PersistentStorage storage;

  @BeforeEach
  void setUp() throws Exception {
    env = new Environment();
    factory = new PastryIdFactory(env);
    storage = new PersistentStorage(factory, new TestStorageSerializer(), root.toString(), 1000000, env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private <T> T await(java.util.function.Consumer<Continuation<T, Exception>> operation) throws Exception {
    CompletableFuture<T> result = new CompletableFuture<T>();
    operation.accept(new Continuation<T, Exception>() {
      @Override
      public void receiveResult(T value) {
        result.complete(value);
      }

      @Override
      public void receiveException(Exception e) {
        result.completeExceptionally(e);
      }
    });
    return result.get(30, TimeUnit.SECONDS);
  }

  @Test
  void flushRemovesTheMetadataCacheFilesWithoutTreatingThemAsObjects() throws Exception {
    Id id = factory.buildId("flushed");
    assertTrue(this.<Boolean>await(continuation -> Futures.forward(storage.store(id, null, "value"), continuation)));
    Path appDirectory = root.resolve(PersistentStorage.BACKUP_DIRECTORY).resolve("default");
    List<Path> directories;
    try (Stream<Path> walk = Files.walk(appDirectory)) {
      directories = walk.filter(Files::isDirectory).toList();
    }
    for (Path directory : directories) {
      Files.write(directory.resolve(PersistentStorage.METADATA_FILENAME), new byte[] {1, 2, 3});
    }

    assertTrue(this.<Boolean>await(continuation -> Futures.forward(storage.flush(), continuation)));

    assertFalse(storage.exists(id));
    try (Stream<Path> walk = Files.walk(appDirectory)) {
      assertTrue(walk.noneMatch(p -> p.getFileName().toString().equals(PersistentStorage.METADATA_FILENAME)));
    }
  }
}
