package rice.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.testing.TestStorageSerializer;

/**
 * Stored objects may be private to the node's owner (apps.md AP20): the storage creates its directories
 * {@code rwx------} and its files {@code rw-------}, whatever the umask, so that no other local user reads them.
 */
class FilePermissionsTest {
  @TempDir
  Path parent;

  private Environment env = new Environment();

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("the storage creates its directories and files readable by the owner only")
  void directoriesAndFilesAreOwnerOnly() throws Exception {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "POSIX permissions");
    Path root = parent.resolve("storage");
    PastryIdFactory factory = new PastryIdFactory(env);
    PersistentStorage storage = new PersistentStorage(factory, new TestStorageSerializer(), root.toString(), 1000000, env);
    Id id = factory.buildId("private");
    assertTrue(Futures.await(storage.store(id, "metadata", "value"), env.getSelectorManager()));
    env.destroy();
    // reopening it writes the metadata cache files
    env = new Environment();
    new PersistentStorage(new PastryIdFactory(env), new TestStorageSerializer(), root.toString(), 1000000, env);

    List<Path> entries;
    try (Stream<Path> walk = Files.walk(root)) {
      entries = walk.toList();
    }
    assertTrue(entries.stream().anyMatch(Files::isRegularFile), "the storage wrote files: " + entries);
    for (Path entry : entries) {
      String expected = Files.isDirectory(entry) ? "rwx------" : "rw-------";
      assertEquals(expected, PosixFilePermissions.toString(Files.getPosixFilePermissions(entry)), entry.toString());
    }
  }
}
