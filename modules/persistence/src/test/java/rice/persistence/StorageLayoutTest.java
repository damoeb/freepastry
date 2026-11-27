package rice.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.environment.Environment;
import rice.pastry.commonapi.PastryIdFactory;

/**
 * The directory prefix cache of the storage layout.
 */
class StorageLayoutTest {

  @TempDir
  Path root;

  private Environment env;
  private StorageLayout layout;

  @BeforeEach
  void setUp() throws IOException {
    env = Environment.directEnvironment(1);
    layout = new StorageLayout(root.toString(), "test", new PastryIdFactory(env), env.getRandomSource(), null,
        env.getLogManager().getLogger(StorageLayoutTest.class, null));
    layout.initDirectories();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void prefixOfTheAppDirectoryStaysEmptyAfterAnotherPrefixWasComputed() throws IOException {
    Path sub = Files.createDirectory(layout.getAppDirectory().resolve("AB"));

    assertEquals("AB", layout.getPrefix(sub));
    assertEquals("AB", layout.getPrefix(sub));
    assertEquals("", layout.getPrefix(layout.getAppDirectory()));
  }
}
