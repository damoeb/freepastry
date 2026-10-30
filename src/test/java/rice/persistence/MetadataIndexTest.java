package rice.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.testing.TestStorageSerializer;

/**
 * Round trips of the metadata cache file ({@code metadata.cache}) of a directory.
 */
class MetadataIndexTest {

  /** The directory holds the keys starting with this prefix. */
  private static final String PREFIX = "A";

  @TempDir
  Path root;

  private Environment env;
  private IdFactory factory;
  private Logger logger;
  private File dir;
  private Function<File, IdRange> rangeForDirectory;
  private Function<File, String> prefixOf;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    logger = env.getLogManager().getLogger(MetadataIndexTest.class, null);
    dir = root.toFile();
    rangeForDirectory = d -> factory.buildIdRangeFromPrefix(PREFIX);
    prefixOf = d -> PREFIX;
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private MetadataIndex newIndex() {
    return new MetadataIndex(factory, new TestStorageSerializer(), logger);
  }

  private Id storedId(String hex) throws IOException {
    Id id = factory.buildIdFromToString(PREFIX + hex);
    Files.createFile(new File(dir, id.toStringFull().substring(PREFIX.length())).toPath());
    return id;
  }

  @Test
  void writesAndReadsTheMetadataOfADirectory() throws IOException {
    Id withMetadata = storedId("1");
    Id withoutMetadata = storedId("2");
    Id withOther = storedId("3");
    MetadataIndex written = newIndex();
    written.put(withMetadata, 17, dir);
    written.put(withoutMetadata, null, dir);
    written.put(withOther, 23, dir);

    written.writeDirty(rangeForDirectory);
    MetadataIndex actual = newIndex();
    long modified = actual.readMetadataFile(dir, rangeForDirectory, prefixOf);

    assertTrue(modified > 0);
    assertEquals(3, actual.sizeUnlocked());
    assertEquals(17, actual.get(withMetadata));
    assertTrue(actual.exists(withoutMetadata));
    assertNull(actual.get(withoutMetadata));
    assertEquals(23, actual.get(withOther));
  }

  @Test
  void skipsEntriesWhoseFileIsGone() throws IOException {
    Id kept = storedId("1");
    Id gone = storedId("2");
    MetadataIndex written = newIndex();
    written.put(kept, 1, dir);
    written.put(gone, 2, dir);
    written.writeDirty(rangeForDirectory);
    Files.delete(new File(dir, gone.toStringFull().substring(PREFIX.length())).toPath());

    MetadataIndex actual = newIndex();
    actual.readMetadataFile(dir, rangeForDirectory, prefixOf);

    assertTrue(actual.exists(kept));
    assertFalse(actual.exists(gone));
  }

  @Test
  void deletesABrokenCacheFile() throws IOException {
    File cache = new File(dir, PersistentStorage.METADATA_FILENAME);
    Files.write(cache.toPath(), new byte[] {1, 2, 3});

    long actual = newIndex().readMetadataFile(dir, rangeForDirectory, prefixOf);

    assertEquals(0L, actual);
    assertFalse(cache.exists());
  }

  @Test
  void reportsAMissingCacheFile() throws IOException {
    assertEquals(-1L, newIndex().readMetadataFile(dir, rangeForDirectory, prefixOf));
  }
}
