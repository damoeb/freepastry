package rice.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SortedMap;
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
  private Path dir;
  private Function<Path, IdRange> rangeForDirectory;
  private Function<Path, String> prefixOf;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    logger = env.getLogManager().getLogger(MetadataIndexTest.class, null);
    dir = root;
    rangeForDirectory = directory -> factory.buildIdRangeFromPrefix(PREFIX);
    prefixOf = directory -> PREFIX;
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private MetadataIndex newIndex() {
    return new MetadataIndex(factory, new TestStorageSerializer(), logger);
  }

  private Id storedId(String hex) throws IOException {
    Id id = factory.buildIdFromString(PREFIX + hex);
    Files.createFile(dir.resolve(id.toStringFull().substring(PREFIX.length())));
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
    Files.delete(dir.resolve(gone.toStringFull().substring(PREFIX.length())));

    MetadataIndex actual = newIndex();
    actual.readMetadataFile(dir, rangeForDirectory, prefixOf);

    assertTrue(actual.exists(kept));
    assertFalse(actual.exists(gone));
  }

  @Test
  void deletesABrokenCacheFile() throws IOException {
    Path cache = dir.resolve(PersistentStorage.METADATA_FILENAME);
    Files.write(cache, new byte[] {1, 2, 3});

    long actual = newIndex().readMetadataFile(dir, rangeForDirectory, prefixOf);

    assertEquals(0L, actual);
    assertFalse(Files.exists(cache));
  }

  @Test
  void reportsAMissingCacheFile() throws IOException {
    assertEquals(-1L, newIndex().readMetadataFile(dir, rangeForDirectory, prefixOf));
  }

  private Id fullId(String firstHexDigit, String lastHexDigit) throws IOException {
    Id id = factory.buildIdFromString(firstHexDigit + "0".repeat(38) + lastHexDigit);
    Files.createFile(dir.resolve(id.toStringFull()));
    return id;
  }

  @Test
  void writesBothEndsOfAWrappingRange() throws IOException {
    Id low = fullId("1", "1");
    Id middle = fullId("8", "0");
    Id high = fullId("F", "1");
    IdRange wrapping = factory.buildIdRange(fullId("E", "0"), fullId("2", "0"));
    if (! wrapping.containsId(low)) {
      wrapping = factory.buildIdRange(wrapping.getClockwiseBound(), wrapping.getCounterClockwiseBound());
    }
    assertTrue(wrapping.containsId(low) && wrapping.containsId(high) && ! wrapping.containsId(middle));
    IdRange range = wrapping;
    MetadataIndex written = newIndex();
    written.put(low, 1, dir);
    written.put(high, 2, dir);

    written.writeDirty(directory -> range);
    MetadataIndex actual = newIndex();
    actual.readMetadataFile(dir, directory -> range, directory -> "");

    assertEquals(1, actual.get(low));
    assertEquals(2, actual.get(high));
  }

  @Test
  void keyMapIsASnapshot() throws IOException {
    Id first = storedId("1");
    MetadataIndex index = newIndex();
    index.put(first, 1, dir);

    SortedMap<Id, Object> snapshot = index.keyMap();
    SortedMap<Id, Object> subSnapshot = index.keySubMap(first, storedId("9"));
    index.put(storedId("2"), 2, dir);

    assertEquals(1, snapshot.size());
    assertEquals(1, subSnapshot.size());
  }
}
