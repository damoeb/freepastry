package rice.persistence;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.testing.TestStorageSerializer;

/**
 * Round trips of the version 3 object file format.
 */
class ObjectFileFormatTest {

  @TempDir
  Path dir;

  private Environment env;
  private IdFactory factory;
  private ObjectFileFormat testObj;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    testObj = new ObjectFileFormat(factory, new TestStorageSerializer(), env.getLogManager().getLogger(ObjectFileFormatTest.class, null));
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void writesAndReadsTheObjectKeyVersionAndMetadata() throws IOException {
    Path file = dir.resolve("object");
    Id key = factory.buildId("key");
    byte[] obj = {1, 2, 3, 4};

    long size = testObj.writeObject(obj, 42, key, 12345L, file);

    assertEquals(Files.size(file), size);
    assertArrayEquals(obj, (byte[]) testObj.readData(file));
    assertEquals(key, testObj.readKeyFromFile(file));
    assertEquals(12345L, testObj.readVersion(file));
    assertEquals(42, testObj.readMetadata(file));
  }

  @Test
  void rewritesTheMetadataAndKeepsTheObject() throws IOException {
    Path file = dir.resolve("object");
    Id key = factory.buildId("key");
    testObj.writeObject("value", 1, key, 7L, file);

    testObj.writeMetadata(file, "a much longer metadata value than before");
    testObj.writeMetadata(file, 3);

    assertEquals(3, testObj.readMetadata(file));
    assertEquals("value", testObj.readData(file));
    assertEquals(key, testObj.readKeyFromFile(file));
    assertEquals(7L, testObj.readVersion(file));
  }

  @Test
  void keepsAMissingObjectAndMissingMetadata() throws IOException {
    Path file = dir.resolve("object");

    testObj.writeObject(null, null, factory.buildId("key"), 0L, file);

    assertNull(testObj.readData(file));
    assertNull(testObj.readMetadata(file));

    testObj.writeMetadata(file, 5);

    assertEquals(5, testObj.readMetadata(file));
    assertNull(testObj.readData(file));
  }

  @Test
  void refusesAFileWithoutTheTrailer() throws IOException {
    Path file = dir.resolve("broken");
    Files.write(file, new byte[] {31, -117, 8, 0});

    assertNull(testObj.readMetadata(file));
    assertThrows(IOException.class, () -> testObj.readData(file));
    assertThrows(IOException.class, () -> testObj.writeMetadata(file, 1));
  }

  @Test
  void refusesAnotherFormatVersion() throws IOException {
    Path file = dir.resolve("object");
    testObj.writeObject("value", 1, factory.buildId("key"), 7L, file);
    byte[] bytes = Files.readAllBytes(file);
    bytes[bytes.length - ObjectFileFormat.TRAILER_LENGTH + 15] = 2; // the low byte of the version long
    Files.write(file, bytes);

    assertNull(testObj.readMetadata(file));
    assertThrows(IOException.class, () -> testObj.readData(file));
  }
}
