package rice.environment.params.simple;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Parameters are overridden only by a file that the caller names, never by a {@code user.params} that happens to be in the working directory. */
class SimpleParametersFileTest {
  private static final String[] DEFAULTS = {"freepastry"};
  private static final String KEY = "pastry_lSetSize";

  private final Path workingDirectoryFile = Path.of("user.params");

  @TempDir
  Path dir;

  @BeforeEach
  void setUp() throws IOException {
    Files.deleteIfExists(workingDirectoryFile);
  }

  @AfterEach
  void tearDown() throws IOException {
    Files.deleteIfExists(workingDirectoryFile);
  }

  @Test
  void userParamsInTheWorkingDirectoryIsNotLoadedImplicitly() throws IOException {
    int defaultValue = new SimpleParameters(DEFAULTS, null).getInt(KEY);
    Files.writeString(workingDirectoryFile, KEY + " = 3\n", StandardCharsets.ISO_8859_1);

    SimpleParameters testObj = new SimpleParameters(DEFAULTS, null);

    assertEquals(defaultValue, testObj.getInt(KEY));
  }

  @Test
  void aFileThatTheCallerNamesIsLoaded() throws IOException {
    Path file = dir.resolve("mine.params");
    Files.writeString(file, KEY + " = 3\n", StandardCharsets.ISO_8859_1);

    SimpleParameters testObj = new SimpleParameters(DEFAULTS, dir.resolve("mine").toString());

    assertEquals(3, testObj.getInt(KEY));
  }

  @Test
  void aStoredFileIsReadableByTheOwnerOnly() throws IOException {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        java.nio.file.FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "POSIX permissions");
    SimpleParameters testObj = new SimpleParameters(DEFAULTS, dir.resolve("stored").toString());
    testObj.setInt(KEY, 5);

    testObj.store();

    assertEquals("rw-------", java.nio.file.attribute.PosixFilePermissions.toString(
        Files.getPosixFilePermissions(dir.resolve("stored.params"))));
  }
}
