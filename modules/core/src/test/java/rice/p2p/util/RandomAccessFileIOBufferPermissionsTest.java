package rice.p2p.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A file that a node creates for its data is readable by its owner only; a file that exists keeps its permissions. */
class RandomAccessFileIOBufferPermissionsTest {
  @TempDir
  Path directory;

  @BeforeEach
  void requirePosix() {
    assumeTrue(PrivateFiles.isPosix(), "needs POSIX file permissions");
  }

  private static String permissionsOf(Path file) throws IOException {
    return PosixFilePermissions.toString(Files.getPosixFilePermissions(file));
  }

  @Test
  void createsANewFileForPathOwnerOnly() throws IOException {
    Path file = directory.resolve("created");

    new RandomAccessFileIOBuffer(file, "rw").close();

    assertEquals("rw-------", permissionsOf(file));
  }

  @Test
  void createsANewFileForNameOwnerOnly() throws IOException {
    Path file = directory.resolve("created-by-name");

    new RandomAccessFileIOBuffer(file.toString(), "rw").close();

    assertEquals("rw-------", permissionsOf(file));
  }

  @Test
  void keepsThePermissionsOfAFileThatExists() throws IOException {
    Path file = Files.createFile(directory.resolve("existing"));
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));

    new RandomAccessFileIOBuffer(file, "rw").close();

    assertEquals("rw-r--r--", permissionsOf(file));
  }
}
