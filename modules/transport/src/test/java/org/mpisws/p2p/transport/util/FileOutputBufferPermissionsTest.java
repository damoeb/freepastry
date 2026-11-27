package org.mpisws.p2p.transport.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.p2p.util.PrivateFiles;

/** A file that a node writes is readable by its owner only; a file that exists keeps its permissions. */
class FileOutputBufferPermissionsTest {
  @TempDir
  Path directory;

  @BeforeEach
  void requirePosix() {
    assumeTrue(PrivateFiles.isPosix(), "needs POSIX file permissions");
  }

  @Test
  void createsANewFileOwnerOnly() throws IOException {
    Path file = directory.resolve("created");

    new FileOutputBuffer(file).close();

    assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)));
  }

  @Test
  void keepsThePermissionsOfAFileThatExists() throws IOException {
    Path file = Files.createFile(directory.resolve("existing"));
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));

    new FileOutputBuffer(file).close();

    assertEquals("rw-r--r--", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)));
  }
}
