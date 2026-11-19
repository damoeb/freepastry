package org.mpisws.p2p.transport.peerreview.history;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import rice.environment.Environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** A temporary history lives in a private directory, not under a predictable name in the shared temp directory. */
class SecureHistoryFactoryTempTest {
  @TempDir
  Path sharedTemp;

  private String previousTmpDir;
  private Environment environment;

  @BeforeEach
  void setUp() {
    previousTmpDir = System.getProperty("java.io.tmpdir");
    System.setProperty("java.io.tmpdir", sharedTemp.toString());
    environment = new Environment();
  }

  @AfterEach
  void tearDown() {
    System.setProperty("java.io.tmpdir", previousTmpDir);
    environment.destroy();
  }

  private List<Path> entriesOfSharedTemp() throws IOException {
    try (Stream<Path> entries = Files.list(sharedTemp)) {
      return entries.toList();
    }
  }

  @Test
  void temporaryHistoryFilesAreNotDirectlyInTheSharedTempDirectory() throws Exception {
    SecureHistoryFactory testObj = new SecureHistoryFactory(new SHA256HashProvider(), environment);

    testObj.createTemp(0, new byte[32]).close();

    for (Path entry : entriesOfSharedTemp()) {
      assertTrue(Files.isDirectory(entry), "a file with a guessable name in the shared temp dir: " + entry);
    }
  }

  @Test
  void temporaryHistoryDirectoryIsOwnerOnly() throws Exception {
    assumeTrue(sharedTemp.getFileSystem().supportedFileAttributeViews().contains("posix"));
    SecureHistoryFactory testObj = new SecureHistoryFactory(new SHA256HashProvider(), environment);

    testObj.createTemp(0, new byte[32]).close();

    List<Path> entries = entriesOfSharedTemp();
    assertEquals(1, entries.size(), entries.toString());
    assertEquals(PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(entries.get(0)));
  }
}
