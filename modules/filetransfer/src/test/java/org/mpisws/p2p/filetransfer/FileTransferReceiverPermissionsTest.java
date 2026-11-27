package org.mpisws.p2p.filetransfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.environment.Environment;
import rice.p2p.util.PrivateFiles;

/** A file that arrives from a peer is created readable by its owner only; a file that exists keeps its permissions. */
class FileTransferReceiverPermissionsTest {
  private final Environment env = Environment.directEnvironment(1);

  @TempDir
  Path directory;

  @BeforeEach
  void requirePosix() {
    assumeTrue(PrivateFiles.isPosix(), "needs POSIX file permissions");
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private void receiveFileAt(Path file) throws Exception {
    FileAllocationStrategy allocation = new FileAllocationStrategy() {
      @Override
      public Path getFile(ByteBuffer metadata, long offset, long length) {
        return file;
      }

      @Override
      public void fileCancelled(ByteBuffer metadata, Path path, long offset, long downloadedLength,
          long requestedLength, Exception reason) {
      }
    };
    FileTransferImpl transfer = new FileTransferImpl(
        new FileTransferImplTest.IdleSocket(), null, allocation, env, null);
    Field receiverField = FileTransferImpl.class.getDeclaredField("receiver");
    receiverField.setAccessible(true);
    FileTransferReceiver receiver = (FileTransferReceiver) receiverField.get(transfer);

    receiver.addIncomingFile(1, new byte[0], 0, 10);
  }

  @Test
  void createsTheFileOwnerOnly() throws Exception {
    Path file = directory.resolve("received");

    receiveFileAt(file);

    assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)));
  }

  @Test
  void keepsThePermissionsOfAFileThatExists() throws Exception {
    Path file = Files.createFile(directory.resolve("existing"));
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));

    receiveFileAt(file);

    assertEquals("rw-r--r--", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)));
  }
}
