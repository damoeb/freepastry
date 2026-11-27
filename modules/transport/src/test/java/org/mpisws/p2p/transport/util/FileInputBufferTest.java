package org.mpisws.p2p.transport.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileInputBufferTest {

  @TempDir
  Path dir;

  @Test
  void bytesRemainingCountsTheUnreadBytes() throws IOException {
    Path file = Files.write(dir.resolve("data"), new byte[] {1, 2, 3});
    try (FileInputBuffer buf = new FileInputBuffer(file, LoggerFactory.getLogger(FileInputBufferTest.class))) {
      buf.readByte();

      assertEquals(2, buf.bytesRemaining());
    }
  }

  @Test
  void bytesRemainingOfAClosedBufferIsMinusOne() throws IOException {
    Path file = Files.write(dir.resolve("data"), new byte[] {1, 2, 3});
    FileInputBuffer buf = new FileInputBuffer(file, LoggerFactory.getLogger(FileInputBufferTest.class));
    buf.close();

    assertEquals(-1, buf.bytesRemaining());
  }
}
