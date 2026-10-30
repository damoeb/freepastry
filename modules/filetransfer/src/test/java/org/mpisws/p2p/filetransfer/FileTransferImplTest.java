package org.mpisws.p2p.filetransfer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.Continuation;
import rice.Executable;
import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.processing.Processor;
import rice.environment.processing.WorkRequest;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.selector.SelectorManager;

import static org.junit.jupiter.api.Assertions.assertFalse;

class FileTransferImplTest {

  private static final int CHUNK_SIZE = 4;
  private static final int CHUNKS_TO_WRITE = 2;

  private final Environment env = Environment.directEnvironment(1);

  @TempDir
  Path dir;

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /** Records the blocking IO requests and never runs them. */
  static class RecordingProcessor implements Processor {
    final List<WorkRequest<?>> blockingIO = new ArrayList<WorkRequest<?>>();

    @Override
    public <R, E extends Exception> Cancellable process(Executable<R, E> task, Continuation<R, E> command, SelectorManager selector, TimeSource ts, LogManager log) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Cancellable processBlockingIO(WorkRequest<?> request) {
      blockingIO.add(request);
      return () -> true;
    }

    @Override
    public void destroy() {
    }
  }

  /** A socket that accepts registrations and does no IO. */
  static class IdleSocket implements AppSocket {
    @Override
    public long read(ByteBuffer dst) { return 0; }
    @Override
    public long write(ByteBuffer src) { return 0; }
    @Override
    public void register(boolean wantToRead, boolean wantToWrite, int timeout, AppSocketReceiver receiver) { }
    @Override
    public void shutdownOutput() { }
    @Override
    public void close() { }
  }

  @Test
  void sendFileReadsThroughTheGivenProcessor() throws IOException {
    RecordingProcessor processor = new RecordingProcessor();
    Path file = Files.write(dir.resolve("data"), new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
    FileTransferImpl transfer = new FileTransferImpl(new IdleSocket(), null, null, env, processor, CHUNK_SIZE, CHUNKS_TO_WRITE);

    transfer.sendFile(file, ByteBuffer.allocate(0), (byte) 0, null);

    assertFalse(processor.blockingIO.isEmpty());
  }

  @Test
  void fiveArgumentConstructorUsesTheGivenProcessor() throws IOException {
    RecordingProcessor processor = new RecordingProcessor();
    Path file = Files.write(dir.resolve("data"), new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
    FileTransferImpl transfer = new FileTransferImpl(new IdleSocket(), null, null, env, processor);

    transfer.sendFile(file, ByteBuffer.allocate(0), (byte) 0, null);

    assertFalse(processor.blockingIO.isEmpty());
  }
}
