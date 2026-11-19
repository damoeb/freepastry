package org.mpisws.p2p.filetransfer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
import rice.selector.SelectorLoop;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    public <R, E extends Exception> Cancellable process(Executable<R, E> task, Continuation<R, E> command, SelectorLoop selector, TimeSource ts, LogManager log) {
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

  /** A socket that collects what is written; each write registration is answered on the selector. */
  class CapturingSocket implements AppSocket {
    final ByteArrayOutputStream written = new ByteArrayOutputStream();

    @Override
    public long read(ByteBuffer dst) { return 0; }

    @Override
    public long write(ByteBuffer src) {
      int n = src.remaining();
      while (src.hasRemaining()) written.write(src.get());
      return n;
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, int timeout, AppSocketReceiver receiver) {
      if (wantToWrite) {
        env.getSelectorManager().invoke(() -> {
          try {
            receiver.receiveSelectResult(this, false, true);
          } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
          }
        });
      }
    }

    @Override
    public void shutdownOutput() { }

    @Override
    public void close() { }
  }

  @Test
  void sendMsgSendsTheBytesOfABufferWithAnArrayOffset() throws Exception {
    // a view of the last three bytes: arrayOffset() == 3, position() == 0
    ByteBuffer msg = ByteBuffer.wrap(new byte[] {9, 9, 9, 1, 2, 3}, 3, 3).slice();
    CapturingSocket socket = new CapturingSocket();
    FileTransferImpl transfer = new FileTransferImpl(socket, null, null, env, null, CHUNK_SIZE, CHUNKS_TO_WRITE);
    CompletableFuture<BBReceipt> sent = new CompletableFuture<BBReceipt>();

    BBReceipt receipt = transfer.sendMessage(msg, (byte) 0, new Continuation<BBReceipt, Exception>() {
      @Override
      public void receiveResult(BBReceipt result) {
        sent.complete(result);
      }

      @Override
      public void receiveException(Exception exception) {
        sent.completeExceptionally(exception);
      }
    });
    sent.get(10, TimeUnit.SECONDS);

    // the message header and the chunk header take 9 bytes each, then comes the payload
    byte[] wire = socket.written.toByteArray();
    assertEquals(9 + 9 + 3, wire.length);
    assertArrayEquals(new byte[] {1, 2, 3}, Arrays.copyOfRange(wire, 18, 21));
    assertArrayEquals(new byte[] {1, 2, 3}, receipt.getBytes());
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

  @Test
  void aFullQueueDropsTheLowestPriorityNewestMessage() {
    FileTransferImpl transfer = new FileTransferImpl(new IdleSocket(), null, null, env, new RecordingProcessor(), CHUNK_SIZE, CHUNKS_TO_WRITE);
    List<Integer> failed = new ArrayList<Integer>();
    int cap = transfer.MAX_QUEUE_SIZE;

    // nothing is written on an idle socket, so every message stays queued
    for (int i = 0; i <= cap + 1; i++) {
      final int index = i;
      byte priority = (byte) (i == cap + 1 ? 0 : 1);
      transfer.sendMessage(ByteBuffer.allocate(1), priority, new Continuation<BBReceipt, Exception>() {
        @Override
        public void receiveResult(BBReceipt result) {
        }

        @Override
        public void receiveException(Exception exception) {
          failed.add(index);
        }
      });
    }

    // the one over the cap goes first, then the newest of the lower priority makes room for the higher one
    assertEquals(List.of(cap, cap - 1), failed);
  }
}
