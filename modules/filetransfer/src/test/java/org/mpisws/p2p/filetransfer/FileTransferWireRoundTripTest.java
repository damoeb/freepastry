package org.mpisws.p2p.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;

/**
 * Sends messages and files from one {@link FileTransferImpl} to another over an in-memory socket pair: what the sender
 * writes (message and file headers, chunk headers, chunks) is all the receiver sees, as on a socket between two
 * nodes. Everything runs on the environment's selector thread, as it does in a node.
 */
class FileTransferWireRoundTripTest {
  private static final int CHUNK = 8;
  private static final int CHUNKS_IN_FLIGHT = 2;
  private static final long TIMEOUT_SECONDS = 20;

  private final Environment env = Environment.directEnvironment(1);

  @TempDir
  Path dir;

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /** One end of an in-memory socket; read and write interest are tracked separately, as a selector does. */
  private final class PipeAppSocket implements AppSocket {
    private PipeAppSocket peer;
    private ByteBuffer incoming = ByteBuffer.allocate(0);
    private volatile boolean closed;
    private AppSocketReceiver readReceiver;
    private AppSocketReceiver writeReceiver;

    @Override
    public synchronized long read(ByteBuffer dst) {
      if (!incoming.hasRemaining()) {
        return peer.closed ? -1 : 0;
      }
      int n = Math.min(dst.remaining(), incoming.remaining());
      for (int i = 0; i < n; i++) {
        dst.put(incoming.get());
      }
      return n;
    }

    @Override
    public long write(ByteBuffer src) {
      int n = src.remaining();
      peer.append(src);
      env.getSelectorManager().invoke(peer::fire);
      return n;
    }

    private synchronized void append(ByteBuffer src) {
      ByteBuffer grown = ByteBuffer.allocate(incoming.remaining() + src.remaining());
      grown.put(incoming).put(src).flip();
      incoming = grown;
    }

    @Override
    public synchronized void register(boolean wantToRead, boolean wantToWrite, int timeout,
        AppSocketReceiver receiver) {
      if (wantToRead) {
        readReceiver = receiver;
      }
      if (wantToWrite) {
        writeReceiver = receiver;
      }
      env.getSelectorManager().invoke(this::fire);
    }

    private void fire() {
      AppSocketReceiver reader = null;
      AppSocketReceiver writer = null;
      synchronized (this) {
        if (closed) {
          return;
        }
        if (readReceiver != null && (incoming.hasRemaining() || peer.closed)) {
          reader = readReceiver;
          readReceiver = null;
        }
        if (writeReceiver != null) {
          writer = writeReceiver;
          writeReceiver = null;
        }
      }
      try {
        if (reader != null) {
          reader.receiveSelectResult(this, true, false);
        }
        if (writer != null) {
          writer.receiveSelectResult(this, false, true);
        }
      } catch (IOException | RuntimeException e) {
        AppSocketReceiver failed = reader != null ? reader : writer;
        failed.receiveException(this, e);
      }
    }

    @Override
    public void shutdownOutput() {
      close();
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  /** What the receiving side delivered, in order. */
  private static final class Inbox implements FileTransferCallback {
    final LinkedBlockingQueue<Object> events = new LinkedBlockingQueue<>();
    /** The socket whose state is sampled at the moment a failure is reported. */
    volatile PipeAppSocket watched;
    /** Whether the watched socket was already closed when the failure was reported. */
    volatile boolean closedWhenFailureReported;

    @Override
    public void messageReceived(ByteBuffer buffer) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      events.add(bytes);
    }

    @Override
    public void fileReceived(Path path, ByteBuffer metadata) {
      byte[] meta = new byte[metadata.remaining()];
      metadata.get(meta);
      events.add(new ReceivedFile(path, meta));
    }

    @Override
    public void receiveException(Exception ioe) {
      PipeAppSocket socket = watched;
      closedWhenFailureReported = socket != null && socket.closed;
      events.add(ioe);
    }

    Object next() throws InterruptedException {
      Object event = events.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertNotNull(event, "nothing arrived at the receiver");
      if (event instanceof Throwable failure) {
        throw new AssertionError("the receiver failed", failure);
      }
      return event;
    }
  }

  private static final class ReceivedFile {
    private final Path file;
    private final byte[] metadata;

    ReceivedFile(Path file, byte[] metadata) {
      this.file = file;
      this.metadata = metadata;
    }

    Path file() {
      return file;
    }

    byte[] metadata() {
      return metadata;
    }
  }

  private record Pair(FileTransferImpl sender, Inbox inbox, FileTransferImpl receiver, PipeAppSocket senderSocket,
      PipeAppSocket receiverSocket) {
  }

  private Pair pair(int senderChunk, int receiverChunk) {
    PipeAppSocket pipeAppSocket = new PipeAppSocket();
    PipeAppSocket b = new PipeAppSocket();
    pipeAppSocket.peer = b;
    b.peer = pipeAppSocket;
    Inbox inbox = new Inbox();
    inbox.watched = b;
    FileTransferImpl sender = new FileTransferImpl(pipeAppSocket, new Inbox(), null, env, null, senderChunk, CHUNKS_IN_FLIGHT);
    FileTransferImpl receiver = new FileTransferImpl(b, inbox, new TempFileAllocationStrategy("ft", null, dir), env,
        null, receiverChunk, CHUNKS_IN_FLIGHT);
    return new Pair(sender, inbox, receiver, pipeAppSocket, b);
  }

  private static byte[] payload(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (seed + i * 31);
    }
    return bytes;
  }

  private static <T> Continuation<T, Exception> into(CompletableFuture<T> future) {
    return new Continuation<>() {
      @Override
      public void receiveResult(T result) {
        future.complete(result);
      }

      @Override
      public void receiveException(Exception exception) {
        future.completeExceptionally(exception);
      }
    };
  }

  @ParameterizedTest
  @ValueSource(ints = {1, CHUNK - 1, CHUNK, CHUNK + 1, 10 * CHUNK + 3, 100_000})
  @DisplayName("A message arrives as sent: smaller than, as large as and larger than one chunk")
  void messageArrivesAsSent(int size) throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    CompletableFuture<ByteBufferReceipt> sent = new CompletableFuture<>();

    pair.sender.sendMessage(ByteBuffer.wrap(payload(size, 3)), (byte) 0, into(sent));

    assertArrayEquals(payload(size, 3), (byte[]) pair.inbox.next());
    sent.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("A message without bytes arrives as an empty message")
  void emptyMessageArrives() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);

    pair.sender.sendMessage(ByteBuffer.allocate(0), (byte) 0, null);

    assertArrayEquals(new byte[0], (byte[]) pair.inbox.next());
  }

  @Test
  @DisplayName("Messages sent one after the other arrive in that order, a small one after a chunked one")
  void messagesArriveInOrder() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    int[] sizes = {50, 1, CHUNK, 3 * CHUNK, 2};

    for (int i = 0; i < sizes.length; i++) {
      pair.sender.sendMessage(ByteBuffer.wrap(payload(sizes[i], i)), (byte) 0, null);
    }

    Object[] received = new Object[sizes.length];
    for (int i = 0; i < sizes.length; i++) {
      received[i] = pair.inbox.next();
    }
    for (int i = 0; i < sizes.length; i++) {
      assertArrayEquals(payload(sizes[i], i), (byte[]) received[i], "message " + i);
    }
  }

  @DisplayName("A message whose sender uses larger chunks than the receiver arrives as sent")
  @ParameterizedTest
  @ValueSource(ints = {CHUNK * 2, CHUNK * 8, 1024})
  void messageWithALargerSenderChunk(int senderChunk) throws Exception {
    Pair pair = pair(senderChunk, CHUNK);

    pair.sender.sendMessage(ByteBuffer.wrap(payload(5000, 1)), (byte) 0, null);

    assertArrayEquals(payload(5000, 1), (byte[]) pair.inbox.next());
  }

  private Path file(String name, byte[] content) throws IOException {
    return Files.write(dir.resolve(name), content);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, CHUNK - 1, CHUNK, CHUNK + 1, 10 * CHUNK + 3, 5000})
  @DisplayName("A file arrives as sent with its metadata: smaller than, as large as and larger than one chunk")
  void fileArrivesAsSent(int size) throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    Path source = file("source", payload(size, 7));
    byte[] metadata = "name ä中.txt".getBytes(StandardCharsets.UTF_8);

    pair.sender.sendFile(source, ByteBuffer.wrap(metadata), (byte) 0, null);

    ReceivedFile received = (ReceivedFile) pair.inbox.next();
    assertArrayEquals(metadata, received.metadata());
    assertArrayEquals(payload(size, 7), Files.readAllBytes(received.file()));
  }

  @Test
  @DisplayName("A file with empty metadata arrives as sent")
  void fileWithoutMetadata() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);

    pair.sender.sendFile(file("source", payload(30, 7)), ByteBuffer.allocate(0), (byte) 0, null);

    ReceivedFile received = (ReceivedFile) pair.inbox.next();
    assertArrayEquals(new byte[0], received.metadata());
    assertArrayEquals(payload(30, 7), Files.readAllBytes(received.file()));
  }

  @Test
  @DisplayName("An empty file arrives as an empty file and the sender is told it is sent")
  void emptyFileArrives() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    CompletableFuture<FileReceipt> sent = new CompletableFuture<>();

    pair.sender.sendFile(file("source", new byte[0]), ByteBuffer.wrap(new byte[] {1}), (byte) 0, into(sent));

    ReceivedFile received = (ReceivedFile) pair.inbox.next();
    assertArrayEquals(new byte[] {1}, received.metadata());
    assertEquals(0, Files.size(received.file()));
    sent.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("A part of a file arrives at the same offset in the received file")
  void partOfAFileArrivesAtItsOffset() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    byte[] content = payload(100, 9);

    pair.sender.sendFile(file("source", content), ByteBuffer.allocate(0), (byte) 0, 10, 50, null);

    ReceivedFile received = (ReceivedFile) pair.inbox.next();
    byte[] stored = Files.readAllBytes(received.file());
    assertEquals(60, stored.length);
    assertArrayEquals(Arrays.copyOfRange(content, 10, 60), Arrays.copyOfRange(stored, 10, 60));
  }

  @Test
  @DisplayName("A file with a sender chunk larger than the receiver's arrives as sent")
  void fileWithALargerSenderChunk() throws Exception {
    Pair pair = pair(CHUNK * 4, CHUNK);

    pair.sender.sendFile(file("source", payload(1000, 2)), ByteBuffer.allocate(0), (byte) 0, null);

    ReceivedFile received = (ReceivedFile) pair.inbox.next();
    assertArrayEquals(payload(1000, 2), Files.readAllBytes(received.file()));
  }

  @Test
  @DisplayName("Messages and files sent together all arrive intact")
  void messagesAndFilesTogether() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    pair.sender.sendMessage(ByteBuffer.wrap(payload(40, 1)), (byte) 0, null);
    pair.sender.sendFile(file("f1", payload(100, 2)), ByteBuffer.wrap(new byte[] {1}), (byte) 0, null);
    pair.sender.sendMessage(ByteBuffer.wrap(payload(3, 3)), (byte) 0, null);
    pair.sender.sendFile(file("f2", payload(17, 4)), ByteBuffer.wrap(new byte[] {2}), (byte) 0, null);

    List<byte[]> messages = new ArrayList<>();
    List<ReceivedFile> files = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Object event = pair.inbox.next();
      if (event instanceof byte[] message) {
        messages.add(message);
      } else {
        files.add((ReceivedFile) event);
      }
    }

    assertEquals(2, messages.size());
    assertEquals(2, files.size());
    for (ReceivedFile file : files) {
      byte[] expected = file.metadata()[0] == 1 ? payload(100, 2) : payload(17, 4);
      assertArrayEquals(expected, Files.readAllBytes(file.file()));
    }
  }

  private static ByteBuffer frame(byte type, int uid, int... ints) {
    ByteBuffer buffer = ByteBuffer.allocate(5 + ints.length * 4);
    buffer.put(type).putInt(uid);
    for (int i : ints) {
      buffer.putInt(i);
    }
    buffer.flip();
    return buffer;
  }

  private static ByteBuffer fileHeader(int uid, long offset, long length, int nameSize, int nameBytes) {
    ByteBuffer buffer = ByteBuffer.allocate(5 + 20 + nameBytes);
    buffer.put(FileTransferImpl.MSG_FILE_HEADER).putInt(uid).putLong(offset).putLong(length).putInt(nameSize);
    buffer.put(new byte[nameBytes]);
    buffer.flip();
    return buffer;
  }

  private void assertRejected(Pair pair, ByteBuffer hostile) throws Exception {
    pair.senderSocket.write(hostile);
    Object event = pair.inbox.events.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertNotNull(event, "the receiver neither delivered nor failed");
    assertTrue(event instanceof IOException, "expected an IOException but got " + event);
    assertTrue(pair.inbox.closedWhenFailureReported,
        "the receiver must close the socket of a peer that breaks the protocol before it reports the failure");
    assertTrue(pair.receiverSocket.closed, "the receiver must close the socket of a peer that breaks the protocol");
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, Integer.MIN_VALUE, Integer.MAX_VALUE, 16 * 1024 * 1024 + 1})
  @DisplayName("A message header with a negative or oversized length is a protocol error that closes the socket")
  void messageHeaderWithBadSize(int size) throws Exception {
    Pair pair = pair(CHUNK, CHUNK);

    assertRejected(pair, frame(FileTransferImpl.MSG_BB_HEADER, 1, size));
  }

  @Test
  @DisplayName("A second message header with a uid already in use is a protocol error that closes the socket")
  void duplicateMessageUid() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    pair.senderSocket.write(frame(FileTransferImpl.MSG_BB_HEADER, 7, 10));

    assertRejected(pair, frame(FileTransferImpl.MSG_BB_HEADER, 7, 10));
  }

  @Test
  @DisplayName("A message chunk larger than the message is a protocol error that closes the socket")
  void chunkLargerThanMessage() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    pair.senderSocket.write(frame(FileTransferImpl.MSG_BB_HEADER, 7, 10));

    assertRejected(pair, frame(FileTransferImpl.MSG_CHUNK, 7, 11));
  }

  @Test
  @DisplayName("A chunk for a uid nobody announced is a protocol error that closes the socket")
  void chunkForUnknownUid() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);

    assertRejected(pair, frame(FileTransferImpl.MSG_CHUNK, 9, 4));
  }

  @Test
  @DisplayName("Messages that together exceed the bytes allowed in flight are a protocol error that closes the socket")
  void tooManyBytesInFlight() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    for (int uid = 0; uid < 4; uid++) {
      pair.senderSocket.write(frame(FileTransferImpl.MSG_BB_HEADER, uid, 16 * 1024 * 1024));
    }

    assertRejected(pair, frame(FileTransferImpl.MSG_BB_HEADER, 4, 16 * 1024 * 1024));
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, Long.MIN_VALUE, Long.MAX_VALUE, 5_000_000_000L})
  @DisplayName("A file header with a negative or oversized length is a protocol error that closes the socket")
  void fileHeaderWithBadLength(long length) throws Exception {
    Pair pair = pair(CHUNK, CHUNK);

    assertRejected(pair, fileHeader(1, 0, length, 0, 0));
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, Long.MIN_VALUE, 11, Long.MAX_VALUE})
  @DisplayName("A file header with a negative offset or one beyond the file length is a protocol error")
  void fileHeaderWithBadOffset(long offset) throws Exception {
    Pair pair = pair(CHUNK, CHUNK);

    assertRejected(pair, fileHeader(1, offset, 10, 0, 0));
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, Integer.MIN_VALUE, Integer.MAX_VALUE, 4097})
  @DisplayName("A file header with a negative or oversized name length is a protocol error")
  void fileHeaderWithBadNameSize(int nameSize) throws Exception {
    Pair pair = pair(CHUNK, CHUNK);

    assertRejected(pair, fileHeader(1, 0, 10, nameSize, 0));
  }

  @Test
  @DisplayName("A duplicate file uid is a protocol error that closes the socket and creates no second temp file")
  void duplicateFileUid() throws Exception {
    Pair pair = pair(CHUNK, CHUNK);
    pair.senderSocket.write(fileHeader(3, 0, 10, 1, 1));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (tempFiles() < 1 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    long before = tempFiles();

    assertRejected(pair, fileHeader(3, 0, 10, 1, 1));
    assertEquals(before, tempFiles());
  }

  private long tempFiles() throws IOException {
    try (var s = Files.list(dir)) {
      return s.count();
    }
  }
}
