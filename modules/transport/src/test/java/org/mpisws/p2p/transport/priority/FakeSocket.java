package org.mpisws.p2p.transport.priority;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;

/** A socket that delivers the bytes it was given (and later {@link #feed fed}), then reports nothing more to read. */
final class FakeSocket implements P2PSocket<String> {
  private final String identifier;
  private ByteBuffer incoming;
  boolean closed;
  boolean registeredForRead;
  /** the receiver of the last {@link #register} */
  P2PSocketReceiver<String> receiver;
  /** thrown by {@link #read} instead of delivering bytes, if set */
  IOException readFailure;

  FakeSocket(byte... incoming) {
    this("peer", incoming);
  }

  FakeSocket(String identifier, byte... incoming) {
    this.identifier = identifier;
    this.incoming = ByteBuffer.wrap(incoming);
  }

  /** Adds bytes for the following reads. */
  void feed(byte... more) {
    ByteBuffer bigger = ByteBuffer.allocate(incoming.remaining() + more.length);
    bigger.put(incoming).put(more).flip();
    incoming = bigger;
  }

  /** The bytes not read yet. */
  int unread() {
    return incoming.remaining();
  }

  /** Plays one selector pass for this socket: calls the receiver if it registered to read. */
  void select() throws IOException {
    if (!registeredForRead || closed) return;
    registeredForRead = false;
    receiver.receiveSelectResult(this, true, false);
  }

  @Override
  public Map<String, Object> getOptions() {
    return Map.of();
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public long read(ByteBuffer dst) throws IOException {
    if (readFailure != null) {
      throw readFailure;
    }
    int byteCount = Math.min(dst.remaining(), incoming.remaining());
    for (int i = 0; i < byteCount; i++) {
      dst.put(incoming.get());
    }
    return byteCount;
  }

  @Override
  public long write(ByteBuffer src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<String> receiver) {
    registeredForRead = wantToRead;
    this.receiver = receiver;
  }

  @Override
  public void shutdownOutput() {}

  @Override
  public void close() {
    closed = true;
  }
}
