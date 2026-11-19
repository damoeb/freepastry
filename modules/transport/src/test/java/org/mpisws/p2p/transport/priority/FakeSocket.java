package org.mpisws.p2p.transport.priority;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;

/** A socket that delivers the bytes it was given, then reports nothing more to read. */
final class FakeSocket implements P2PSocket<String> {
  private final ByteBuffer incoming;
  boolean closed;
  boolean registeredForRead;
  /** thrown by {@link #read} instead of delivering bytes, if set */
  IOException readFailure;

  FakeSocket(byte... incoming) {
    this.incoming = ByteBuffer.wrap(incoming);
  }

  @Override
  public Map<String, Object> getOptions() {
    return Map.of();
  }

  @Override
  public String getIdentifier() {
    return "peer";
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
  }

  @Override
  public void shutdownOutput() {}

  @Override
  public void close() {
    closed = true;
  }
}
