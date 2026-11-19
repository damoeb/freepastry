package org.mpisws.p2p.transport.fuzz;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;

/**
 * A socket for fuzzing the readers that sit on a socket: it delivers the bytes it was given, then reports the end of
 * the stream, swallows everything written to it, and runs whichever receiver is registered for as long as there is one.
 */
public final class FuzzSocket<Identifier> implements P2PSocket<Identifier> {
  private static final int MAX_STEPS = 1000;

  private final Identifier identifier;
  private final ByteBuffer incoming;
  private P2PSocketReceiver<Identifier> receiver;
  private boolean wantsRead;
  private boolean wantsWrite;
  private boolean closed;

  public FuzzSocket(Identifier identifier, byte[] incoming) {
    this.identifier = identifier;
    this.incoming = ByteBuffer.wrap(incoming);
  }

  public boolean isClosed() {
    return closed;
  }

  /** Runs the registered receivers until none is left, the socket is closed or a reader failed with an IOException. */
  public void drive() {
    for (int step = 0; step < MAX_STEPS && receiver != null && !closed; step++) {
      P2PSocketReceiver<Identifier> next = receiver;
      boolean canRead = wantsRead;
      boolean canWrite = wantsWrite;
      receiver = null;
      try {
        next.receiveSelectResult(this, canRead, canWrite);
      } catch (IOException expected) {
        // the accepted way to reject malformed input
        return;
      }
    }
  }

  @Override
  public Identifier getIdentifier() {
    return identifier;
  }

  @Override
  public Map<String, Object> getOptions() {
    return Map.of();
  }

  @Override
  public long read(ByteBuffer dst) {
    if (!incoming.hasRemaining()) {
      return -1;
    }
    int byteCount = Math.min(dst.remaining(), incoming.remaining());
    for (int i = 0; i < byteCount; i++) {
      dst.put(incoming.get());
    }
    return byteCount;
  }

  @Override
  public long write(ByteBuffer src) {
    int byteCount = src.remaining();
    src.position(src.limit());
    return byteCount;
  }

  @Override
  public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<Identifier> registered) {
    wantsRead = wantToRead;
    wantsWrite = wantToWrite;
    receiver = registered;
  }

  @Override
  public void shutdownOutput() {
  }

  @Override
  public void close() {
    closed = true;
  }
}
