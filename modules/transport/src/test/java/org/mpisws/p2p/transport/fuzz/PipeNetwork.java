package org.mpisws.p2p.transport.fuzz;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A network of {@link #lower(InetSocketAddress)} transport layers that moves the bytes of messages and sockets from one
 * layer to another in memory, on the thread that calls {@link #run()}: what a layer under test writes is what the layer
 * of the peer reads, as on a real wire, but without selectors and ports. Everything a callback throws is collected in
 * {@link #failures()}, the way a selector thread would lose it.
 */
public final class PipeNetwork {
  /** Opening nodes show up at the acceptor on ports from here on. */
  public static final int EPHEMERAL_PORT_BASE = 40_000;
  private static final int MAX_STEPS = 100_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeNetwork.class);

  private final Map<InetSocketAddress, Lower> layers = new HashMap<>();
  private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
  private final List<PipeSocket> sockets = new ArrayList<>();
  private final List<Throwable> failures = new ArrayList<>();

  /** The layer at the bottom of a stack that lives at the address; its callback is the stack's lowest layer. */
  public TransportLayer<InetSocketAddress, ByteBuffer> lower(InetSocketAddress address) {
    return layers.computeIfAbsent(address, Lower::new);
  }

  /** What callbacks and receivers threw since the last call. */
  public List<Throwable> failures() {
    List<Throwable> ret = new ArrayList<>(failures);
    failures.clear();
    return ret;
  }

  /** Delivers messages and runs socket receivers until nothing is left to do. */
  public void run() {
    for (int step = 0; step < MAX_STEPS; step++) {
      Runnable task = tasks.poll();
      if (task != null) {
        task.run();
        continue;
      }
      boolean progress = false;
      for (PipeSocket socket : new ArrayList<>(sockets)) {
        progress |= socket.runReceiver();
      }
      if (!progress) {
        return;
      }
    }
    throw new IllegalStateException("the network does not settle");
  }

  private final class Lower implements TransportLayer<InetSocketAddress, ByteBuffer> {
    private final InetSocketAddress address;
    private TransportLayerCallback<InetSocketAddress, ByteBuffer> callback;

    Lower(InetSocketAddress address) {
      this.address = address;
    }

    @Override
    public SocketRequestHandle<InetSocketAddress> openSocket(InetSocketAddress i,
        SocketCallback<InetSocketAddress> deliverSocketToMe, Map<String, Object> options) {
      SocketRequestHandleImpl<InetSocketAddress> handle = new SocketRequestHandleImpl<>(i, options, LOGGER);
      Lower target = layers.get(i);
      tasks.add(() -> {
        if (target == null) {
          deliverSocketToMe.receiveException(handle, new IOException("nobody at " + i));
          return;
        }
        PipeSocket client = new PipeSocket(i);
        // the acceptor sees the opener on an ephemeral port, never on the address the opener listens on
        PipeSocket server = new PipeSocket(new InetSocketAddress(address.getAddress(), EPHEMERAL_PORT_BASE + sockets.size() / 2));
        client.peer = server;
        server.peer = client;
        sockets.add(client);
        sockets.add(server);
        deliverSocketToMe.receiveResult(handle, client);
        try {
          target.callback.incomingSocket(server);
        } catch (IOException | RuntimeException e) {
          failures.add(e);
        }
      });
      return handle;
    }

    @Override
    public MessageRequestHandle<InetSocketAddress, ByteBuffer> sendMessage(InetSocketAddress i, ByteBuffer msg,
        MessageCallback<InetSocketAddress, ByteBuffer> deliverAckToMe, Map<String, Object> options) {
      MessageRequestHandleImpl<InetSocketAddress, ByteBuffer> handle = new MessageRequestHandleImpl<>(i, msg, options);
      ByteBuffer copy = ByteBuffer.allocate(msg.remaining());
      copy.put(msg.duplicate());
      copy.flip();
      msg.position(msg.limit());
      Lower target = layers.get(i);
      tasks.add(() -> {
        if (target == null) {
          if (deliverAckToMe != null) {
            deliverAckToMe.sendFailed(handle, new IOException("nobody at " + i));
          }
          return;
        }
        try {
          target.callback.messageReceived(address, copy, options);
        } catch (IOException | RuntimeException e) {
          failures.add(e);
        }
        if (deliverAckToMe != null) {
          deliverAckToMe.ack(handle);
        }
      });
      return handle;
    }

    @Override
    public InetSocketAddress getLocalIdentifier() {
      return address;
    }

    @Override
    public void acceptSockets(boolean b) {
    }

    @Override
    public void acceptMessages(boolean b) {
    }

    @Override
    public void setCallback(TransportLayerCallback<InetSocketAddress, ByteBuffer> callback) {
      this.callback = callback;
    }

    @Override
    public void setErrorHandler(ErrorHandler<InetSocketAddress> handler) {
    }

    @Override
    public void destroy() {
    }
  }

  private final class PipeSocket implements P2PSocket<InetSocketAddress> {
    private final InetSocketAddress identifier;
    private PipeSocket peer;
    private ByteBuffer incoming = ByteBuffer.allocate(64);
    private boolean closed;
    private boolean outputShutdown;
    private P2PSocketReceiver<InetSocketAddress> receiver;
    private boolean wantsRead;
    private boolean wantsWrite;

    PipeSocket(InetSocketAddress identifier) {
      this.identifier = identifier;
      incoming.flip();
    }

    boolean runReceiver() {
      if (receiver == null || closed) {
        return false;
      }
      boolean canRead = wantsRead && (incoming.hasRemaining() || peer.closed || peer.outputShutdown);
      boolean canWrite = wantsWrite;
      if (!canRead && !canWrite) {
        return false;
      }
      P2PSocketReceiver<InetSocketAddress> next = receiver;
      receiver = null;
      try {
        next.receiveSelectResult(this, canRead, canWrite);
      } catch (IOException | RuntimeException e) {
        try {
          next.receiveException(this, e);
        } catch (RuntimeException secondary) {
          failures.add(secondary);
        }
      }
      return true;
    }

    @Override
    public InetSocketAddress getIdentifier() {
      return identifier;
    }

    @Override
    public Map<String, Object> getOptions() {
      return Map.of();
    }

    @Override
    public long read(ByteBuffer dst) {
      if (!incoming.hasRemaining()) {
        return peer.closed || peer.outputShutdown ? -1 : 0;
      }
      int n = Math.min(dst.remaining(), incoming.remaining());
      for (int i = 0; i < n; i++) {
        dst.put(incoming.get());
      }
      return n;
    }

    @Override
    public long write(ByteBuffer src) {
      if (closed || outputShutdown || peer.closed) {
        return -1;
      }
      int n = src.remaining();
      peer.append(src);
      return n;
    }

    private void append(ByteBuffer src) {
      ByteBuffer grown = ByteBuffer.allocate(incoming.remaining() + src.remaining() + 64);
      grown.put(incoming).put(src).flip();
      incoming = grown;
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<InetSocketAddress> registered) {
      wantsRead = wantToRead;
      wantsWrite = wantToWrite;
      receiver = registered;
    }

    @Override
    public void shutdownOutput() {
      outputShutdown = true;
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
