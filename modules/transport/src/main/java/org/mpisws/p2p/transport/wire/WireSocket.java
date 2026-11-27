/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/
package org.mpisws.p2p.transport.wire;

import org.mpisws.p2p.transport.util.OptionsFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.util.ByteBuffers;
import org.mpisws.p2p.transport.wire.exception.StalledSocketException;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import rice.selector.SelectionKeyHandler;
import rice.selector.TimerTask;

public class WireSocket extends SelectionKeyHandler implements P2PSocket<InetSocketAddress>, SocketRequestHandle<InetSocketAddress> {

  // the key to read from
  private @Nullable SelectionKey key;

  // the channel we are associated with
  private final SocketChannel channel;

  private final TCPLayer tcp;

  Logger logger;

  InetSocketAddress addr;

  @Nullable Map<String, Object> options;

  private @Nullable P2PSocketReceiver<InetSocketAddress> reader, writer;

  /**
   * becomes true before we deliver this to the SocketCallback, this invalidates the cancel() operation
   */
  boolean delivered = false;

  /**
   * True while this incoming socket is in the handshake: from accept until a layer calls {@link #handshakeCompleted()},
   * or the socket closes. Counts against {@code transport_wire_max_handshaking_sockets_per_prefix} and
   * {@code transport_wire_max_handshaking_sockets}.
   */
  private boolean isInHandshake = false;

  /** Closes this incoming socket when its handshake takes too long; null when there is no deadline (any more). */
  private @Nullable TimerTask handshakeDeadline;

  /**
   * Constructor which accepts an incoming connection, represented by the
   * selection key. This constructor builds a new WireSocket, and waits
   * until the greeting message is read from the other end. Once the greeting
   * is received, the manager makes sure that a socket for this handle is not
   * already open, and then proceeds as normal.
   *
   * @param channel the channel that the server socket accepted
   * @throws IOException if the connection cannot be accepted
   */
  public WireSocket(TCPLayer tcp, SocketChannel channel) throws IOException {
    this.tcp = tcp;
    logger = tcp.logger;

    this.channel = channel;
    channel.socket().setSendBufferSize(tcp.SOCKET_BUFFER_SIZE);
    channel.socket().setReceiveBufferSize(tcp.SOCKET_BUFFER_SIZE);
    channel.socket().setTcpNoDelay(tcp.isTcpNoDelay());

    channel.configureBlocking(false);
    addr = (InetSocketAddress)channel.socket().getRemoteSocketAddress();
    options = OptionsFactory.addOption(null, WireTransportLayer.OPTION_REMOTE_ADDRESS, addr);

    logger.debug("(SA) Accepted incoming connection from {}", addr);

    key = tcp.wire.getEnvironment().getSelectorManager().register(channel, this, 0);

    isInHandshake = true;
    int handshakeTimeoutMillis = tcp.getHandshakeTimeoutMillis();
    if (handshakeTimeoutMillis > 0) {
      TimerTask deadline = new TimerTask() {
        @Override
        public void run() {
          handshakeExpired(handshakeTimeoutMillis);
        }
      };
      handshakeDeadline = deadline;
      tcp.wire.getEnvironment().getSelectorManager().getTimer().schedule(deadline, handshakeTimeoutMillis);
    }
  }

  private void handshakeExpired(int handshakeTimeoutMillis) {
    synchronized (this) {
      if (!isInHandshake) return;
    }
    tcp.wire.getEnvironment().getLogRateLimiter().at(logger, Level.INFO, "wire.handshake-timeout")
        .log("Closing {}: its handshake did not finish within {} ms", this, handshakeTimeoutMillis);
    close();
    tcp.wire.getErrorHandler().receivedException(addr,
        new StalledSocketException(addr, "Handshake did not finish within " + handshakeTimeoutMillis + " ms."));
  }

  @Override
  public void handshakeCompleted() {
    leaveHandshake();
  }

  /**
   * Ends the handshake of this incoming socket: stops its deadline and frees its place in the count of handshaking
   * sockets from its address. Does nothing on an outgoing socket, or when called again.
   */
  private void leaveHandshake() {
    TimerTask deadline;
    synchronized (this) {
      if (!isInHandshake) return;
      isInHandshake = false;
      deadline = handshakeDeadline;
      handshakeDeadline = null;
    }
    if (deadline != null) deadline.cancel();
    tcp.handshakeEnded(addr.getAddress());
  }

  /**
   * Constructor which creates an outgoing connection to the given address.
   * This creates the connection by building the socket and sending
   * across the greeting message. Once the response greeting message is
   * received, everything proceeds as normal.
   *
   * @param address The destination of this socket
   * @param socketCallback receives the socket once it is connected
   * @throws IOException An error
   */
  public WireSocket(final TCPLayer tcp, final InetSocketAddress address, final SocketCallback<InetSocketAddress> socketCallback, @Nullable Map<String, Object> options) throws IOException {
    this.tcp = tcp;
    this.options = OptionsFactory.addOption(options, WireTransportLayer.OPTION_REMOTE_ADDRESS, address);
    logger = tcp.logger;
    this.addr = address;

    channel = SocketChannel.open();
    channel.socket().setSendBufferSize(tcp.SOCKET_BUFFER_SIZE);
    channel.socket().setReceiveBufferSize(tcp.SOCKET_BUFFER_SIZE);
    if (tcp.wire.isForceBindAddress() && tcp.wire.getLocalIdentifier() != null) channel.socket().bind(new InetSocketAddress(tcp.wire.getLocalIdentifier().getAddress(),0));
    channel.configureBlocking(false);

    logger.debug("(SM) Initiating socket connection to {}", address);

    if (channel.connect(address)) {
      key = tcp.wire.getEnvironment().getSelectorManager().register(channel, this, 0);
      delivered = true;
      logger.debug("delivering1 {}", WireSocket.this);
      socketCallback.receiveResult(WireSocket.this, WireSocket.this);
    } else {
      key = tcp.wire.getEnvironment().getSelectorManager().register(channel, new SelectionKeyHandler(){

        @Override
        public void write(SelectionKey key) {
    assert tcp.wire.getEnvironment().getSelectorManager().isSelectorThread() : "selection keys are handled on the selector thread";
          WireSocket.this.write(key);
        }
        @Override
        public void read(SelectionKey key) {
    assert tcp.wire.getEnvironment().getSelectorManager().isSelectorThread() : "selection keys are handled on the selector thread";
          WireSocket.this.read(key);
        }
        @Override
        public void modifyKey(SelectionKey key) {
          WireSocket.this.modifyKey(key);
        }
        /**
         * Specified by the SelectionKeyHandler interface - calling this tells this
         * socket manager that the connection has completed and we can now
         * read/write.
         *
         * @param key The key which is connectable.
         */
        @Override
        public void connect(SelectionKey key) {
          try {
            // unregister interest in connecting to this socket
            if (channel.finishConnect()) {
              try {
                key = tcp.wire.getEnvironment().getSelectorManager().register(channel, WireSocket.this, key.interestOps() & ~SelectionKey.OP_CONNECT);
              } catch (CancelledKeyException closing) {
                // close() on another thread (TCPLayer.destroy() closes all sockets on the caller's) cancelled the key
                // and is about to close the channel: fail like a connect on a closed channel
                throw new ClosedChannelException("Socket " + address + " was closed while connecting.");
              }
              delivered = true;
              logger.debug("delivering2 {}", WireSocket.this);
              tcp.wire.broadcastChannelOpened(address, WireSocket.this.options, true);
              socketCallback.receiveResult(WireSocket.this, WireSocket.this);
            }
          } catch (IOException e) {
            if (socketCallback == null) {
              tcp.wire.getErrorHandler().receivedException(address, e);
            } else {
              socketCallback.receiveException(WireSocket.this, e);
            }
            close();
          }
        }
      }, SelectionKey.OP_CONNECT);
    }
  }

  @Override
  public String toString() {
    return "SM "+addr+" "+channel;
  }

  /**
   * Method which closes down this socket manager, by closing the socket,
   * cancelling the key and setting the key to be interested in nothing
   */
  @Override
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void close() {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Closing " + this +" r:"+reader+" w:"+writer);
      }

      // the selector thread and a thread destroying the transport layer may close at the same time: only one may
      SelectionKey closing;
      synchronized (this) {
        closing = key;
        key = null;
      }
      if (closing == null) {
        // we were already closed
        return;
      }
      closing.cancel();
      closing.attach(null);

      leaveHandshake(); // a closed socket no longer counts against its address

      if (channel != null) {
        channel.close();
      }
      tcp.socketClosed(this);


      tcp.wire.getEnvironment().getSelectorManager().invoke(() -> {
        // notify the writer/reader because an intermediate layer may have closed the socket, and they need to know
        if (writer != null) {
          if (writer == reader) {
            P2PSocketReceiver<InetSocketAddress> temp = writer;
            writer = null;
            reader = null;
            temp.receiveException(WireSocket.this, new ClosedChannelException("Channel closed. "+WireSocket.this));
          } else {
            P2PSocketReceiver<InetSocketAddress> temp = writer;
            writer = null;
            temp.receiveException(WireSocket.this, new ClosedChannelException("Channel closed. "+WireSocket.this));
          }
        }

        if (reader != null) {
          if (tcp.isDestroyed()) return;
          P2PSocketReceiver<InetSocketAddress> temp = reader;
          reader = null;
          temp.receiveException(WireSocket.this, new ClosedChannelException("Channel closed."));
        }
      });
    } catch (IOException e) {
      logger.atError().log(() -> "ERROR: Received exception " + e + " while closing socket!");
    }
  }

  /**
   * Method which should change the interestOps of the handler's key. This
   * method should *ONLY* be called by the selection thread in the context of
   * a select().
   *
   * @param key The key in question
   */
  @Override
  public synchronized void modifyKey(SelectionKey key) {
    int flag = 0;
    if (reader != null) {
      flag |= SelectionKey.OP_READ;
    }
    if (writer != null) {
      flag |= SelectionKey.OP_WRITE;
    }
    setInterest(key, flag);
  }

  /**
   * Sets the interest of this socket's key, unless the key is cancelled already: close() may run on any thread
   * (TCPLayer.destroy() closes all sockets on the caller's), also while the selector thread handles the key. The
   * socket is closed then, and close() tells the reader and writer.
   */
  private static void setInterest(SelectionKey key, int ops) {
    try {
      key.interestOps(ops);
    } catch (CancelledKeyException closed) {
      // closed meanwhile, there is no interest left to set
    }
  }

  private static void dropInterest(SelectionKey key, int op) {
    try {
      key.interestOps(key.interestOps() & ~op);
    } catch (CancelledKeyException closed) {
      // closed meanwhile, see setInterest()
    }
  }

  /**
   * Reads from the socket attached to this connector.
   *
   * @param key The selection key for this manager
   */
  @Override
  public void read(SelectionKey key) {
    P2PSocketReceiver<InetSocketAddress> temp = null;
    synchronized(this) {
      if (reader == null) {
        dropInterest(key, SelectionKey.OP_READ);
        return;
      }
      temp = reader;
      reader = null;
    } // synchronized(this)
    try {
      temp.receiveSelectResult(this, true, false);
    } catch (IOException ioe) {
      temp.receiveException(this, ioe);
    } catch (RuntimeException e) {
      receiverFailed(temp, e);
    }
    tcp.wire.getEnvironment().getSelectorManager().modifyKey(key);
  }

  /**
   * A receiver threw an unchecked exception while handling this socket. Every layer above parses what the peer sent
   * in its receiver, so a malformed stream that one of them does not reject cleanly must cost this socket, not the
   * selector thread: an exception that escapes to the selector reaches the ExceptionStrategy (which by default
   * destroyed the environment and every node in it, and still does for an Error of the virtual machine). The exception may also be a local bug, so it is logged with its
   * stack trace and reported to the error handler. Errors (OutOfMemoryError and the like) still propagate.
   */
  private void receiverFailed(P2PSocketReceiver<InetSocketAddress> receiver, RuntimeException e) {
    tcp.wire.getEnvironment().getLogRateLimiter().at(logger, Level.WARN, "wire.receiver-threw")
        .setCause(e).log("Closing {}: its receiver {} threw", this, receiver);
    tcp.wire.getErrorHandler().receivedException(addr, e);
    close();
    try {
      receiver.receiveException(this, new IOException("Socket closed because its receiver threw " + e, e));
    } catch (RuntimeException again) {
      logger.warn("Receiver {} of {} threw again when told that the socket closed", receiver, this, again);
    }
  }

  /**
   * Writes to the socket attached to this socket manager.
   *
   * @param key The selection key for this manager
   */
  @Override
  public void write(SelectionKey key) {
    P2PSocketReceiver<InetSocketAddress> temp = null;
    synchronized(this) {
      if (writer == null) {
        dropInterest(key, SelectionKey.OP_WRITE);
        return;
      }
      temp = writer;
      writer = null;
    }
    try {
      temp.receiveSelectResult(this, false, true);
    } catch (IOException ioe) {
      temp.receiveException(this, ioe);
    } catch (RuntimeException e) {
      receiverFailed(temp, e);
    }
    tcp.wire.getEnvironment().getSelectorManager().modifyKey(key);
  }

  @Override
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public synchronized void register(final boolean wantToRead, final boolean wantToWrite, P2PSocketReceiver<InetSocketAddress> receiver) {
    logger.atTrace().log(() -> this+".register("+(wantToRead?"r":"")+(wantToWrite?"w":"")+","+receiver+")");
    if (key == null) {
      ClosedChannelException cce = new ClosedChannelException("Socket "+addr+" "+WireSocket.this+" is already closed.");
      logger.info("Socket {} {} is already closed.", addr, this, cce);
      receiver.receiveException(this, cce);
      return;
    }
    // this check happens before setting the reader because we don't want to change any state if the exception is going ot be thrown
    // so don't put this check down below!
    if (wantToWrite) {
      if (channel.socket().isOutputShutdown()) {
        receiver.receiveException(this,
            new ClosedChannelException("Socket "+addr+" "+WireSocket.this+" already shut down output."));
        return;
      }
      if (writer != null) {
        if (writer != receiver) {
          throw new IllegalStateException("Already registered "+writer+" for writing, you can't register "+receiver+" for writing as well! SM:"+this);
        }
      }
    }

    if (wantToRead) {
      if (reader != null) {
        if (reader != receiver) throw new IllegalStateException("Already registered "+reader+" for reading, you can't register "+receiver+" for reading as well!");
      }
      reader = receiver;
    }

    if (wantToWrite) {
      writer = receiver;
    }
    tcp.wire.getEnvironment().getSelectorManager().modifyKey(key);
  }

  /**
   * Method which initiates a shutdown of this socket by calling
   * shutdownOutput().  This has the effect of removing the manager from
   * the open list.
   */
  @Override
  public void shutdownOutput() {
    boolean closeMe = false;
    synchronized(this) {
      if (key == null) {
        throw new IllegalStateException("Socket already closed.");
      }

      if (channel.socket().isClosed()) {
      }

      try {
        logger.debug("Shutting down output on app connection {}", this);

        // do we need to do this?  or does this happen twice now?

            channel.socket().shutdownOutput();

            tcp.wire.getEnvironment().getSelectorManager().invoke(() -> {
              // notify the writer/reader because an intermediate layer may have closed the socket, and they need to know
              if (writer != null) {
                  writer.receiveException(WireSocket.this, new ClosedChannelException("Channel shut down."));
                writer = null;
              }
            });

      } catch (IOException e) {
        logger.error("ERROR: Received exception {} while shutting down output for socket {}", e, this);
        closeMe = true;
      }
    } // synchronized(this)
    tcp.wire.getEnvironment().getSelectorManager().modifyKey(key);

    // close has it's own synchronization semantics, don't want to be holding a lock when calling
    if (closeMe) {
      close();
    }
  }

  @Override
  public long read(ByteBuffer dst) throws IOException {
    if (key == null || channel.socket().isInputShutdown()) return -1;
    try {
      long ret = channel.read(dst);
      if (logger.isTraceEnabled()) {
        logger.trace(this+"read("+ret+"):"+Arrays.toString(ByteBuffers.peekPreceding(dst, ret)));
      }
      return ret;
    } catch (IOException ioe) {
      if (logger.isDebugEnabled()) {
        logger.debug(this+" error reading", ioe);
      } else tcp.wire.getEnvironment().getLogRateLimiter().at(logger, Level.INFO, "wire.read-error").log("{} error reading", this);
      close();
      throw ioe;
    }
  }

  @Override
  public long write(ByteBuffer src) throws IOException {
    if (key == null || channel.socket().isOutputShutdown()) return -1;
    try {
      long ret = channel.write(src);
      if (logger.isTraceEnabled()) {
        logger.trace(this+"write("+ret+"):"+Arrays.toString(ByteBuffers.peekPreceding(src, ret)));
      }
      return ret;
    } catch (IOException ioe) {
      if (logger.isTraceEnabled()) {
        logger.trace(this+" error writing", ioe);
      } else logger.debug("{} error writing", this);
      close();
      throw ioe;
    }
  }

  @Override
  public boolean cancel() {
    if (key == null) return false;
    if (delivered) throw new IllegalStateException(this+".cancel() Can't cancel, already delivered");
    close();
    return true;
  }

  @Override
  public InetSocketAddress getIdentifier() {
    return addr;
  }

  @Override
  public @Nullable Map<String, Object> getOptions() {
    return options;
  }

  public SocketChannel getSocketChannel() {
    if (key != null) tcp.wire.getEnvironment().getSelectorManager().cancel(key);
    return channel;
  }

}
