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
package org.mpisws.p2p.transport.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.security.auth.x500.X500Principal;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.util.ByteBuffers;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import rice.Continuation;
import rice.selector.SelectorLoop;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;

public class SSLSocketManager<Identifier> implements P2PSocket<Identifier>,
    P2PSocketReceiver<Identifier> {
  /**
   * The cert of the remote node. (not applicable for non-clientAuth servers.
   */
  private @Nullable X509Certificate peerCert = null;

  private final P2PSocket<Identifier> socket;

  /** the only protocol version spoken: TLS 1.3 (no peer of this code base needs TLS 1.2) */
  private static final String[] ENABLED_PROTOCOLS = {"TLSv1.3"};
  /** the TLS 1.3 cipher suites of RFC 8446 that this code base uses, all AEAD */
  private static final String[] ENABLED_CIPHER_SUITES = {"TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256", "TLS_CHACHA20_POLY1305_SHA256"};

  private final SSLEngine engine;
  private final SSLTransportLayer<Identifier, ?> sslTL;

  private final Logger logger;


  private HandshakeStatus status = HandshakeStatus.NOT_HANDSHAKING; // replaced by every handleResult()

  @SuppressWarnings("JdkObsolete") // a short FIFO; LinkedList is fine here
  private final LinkedList<ByteBuffer> writeMe = new LinkedList<ByteBuffer>(); // cipher, outgoing
  @SuppressWarnings("JdkObsolete") // a short FIFO; LinkedList is fine here
  private final LinkedList<ByteBuffer> unwrapMe = new LinkedList<ByteBuffer>(); // cipher, incoming
  private final LinkedList<ByteBuffer> readMe; // plain, incoming


  private final ByteBuffer bogusEncryptMe;

  private final int appBufferMax;
  private final int netBufferMax;

  private final Continuation<SSLSocketManager<Identifier>, Exception> continuation;

  private boolean doneHandshaking = false;

  private @Nullable Map<String, Object> options;

  private final boolean useClientAuth;
  private final boolean server;

  private @Nullable String name;

  /**
   * Called on incoming side
   */
  @SuppressWarnings("JdkObsolete") // a short FIFO; LinkedList is fine here
  public SSLSocketManager(SSLTransportLayer<Identifier, ?> sslTL,
      P2PSocket<Identifier> s,
      Continuation<SSLSocketManager<Identifier>, Exception> continuation, boolean server, boolean useClientAuth) {
    this.server = server;
    this.useClientAuth = useClientAuth;
    this.sslTL = sslTL;
    this.socket = s;
    this.continuation = continuation;
    this.logger = sslTL.getLogger();

    engine = sslTL.getContext().createSSLEngine(s.getIdentifier().toString(), 0);
    // pinned, not the JDK defaults (which a security property or a newer JDK may widen): TLS 1.3 with AEAD cipher suites only
    engine.setEnabledProtocols(ENABLED_PROTOCOLS.clone());
    engine.setEnabledCipherSuites(ENABLED_CIPHER_SUITES.clone());
    engine.setUseClientMode(!server);
    if (server && useClientAuth) engine.setNeedClientAuth(true);

    appBufferMax = engine.getSession().getApplicationBufferSize();
    netBufferMax = engine.getSession().getPacketBufferSize();

    bogusEncryptMe = ByteBuffer.allocate(0);
    readMe = new LinkedList<ByteBuffer>();

    socket.register(true, false, this);

    handshakeWrap();
  }

  @Override
  public String toString() {
    return "SSLSocket to "+(name == null ? "unknown" : name)+" at "+socket.toString();
  }

  protected void handleResult(SSLEngineResult result) {
    this.status = result.getHandshakeStatus();
  }

  boolean handshakeFail = false;

  /** the wire returned the end of its stream after the handshake (selector thread only) */
  private boolean hasPeerClosedWire = false;
  /** read(ByteBuffer) returned -1 (selector thread only) */
  private boolean hasDeliveredEndOfStream = false;

  /**
   * Whether no more plaintext can come: the peer sent close_notify, or its TCP connection ended (without close_notify
   * that is a truncation, which the reader sees as the same end of the stream; what is buffered is still delivered).
   */
  private boolean isAtEndOfStream() {
    return hasPeerClosedWire || engine.isInboundDone();
  }

  @Override
  public void receiveSelectResult(P2PSocket<Identifier> socket,
      boolean canRead, boolean canWrite) throws IOException {
    if (handshakeFail) return;
    if (canWrite) {
      Iterator<ByteBuffer> iterator = writeMe.iterator();
      while (iterator.hasNext()) {
        ByteBuffer buffer = iterator.next();
        socket.write(buffer);
        if (buffer.hasRemaining()) break;
        iterator.remove();
      }
      if (writeMe.isEmpty()) {
        deliverWritable();
      } else {
        socket.register(false, true, this);
      }
    }
    if (canRead) {
      if (doneHandshaking) {
        // bytes, or the end of the stream: either way the reader must hear of it
        if ((read() || hasPeerClosedWire) && registeredToRead != null) {
          deliverBufferedReads(true);
        }
      } else {
        read();
        continueHandshaking();
      }
    }
  }

  /**
   * Reads cipher text off the wire into unwrapMe.
   *
   * @return whether bytes were read
   */
  protected boolean read() throws IOException {
    if (hasPeerClosedWire) return false;
    ByteBuffer foo = ByteBuffer.allocate(netBufferMax);
    if (socket.read(foo) < 0) {
      if (doneHandshaking) {
        // after the handshake the reader learns of it: read(ByteBuffer) returns -1 once the plaintext is consumed
        hasPeerClosedWire = true;
      } else {
        fail(new ClosedChannelException("Unexpected socket closure "+this));
      }
    }
    if (foo.position() != 0) {
      foo.flip();
      unwrapMe.addLast(foo);
      return true;
    }
    return false;
  }

  protected void handshakeWrap() {
      try {
        ByteBuffer outgoing = ByteBuffer.allocate(netBufferMax);
        handleResult(engine.wrap(bogusEncryptMe, outgoing));
        if (outgoing.position() != 0) {
          outgoing.flip();
          writeMe.addLast(outgoing);
          socket.register(false, true, this);
        }
      } catch (SSLException e) {
        fail(e);
        return;
      }
      continueHandshaking();
  }

  /**
   * Unwraps as many complete TLS records as are buffered. Stops early when the handshake needs to
   * run a task or send something, so that continueHandshaking() can do it first.
   */
  protected void unwrap() throws SSLException{
    if (unwrapMe.isEmpty()) return;
    if (unwrapMe.size() > 1) {
      // a record can be split across socket reads: coalesce, so that the engine sees it whole
      int len = 0;
      for (ByteBuffer buffer : unwrapMe) len += buffer.remaining();
      ByteBuffer all = ByteBuffer.allocate(len);
      for (ByteBuffer b : unwrapMe) all.put(b);
      all.flip();
      unwrapMe.clear();
      unwrapMe.add(all);
    }
    ByteBuffer b = unwrapMe.getFirst();
    while (b.hasRemaining()) {
      ByteBuffer foo = ByteBuffer.allocate(appBufferMax);
      SSLEngineResult unwrapped = engine.unwrap(b, foo);
      handleResult(unwrapped);
      if (foo.position() != 0) {
        foo.flip();
        readMe.addLast(foo);
      }
      if (unwrapped.getStatus() != SSLEngineResult.Status.OK || unwrapped.bytesConsumed() == 0) break; // need more bytes, or closed
      if (status == NEED_TASK || status == NEED_WRAP) break; // let the handshake proceed first
    }
    if (!b.hasRemaining()) unwrapMe.removeFirst();
  }

  protected void handshakeUnwrap() {
    try {
      if (unwrapMe.isEmpty()) {
        if (read()) {
        } else {
          socket.register(true, false, this);
          return; // wait for bytes
        }
      }
      unwrap();
    } catch (Exception e) {
      fail(e);
      return;
    }
    continueHandshaking();
  }

  protected void fail(Exception e) {
    if (doneHandshaking) return;
    // a peer can fail handshakes as fast as it can connect: one rate-limited line, the stack trace at debug
    Throwable rootCause = e;
    while (rootCause.getCause() != null) rootCause = rootCause.getCause();
    sslTL.getEnvironment().getLogRateLimiter().at(logger, Level.WARN, "ssl.handshake-failed")
        .log("TLS handshake of {} failed: {} (root cause {})", this, e, rootCause);
    logger.debug("TLS handshake of {} failed", this, e);
    handshakeFail = true;
    continuation.receiveException(e);
    socket.close();
    doneHandshaking = true;
  }

  protected void continueHandshaking() {
    if (runningTaskLock) {
      return; // wait for processing to finish
    }
    switch(status) {
    case NOT_HANDSHAKING:
      return;
    case FINISHED:
      checkDone();
      return;
    case NEED_TASK:
      // continueHandshaking() is called again when the tasks are done
      runDelegatedTasks();
      return;
    case NEED_WRAP:
      handshakeWrap();
      return;
    case NEED_UNWRAP:
      handshakeUnwrap();
      return;
    default:
      // NEED_UNWRAP_AGAIN only occurs with DTLS
      return;
    }
  }

  /**
   * Return true when done.
   */
  protected boolean checkDone() {
    if (((status == HandshakeStatus.FINISHED) || (status == HandshakeStatus.NOT_HANDSHAKING))) {
      if (!server || useClientAuth) {
        try {
          peerCert = ((X509Certificate) engine.getSession().getPeerCertificates()[0]);
          // RFC 1779 is the format the deprecated getSubjectDN().getName() produced (", " separated RDNs)
          name = peerCert.getSubjectX500Principal().getName(X500Principal.RFC1779);
          if (name.startsWith("CN=")) {
            name = name.substring(3);
            if (!sslTL.getSubjectValidator().test(name)) {
              fail(new IllegalArgumentException("Certificate CN is not acceptable: "+name+" "+this));
              return false;
            }
            options = OptionsFactory.addOption(socket.getOptions(), SSLTransportLayer.OPTION_CERT_SUBJECT, name,
                SSLTransportLayer.OPTION_OUTGOING, Boolean.valueOf(!server));
          } else {
            fail(new IllegalArgumentException("CN must start with CN= "+name+" "+this));
            return false;
          }
        } catch (Exception e) {
          fail(e);
          return false;
        }
      }
      doneHandshaking = true;
      continuation.receiveResult(this);
      return true;
    }
    return false;
  }

  /**
   * True while the delegated tasks of the handshake run on the handshake executor of {@link SSLTransportLayer}; the
   * handshake waits for them (selector thread only).
   */
  boolean runningTaskLock = false;

  /**
   * Runs the delegated tasks of the engine (the RSA and key exchange work) on the handshake executor of the layer, not
   * on the selector thread nor on the shared processing thread; the result comes back to the selector thread. An
   * incoming handshake that finds the executor's queue full fails, which closes its socket.
   */
  private void runDelegatedTasks() {
    if (status != HandshakeStatus.NEED_TASK) return;
    runningTaskLock = true;
    SelectorLoop selector = sslTL.getEnvironment().getSelectorManager();
    try {
      sslTL.executeHandshakeTask(server, () -> {
        RuntimeException failure = null;
        try {
          Runnable task;
          while ((task = engine.getDelegatedTask()) != null) {
            task.run();
          }
        } catch (RuntimeException e) {
          failure = e;
        }
        RuntimeException taskFailure = failure;
        selector.invoke(() -> delegatedTasksDone(taskFailure));
      });
    } catch (RejectedExecutionException full) {
      runningTaskLock = false;
      fail(new IOException("Refused the TLS handshake: no room in the handshake queue", full));
    }
  }

  /** Continues the handshake after its delegated tasks ran (selector thread). */
  private void delegatedTasksDone(@Nullable RuntimeException failure) {
    runningTaskLock = false;
    if (handshakeFail) return;
    if (failure != null) {
      fail(failure);
      return;
    }
    status = engine.getHandshakeStatus();
    if (status == HandshakeStatus.NEED_TASK) {
      fail(new IOException("handshake shouldn't need additional tasks"));
      return;
    }
    continueHandshaking();
  }

  @Override
  public void register(boolean wantToRead, boolean wantToWrite,
      P2PSocketReceiver<Identifier> receiver) {
    if (wantToRead) {
      // TODO: check that no other receiver is registered to read; it would be silently replaced
      registeredToRead = receiver;
      if (!deliveringBufferedReads) {
        // a receiver registering from inside a read callback is served by the running loop, not by a nested call
        deliverBufferedReads(false);
      }
    }

    if (wantToWrite) {
      // TODO: check that no other receiver is registered to write; it would be silently replaced
      registeredToWrite = receiver;
      if (!deliveringWritable) {
        // a receiver registering from inside a write callback is served by the running loop, not by a nested call
        deliverWritable();
      }
    }
  }

  /** true while deliverWritable() runs write callbacks; a register() from one of them only records the receiver */
  private boolean deliveringWritable = false;

  /**
   * Tells the receiver registered to write that it can write, as long as no cipher text waits for the wire, one
   * callback after another in a loop: a receiver that writes a little and registers again is called again by this
   * loop, not from inside its own register(), so the stack depth does not grow with the writes. After
   * {@link SSLTransportLayer#getMaxReadCallbacksPerSelect()} callbacks (the same budget as for reads) the rest waits
   * for a later turn of the selector, so that one socket cannot keep the selector thread from other sockets.
   */
  private void deliverWritable() {
    if (deliveringWritable) return; // the running loop calls the receiver that registered meanwhile
    deliveringWritable = true;
    try {
      int callbacks = 0;
      P2PSocketReceiver<Identifier> receiver;
      while ((receiver = registeredToWrite) != null) {
        if (!writeMe.isEmpty() || callbacks >= sslTL.getMaxReadCallbacksPerSelect()) {
          // the wire calls receiveSelectResult() when it can take more, at the latest on the next turn
          socket.register(false, true, this);
          return;
        }
        registeredToWrite = null;
        callbacks++;
        try {
          receiver.receiveSelectResult(this, false, true);
        } catch (IOException ioe) {
          receiver.receiveException(this, ioe);
        }
      }
    } finally {
      deliveringWritable = false;
    }
  }

  @Nullable P2PSocketReceiver<Identifier> registeredToRead;
  @Nullable P2PSocketReceiver<Identifier> registeredToWrite;

  /** true while deliverBufferedReads() runs read callbacks; a register() from one of them only records the receiver */
  private boolean deliveringBufferedReads = false;
  /** a deliverBufferedReads() is queued on the selector for the plaintext left over from an exhausted turn */
  private boolean deliveryContinuationQueued = false;

  /**
   * Hands decrypted bytes to the receiver registered to read, one callback after another in a loop: a receiver that
   * reads a little and registers again (the priority layer does so for every frame) is called again by this loop,
   * not from inside its own register(), so the stack depth does not grow with the bytes buffered. After
   * {@link SSLTransportLayer#getMaxReadCallbacksPerSelect()} callbacks the rest waits for a later turn of the selector,
   * so that one socket cannot keep the selector thread from other sockets. Callbacks run in the order of the
   * registrations, on the bytes in the order received.
   *
   * @param bytesJustRead the selector reported the socket readable and bytes were read: call the receiver once even
   *     if they do not complete a TLS record yet, as before (it reads 0 bytes and registers again)
   */
  private void deliverBufferedReads(boolean bytesJustRead) {
    deliveringBufferedReads = true;
    try {
      int callbacks = 0;
      P2PSocketReceiver<Identifier> receiver;
      while ((receiver = registeredToRead) != null) {
        if (!(bytesJustRead && callbacks == 0)) {
          try {
            if (!hasBufferedPlaintext()) {
              if (isAtEndOfStream()) {
                // the receiver reads -1, once: after that it is not called again
                if (hasDeliveredEndOfStream) return;
              } else {
                // nothing decrypted is left: wait for the wire
                socket.register(true, false, this);
                return;
              }
            }
          } catch (SSLException ssle) {
            registeredToRead = null;
            receiver.receiveException(this, ssle);
            return;
          }
          if (callbacks >= sslTL.getMaxReadCallbacksPerSelect()) {
            queueDeliveryContinuation();
            return;
          }
        }
        registeredToRead = null;
        callbacks++;
        try {
          receiver.receiveSelectResult(this, true, false);
        } catch (IOException ioe) {
          receiver.receiveException(this, ioe);
        }
      }
    } finally {
      deliveringBufferedReads = false;
    }
  }

  /** whether decrypted bytes are ready for read(ByteBuffer), decrypting buffered records if needed */
  private boolean hasBufferedPlaintext() throws SSLException {
    if (readMe.isEmpty() && !unwrapMe.isEmpty()) {
      // for example records that arrived together with the end of the handshake
      unwrap();
    }
    return !readMe.isEmpty();
  }

  private void queueDeliveryContinuation() {
    if (deliveryContinuationQueued) return;
    deliveryContinuationQueued = true;
    sslTL.getEnvironment().getSelectorManager().invoke(() -> {
      deliveryContinuationQueued = false;
      if (!deliveringBufferedReads) deliverBufferedReads(false);
    });
  }

  @Override
  public long read(ByteBuffer dsts) throws IOException {

    long start = dsts.position();
    unwrap();
    while(dsts.hasRemaining() && !readMe.isEmpty()) {
      // dsts.put() can overflow, must do the bounds checking manually
      ByteBuffer foo = readMe.getFirst();
      int length = Math.min(dsts.remaining(), foo.remaining());
      int pos = foo.position();
      dsts.put(ByteBuffers.array(foo), foo.arrayOffset() + pos, length);
      foo.position(pos+length);
      if (foo.hasRemaining()) {
        return dsts.position()-start;
      } else {
        readMe.removeFirst();
      }
    }

    // now try reading off the socket
    if (dsts.hasRemaining() && readMe.isEmpty()) {
      if (read()) {
        unwrap();
        while(dsts.hasRemaining() && !readMe.isEmpty()) {
          ByteBuffer foo = readMe.getFirst();
          int len = Math.min(dsts.remaining(), foo.remaining());
          int pos = foo.position();
          dsts.put(ByteBuffers.array(foo), foo.arrayOffset() + pos, len);
          foo.position(pos+len);
          if (!foo.hasRemaining()) readMe.removeFirst();
        }
      }
    }
    if (dsts.position() == start && dsts.hasRemaining() && readMe.isEmpty() && isAtEndOfStream()) {
      hasDeliveredEndOfStream = true;
      return -1;
    }
    return dsts.position()-start;
  }

  @Override
  public long write(ByteBuffer srcs) throws IOException {
    ByteBuffer outgoing = ByteBuffer.allocate(netBufferMax);
    SSLEngineResult tempResult = engine.wrap(srcs, outgoing);
    if (outgoing.position() != 0) {
      outgoing.flip();
      writeMe.addLast(outgoing);
      // try to write it to the wire
      receiveSelectResult(socket, false, true);
    }
    return tempResult.bytesConsumed();
  }

  @Override
  public void close() {
    socket.close();
  }

  @Override
  public void handshakeCompleted() {
    socket.handshakeCompleted();
  }

  @Override
  public Identifier getIdentifier() {
    return socket.getIdentifier();
  }

  @Override
  public @Nullable Map<String, Object> getOptions() {
    return options;
  }

  @Override
  public void shutdownOutput() {
    engine.closeOutbound();
  }

  /**
   * The wire socket failed (for example a connection reset). During the handshake that fails the handshake; after it
   * the receivers registered to read and to write are told, as a plain socket would tell them.
   */
  @Override
  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
    if (!doneHandshaking) {
      continuation.receiveException(ioe);
      return;
    }
    P2PSocketReceiver<Identifier> reader = registeredToRead;
    P2PSocketReceiver<Identifier> writer = registeredToWrite;
    registeredToRead = null;
    registeredToWrite = null;
    if (reader != null) reader.receiveException(this, ioe);
    // the same receiver may be registered for both: tell it once
    if (writer != null && !Objects.equals(writer, reader)) writer.receiveException(this, ioe);
  }
}
