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
import rice.Continuation;

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

  private final SSLEngine engine;
  private final SSLTransportLayerImpl<Identifier, ?> sslTL;

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

  private final Continuation<SSLSocketManager<Identifier>, Exception> c;

  private boolean doneHandshaking = false;

  private @Nullable Map<String, Object> options;

  private final boolean useClientAuth;
  private final boolean server;

  private @Nullable String name;

  /**
   * Called on incoming side
   */
  @SuppressWarnings("JdkObsolete") // a short FIFO; LinkedList is fine here
  public SSLSocketManager(SSLTransportLayerImpl<Identifier, ?> sslTL,
      P2PSocket<Identifier> s,
      Continuation<SSLSocketManager<Identifier>, Exception> c, boolean server, boolean useClientAuth) {
    this.server = server;
    this.useClientAuth = useClientAuth;
    this.sslTL = sslTL;
    this.socket = s;
    this.c = c;
    this.logger = sslTL.getLogger();

    engine = sslTL.getContext().createSSLEngine(s.getIdentifier().toString(), 0);
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

  @Override
  public void receiveSelectResult(P2PSocket<Identifier> socket,
      boolean canRead, boolean canWrite) throws IOException {
    if (handshakeFail) return;
    if (canWrite) {
      Iterator<ByteBuffer> i = writeMe.iterator();
      while (i.hasNext()) {
        ByteBuffer b = i.next();
        socket.write(b);
        if (b.hasRemaining()) break;
        i.remove();
      }
      if (writeMe.isEmpty()) {
        if (registeredToWrite != null) {
          P2PSocketReceiver<Identifier> temp = registeredToWrite;
          registeredToWrite = null;
          temp.receiveSelectResult(this, false, true);
        }
      } else {
        socket.register(false, true, this);
      }
    }
    if (canRead) {
      if (doneHandshaking) {
        if (read()) {
          if (registeredToRead != null) {
            P2PSocketReceiver<Identifier> temp = registeredToRead;
            registeredToRead = null;
            temp.receiveSelectResult(this, true, false);
          }
        }
      } else {
        read();
        continueHandshaking();
      }
    }
  }

  protected boolean read() throws IOException {
    ByteBuffer foo = ByteBuffer.allocate(netBufferMax);
    if (socket.read(foo) < 0) {
      fail(new ClosedChannelException("Unexpected socket closure "+this));
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
      for (ByteBuffer b : unwrapMe) len += b.remaining();
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
    logger.warn("fail:", e);
    Throwable e2 = e;
    while (e2.getCause() != null) {
      logger.warn("fail cause:", e2.getCause());
      e2 = e2.getCause();
    }
    handshakeFail = true;
    c.receiveException(e);
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
            options = OptionsFactory.addOption(socket.getOptions(), SSLTransportLayer.OPTION_CERT_SUBJECT, name);
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
      c.receiveResult(this);
      return true;
    }
    return false;
  }

  /*
   * If the result indicates that we have outstanding tasks to do, go ahead and
   * run them in this thread.
   */
  boolean runningTaskLock = false;

  @SuppressWarnings("NullAway") // the delegated-task job has no result
  private void runDelegatedTasks() {
    if (status == HandshakeStatus.NEED_TASK) {
      runningTaskLock = true;
      sslTL.getEnvironment().getProcessor().process(() -> {
        Runnable runnable;
        while ((runnable = engine.getDelegatedTask()) != null) {
          runnable.run();
        }
        status = engine.getHandshakeStatus();
        if (status == HandshakeStatus.NEED_TASK) {
          fail(new IOException("handshake shouldn't need additional tasks"));
          return null;
        }
        return null;
      },new Continuation<Object, Exception>() {
        @Override
        public void receiveException(Exception exception) {
          logger.warn("SSL handshake task failed", exception);
        };
        @Override
        public void receiveResult(Object result) {
          runningTaskLock = false;
          continueHandshaking();
        };
      }
      , sslTL.getEnvironment().getSelectorManager(), sslTL.getEnvironment().getTimeSource(), sslTL.getEnvironment().getLogManager());
    }
  }

  @Override
  public void register(boolean wantToRead, boolean wantToWrite,
      P2PSocketReceiver<Identifier> receiver) {
    if (wantToRead) {
      // try to read, decrypt
      if (readMe.isEmpty() && !unwrapMe.isEmpty()) {
        // records that arrived together with the end of the handshake are already buffered
        try {
          unwrap();
        } catch (SSLException ssle) {
          receiver.receiveException(this, ssle);
          return;
        }
      }
      if (!readMe.isEmpty()) {
        try {
          receiver.receiveSelectResult(this, true, false);
        } catch (IOException ioe) {
          receiver.receiveException(this, ioe);
        }
      } else {
        // TODO: check that no other receiver is registered to read; it would be silently replaced
        registeredToRead = receiver;
        socket.register(true, false, this);
      }
    }

    if (wantToWrite) {
      if (writeMe.isEmpty()) {
        try {
          receiver.receiveSelectResult(this, false, true);
        } catch (IOException ioe) {
          receiver.receiveException(this, ioe);
        }
      } else {
        // TODO: check that no other receiver is registered to write; it would be silently replaced
        registeredToWrite = receiver;
        socket.register(false, true, this);
      }
    }
  }

  @Nullable P2PSocketReceiver<Identifier> registeredToRead;
  @Nullable P2PSocketReceiver<Identifier> registeredToWrite;

  @Override
  public long read(ByteBuffer dsts) throws IOException {

    long start = dsts.position();
    unwrap();
    while(dsts.hasRemaining() && !readMe.isEmpty()) {
      // dsts.put() can overflow, must do the bounds checking manually
      ByteBuffer foo = readMe.getFirst();
      int len = Math.min(dsts.remaining(), foo.remaining());
      int pos = foo.position();
      dsts.put(ByteBuffers.array(foo), foo.arrayOffset() + pos, len);
      foo.position(pos+len);
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

  @Override
  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
    c.receiveException(ioe);
  }
}
