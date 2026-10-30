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
package org.mpisws.p2p.testing.transportlayer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.pki.x509.CAToolImpl;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.mpisws.p2p.transport.ssl.SSLTransportLayerImpl;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;
import rice.environment.Environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Two SSL transport layers ("alice" accepts, "bob" connects) over real loopback TCP sockets, with
 * certificates from {@link CAToolImpl}'s defaults (2048-bit RSA, SHA256withRSA, PKCS12 stores), which
 * the JDK's TLS disabled-algorithm constraints accept.
 * <p>
 * Replaces the former main() programs SSLTest and SSLTestNoClientAuth (and the empty SSLTest2).
 */
public class SSLTest {
  static final long TIMEOUT_SECONDS = 30;
  static final String PAYLOAD = "foo";
  static final char[] NO_PASSWORD = "".toCharArray();

  static CAToolImpl ca;
  static CAToolImpl bogusCa;

  Environment rootEnv;
  List<SSLTransportLayerImpl<InetSocketAddress, ByteBuffer>> layers = new ArrayList<SSLTransportLayerImpl<InetSocketAddress, ByteBuffer>>();
  Map<Object, Environment> environments = new IdentityHashMap<Object, Environment>();
  InetAddress addr;
  int alicePort;
  int bobPort;

  /** what the accepting side read, and the certificate subject it saw */
  CompletableFuture<String> aliceRead = new CompletableFuture<String>();
  CompletableFuture<Object> aliceSawSubject = new CompletableFuture<Object>();
  /** exceptions the accepting side reported to its error handler */
  CompletableFuture<Throwable> aliceError = new CompletableFuture<Throwable>();

  static synchronized CAToolImpl ca(boolean bogus) throws Exception {
    if (ca == null) {
      ca = newCa("MyCA");
      bogusCa = newCa("Bogus");
    }
    return bogus ? bogusCa : ca;
  }

  static CAToolImpl newCa(String cn) throws Exception {
    KeyPair pair = CAToolImpl.generateKeyPair();
    Instant now = Instant.now();
    Instant expiry = now.plus(Duration.ofDays(1));
    X509Certificate cert = CAToolImpl.generateNewCA(cn, now.minus(Duration.ofMinutes(1)), expiry, 1, pair, CAToolImpl.DEFAULT_SIGNATURE_ALGORITHM);
    return new CAToolImpl(cert, pair);
  }

  /** a PKCS12 store with a new key signed by signer (if non-null) and trusting trusted (if non-null) */
  static KeyStore store(String cn, CAToolImpl signer, CAToolImpl trusted) throws Exception {
    KeyStore store = KeyStore.getInstance(CAToolImpl.DEFAULT_KEYSTORE_TYPE);
    store.load(null, null);
    if (signer != null) {
      KeyPair pair = CAToolImpl.generateKeyPair();
      X509Certificate cert = signer.sign(cn, pair.getPublic());
      store.setKeyEntry("private", pair.getPrivate(), NO_PASSWORD, new Certificate[] {cert});
    }
    if (trusted != null) store.setCertificateEntry("cert", trusted.getCertificate());
    return store;
  }

  @BeforeEach
  void setUp() throws Exception {
    addr = InetAddress.getLoopbackAddress();
    int[] ports = FreePorts.freePorts(addr, 2);
    alicePort = ports[0];
    bobPort = ports[1];
    rootEnv = new Environment();
  }

  @AfterEach
  void tearDown() {
    for (SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> tl : layers) tl.destroy();
    rootEnv.destroy();
  }

  SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> layer(String name, int port, KeyStore keys, KeyStore trust, int clientAuth) throws IOException {
    Environment env = rootEnv.cloneEnvironment(name);
    WireTransportLayerImpl wire = new WireTransportLayerImpl(new InetSocketAddress(addr, port), env, null);
    SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> ret = new SSLTransportLayerImpl<InetSocketAddress, ByteBuffer>(wire, keys, trust, clientAuth, env);
    layers.add(ret);
    environments.put(ret, env);
    return ret;
  }

  /** makes alice read PAYLOAD from incoming sockets into aliceRead */
  void acceptOn(SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> alice) {
    alice.setErrorHandler(new ErrorHandler<InetSocketAddress>() {
      @Override
      public void receivedUnexpectedData(InetSocketAddress i, byte[] bytes, int location, Map<String, Object> options) {
        aliceError.complete(new IOException("unexpected data from " + i));
      }

      @Override
      public void receivedException(InetSocketAddress i, Throwable error) {
        aliceError.complete(error);
      }
    });
    alice.setCallback(new TransportLayerCallback<InetSocketAddress, ByteBuffer>() {
      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) throws IOException {
        Map<String, Object> options = s.getOptions();
        aliceSawSubject.complete(options == null ? "" : String.valueOf(options.get(SSLTransportLayer.OPTION_CERT_SUBJECT)));
        s.register(true, false, new P2PSocketReceiver<InetSocketAddress>() {
          ByteBuffer readMe = ByteBuffer.allocate(PAYLOAD.getBytes(StandardCharsets.UTF_8).length);

          @Override
          public void receiveSelectResult(P2PSocket<InetSocketAddress> socket, boolean canRead, boolean canWrite) throws IOException {
            if (canRead) socket.read(readMe);
            if (readMe.hasRemaining()) {
              socket.register(true, false, this);
            } else {
              aliceRead.complete(new String(readMe.array(), StandardCharsets.UTF_8));
            }
          }

          @Override
          public void receiveException(P2PSocket<InetSocketAddress> socket, Exception ioe) {
            aliceRead.completeExceptionally(ioe);
          }
        });
      }

      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, Map<String, Object> options) throws IOException {
      }
    });
  }

  /**
   * bob opens a socket to alice and writes PAYLOAD
   *
   * @return completes with the opened socket, or exceptionally if opening it failed
   */
  CompletableFuture<P2PSocket<InetSocketAddress>> sendPayload(SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> bob, InetSocketAddress to) {
    final CompletableFuture<P2PSocket<InetSocketAddress>> opened = new CompletableFuture<P2PSocket<InetSocketAddress>>();
    environments.get(bob).getSelectorManager().invoke(new Runnable() {
      @Override
      public void run() {
        bob.openSocket(to, new SocketCallback<InetSocketAddress>() {
          @Override
          public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
            opened.complete(sock);
            sock.register(false, true, new P2PSocketReceiver<InetSocketAddress>() {
              ByteBuffer writeMe = ByteBuffer.wrap(PAYLOAD.getBytes(StandardCharsets.UTF_8));

              @Override
              public void receiveSelectResult(P2PSocket<InetSocketAddress> socket, boolean canRead, boolean canWrite) throws IOException {
                socket.write(writeMe);
                if (writeMe.hasRemaining()) socket.register(false, true, this);
              }

              @Override
              public void receiveException(P2PSocket<InetSocketAddress> socket, Exception ioe) {
              }
            });
          }

          @Override
          public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception ex) {
            opened.completeExceptionally(ex);
          }
        }, null);
      }
    });
    return opened;
  }

  @Test
  void mutuallyAuthenticatedSocketDeliversDataAndPeerNames() throws Exception {
    SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(false), ca(false)), store("alice", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, store("bob", ca(false), null), store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    acceptOn(alice);

    P2PSocket<InetSocketAddress> bobSocket = sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertEquals(PAYLOAD, aliceRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals("bob", aliceSawSubject.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals("alice", bobSocket.getOptions().get(SSLTransportLayer.OPTION_CERT_SUBJECT));
  }

  @Test
  void serverOnlyAuthenticationDeliversData() throws Exception {
    // alice authenticates to bob, but does not ask for a client certificate (formerly SSLTestNoClientAuth)
    SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(false), null), null, SSLTransportLayer.CLIENT_AUTH_NONE);
    SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, null, store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    acceptOn(alice);

    P2PSocket<InetSocketAddress> bobSocket = sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertEquals(PAYLOAD, aliceRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals("alice", bobSocket.getOptions().get(SSLTransportLayer.OPTION_CERT_SUBJECT));
  }

  @Test
  void clientRejectsServerCertificateFromUntrustedCa() throws Exception {
    SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(true), ca(false)), store("alice", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, store("bob", ca(false), null), store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    acceptOn(alice);

    Exception e = assertThrows(Exception.class, () -> sendPayload(bob, new InetSocketAddress(addr, alicePort)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

    assertFalse(e instanceof java.util.concurrent.TimeoutException, "handshake should fail, not hang: " + e);
    assertFalse(aliceRead.isDone() && !aliceRead.isCompletedExceptionally(), "alice must not receive data");
  }

  @Test
  void serverRejectsClientCertificateFromUntrustedCa() throws Exception {
    SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> alice = layer("alice", alicePort, store("alice", ca(false), null), store("alice", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    SSLTransportLayerImpl<InetSocketAddress, ByteBuffer> bob = layer("bob", bobPort, store("bob", ca(true), null), store("bob", null, ca(false)), SSLTransportLayer.CLIENT_AUTH_REQUIRED);
    acceptOn(alice);

    sendPayload(bob, new InetSocketAddress(addr, alicePort));

    assertNotNull(aliceError.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertFalse(aliceRead.isDone() && !aliceRead.isCompletedExceptionally(), "alice must not receive data");
  }
}
