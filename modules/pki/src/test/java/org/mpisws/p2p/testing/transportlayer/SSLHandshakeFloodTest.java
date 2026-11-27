package org.mpisws.p2p.testing.transportlayer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import rice.environment.Environment;

/**
 * C1 (R2): an outsider without a certificate can make a node do the expensive part of a TLS handshake (the server
 * signs with its RSA key before it sees that the client has no certificate) as fast as it can connect. That work must
 * not run on the shared processing thread, where it delays every other {@code process()} job of the node (Glacier,
 * replication, ...).
 */
class SSLHandshakeFloodTest {
  private static final int FLOOD_THREADS = 128;
  private static final long FLOOD_MILLIS = 4000;
  /** the flood needs this long to fill the queues before the probes count */
  private static final long WARM_UP_MILLIS = 500;
  private static final long PROBE_INTERVAL_MILLIS = 50;
  private static final long MAX_PROCESSING_DELAY_MILLIS = 100;
  private static final int CONNECT_TIMEOUT_MILLIS = 2000;
  /** high enough that the wire-layer budgets let the whole flood through to TLS */
  private static final int UNLIMITED = 1_000_000;

  private final Environment env = new Environment();
  private final ExecutorService flooders = Executors.newFixedThreadPool(FLOOD_THREADS);
  private final AtomicBoolean isFlooding = new AtomicBoolean(true);

  @AfterEach
  void tearDown() throws InterruptedException {
    isFlooding.set(false);
    flooders.shutdownNow();
    flooders.awaitTermination(10, TimeUnit.SECONDS);
    env.destroy();
  }

  @Test
  @DisplayName("R2: a TLS handshake flood without certificates does not delay the processing thread")
  void handshakeFloodDoesNotDelayProcessing() throws Exception {
    for (String param : List.of("transport_wire_max_handshaking_sockets_per_address", "transport_wire_max_handshaking_sockets_per_prefix",
        "transport_wire_max_handshaking_sockets", "transport_wire_accept_rate_per_prefix_per_second", "transport_wire_accept_burst_per_prefix",
        "transport_wire_accept_rate_per_second", "transport_wire_accept_burst")) {
      env.getParameters().setInt(param, UNLIMITED);
    }
    InetAddress loopback = InetAddress.getLoopbackAddress();
    InetSocketAddress address = new InetSocketAddress(loopback, FreePorts.freePort(loopback));
    KeyStore keys = SSLTest.store("node", SSLTest.ca(false), SSLTest.ca(false));
    WireTransportLayer wire = new WireTransportLayer(address, env, null);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> tls =
        new SSLTransportLayer<>(wire, keys, keys, SSLTransportLayer.CLIENT_AUTH_REQUIRED, env);
    tls.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, @Nullable Map<String, Object> options) {
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) {
        s.close();
      }
    });

    SSLContext outsider = SSLContext.getInstance("TLSv1.3");
    TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trust.init(SSLTest.store("outsider", null, SSLTest.ca(false)));
    outsider.init(null, trust.getTrustManagers(), null);
    AtomicInteger handshakesStarted = new AtomicInteger();
    for (int i = 0; i < FLOOD_THREADS; i++) {
      flooders.execute(() -> {
        while (isFlooding.get()) {
          try (SSLSocket socket = (SSLSocket) outsider.getSocketFactory().createSocket()) {
            socket.connect(address, CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(CONNECT_TIMEOUT_MILLIS);
            handshakesStarted.incrementAndGet();
            socket.startHandshake();
            // TLS 1.3: the client learns that the server wants a certificate it does not have only on its first read
            socket.getInputStream().read();
          } catch (IOException expected) {
            // refused for want of a certificate, or closed by the node
          }
        }
      });
    }

    Thread.sleep(WARM_UP_MILLIS);
    List<Long> delaysMillis = new ArrayList<>();
    long floodEnd = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(FLOOD_MILLIS);
    while (System.nanoTime() < floodEnd) {
      long submitted = System.nanoTime();
      CompletableFuture<Long> started = env.getProcessor().process(System::nanoTime,
          env.getSelectorManager(), env.getTimeSource(), env.getLogManager());
      delaysMillis.add(TimeUnit.NANOSECONDS.toMillis(started.get(30, TimeUnit.SECONDS) - submitted));
      Thread.sleep(PROBE_INTERVAL_MILLIS);
    }
    isFlooding.set(false);

    long maxDelayMillis = delaysMillis.stream().mapToLong(Long::longValue).max().orElse(0);
    System.out.println("R2: " + handshakesStarted.get() + " handshakes started, " + delaysMillis.size()
        + " probes, longest delay of a processing job " + maxDelayMillis + " ms");
    assertTrue(handshakesStarted.get() > FLOOD_THREADS, "the flood ran: " + handshakesStarted.get() + " handshakes");
    assertTrue(maxDelayMillis < MAX_PROCESSING_DELAY_MILLIS,
        "a processing job waited " + maxDelayMillis + " ms behind the handshake flood; delays " + delaysMillis);
  }
}
