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
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.LivenessTypes;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.proximity.ProximityListener;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import rice.environment.Environment;
import rice.environment.exception.ExceptionStrategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A peer writes a burst of many minimal framed messages (a 4-byte size of 1, then one byte) on one primary socket of a
 * {@link PriorityTransportLayer}, once over TLS ({@link SSLTransportLayer}) and once over plain TCP. The receiving
 * node must deliver every message without an {@link Error} on its selector thread (a {@link StackOverflowError} reaches
 * the ExceptionStrategy, which by default destroys the Environment), and the stack depth at delivery must not grow
 * with the number of messages already buffered.
 */
public class PriorityBurstTest {
  static final long TIMEOUT_SECONDS = 60;
  /** messages in the burst: 500 000 bytes of frames, far more than one TLS record or one socket read */
  static final int MESSAGES = 100_000;
  /** a 4-byte size and a 1-byte message */
  static final int FRAME_BYTES = Integer.BYTES + 1;
  /** how often the stack depth at delivery is sampled */
  static final int SAMPLE_EVERY_MESSAGES = 1_000;
  /** more stack frames than any non-recursive delivery path needs, far fewer than one frame per buffered message */
  static final int MAX_DELIVERY_STACK_DEPTH = 500;
  /** read callbacks one TLS socket may run per selector turn, set for this test; a message takes two (size, body) */
  static final int MAX_READ_CALLBACKS_PER_SELECT = 16;

  Environment env;
  final List<Throwable> selectorErrors = Collections.synchronizedList(new ArrayList<Throwable>());
  final List<TransportLayer<InetSocketAddress, ByteBuffer>> layers = new ArrayList<TransportLayer<InetSocketAddress, ByteBuffer>>();
  InetAddress addr;
  int alicePort;
  Socket client;

  final AtomicInteger delivered = new AtomicInteger();
  /** messages that arrived with another payload than the next one sent */
  final AtomicInteger outOfOrder = new AtomicInteger();
  final CountDownLatch allDelivered = new CountDownLatch(MESSAGES);
  /** stack depth at the delivery of every SAMPLE_EVERY_MESSAGES-th message */
  final List<Integer> depthSamples = Collections.synchronizedList(new ArrayList<Integer>());
  /** loops of the selector thread so far, counted by an invocation that re-invokes itself; selector thread only */
  long selectorTurns;
  volatile boolean countingTurns = true;
  long turnOfLastDelivery = -1;
  int deliveriesInThisTurn;
  int maxDeliveriesPerTurn;

  @BeforeEach
  void setUp() throws Exception {
    addr = InetAddress.getLoopbackAddress();
    alicePort = FreePorts.freePorts(addr, 1)[0];
    env = new Environment();
    env.getParameters().setInt("transport_ssl_max_read_callbacks_per_select", MAX_READ_CALLBACKS_PER_SELECT);
    env.getSelectorManager().invoke(new Runnable() {
      @Override
      public void run() {
        selectorTurns++;
        if (countingTurns) env.getSelectorManager().invoke(this);
      }
    });
    ExceptionStrategy destroying = env.getExceptionStrategy();
    env.setExceptionStrategy(new ExceptionStrategy() {
      @Override
      public void handleException(Object source, Throwable throwable) {
        // keep only the class and message: the stack trace of a StackOverflowError is huge
        selectorErrors.add(new AssertionError(throwable.getClass().getName() + ": " + throwable.getMessage()));
        destroying.handleException(source, throwable);
      }
    });
  }

  @AfterEach
  void tearDown() throws Exception {
    countingTurns = false;
    try {
      if (client != null) client.close();
      for (TransportLayer<InetSocketAddress, ByteBuffer> layer : layers) layer.destroy();
    } finally {
      env.destroy();
    }
  }

  @Test
  @DisplayName("a burst of minimal messages over one TLS socket is delivered with a bounded stack depth")
  void tlsBurstIsDeliveredWithoutRecursion() throws Exception {
    WireTransportLayer wire = new WireTransportLayer(new InetSocketAddress(addr, alicePort), env, null);
    SSLTransportLayer<InetSocketAddress, ByteBuffer> ssl = new SSLTransportLayer<InetSocketAddress, ByteBuffer>(wire,
        SSLTest.store("alice", SSLTest.ca(false), null), SSLTest.NO_PASSWORD, null, SSLTransportLayer.CLIENT_AUTH_NONE,
        subject -> true, env);
    layers.add(ssl);
    aliceOn(ssl);

    TrustManagerFactory trustManagers = TrustManagerFactory.getInstance("SunX509");
    trustManagers.init(SSLTest.store("client", null, SSLTest.ca(false)));
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, trustManagers.getTrustManagers(), null);
    SSLSocket tls = (SSLSocket) context.getSocketFactory().createSocket();
    client = tls;
    tls.connect(new InetSocketAddress(addr, alicePort), (int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    tls.setEnabledProtocols(new String[] {"TLSv1.3"});
    tls.startHandshake();

    sendBurstAndAwaitDelivery();
  }

  @Test
  @DisplayName("a burst of minimal messages over one plain TCP socket is delivered with a bounded stack depth")
  void plainBurstIsDeliveredWithoutRecursion() throws Exception {
    aliceOn(new WireTransportLayer(new InetSocketAddress(addr, alicePort), env, null));

    client = new Socket();
    client.connect(new InetSocketAddress(addr, alicePort), (int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));

    sendBurstAndAwaitDelivery();
  }

  /** a priority layer on top of lower that counts what it receives */
  void aliceOn(TransportLayer<InetSocketAddress, ByteBuffer> lower) {
    if (!layers.contains(lower)) layers.add(lower);
    PriorityTransportLayer<InetSocketAddress> alice = new PriorityTransportLayer<InetSocketAddress>(lower,
        new NoLiveness(), new NoProximity(), env, 1024, 30, null);
    layers.add(alice);
    alice.setCallback(new TransportLayerCallback<InetSocketAddress, ByteBuffer>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer msg, Map<String, Object> options) {
        int count = delivered.incrementAndGet();
        if (msg.remaining() != 1 || msg.get() != (byte) (count - 1)) outOfOrder.incrementAndGet();
        // runs on the selector thread, like the turn counter
        if (turnOfLastDelivery == selectorTurns) {
          deliveriesInThisTurn++;
        } else {
          turnOfLastDelivery = selectorTurns;
          deliveriesInThisTurn = 1;
        }
        maxDeliveriesPerTurn = Math.max(maxDeliveriesPerTurn, deliveriesInThisTurn);
        if (count % SAMPLE_EVERY_MESSAGES == 0) {
          depthSamples.add(StackWalker.getInstance().walk(frames -> (int) frames.count()));
        }
        allDelivered.countDown();
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> socket) {
        socket.close();
      }
    });
  }

  /** writes the primary socket header and MESSAGES empty frames in one write, then waits for all of them to arrive */
  void sendBurstAndAwaitDelivery() throws Exception {
    // one byte of payload, not none: a frame of size 0 at the very end of the stream is only read with the next bytes
    ByteBuffer burst = ByteBuffer.allocate(1 + MESSAGES * FRAME_BYTES);
    burst.put(PriorityTransportLayer.PRIMARY_SOCKET_B);
    for (int i = 0; i < MESSAGES; i++) burst.putInt(1).put((byte) i);
    OutputStream out = client.getOutputStream();
    // the write blocks until alice has read most of it; on its own thread, so that a dead receiver fails the wait below
    CompletableFuture<Void> written = CompletableFuture.runAsync(() -> {
      try {
        out.write(burst.array());
        out.flush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });

    boolean complete = allDelivered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    int maxDepth = depthSamples.stream().mapToInt(Integer::intValue).max().orElse(-1);
    String evidence = "delivered " + delivered.get() + " of " + MESSAGES + ", selector errors " + selectorErrors
        + ", at most " + maxDeliveriesPerTurn + " messages in one selector turn"
        + ", stack depth every " + SAMPLE_EVERY_MESSAGES + " messages " + depthSamples;
    System.out.println(getClass().getSimpleName() + ": " + evidence);

    assertEquals(List.of(), selectorErrors, evidence);
    assertTrue(complete, evidence);
    assertEquals(0, outOfOrder.get(), "messages out of order; " + evidence);
    assertTrue(maxDepth <= MAX_DELIVERY_STACK_DEPTH, "max stack depth " + maxDepth + "; " + evidence);
    // other sockets get a turn: one socket's backlog is spread over many turns of the selector
    assertTrue(maxDeliveriesPerTurn <= MAX_READ_CALLBACKS_PER_SELECT, "messages in one turn " + maxDeliveriesPerTurn + "; " + evidence);
    written.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  static final class NoLiveness implements LivenessProvider<InetSocketAddress> {
    @Override
    public int getLiveness(InetSocketAddress identifier, Map<String, Object> options) {
      return LivenessTypes.LIVENESS_ALIVE;
    }

    @Override
    public boolean checkLiveness(InetSocketAddress identifier, Map<String, Object> options) {
      return false;
    }

    @Override
    public void addLivenessListener(LivenessListener<InetSocketAddress> name) {
    }

    @Override
    public boolean removeLivenessListener(LivenessListener<InetSocketAddress> name) {
      return false;
    }

    @Override
    public void clearState(InetSocketAddress identifier) {
    }
  }

  static final class NoProximity implements ProximityProvider<InetSocketAddress> {
    @Override
    public int proximity(InetSocketAddress identifier, Map<String, Object> options) {
      return DEFAULT_PROXIMITY;
    }

    @Override
    public void addProximityListener(ProximityListener<InetSocketAddress> listener) {
    }

    @Override
    public boolean removeProximityListener(ProximityListener<InetSocketAddress> listener) {
      return false;
    }

    @Override
    public void clearState(InetSocketAddress identifier) {
    }
  }
}
