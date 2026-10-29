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
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.TransportLayerListener;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayerImpl;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayerImpl;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.priority.PriorityTransportLayerImpl;
import org.mpisws.p2p.transport.proximity.MinRTTProximityProvider;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;
import rice.environment.Environment;
import rice.environment.exception.ExceptionStrategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A TCP message (or a passthrough socket) that reaches a PriorityTransportLayerImpl before the layer above set
 * its callback must be dropped, instead of throwing a NullPointerException on the selector thread (which by default
 * destroys the whole Environment).
 */
public class PriorityMissingCallbackTest {

  static final long WAIT_MILLIS = 10000;

  Environment env;
  final List<Throwable> selectorExceptions = Collections.synchronizedList(new ArrayList<Throwable>());
  PriorityTransportLayerImpl<MultiInetSocketAddress> alice, bob;
  MultiInetSocketAddress bobAddr;

  @BeforeEach
  public void setUp() throws IOException {
    env = new Environment();
    env.setExceptionStrategy(new ExceptionStrategy() {
      public void handleException(Object source, Throwable t) {
        selectorExceptions.add(t);
      }
    });
    InetAddress addr = InetAddress.getLoopbackAddress();
    alice = buildTL(new InetSocketAddress(addr, freePort()));
    bobAddr = new MultiInetSocketAddress(new InetSocketAddress(addr, freePort()));
    bob = buildTL(bobAddr.getInnermostAddress());
    alice.setCallback(new Recorder());
    // bob's callback is deliberately not set
  }

  @AfterEach
  public void tearDown() throws InterruptedException {
    alice.destroy();
    bob.destroy();
    env.destroy();
    env.getSelectorManager().join(WAIT_MILLIS);
  }

  static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  private PriorityTransportLayerImpl<MultiInetSocketAddress> buildTL(InetSocketAddress addr) throws IOException {
    LivenessTransportLayerImpl<MultiInetSocketAddress> ltli = new LivenessTransportLayerImpl<MultiInetSocketAddress>(
        new MultiInetAddressTransportLayerImpl(new MultiInetSocketAddress(addr),
          new WireTransportLayerImpl(addr, env, null), env, null, null),
        env, null, 5000);
    return new PriorityTransportLayerImpl<MultiInetSocketAddress>(ltli, ltli,
        new MinRTTProximityProvider<MultiInetSocketAddress>(ltli, env), env, 1024, 30, null);
  }

  static class Recorder implements TransportLayerCallback<MultiInetSocketAddress, ByteBuffer> {
    final BlockingQueue<ByteBuffer> messages = new ArrayBlockingQueue<ByteBuffer>(10);
    public void messageReceived(MultiInetSocketAddress i, ByteBuffer m, Map<String, Object> options) {
      messages.add(m);
    }
    public void incomingSocket(P2PSocket<MultiInetSocketAddress> s) {
      s.close();
    }
  }

  /** Sends a TCP message from alice to bob and waits until it was written. */
  private void send(byte[] bytes) throws InterruptedException {
    final CountDownLatch acked = new CountDownLatch(1);
    alice.sendMessage(bobAddr, ByteBuffer.wrap(bytes), new MessageCallback<MultiInetSocketAddress, ByteBuffer>() {
      public void ack(MessageRequestHandle<MultiInetSocketAddress, ByteBuffer> msg) {
        acked.countDown();
      }
      public void sendFailed(MessageRequestHandle<MultiInetSocketAddress, ByteBuffer> msg, Exception reason) {
        selectorExceptions.add(reason);
        acked.countDown();
      }
    }, new HashMap<String, Object>());
    assertTrue(acked.await(WAIT_MILLIS, TimeUnit.MILLISECONDS), "message was not sent");
  }

  /** Waits until everything the selector was asked to do so far has been done. */
  private void syncWithSelector() throws InterruptedException {
    final CountDownLatch done = new CountDownLatch(1);
    env.getSelectorManager().invoke(done::countDown);
    assertTrue(done.await(WAIT_MILLIS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void tcpMessageWithoutCallbackIsDropped() throws Exception {
    final byte[] first = new byte[] {1, 2, 3};
    final CountDownLatch bobRead = new CountDownLatch(1);
    bob.addTransportLayerListener(new TransportLayerListener<MultiInetSocketAddress>() {
      public void read(int bytes, MultiInetSocketAddress i, Map<String, Object> options, boolean passthrough, boolean socket) {
        if (bytes == first.length) bobRead.countDown();
      }
      public void wrote(int bytes, MultiInetSocketAddress i, Map<String, Object> options, boolean passthrough, boolean socket) {
      }
    });

    send(first);
    assertTrue(bobRead.await(WAIT_MILLIS, TimeUnit.MILLISECONDS), "bob did not read the message");
    syncWithSelector(); // the callback is called right after the listeners, on the selector thread

    assertEquals(List.of(), selectorExceptions);
    assertTrue(env.getSelectorManager().isAlive(), "the environment must survive");

    // once the callback is set, messages are delivered (over the same connection)
    Recorder bobCallback = new Recorder();
    bob.setCallback(bobCallback);
    send(new byte[] {4, 5});
    ByteBuffer received = bobCallback.messages.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertNotNull(received, "message after setCallback() not delivered");
    assertEquals(2, received.remaining());
    assertEquals(4, received.get());
    assertEquals(List.of(), selectorExceptions);
  }

  @Test
  public void datagramWithoutCallbackIsDropped() throws Exception {
    bob.setCallback(null); // also restores the "missing callback" behavior
    final Exception[] thrown = new Exception[1];
    final CountDownLatch done = new CountDownLatch(1);
    env.getSelectorManager().invoke(() -> {
      try {
        bob.messageReceived(bobAddr, ByteBuffer.wrap(new byte[] {7}), new HashMap<String, Object>());
      } catch (Exception e) {
        thrown[0] = e;
      } finally {
        done.countDown();
      }
    });
    assertTrue(done.await(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    assertNull(thrown[0]);
  }

  @Test
  public void passthroughSocketWithoutCallbackIsClosed() throws Exception {
    final BlockingQueue<Object> outcome = new ArrayBlockingQueue<Object>(2);
    env.getSelectorManager().invoke(() ->
      alice.openSocket(bobAddr, new SocketCallback<MultiInetSocketAddress>() {
        public void receiveResult(SocketRequestHandle<MultiInetSocketAddress> cancellable,
            P2PSocket<MultiInetSocketAddress> sock) {
          // wait for bob to close it
          sock.register(true, false, new P2PSocketReceiver<MultiInetSocketAddress>() {
            public void receiveSelectResult(P2PSocket<MultiInetSocketAddress> socket, boolean canRead, boolean canWrite) throws IOException {
              long ret = socket.read(ByteBuffer.allocate(1));
              if (ret < 0) {
                outcome.add("closed");
                socket.close();
              } else {
                socket.register(true, false, this);
              }
            }
            public void receiveException(P2PSocket<MultiInetSocketAddress> socket, Exception e) {
              outcome.add("closed: "+e);
            }
          });
        }
        public void receiveException(SocketRequestHandle<MultiInetSocketAddress> s, Exception ex) {
          outcome.add(ex);
        }
      }, new HashMap<String, Object>()));
    Object o = outcome.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    assertNotNull(o, "bob should close the passthrough socket");
    assertTrue(o.toString().startsWith("closed"), "unexpected: "+o);
    syncWithSelector();
    assertEquals(List.of(), selectorExceptions);
  }
}
