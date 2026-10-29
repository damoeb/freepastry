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
package rice.selector.testing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.Destructable;
import rice.environment.Environment;
import rice.selector.SelectionKeyHandler;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@link SelectorManager#destroy()} runs the pending invocations and releases the registered channels,
 * instead of dropping them (which is how transport layers leaked bound ports when an Environment was destroyed).
 */
public class SelectorManagerDestroyTest {

  static final long JOIN_MILLIS = 10000;

  Environment env;
  SelectorManager sm;

  @BeforeEach
  public void setUp() {
    env = new Environment();
    sm = env.getSelectorManager();
  }

  @AfterEach
  public void tearDown() throws InterruptedException {
    if (sm.isAlive()) {
      env.destroy();
      sm.join(JOIN_MILLIS);
    }
    // the tests that destroy the selector directly don't reach the processor
    env.getProcessor().destroy();
  }

  private void destroyAndJoin() throws InterruptedException {
    env.destroy();
    sm.join(JOIN_MILLIS);
    assertFalse(sm.isAlive(), "selector thread should have stopped");
  }

  @Test
  public void invocationsScheduledByDestructablesRun() throws Exception {
    final AtomicBoolean ran = new AtomicBoolean();
    final AtomicBoolean onSelector = new AtomicBoolean();
    // like a transport layer that closes its sockets in an invocation when destroyed
    env.addDestructable(new Destructable() {
      public void destroy() {
        sm.invoke(new Runnable() {
          public void run() {
            onSelector.set(sm.isSelectorThread());
            ran.set(true);
          }
        });
      }
    });
    destroyAndJoin();
    assertTrue(ran.get(), "invocation scheduled during destroy was dropped");
    assertTrue(onSelector.get(), "pending invocations must still run on the selector thread");
  }

  @Test
  public void destroyOnSelectorThreadRunsLaterInvocations() throws Exception {
    final AtomicInteger order = new AtomicInteger();
    final AtomicInteger second = new AtomicInteger();
    final AtomicInteger third = new AtomicInteger();
    sm.invoke(new Runnable() {
      public void run() {
        sm.destroy(); // re-entrant: from an invocation on the selector thread
        sm.invoke(new Runnable() {
          public void run() {
            second.set(order.incrementAndGet());
            sm.invoke(new Runnable() { // scheduled while draining
              public void run() {
                third.set(order.incrementAndGet());
              }
            });
          }
        });
      }
    });
    sm.join(JOIN_MILLIS);
    assertFalse(sm.isAlive());
    assertEquals(1, second.get());
    assertEquals(2, third.get());
  }

  @Test
  public void failingInvocationDoesNotStopTheOthers() throws Exception {
    final AtomicBoolean ran = new AtomicBoolean();
    final CountDownLatch inInvocation = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    // hold the selector thread, so that the next two are still pending when it is destroyed
    sm.invoke(new Runnable() {
      public void run() {
        inInvocation.countDown();
        try {
          proceed.await(JOIN_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    });
    assertTrue(inInvocation.await(JOIN_MILLIS, TimeUnit.MILLISECONDS));
    sm.invoke(new Runnable() {
      public void run() {
        throw new IllegalStateException("expected by the test");
      }
    });
    sm.invoke(new Runnable() {
      public void run() {
        ran.set(true);
      }
    });
    sm.destroy();
    proceed.countDown();
    sm.join(JOIN_MILLIS);
    assertFalse(sm.isAlive());
    assertTrue(ran.get());
  }

  @Test
  public void runsOtherInvocationsScheduledRepeatedly() throws Exception {
    // distinct invocations, each scheduling the next: all of them run (up to the round limit)
    final AtomicInteger runs = new AtomicInteger();
    sm.invoke(new Runnable() {
      public void run() {
        sm.destroy();
        chain(10);
      }
      void chain(final int remaining) {
        if (remaining == 0) return;
        sm.invoke(new Runnable() {
          public void run() {
            runs.incrementAndGet();
            chain(remaining - 1);
          }
        });
      }
    });
    sm.join(JOIN_MILLIS);
    assertFalse(sm.isAlive());
    assertEquals(10, runs.get());
  }

  @Test
  public void selfReschedulingInvocationIsBounded() throws Exception {
    final AtomicInteger runs = new AtomicInteger();
    sm.invoke(new Runnable() {
      public void run() {
        if (runs.incrementAndGet() == 1) sm.destroy();
        sm.invoke(this);
      }
    });
    sm.join(JOIN_MILLIS);
    assertFalse(sm.isAlive(), "shutdown must terminate");
    // pending at destroy, so it runs once more, but its rescheduling is not followed
    assertEquals(2, runs.get());
  }

  @Test
  public void registeredChannelsAreClosed() throws Exception {
    final ServerSocketChannel server = ServerSocketChannel.open();
    server.configureBlocking(false);
    server.socket().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    final int port = server.socket().getLocalPort();
    final AtomicReference<SelectionKey> key = new AtomicReference<SelectionKey>();
    final CountDownLatch registered = new CountDownLatch(1);
    sm.invoke(new Runnable() {
      public void run() {
        try {
          key.set(sm.register(server, new SelectionKeyHandler(), SelectionKey.OP_ACCEPT));
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        } finally {
          registered.countDown();
        }
      }
    });
    assertTrue(registered.await(JOIN_MILLIS, TimeUnit.MILLISECONDS));
    assertNotNull(key.get());

    destroyAndJoin();

    assertFalse(server.isOpen(), "channel registered with the selector should be closed");
    assertFalse(key.get().isValid());
    assertFalse(sm.getSelector().isOpen());
    // the port is free again
    try (ServerSocketChannel rebind = ServerSocketChannel.open()) {
      rebind.socket().setReuseAddress(false);
      rebind.socket().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
    }
  }

  @Test
  public void useAfterDestroyIsHarmless() throws Exception {
    destroyAndJoin();
    final AtomicBoolean ran = new AtomicBoolean();
    sm.invoke(new Runnable() {
      public void run() {
        ran.set(true);
      }
    });
    assertEquals(0, sm.getNumInvocations());
    sm.schedule(new TimerTask() {
      public void run() {
        ran.set(true);
      }
    }, 1);
    sm.destroy(); // idempotent
    assertFalse(ran.get());
  }
}
