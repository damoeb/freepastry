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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.slf4j.Logger;
import rice.Destructible;
import rice.environment.Environment;
import rice.selector.SelectorLoop;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class TLTest<Identifier> {

  static Environment env;
  static Logger logger;
  static Map<String, Object> options = new HashMap<String, Object>();
  static TransportLayer<?, ByteBuffer> alice, bob;
  static final byte[] sentBytes = {0,1,2,3,4,5,6,7};
  /** A port that no test binds, for getBogusIdentifier(). */
  static final int BOGUS_PORT = 5007;
  /**
   * Additional transport layers built by a subclass (besides alice and bob), destroyed in tearDownAfterClass().
   */
  static List<Destructible> otherTLs = new ArrayList<Destructible>();

  /**
   * Creates the shared Environment. Not a {@code @BeforeAll} itself: every subclass's setup calls it explicitly.
   */
  public static void setUpBeforeClass() throws Exception {
    env = new Environment();
  }

  /**
   * Jupiter runs a superclass's {@code @BeforeAll} for its subclasses too, even when they hide it with their own
   * (JUnit 4 did not). A concrete test class that is also extended uses this to skip its setup when a subclass runs,
   * since each subclass builds its own transport layers on the same ports.
   */
  static boolean runsForSubclass(TestInfo info, Class<?> declaringClass) {
    return info.getTestClass().orElseThrow() != declaringClass;
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    // Destroy the transport layers so that their ports are released: Environment.destroy() alone only stops the
    // selector (dropping pending invocations), leaving the server sockets bound for the next test class in this JVM.
    final List<Destructible> tls = new ArrayList<Destructible>();
    if (alice != null) tls.add(alice);
    if (bob != null) tls.add(bob);
    tls.addAll(otherTLs);
    otherTLs.clear();
    alice = null;
    bob = null;
    SelectorLoop selector = env.getSelectorManager();
    Runnable destroyTLs = () -> {
      for (Destructible destructible : tls) {
        try {
          destructible.destroy();
        } catch (RuntimeException runtimeException) {
          // may already have been destroyed by a test
          if (logger != null && logger.isDebugEnabled()) logger.debug("destroying "+destructible, runtimeException);
        }
      }
    };
    if (selector.isAlive()) {
      selector.invoke(destroyTLs);
      env.destroy(); // runs after the invocation above, on the selector
      selector.join(10000);
    } else {
      // the selector died (e.g. due to an exception in a test), clean up on this thread
      destroyTLs.run();
      env.destroy();
    }
  }

  /**
   * The subclass assigns a TransportLayer&lt;Identifier, ByteBuffer&gt; to the static alice field.
   */
  @SuppressWarnings("unchecked")
  protected TransportLayer<Identifier, ByteBuffer> alice() {
    return (TransportLayer<Identifier, ByteBuffer>)alice;
  }

  /**
   * The subclass assigns a TransportLayer&lt;Identifier, ByteBuffer&gt; to the static bob field.
   */
  @SuppressWarnings("unchecked")
  protected TransportLayer<Identifier, ByteBuffer> bob() {
    return (TransportLayer<Identifier, ByteBuffer>)bob;
  }

  /**
   * A's view of b
   *
   * @return A's view of b
   */
  public Identifier getIdentifier(
      TransportLayer<Identifier, ByteBuffer> transportLayer,
      TransportLayer<Identifier, ByteBuffer> b) {
    return b.getLocalIdentifier();
  }

  /**
   * The identifier to a bogus node.
   */
  public abstract Identifier getBogusIdentifier(Identifier local) throws IOException;

  /*********************** TCP *************************/
  /**
   * 3 parts:
   *
   * - Alice opens a TCP socket to Bob
   * - Alice sends to Bob
   * - Alice closes the Socket
   */
  @Test
  public void openTCP() throws Exception {
    final List<P2PSocket<Identifier>> aliceSockets = new ArrayList<P2PSocket<Identifier>>();
    final List<P2PSocket<Identifier>> bobSockets = new ArrayList<P2PSocket<Identifier>>();
    final List<Exception> exceptionList = new ArrayList<Exception>(1);
    final List<ByteBuffer> receivedList = new ArrayList<ByteBuffer>(1);
    final List<ByteBuffer> sentList = new ArrayList<ByteBuffer>(1);
    final Object lock = new Object();

    // Part I opening a connection
    bob().setCallback(new TransportLayerCallback<Identifier, ByteBuffer>() {

      @Override
      public void messageReceived(Identifier identifier, ByteBuffer msg, Map<String, Object> options)
          throws IOException {

      }

      @Override
      public void incomingSocket(P2PSocket<Identifier> s)
          throws IOException {
        synchronized(lock) {
          bobSockets.add(s);
          lock.notify();
        }
      }

    });

    alice().openSocket(getIdentifier(alice(), bob()), new SocketCallback<Identifier>(){
      @Override
      public void receiveResult(SocketRequestHandle<Identifier> request, P2PSocket<Identifier> result) {
        synchronized(lock) {
          aliceSockets.add(result);
          lock.notify();
        }
      }

      @Override
      public void receiveException(SocketRequestHandle<Identifier> s, Exception exception) {
        synchronized(lock) {
          exceptionList.add(exception);
          lock.notify();
        }
      }
    },
    options);

    // wait for it to open
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && (aliceSockets.isEmpty() || bobSockets.isEmpty())) {
        lock.wait(1000);
      }
    }
    assertTrue(aliceSockets.size() == 1);
    assertTrue(bobSockets.size() == 1);
    assertTrue(exceptionList.isEmpty());


    // Part II sending data
    final byte[] sentBytes = {0,1,2,3,4,5,6,7};
    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes);
    final ByteBuffer buffer = ByteBuffer.allocate(sentBytes.length);

    bobSockets.get(0).register(true, false, new P2PSocketReceiver<Identifier>() {
      @Override
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead,
          boolean canWrite) throws IOException {

        socket.read(buffer);

        if (buffer.hasRemaining()) {
          socket.register(true, false, this);
        } else {
          synchronized(lock) {
            receivedList.add(buffer);
            lock.notify();
          }
        }
      }

      @Override
      public void receiveException(P2PSocket<Identifier> socket, Exception e) {
        synchronized(lock) {
          exceptionList.add(e);
          lock.notify();
        }
      }
    });

    aliceSockets.get(0).register(false, true, new P2PSocketReceiver<Identifier>() {
      @Override
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead,
          boolean canWrite) throws IOException {
        socket.write(sentBuffer);

        if (sentBuffer.hasRemaining()) {
          socket.register(false, true, this);
        } else {
          synchronized(lock) {
            sentList.add(sentBuffer);
            lock.notify();
          }
        }
      }

      @Override
      public void receiveException(P2PSocket<Identifier> socket, Exception e) {
        synchronized(lock) {
          exceptionList.add(e);
          lock.notify();
        }
      }

    });

    timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && (receivedList.isEmpty() || sentList.isEmpty())) {
        lock.wait(1000);
      }
    }

    assertTrue(sentList.size() == 1);
    assertTrue(receivedList.size() == 1);
    assertTrue(Arrays.equals(sentList.get(0).array(), receivedList.get(0).array()), "recList.arr:"+receivedList.get(0).array()[0]+","+receivedList.get(0).array()[1]);
    assertTrue(exceptionList.isEmpty());

    // Part III test shutdown/close()
    final List<P2PSocket<Identifier>> closed = new ArrayList<P2PSocket<Identifier>>();

    aliceSockets.get(0).shutdownOutput();

    bobSockets.get(0).register(true, false, new P2PSocketReceiver<Identifier>() {
      @Override
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead,
          boolean canWrite) throws IOException {

        ByteBuffer bogus = ByteBuffer.allocate(1);
        if (socket.read(bogus) == -1) {
          socket.close();
          synchronized(lock) {
            closed.add(socket);
            lock.notify();
          }
        } else {
          synchronized(lock) {
            receivedList.add(bogus);
            lock.notify();
          }
        }
      }

      @Override
      public void receiveException(P2PSocket<Identifier> socket, Exception e) {
        synchronized(lock) {
          exceptionList.add(e);
          lock.notify();
        }
      }
    });

    aliceSockets.get(0).register(true, false, new P2PSocketReceiver<Identifier>() {
      @Override
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead,
          boolean canWrite) throws IOException {

        ByteBuffer bogus = ByteBuffer.allocate(1);
        if (socket.read(bogus) == -1) {
          socket.close();
          synchronized(lock) {
            closed.add(socket);
            lock.notify();
          }
        } else {
          synchronized(lock) {
            receivedList.add(bogus);
            lock.notify();
          }
        }
      }

      @Override
      public void receiveException(P2PSocket<Identifier> socket, Exception e) {
        synchronized(lock) {
          exceptionList.add(e);
          lock.notify();
        }
      }
    });


    timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) &&
          exceptionList.isEmpty() &&
          receivedList.size() == 1 &&
          (closed.size() < 2)) {
        lock.wait(1000);
      }
    }

    bob().setCallback(null);

    assertTrue(exceptionList.isEmpty());
    assertTrue(receivedList.size() == 1, "receivedList.size():"+receivedList.size());
    assertTrue(closed.size() == 2);
    assertTrue(closed.contains(aliceSockets.get(0)));
    assertTrue(closed.contains(bobSockets.get(0)));
  }

  public void testSocketThreadSafety() {
    // send on the selector
    // send not on the selector

    // TODO: this test is empty: send on and off the selector thread, over TCP and UDP
  }

  /*********************** UDP *************************/
  /**
   * Sends udp message from alice to bob.
   */
  @Test
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void sendUDP() throws Exception {
    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes);

    class Tupel<Identifier2> {
      Identifier2 i;
      ByteBuffer buf;
      Tupel(Identifier2 identifier2, ByteBuffer buf) {
        this.i = identifier2;
        this.buf = buf;
      }
    }

    // added to every time bob receives something
    final List<Tupel<Identifier>> receivedList = new ArrayList<Tupel<Identifier>>(1);
    final List<Exception> exceptionList = new ArrayList<Exception>(1);
    final List<MessageRequestHandle<Identifier, ByteBuffer>> sentList = new ArrayList<MessageRequestHandle<Identifier, ByteBuffer>>(1);
    final Object lock = new Object();

    // make a way for bob to receive the callback
    bob().setCallback(new TransportLayerCallback<Identifier, ByteBuffer>() {
      @Override
      public void messageReceived(Identifier identifier, ByteBuffer buf, Map<String, Object> options) throws IOException {
        synchronized(lock) {
          receivedList.add(new Tupel<Identifier>(identifier, buf));
          lock.notify();
        }
      }
      @Override
      public void incomingSocket(P2PSocket<Identifier> socket) {}
    });

    MessageRequestHandle<Identifier, ByteBuffer> handle = alice().sendMessage(
        getIdentifier(alice(), bob()),
        sentBuffer,
        new MessageCallback<Identifier, ByteBuffer>() {

          @Override
          public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
            synchronized(lock) {
              sentList.add(msg);
              lock.notify();
            }
          }

          @Override
          public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception ex) {
            synchronized(lock) {
              ex.printStackTrace();
              exceptionList.add(ex);
              lock.notify();
            }
          }
    },
    options);

    // block for completion
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && (receivedList.isEmpty() || sentList.isEmpty())) {
        lock.wait(1000);
      }
    }

    // results
    if (!exceptionList.isEmpty()) throw exceptionList.get(0); // we got no exceptions
    assertTrue(receivedList.size() == 1, "receivedList.size():"+receivedList.size());  // we got 1 result
    byte[] rec = new byte[receivedList.get(0).buf.remaining()];
    receivedList.get(0).buf.get(rec);
    assertTrue(Arrays.equals(rec, sentBuffer.array())); // it is equal
    assertTrue(receivedList.get(0).i.equals(getIdentifier(bob(), alice())), "i:"+receivedList.get(0).i+" getI():"+getIdentifier(bob(), alice()));
    assertTrue(sentList.size() == 1, "sentList.size():"+sentList.size()); // we got one notification
    assertTrue(sentList.get(0) == handle); // it is the buffer we sent
    assertTrue(handle.getMessage() == sentBuffer, "handle.getMessage():"+handle.getMessage()+" sentBuffer:"+sentBuffer); // it is the buffer we sent
    bob().setCallback(null);
  }

  /**
   * Sends huge udp message from alice to bob, should fail.
   */
  @Test
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void messageTooBigUDP() throws Exception {
    final byte[] sentBytes = new byte[1000000]; // a big message
    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes);

    // added to every time bob receives something
    final List<MessageRequestHandle<Identifier, ByteBuffer>> failedList = new ArrayList<MessageRequestHandle<Identifier, ByteBuffer>>(1);
    final List<MessageRequestHandle<Identifier, ByteBuffer>> sentList = new ArrayList<MessageRequestHandle<Identifier, ByteBuffer>>(1);
    final List<Exception> exceptionList = new ArrayList<Exception>(1);
    final Object lock = new Object();

    MessageRequestHandle<Identifier, ByteBuffer> cancellable = alice().sendMessage(
        getIdentifier(alice(), bob()),
        sentBuffer,
        new MessageCallback<Identifier, ByteBuffer>() {

          @Override
          public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
            synchronized(lock) {
              sentList.add(msg);
              lock.notify();
            }
          }
          @Override
          public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception exception) {
            synchronized(lock) {
              failedList.add(msg);
              exceptionList.add(exception);
              lock.notify();
            }
          }
        },
        options);

    // block for completion
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && sentList.isEmpty()) {
        lock.wait(1000);
      }
    }

    // results
    assertTrue(sentList.isEmpty());  // we got 1 result
    assertTrue(exceptionList.size() == 1, "exceptionList.size():"+exceptionList.size());
    assertTrue(failedList.size() == 1);
    assertTrue(failedList.get(0) == cancellable);
    assertTrue(cancellable.getMessage() == sentBuffer);

  }

  @Test
  public void noCallbackTest() throws Exception {
    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes);

    class Tupel<Identifier2> {
      Identifier2 i;
      ByteBuffer buf;
      Tupel(Identifier2 identifier2, ByteBuffer buf) {
        this.i = identifier2;
        this.buf = buf;
      }
    }

    // added to every time bob receives something
    final List<Tupel<Identifier>> receivedList = new ArrayList<Tupel<Identifier>>(1);
    final List<Exception> exceptionList = new ArrayList<Exception>(1);
    final Object lock = new Object();

    // make a way for bob to receive the callback
    bob().setCallback(new TransportLayerCallback<Identifier, ByteBuffer>() {
      @Override
      public void messageReceived(Identifier identifier, ByteBuffer buf, Map<String, Object> options) throws IOException {
        synchronized(lock) {
          receivedList.add(new Tupel<Identifier>(identifier, buf));
          lock.notify();
        }
      }
      @Override
      public void incomingSocket(P2PSocket<Identifier> socket) {}
    });

    alice().sendMessage(
        getIdentifier(alice(), bob()),
        sentBuffer,
        null,
        null);

    // block for completion
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && receivedList.isEmpty()) {
        lock.wait(1000);
      }
    }

    // results
    if (!exceptionList.isEmpty()) throw exceptionList.get(0); // we got no exceptions
    assertTrue(receivedList.size() == 1, "receivedList.size():"+receivedList.size());  // we got 1 result
    byte[] rec = new byte[receivedList.get(0).buf.remaining()];
    receivedList.get(0).buf.get(rec);
    assertTrue(Arrays.equals(rec, sentBuffer.array())); // it is equal
    assertTrue(receivedList.get(0).i.equals(getIdentifier(bob(), alice())));
    bob().setCallback(null);

    // opening a socket without a callback is rejected
    assertThrows(IllegalArgumentException.class, () -> alice().openSocket(getIdentifier(alice(), bob()), null, options));
  }

}
