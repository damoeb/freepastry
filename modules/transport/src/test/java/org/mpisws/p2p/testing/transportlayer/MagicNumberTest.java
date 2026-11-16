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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import org.mpisws.p2p.transport.wire.exception.StalledSocketException;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;
import rice.environment.Environment;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MagicNumberTest extends WireTest {
  static TransportLayer<InetSocketAddress, ByteBuffer> carol, dave;

  /**
   * Goes to Alice/Bob
   */
  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] GOOD_HDR = {(byte)0xDE,(byte)0xAD,(byte)0xBE,(byte)0xEF};

  /**
   * Goes to Carol
   */
  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] BAD_HDR = {(byte)0xDE,(byte)0xED,(byte)0xBE,(byte)0xEF};

  /**
   * Goes to Dave
   */
  public static final byte[] NO_HDR = {};



  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    env = new Environment();
    logger = env.getLogManager().getLogger(MagicNumberTest.class, null);
    InetAddress addr = InetAddress.getLocalHost();
    int[] ports = FreePorts.freePorts(addr, 4);
    alice = new MagicNumberTransportLayer<InetSocketAddress>(
        new WireTransportLayer(new InetSocketAddress(addr,ports[0]),env, null)
        ,env, null,GOOD_HDR, 2000);
    bob = new MagicNumberTransportLayer<InetSocketAddress>(
      new WireTransportLayer(new InetSocketAddress(addr,ports[1]),env, null)
      ,env, null, GOOD_HDR, 2000);
    carol = new MagicNumberTransportLayer<InetSocketAddress>(
        new WireTransportLayer(new InetSocketAddress(addr,ports[2]),env, null)
        ,env, null,BAD_HDR, 2000);
    dave = new MagicNumberTransportLayer<InetSocketAddress>(
        new WireTransportLayer(new InetSocketAddress(addr,ports[3]),env, null)
        ,env, null,NO_HDR, 2000);
    otherTLs.add(carol);
    otherTLs.add(dave);
  }

  /*********************** TCP *************************/
  /**
   * 3 parts:
   *
   * - Alice opens a TCP socket to Bob
   * - Alice sends to Bob
   * - Alice closes the Socket
   */
  @Test
  public void wrongHeaderTCP() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(WireTransportLayer.OPTION_TRANSPORT_TYPE, WireTransportLayer.TRANSPORT_TYPE_GUARANTEED);

    final List<P2PSocket<InetSocketAddress>> aliceSockets = new ArrayList<P2PSocket<InetSocketAddress>>();
    final List<P2PSocket<InetSocketAddress>> carolSockets = new ArrayList<P2PSocket<InetSocketAddress>>();
    final List<Throwable> exceptionList = new ArrayList<Throwable>(1);
    final List<byte[]> unexpectedData = new ArrayList<byte[]>(1);
    final Object lock = new Object();

    // Part I opening a connection
    carol.setCallback(new TransportLayerCallback<InetSocketAddress, ByteBuffer>() {

      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, Map<String, Object> options)
          throws IOException {

      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s)
          throws IOException {
        synchronized(lock) {
          carolSockets.add(s);
          lock.notify();
        }
      }

    });
    carol.setErrorHandler(new ErrorHandler<InetSocketAddress>() {
      @Override
      public void receivedException(InetSocketAddress i, Throwable error) {
        synchronized(lock) {
          error.printStackTrace();
          exceptionList.add(error); // should not happen
          lock.notify();
        }
      }
      @Override
      public void receivedUnexpectedData(InetSocketAddress i, byte[] bytes, int pos, Map<String, Object> options) {
        synchronized(lock) {
          unexpectedData.add(bytes);
          lock.notify();
        }
      }
    });


    alice().openSocket(carol.getLocalIdentifier(), new SocketCallback<InetSocketAddress>(){
      @Override
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> result) {
        synchronized(lock) {
          aliceSockets.add(result);
          lock.notify();
        }
      }

      @Override
      public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception exception) {
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
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && (aliceSockets.isEmpty() || unexpectedData.isEmpty())) {
        lock.wait(1000);
      }
    }

    carol.setCallback(null);
    carol.setErrorHandler(null);

    assertTrue(aliceSockets.size() == 1);
    assertTrue(carolSockets.size() == 0);
    assertTrue(unexpectedData.size() == 1);
    assertTrue(exceptionList.isEmpty());
    assertTrue(unexpectedData.size() == 1);  // we got 1 result

    //hdr was the good hdr:
    assertTrue(unexpectedData.get(0).length >= GOOD_HDR.length);
    byte[] hdrpart = new byte[GOOD_HDR.length];
    System.arraycopy(unexpectedData.get(0), 0, hdrpart, 0, GOOD_HDR.length);
    assertTrue(Arrays.equals(hdrpart, GOOD_HDR)); // it is equal

  }

  /**
   * 3 parts:
   *
   * - Dave opens a TCP socket to Bob but withholds the HEADER
   * - Bob closes the Socket
   */
  @Test
  public void stallTCP() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(WireTransportLayer.OPTION_TRANSPORT_TYPE, WireTransportLayer.TRANSPORT_TYPE_GUARANTEED);

    final List<P2PSocket<InetSocketAddress>> aliceSockets = new ArrayList<P2PSocket<InetSocketAddress>>();
    final List<P2PSocket<InetSocketAddress>> daveSockets = new ArrayList<P2PSocket<InetSocketAddress>>();
    final List<Throwable> exceptionList = new ArrayList<Throwable>(1);
    final Object lock = new Object();

    // Part I opening a connection
    alice().setCallback(new TransportLayerCallback<InetSocketAddress, ByteBuffer>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m, Map<String, Object> options)
          throws IOException {
      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s)
          throws IOException {
        synchronized(lock) {
          aliceSockets.add(s);
          lock.notify();
        }
      }
    });

    alice().setErrorHandler(new ErrorHandler<InetSocketAddress>(){

      @Override
      public void receivedUnexpectedData(InetSocketAddress i, byte[] bytes, int pos, Map<String, Object> options) {
      }

      @Override
      public void receivedException(InetSocketAddress i, Throwable error) {
        synchronized(lock) {
          exceptionList.add(error); // should not happen
          lock.notify();
        }
      }
    });

    dave.openSocket(alice().getLocalIdentifier(), new SocketCallback<InetSocketAddress>(){
      @Override
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> result) {
        synchronized(lock) {
          daveSockets.add(result);
          lock.notify();
        }
      }

      @Override
      public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception exception) {
        synchronized(lock) {
          exceptionList.add(exception);
          lock.notify();
        }
      }
    },
    options);

    // wait for it to open
    long timeout = env.getTimeSource().currentTimeMillis()+34000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && (daveSockets.isEmpty() || exceptionList.isEmpty())) {
        lock.wait(1000);
      }
    }

    alice().setCallback(null);
    alice().setErrorHandler(null);

    assertTrue(aliceSockets.size() == 0);
    assertTrue(daveSockets.size() == 1);

    // examine exception
    assertTrue(exceptionList.size() == 1);
    Throwable e = exceptionList.get(0);
    assertTrue(e instanceof StalledSocketException);

    // the exception's address is not checked: the stalled socket comes from an ephemeral port, not dave's identifier
  }


  /*********************** UDP *************************/
  /**
   * Sends udp message from alice to carol, should fail.
   */
  @Test
  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  public void wrongHeaderUDP() throws Throwable {
    final byte[] sentBytes = {0,1,2,3,4,5,6,7};
    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes);

    // added to every time bob receives something
    final List<ByteBuffer> receivedList = new ArrayList<ByteBuffer>(1);
    final List<Throwable> exceptionList = new ArrayList<Throwable>(1);
    final List<MessageRequestHandle<InetSocketAddress, ByteBuffer>> sentList = new ArrayList<MessageRequestHandle<InetSocketAddress, ByteBuffer>>(1);
    final List<byte[]> unexpectedData = new ArrayList<byte[]>(1);
    final Object lock = new Object();

    Map<String, Object> options = new HashMap<String, Object>();
    options.put(WireTransportLayer.OPTION_TRANSPORT_TYPE, WireTransportLayer.TRANSPORT_TYPE_DATAGRAM);



    // make a way for bob to receive the callback
    carol.setCallback(new TransportLayerCallback<InetSocketAddress, ByteBuffer>() {
      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer buf, Map<String, Object> options) throws IOException {
        synchronized(lock) {
          receivedList.add(buf);
          lock.notify();
        }
      }
      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) {}
    });

    carol.setErrorHandler(new ErrorHandler<InetSocketAddress>() {

      @Override
      public void receivedException(InetSocketAddress i, Throwable error) {
        synchronized(lock) {
          error.printStackTrace();
          exceptionList.add(error); // should not happen
          lock.notify();
        }
      }
      @Override
      public void receivedUnexpectedData(InetSocketAddress i, byte[] bytes, int pos, Map<String, Object> options) {
        synchronized(lock) {
          unexpectedData.add(bytes);
          lock.notify();
        }
      }
    });

    MessageCallback<InetSocketAddress, ByteBuffer> ret = new MessageCallback<InetSocketAddress, ByteBuffer>() {
      @Override
      public void ack(MessageRequestHandle<InetSocketAddress, ByteBuffer> msg) {
        synchronized(lock) {
          sentList.add(msg);
          lock.notify();
        }
      }
      @Override
      public void sendFailed(MessageRequestHandle<InetSocketAddress, ByteBuffer> msg, Exception reason) {
        synchronized(lock) {
          exceptionList.add(reason);
          lock.notify();
        }
      }
    };

    MessageRequestHandle<InetSocketAddress, ByteBuffer> handle = alice().sendMessage(
          carol.getLocalIdentifier(),
          sentBuffer,
          ret,
          options);

    // block for completion
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && receivedList.isEmpty() && (sentList.isEmpty() || unexpectedData.isEmpty())) {
        lock.wait(1000);
      }
    }

    // results
    carol.setCallback(null);
    carol.setErrorHandler(null);

    if (!exceptionList.isEmpty()) throw exceptionList.get(0); // we got no exceptions
    assertTrue(receivedList.isEmpty());  // we got 1 result
    assertTrue(sentList.size() >= 1); // we got one notification
    assertTrue(sentList.get(0) == handle); // it is the buffer we sent
    assertTrue(handle.getMessage() == sentBuffer); // it is the buffer we sent
    assertTrue(unexpectedData.size() >= 1);  // we got 1 result

    //hdr was the good hdr:
    assertTrue(unexpectedData.get(0).length >= GOOD_HDR.length);
    byte[] hdrpart = new byte[GOOD_HDR.length];
    System.arraycopy(unexpectedData.get(0), 0, hdrpart, 0, GOOD_HDR.length);
    assertTrue(Arrays.equals(hdrpart, GOOD_HDR)); // it is equal
  }



}
