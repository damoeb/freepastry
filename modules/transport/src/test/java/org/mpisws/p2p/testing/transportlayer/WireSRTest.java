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

import org.mpisws.p2p.transport.sourceroute.AddressPolicyForwardStrategy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTap;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayer;
import org.mpisws.p2p.transport.sourceroute.factory.WireSourceRouteFactory;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;
import rice.environment.Environment;
import rice.environment.logging.Slf4jLogManager;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class WireSRTest extends TLTest<SourceRoute<InetSocketAddress>> {
  static SourceRouteTransportLayer<InetSocketAddress> carol_tap; // going to be the middle hop for alice/bob
  static TransportLayer<SourceRoute<InetSocketAddress>, ByteBuffer> carol;
  static SourceRouteFactory<InetSocketAddress> srFactory;

  @Override
  public SourceRoute<InetSocketAddress> getIdentifier(
      TransportLayer<SourceRoute<InetSocketAddress>, ByteBuffer> a,
      TransportLayer<SourceRoute<InetSocketAddress>, ByteBuffer> b) {

    SourceRoute<InetSocketAddress> src = a.getLocalIdentifier();
    SourceRoute<InetSocketAddress> intermediate = carol.getLocalIdentifier();
    SourceRoute<InetSocketAddress> dest = b.getLocalIdentifier();

    List<InetSocketAddress> retArr = new ArrayList<InetSocketAddress>(3);
    retArr.add(src.getFirstHop());
    retArr.add(intermediate.getFirstHop());
    retArr.add(dest.getFirstHop());

    return srFactory.getSourceRoute(retArr);
  }

  /**
   * Goes to Alice/Bob
   */
  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] GOOD_HDR = {(byte)0xDE,(byte)0xAD,(byte)0xBE,(byte)0xEF};

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    srFactory = new WireSourceRouteFactory();
    TLTest.setUpBeforeClass();
    logger = env.getLogManager().getLogger(MagicNumberTest.class, null);
    InetAddress addr = InetAddress.getLocalHost();
    int[] ports = FreePorts.freePorts(addr, 3);

    alice = buildTL("alice", addr, ports[0], env);
    bob = buildTL("bob", addr, ports[1], env);
    carol_tap = buildTL("carol", addr, ports[2], env);
    carol = carol_tap;
    otherTLs.add(carol);
  }

  private static SourceRouteTransportLayer<InetSocketAddress> buildTL(String name, InetAddress addr, int port, Environment env) throws IOException {
    Environment env_a = new Environment(
        env.getSelectorManager(),
        env.getProcessor(),
        env.getRandomSource(),
        env.getTimeSource(),
        ((Slf4jLogManager) env.getLogManager()).clone(name),
        env.getParameters(),
        env.getExceptionStrategy());
    env.addDestructable(env_a);
    InetSocketAddress addr_a = new InetSocketAddress(addr,port);
    return
      new SourceRouteTransportLayer<InetSocketAddress>(srFactory,
        new MagicNumberTransportLayer<InetSocketAddress>(
          new WireTransportLayer(addr_a,env_a, null),
        env_a, null,GOOD_HDR, 2000),
      AddressPolicyForwardStrategy.forInetSocketAddress(addr_a), env_a, null);
  }

  static class Triplet {
    SourceRoute<InetSocketAddress> path;
    P2PSocket<InetSocketAddress> a,b;
    ByteBuffer m;

    public Triplet(SourceRoute<InetSocketAddress> path, P2PSocket<InetSocketAddress> a, P2PSocket<InetSocketAddress> b) {
      this.path = path;
      this.a = a;
      this.b = b;
    }
  }

  @Test
  @Override
  @SuppressWarnings("JUnitMethodInvoked") // reruns the inherited test with other options
  public void openTCP() throws Exception {
    final Object lock = new Object();
    final List<Triplet> opened = new ArrayList<Triplet>(1);
    final List<Triplet> closed = new ArrayList<Triplet>(1);
    final List<Triplet> received = new ArrayList<Triplet>(1);
    final List<Triplet> bad = new ArrayList<Triplet>(1);

    SourceRouteTap<InetSocketAddress> tap = new SourceRouteTap<InetSocketAddress>(){
      @Override
      public void socketOpened(SourceRoute<InetSocketAddress> path, P2PSocket<InetSocketAddress> a, P2PSocket<InetSocketAddress> b) {
        synchronized(lock) {
          opened.add(new Triplet(path, a, b));
          lock.notify();
        }
      }

      @Override
      public void receivedMessage(ByteBuffer m, SourceRoute<InetSocketAddress> path) {
        bad.add(new Triplet(path, null, null));
      }

      @Override
      public void socketClosed(SourceRoute<InetSocketAddress> path, P2PSocket<InetSocketAddress> a, P2PSocket<InetSocketAddress> b) {
        synchronized(lock) {
          closed.add(new Triplet(path, a, b));
          lock.notify();
        }
      }

      @Override
      public void receivedBytes(ByteBuffer m, SourceRoute<InetSocketAddress> path, P2PSocket<InetSocketAddress> a, P2PSocket<InetSocketAddress> b) {
        synchronized(lock) {
          Triplet t = new Triplet(path, a, b);
          t.m = m;
          received.add(t);
          lock.notify();
        }
      }
    };

    carol_tap.addSourceRouteTap(tap);
    super.openTCP();

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && bad.isEmpty() && (received.isEmpty() || opened.isEmpty() || closed.isEmpty())) {
        lock.wait(1000);
      }
    }

    carol_tap.removeSourceRouteTap(tap);

    // verify that carol's tap saw the forwarded socket open, carry bytes and close
    assertTrue(bad.isEmpty(), "bad:"+bad.size());
    assertTrue(opened.size() == 1, "opened:"+opened.size());
    assertTrue(!received.isEmpty(), "received:"+received.size());
    assertTrue(closed.size() == 1, "closed:"+closed.size());
    int numBytes = 0;
    synchronized(lock) {
      for (Triplet t : received) {
        numBytes += t.m.remaining();
      }
    }
    assertTrue(numBytes == sentBytes.length, "numBytes:"+numBytes);
  }

  @Test
  @Override
  @SuppressWarnings("JUnitMethodInvoked") // reruns the inherited test with other options
  public void sendUDP() throws Exception {
    class TapTupel {
      SourceRoute<InetSocketAddress> path;
      TapTupel(SourceRoute<InetSocketAddress> path) {
        this.path = path;
      }
    }
    final Object lock = new Object();
    final List<TapTupel> taptupels = new ArrayList<TapTupel>(1);

    SourceRouteTap<InetSocketAddress> tap = new SourceRouteTap<InetSocketAddress>(){
      @Override
      public void socketOpened(SourceRoute<InetSocketAddress> path, P2PSocket<InetSocketAddress> a, P2PSocket<InetSocketAddress> b) {
      }

      @Override
      public void receivedBytes(ByteBuffer m, SourceRoute<InetSocketAddress> path, P2PSocket<InetSocketAddress> a, P2PSocket<InetSocketAddress> b) {

      }

      @Override
      public void socketClosed(SourceRoute<InetSocketAddress> path, P2PSocket<InetSocketAddress> a, P2PSocket<InetSocketAddress> b) {

      }

      @Override
      public void receivedMessage(ByteBuffer m, SourceRoute<InetSocketAddress> path) {
        synchronized(lock) {
          taptupels.add(new TapTupel(path));
          lock.notify();
        }
      }
    };

    carol_tap.addSourceRouteTap(tap);

    super.sendUDP();

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && taptupels.isEmpty()) {
        lock.wait(1000);
      }
    }

    carol_tap.removeSourceRouteTap(tap);

    // verify that we got the tap info, and it is all correct
    assertTrue(taptupels.size() == 1);
    assertTrue(taptupels.get(0).path.equals(getIdentifier(alice(), bob())));
  }

  @Override
  public SourceRoute<InetSocketAddress> getBogusIdentifier(SourceRoute<InetSocketAddress> local) throws IOException {
    return srFactory.getSourceRoute(local.getFirstHop(),
        new InetSocketAddress(InetAddress.getLocalHost(), BOGUS_PORT));
  }

}
