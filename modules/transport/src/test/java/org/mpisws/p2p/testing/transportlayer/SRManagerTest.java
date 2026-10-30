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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayerImpl;
import org.mpisws.p2p.transport.liveness.LivenessTypes;
import org.mpisws.p2p.transport.liveness.PingListener;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayer;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayerImpl;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.proximity.MinRTTProximityProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayerImpl;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.manager.SourceRouteManagerImpl;
import org.mpisws.p2p.transport.sourceroute.manager.SourceRouteStrategy;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;
import rice.environment.Environment;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.params.Parameters;
import rice.selector.Timer;
import rice.selector.TimerTask;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SRManagerTest extends TLTest<MultiInetSocketAddress> {
  /**
   * Goes to Alice/Bob
   */
  @SuppressWarnings("MutablePublicArray") // a shared constant that callers do not modify
  public static final byte[] GOOD_HDR = {(byte)0xDE,(byte)0xAD,(byte)0xBE,(byte)0xEF};

  static TransportLayer<MultiInetSocketAddress, ByteBuffer> dave; // going to be the middle hop for alice/bob
  static ProximityProvider<SourceRoute<MultiInetSocketAddress>> bob_prox; // going to be the middle hop for alice/bob
  /**
   * bob's liveness layer (the SourceRouteManager doesn't implement Pinger, the layer below it does)
   */
  static Pinger<SourceRoute<MultiInetSocketAddress>> bob_pinger;
  static SourceRouteTransportLayer<MultiInetSocketAddress> carol_tap; // going to be the middle hop for alice/bob
  static TransportLayer<MultiInetSocketAddress, ByteBuffer> carol;
  static SourceRouteFactory<MultiInetSocketAddress> srFactory;

  static Timer timer;

  static MultiInetSocketAddress alice_addr, bob_addr;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    srFactory = new MultiAddressSourceRouteFactory();
    TLTest.setUpBeforeClass();
    logger = env.getLogManager().getLogger(MagicNumberTest.class, null);
    InetAddress addr = InetAddress.getLocalHost();
    int[] ports = FreePorts.freePorts(addr, 4);
    Parameters p = env.getParameters();
    timer = env.getSelectorManager().getTimer();

    // this makes the liveness check super fast
      p.setInt("pastry_socket_scm_num_ping_tries", 2);
      p.setInt("pastry_socket_srm_default_rto", 400); // 3000 // 3 seconds
      p.setInt("pastry_socket_srm_rto_ubound", 500);//240000; // 240 seconds

    alice = buildTL("alice", addr, ports[0], env);
    bob = buildTL("bob", addr, ports[1], env);
    carol = buildTL("carol", addr, ports[2], env);
    dave = buildTL("dave", addr, ports[3], env);
    otherTLs.add(carol);
    otherTLs.add(dave);

    waitForAliceBobRoute();
  }

  /**
   * The direct link between alice and bob is blackholed (see connectionAllowed()), which can only be detected
   * by liveness checks.  Until the SourceRouteManagers have detected that and switched to the route through carol,
   * sockets/messages between alice and bob would silently go nowhere.  So do the liveness checks up front instead
   * of depending on another test (e.g. testProximity) to have run first.
   */
  @SuppressWarnings("unchecked")
  static void waitForAliceBobRoute() throws InterruptedException {
    final LivenessProvider<MultiInetSocketAddress> aliceLP = (LivenessProvider<MultiInetSocketAddress>)alice;
    final LivenessProvider<MultiInetSocketAddress> bobLP = (LivenessProvider<MultiInetSocketAddress>)bob;
    final MultiInetSocketAddress aliceId = (MultiInetSocketAddress)alice.getLocalIdentifier();
    final MultiInetSocketAddress bobId = (MultiInetSocketAddress)bob.getLocalIdentifier();
    final Object lock = new Object();
    LivenessListener<MultiInetSocketAddress> ll = (i, val, options) -> {
      synchronized(lock) {
        lock.notify();
      }
    };
    aliceLP.addLivenessListener(ll);
    bobLP.addLivenessListener(ll);
    aliceLP.checkLiveness(bobId, null);
    bobLP.checkLiveness(aliceId, null);
    long timeout = env.getTimeSource().currentTimeMillis()+10000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) &&
          (aliceLP.getLiveness(bobId, null) != LivenessTypes.LIVENESS_ALIVE ||
           bobLP.getLiveness(aliceId, null) != LivenessTypes.LIVENESS_ALIVE)) {
        lock.wait(500);
      }
    }
    aliceLP.removeLivenessListener(ll);
    bobLP.removeLivenessListener(ll);
    assertTrue(aliceLP.getLiveness(bobId, null) == LivenessTypes.LIVENESS_ALIVE, "alice->bob:"+aliceLP.getLiveness(bobId, null));
    assertTrue(bobLP.getLiveness(aliceId, null) == LivenessTypes.LIVENESS_ALIVE, "bob->alice:"+bobLP.getLiveness(aliceId, null));
  }

  private static TransportLayer<MultiInetSocketAddress, ByteBuffer> buildTL(String name, InetAddress addr, int port, Environment env) throws IOException {
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
    MultiInetAddressTransportLayer myEpochLayer = new MultiInetAddressTransportLayerImpl(new MultiInetSocketAddress(addr_a),
        new MagicNumberTransportLayer<InetSocketAddress>(
          new WireTransportLayerImpl(addr_a,env_a, null),
        env_a, null, GOOD_HDR, 2000),
      env_a, null, null);
    final MultiInetSocketAddress myAddr = myEpochLayer.getLocalIdentifier();
    if (name.equals("alice")) alice_addr = myAddr;
    if (name.equals("bob")) bob_addr = myAddr;

    // don't allow direct connections from alice <-> bob
    SourceRouteTransportLayer<MultiInetSocketAddress> srtl =
        new SourceRouteTransportLayerImpl<MultiInetSocketAddress>(
            srFactory,
            myEpochLayer,
            null, env_a, null) {

          @Override
          public void incomingSocket(P2PSocket<MultiInetSocketAddress> socka) throws IOException {
            if (connectionAllowed(myAddr, socka.getIdentifier(), "socket")) {
              super.incomingSocket(socka);
            }
          }

          @Override
          public void messageReceived(MultiInetSocketAddress i, ByteBuffer m, Map<String, Object> options) throws IOException {
            if (connectionAllowed(myAddr, i, "message")) {
              super.messageReceived(i, m, options);
            }
          }

        };
    if (name.equals("carol")) carol_tap = srtl;

    TestLivenessTransportLayerImpl temp = new TestLivenessTransportLayerImpl(srtl,env_a, null);
    MinRTTProximityProvider<SourceRoute<MultiInetSocketAddress>> prox =
      new MinRTTProximityProvider<SourceRoute<MultiInetSocketAddress>>(temp, env_a);
    if (name.equals("bob")) {
      bob_prox = prox;
      bob_pinger = temp;
    }

    return new SourceRouteManagerImpl<MultiInetSocketAddress>(srFactory, temp, temp, prox, env_a,
        new TestSRS(temp.getLocalIdentifier().getLastHop()));
  }

  static boolean connectionAllowed(MultiInetSocketAddress a, MultiInetSocketAddress b, String context) {
    if (a.equals(alice_addr)) {
      if (b.equals(bob_addr)) {
        return false;
      }
    }
    if (b.equals(alice_addr)) {
      if (a.equals(bob_addr)) {
        return false;
      }
    }
    return true;
  }

  static class TestSRS implements SourceRouteStrategy<MultiInetSocketAddress> {
    MultiInetSocketAddress local;

    public TestSRS(MultiInetSocketAddress local) {
      this.local = local;
    }

    @Override
    public Collection<SourceRoute<MultiInetSocketAddress>> getSourceRoutes(MultiInetSocketAddress destination) {
      ArrayList<MultiInetSocketAddress> eList = new ArrayList<MultiInetSocketAddress>();
      eList.add((MultiInetSocketAddress)alice.getLocalIdentifier());
      eList.add((MultiInetSocketAddress)bob.getLocalIdentifier());
      eList.add((MultiInetSocketAddress)carol.getLocalIdentifier());

      eList.remove(local);
      eList.remove(destination);

      ArrayList<SourceRoute<MultiInetSocketAddress>> srList = new ArrayList<SourceRoute<MultiInetSocketAddress>>();
      ArrayList<MultiInetSocketAddress> path = new ArrayList<MultiInetSocketAddress>(2);
      path.add(local);
      path.add(destination);
      srList.add(srFactory.getSourceRoute(path));

      for (MultiInetSocketAddress eAddr : eList) {
        path = new ArrayList<MultiInetSocketAddress>(3);
        path.add(local);
        path.add(eAddr);
        path.add(destination);
        srList.add(srFactory.getSourceRoute(path));
      }

      return srList;
    }
  }

  static class TestLivenessTransportLayerImpl extends LivenessTransportLayerImpl<SourceRoute<MultiInetSocketAddress>> {
    public TestLivenessTransportLayerImpl(TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> tl, Environment env, ErrorHandler<SourceRoute<MultiInetSocketAddress>> errorHandler) {
      super(tl, env, errorHandler, 5000);
    }

    @Override
    public void pong(final SourceRoute<MultiInetSocketAddress> i, final long senderTime, final Map<String, Object> options) {
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          TestLivenessTransportLayerImpl.super.pong(i, senderTime, options);
        }
      }, getDelay(getLocalIdentifier(), i));
    }
  }

  public static int getDelay(SourceRoute<MultiInetSocketAddress> a, SourceRoute<MultiInetSocketAddress> b) {
    if (b.getLastHop().equals(bob.getLocalIdentifier()) &&
        b.getFirstHop().equals(alice.getLocalIdentifier())) {
       return 150;
    }
    return 2;
  }

  @Test
  public void testProximity() throws Exception {
    final Map<MultiInetSocketAddress, Tupel> pingResponse = new HashMap<MultiInetSocketAddress, Tupel>();
    final Object lock = new Object();

    PingListener<SourceRoute<MultiInetSocketAddress>> pl = new PingListener<SourceRoute<MultiInetSocketAddress>>() {
      @Override
      public void pingResponse(SourceRoute<MultiInetSocketAddress> i, int rtt, Map<String, Object> options) {
        synchronized(lock) {
          pingResponse.put(i.getLastHop(), new Tupel(i.getLastHop(), rtt));
          lock.notify();
        }
      }

      @Override
      public void pingReceived(SourceRoute<MultiInetSocketAddress> i, Map<String, Object> options) {

      }
    };

    bob_pinger.addPingListener(pl);

    ((ProximityProvider<MultiInetSocketAddress>)bob).proximity(alice().getLocalIdentifier(), options); // initial check, causes ping if no data
    ((ProximityProvider<MultiInetSocketAddress>)bob).proximity(carol.getLocalIdentifier(), options); // initial check, causes ping if no data

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) &&
         (!pingResponse.containsKey((MultiInetSocketAddress)alice.getLocalIdentifier()) ||
          !pingResponse.containsKey((MultiInetSocketAddress)carol.getLocalIdentifier()))) {
        lock.wait(1000);
      }
    }
    bob_pinger.removePingListener(pl);

    int aliceProx = ((ProximityProvider<MultiInetSocketAddress>)bob).proximity(alice().getLocalIdentifier(), options);
    int carolProx = ((ProximityProvider<MultiInetSocketAddress>)bob).proximity(carol.getLocalIdentifier(), options);

    assertTrue(aliceProx > carolProx+100, "aliceProx:"+aliceProx+" carolProx:"+carolProx);
  }

  static class Tupel {
    MultiInetSocketAddress sr;
    int val;

    public Tupel(MultiInetSocketAddress i, int val) {
      this.sr = i;
      this.val = val;
    }
  }

  @Test
  public void testLiveness() throws Exception {
    LivenessProvider<MultiInetSocketAddress> alice = (LivenessProvider<MultiInetSocketAddress>)SRManagerTest.alice;
    MultiInetSocketAddress daveAddress = (MultiInetSocketAddress)dave.getLocalIdentifier();
    final List<Tupel> tupels = new ArrayList<Tupel>(3);
    final Object lock = new Object();

    alice.addLivenessListener((i, val, options) -> {
      synchronized(lock) {
        tupels.add(new Tupel(i,val));
        lock.notify();
      }
    });

    // starts out suspected
    assertTrue(
        alice.getLiveness(daveAddress, null) ==
        LivenessTypes.LIVENESS_SUSPECTED);

    // becomes alive

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && tupels.isEmpty()) {
        lock.wait(1000);
      }
    }

    int result = alice.getLiveness(daveAddress, null);
    assertTrue(result == LivenessTypes.LIVENESS_ALIVE, "result = "+result);
    assertTrue(tupels.size() == 1);


    dave.destroy();

    try { Thread.sleep(500); } catch (InterruptedException ie) {return;}

    // becomes alive
    assertTrue(alice.checkLiveness(daveAddress, null));

    timeout = env.getTimeSource().currentTimeMillis()+5000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && tupels.size() <= 2) {
        lock.wait(1000);
      }
    }

    result = alice.getLiveness(daveAddress, null);
    assertTrue(result == LivenessTypes.LIVENESS_DEAD, "result = "+result);
    // suspected/dead
    assertTrue(tupels.size() == 3);
  }

  @Override
  public MultiInetSocketAddress getBogusIdentifier(MultiInetSocketAddress local) throws IOException {
    return new MultiInetSocketAddress(new InetSocketAddress(InetAddress.getLocalHost(), BOGUS_PORT));
  }

}
