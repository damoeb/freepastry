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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayerImpl;
import org.mpisws.p2p.transport.liveness.PingListener;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayerImpl;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.proximity.MinRTTProximityProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayerImpl;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;
import rice.environment.Environment;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.params.Parameters;
import rice.selector.Timer;
import rice.selector.TimerTask;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class LivenessTest extends SRTest {
  static TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> dave; // going to be the middle hop for alice/bob
  static ProximityProvider<SourceRoute<MultiInetSocketAddress>> bob_prox; // going to be the middle hop for alice/bob
  // srFactory is inherited from SRTest (redeclaring it here would hide the field used by SRTest.getBogusIdentifier())

  @Override
  public SourceRoute<MultiInetSocketAddress> getIdentifier(
      TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> a,
      TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> b) {
    SourceRoute<MultiInetSocketAddress> src = a.getLocalIdentifier();
    SourceRoute<MultiInetSocketAddress> intermediate = carol_tap.getLocalIdentifier();
    SourceRoute<MultiInetSocketAddress> dest = b.getLocalIdentifier();

    if (a.equals(carol) || b.equals(carol)) {

      List<MultiInetSocketAddress> retArr = new ArrayList<MultiInetSocketAddress>(2);
      retArr.add(src.getFirstHop());
      retArr.add(dest.getFirstHop());

      return srFactory.getSourceRoute(retArr);
    }

    List<MultiInetSocketAddress> retArr = new ArrayList<MultiInetSocketAddress>(3);
    retArr.add(src.getFirstHop());
    retArr.add(intermediate.getFirstHop());
    retArr.add(dest.getFirstHop());

    return srFactory.getSourceRoute(retArr);
  }

  static Timer timer;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    srFactory = new MultiAddressSourceRouteFactory();
    TLTest.setUpBeforeClass();
    int startPort = 5009;
    logger = env.getLogManager().getLogger(MagicNumberTest.class, null);
    InetAddress addr = InetAddress.getLocalHost();
    Parameters p = env.getParameters();
    timer = env.getSelectorManager().getTimer();

    // this makes the liveness check super fast
      p.setInt("pastry_socket_scm_num_ping_tries", 2);
      p.setInt("pastry_socket_srm_default_rto", 400); // 3000 // 3 seconds
      p.setInt("pastry_socket_srm_rto_ubound", 500);//240000; // 240 seconds

    alice = buildTL("alice", addr, startPort, env);
    bob = buildTL("bob", addr, startPort+1, env);
    carol = buildTL("carol", addr, startPort+2, env);
    dave = buildTL("dave", addr, startPort+3, env);
    otherTLs.add(carol);
    otherTLs.add(dave);
    bob_prox = new MinRTTProximityProvider<SourceRoute<MultiInetSocketAddress>>((TestLivenessTransportLayerImpl)bob, env);
  }

  private static TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> buildTL(String name, InetAddress addr, int port, Environment env) throws IOException {
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
    SourceRouteTransportLayer<MultiInetSocketAddress> srtl =
        new SourceRouteTransportLayerImpl<MultiInetSocketAddress>(srFactory,
          new MultiInetAddressTransportLayerImpl(new MultiInetSocketAddress(addr_a),
            new MagicNumberTransportLayer<InetSocketAddress>(
              new WireTransportLayerImpl(addr_a,env_a, null),
            env_a, null,GOOD_HDR, 2000),
          env_a, null, null),
        null, env_a, null);
    if (name.equals("carol")) carol_tap = srtl;
    return new TestLivenessTransportLayerImpl(srtl,env_a, null);
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
    TestLivenessTransportLayerImpl bob = (TestLivenessTransportLayerImpl)LivenessTest.bob;
    if (b.getLastHop().equals(bob.getLocalIdentifier().getLastHop()) &&
        a.equals(alice.getLocalIdentifier())) {
       return 150;
    }
    return 0;
  }

  @Test
  public void testProximity() throws Exception {
    final Map<MultiInetSocketAddress, Tupel> pingResponse = new HashMap<MultiInetSocketAddress, Tupel>();
    final Object lock = new Object();

    PingListener<SourceRoute<MultiInetSocketAddress>> pl = new PingListener<SourceRoute<MultiInetSocketAddress>>() {
      public void pingResponse(SourceRoute<MultiInetSocketAddress> i, int rtt, Map<String, Object> options) {
        synchronized(lock) {
          pingResponse.put(i.getLastHop(), new Tupel(i, rtt));
          lock.notify();
        }
      }

      public void pingReceived(SourceRoute<MultiInetSocketAddress> i, Map<String, Object> options) {

      }
    };

    ((Pinger<SourceRoute<MultiInetSocketAddress>>)LivenessTest.bob).addPingListener(pl);

    bob_prox.proximity(getIdentifier(bob(), alice()), options); // initial check, causes ping if no data
    bob_prox.proximity(getIdentifier(bob(), carol), options); // initial check, causes ping if no data

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) &&
         (!pingResponse.containsKey(alice().getLocalIdentifier().getLastHop()) ||
          !pingResponse.containsKey(carol.getLocalIdentifier().getLastHop()))) {
        lock.wait(1000);
      }
    }

    int aliceProx = bob_prox.proximity(getIdentifier(bob(), alice()), options);
    int carolProx = bob_prox.proximity(getIdentifier(bob(), carol), options);

    assertTrue(aliceProx > carolProx+100, "aliceProx:"+aliceProx+" carolProx:"+carolProx);
  }

  class Tupel {
    SourceRoute<MultiInetSocketAddress> sr;
    int val;

    public Tupel(SourceRoute<MultiInetSocketAddress> i, int val) {
      this.sr = i;
      this.val = val;
    }
  }

  @Test
  public void testLiveness() throws Exception {
    LivenessProvider<SourceRoute<MultiInetSocketAddress>> alice = (LivenessProvider<SourceRoute<MultiInetSocketAddress>>)LivenessTest.alice;
    SourceRoute<MultiInetSocketAddress> aliceToDave = getIdentifier(
        alice(), dave);
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
        alice.getLiveness(aliceToDave, null) ==
          LivenessListener.LIVENESS_SUSPECTED);

    // becomes alive
    assertTrue(alice.checkLiveness(aliceToDave, null));

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && tupels.isEmpty()) {
        lock.wait(1000);
      }
    }

    assertTrue(
        alice.getLiveness(aliceToDave, null) ==
        LivenessListener.LIVENESS_ALIVE);
    assertTrue(tupels.size() == 1);


    dave.destroy();

    try { Thread.sleep(500); } catch (InterruptedException ie) {return;}

    // becomes alive
    assertTrue(alice.checkLiveness(aliceToDave, null));

    timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && tupels.size() <= 2) {
        lock.wait(1000);
      }
    }

    assertTrue(
        alice.getLiveness(aliceToDave, null) ==
        LivenessListener.LIVENESS_DEAD);
    // suspected/dead
    assertTrue(tupels.size() == 3);
  }

}
