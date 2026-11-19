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
package rice.pastry.testing;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.NoSuchElementException;

import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.direct.TestRecord;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * SinglePingTest
 *
 * A performance test suite for pastry.
 *
 * @version $Id: SinglePingTest.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Rongmei Zhang
 */

@SuppressWarnings("unchecked")
public class SinglePingTest {
  private DirectPastryNodeFactory factory;

  private NetworkSimulator<DirectNodeHandle, RawMessage> simulator;

  private TestRecord testRecord;

  private ArrayList<PastryNode> pastryNodes;

  private ArrayList<Object> pingClients;

  private Environment environment;

  public SinglePingTest(TestRecord tr, Environment env) {
    environment = env;
    simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(environment), simulator, env);
    simulator.setTestRecord(tr);
    testRecord = tr;

    pastryNodes = new ArrayList<PastryNode>();
    pingClients = new ArrayList<Object>();
  }

  private NodeHandle getBootstrap() {
    NodeHandle bootstrap = null;
    try {
      PastryNode lastnode = (PastryNode) pastryNodes.getLast();
      bootstrap = lastnode.getLocalHandle();
    } catch (NoSuchElementException e) {
      // no node yet: boot without a bootstrap
    }
    return bootstrap;
  }

  public PastryNode makePastryNode() {
    NodeHandle bootHandle = getBootstrap();
    PastryNode pn;
    try {
      pn = factory.newNode();
    } catch (java.io.IOException ioe) {
      throw new RuntimeException(ioe);
    }
    pn.boot(bootHandle);
    pastryNodes.add(pn);

    Ping pc = new Ping(pn);
    pingClients.add(pc);

    long start = System.currentTimeMillis();
    synchronized (pn) {
      while(!pn.isReady()) {
        try {
          pn.wait(300);
        } catch (InterruptedException ie) {
          // a pause cut short is fine here
        }
      }
    }
    long nowMillis = System.currentTimeMillis();
    if (nowMillis-start > 10000)
      System.out.println("Took "+(nowMillis-start)+" to create node "+pn);

    return pn;
  }

  public void sendPings(int k) {
    int n = pingClients.size();

    for (int i = 0; i < k; i++) {
      int from = environment.getRandomSource().nextInt(n);
      int to = environment.getRandomSource().nextInt(n);

      Ping pc = (Ping) pingClients.get(from);
      PastryNode pn = (PastryNode) pastryNodes.get(to);

      pc.sendPing(pn.getNodeId());
      while (simulate());
    }
  }

  public boolean simulate() {
    return false;
  }

  public void checkRoutingTable() {
    int i;
    Instant prev = Instant.now();

    for (i = 0; i < testRecord.getNodeNumber(); i++) {
      PastryNode pn = makePastryNode();
      while (simulate());
      System.out.println(pn.getLeafSet());

      if (i != 0 && i % 1000 == 0)
        System.out.println(i + " nodes constructed");
    }
    System.out.println(i + " nodes constructed");

    long msec = Duration.between(prev, Instant.now()).toMillis();
    System.out.println("time used " + (msec / 60000) + ":"
        + ((msec % 60000) / 1000) + ":" + ((msec % 60000) % 1000));

  }

  public void test() {
    int i;
    long prev = environment.getTimeSource().currentTimeMillis();

    System.out.println("-------------------------");
    for (i = 0; i < testRecord.getNodeNumber(); i++) {
      PastryNode pn = makePastryNode();

      synchronized (pn) {
        while(!pn.isReady()) {
          try {
            pn.wait(500); // PastryNode no longer notifies its monitor, so poll
          } catch (InterruptedException ie) {
            // a pause cut short is fine here
          }
        }
      }

      if (i != 0 && i % 100 == 0)
        System.out.println(i + " nodes constructed");
    }
    System.out.println(i + " nodes constructed");

    long curr = environment.getTimeSource().currentTimeMillis();
    long msec = curr - prev;
    System.out.println("time used " + (msec / 60000) + ":"
        + ((msec % 60000) / 1000) + ":" + ((msec % 60000) % 1000));
    prev = curr;

    sendPings(testRecord.getTestNumber());
    System.out.println(testRecord.getTestNumber() + " lookups done");

    curr = environment.getTimeSource().currentTimeMillis();
    msec = curr - prev;
    System.out.println("time used " + (msec / 60000) + ":"
        + ((msec % 60000) / 1000) + ":" + ((msec % 60000) % 1000));

    testRecord.doneTest();
  }
}
