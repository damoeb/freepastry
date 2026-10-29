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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Vector;

import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.direct.SphereNetwork;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * ClosestRegrTest
 *
 * A test suite for the getClosest algorithm.  getClosest attempts to choose
 * routing table entries with the closet proximity.
 *
 * Consider this test a PASS if the closest node is there more than 50% of the
 * time.  Potentially this test should be run daily and the proximity recorded
 * over time to see if there was a drastic change based on algorithmic change.
 *
 * @version $Id: ClosestRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author alan mislove
 */
public class ClosestRegrTest {

  public static int NUM_NODES = 1000;

  private PastryNodeFactory factory;
  private NetworkSimulator<DirectNodeHandle, RawMessage> simulator;
  private Vector<PastryNode> pastryNodes;

  int incorrect = 0;
  double sum = 0;

  private Environment environment;
  /**
   * constructor
   */
  private ClosestRegrTest() throws IOException {
    environment = Environment.directEnvironment();
    simulator = new SphereNetwork<DirectNodeHandle, RawMessage>(environment);
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(environment), simulator, environment);
    pastryNodes = new Vector<PastryNode>();
  }

  /**
   * Get pastryNodes.last() to bootstrap with, or return null.
   */
  protected NodeHandle getBootstrap() {
    NodeHandle bootstrap = null;

    try {
      PastryNode lastnode = (PastryNode) pastryNodes.lastElement();
      bootstrap = lastnode.getLocalHandle();
    } catch (NoSuchElementException e) {
    }

    return bootstrap;
  }

  /**
   * initializes the network and prepares for testing
   */
  protected void run() {
    for (int i=0; i<NUM_NODES; i++) {
      NodeHandle bootHandle = getBootstrap();
      PastryNode node;
      try {
        node = factory.newNode();
      } catch (java.io.IOException ioe) {
        throw new RuntimeException(ioe);
      }
      node.boot(bootHandle);
      synchronized(node) {
        while(!node.isReady()) {
          try {
            node.wait(500);
          } catch (InterruptedException ie) {
            return;
          }
        }
      }


      if (i > 0)
        test(i, (DirectNodeHandle)node.getLocalHandle());

      System.out.println("CREATED NODE " + i + " " + node.getNodeId());

      pastryNodes.add(node);
      double ave = getAvgNumEntries(pastryNodes);
      System.out.println("Avg Num Entries:"+ave);
    }

    System.out.println("SO FAR: " + incorrect + "/" + NUM_NODES + " PERCENTAGE: " + (sum/incorrect));
  }

  protected double getAvgNumEntries(Collection<PastryNode> nds) {
    double sum = 0;
    Iterator<PastryNode> i = nds.iterator();
    while(i.hasNext()) {
      PastryNode pn = (PastryNode)i.next();
      sum+=pn.getRoutingTable().numUniqueEntries();
    }
    return sum/nds.size();
  }

  /**
   * starts the testing process
   */
  protected void test(int i, DirectNodeHandle handle) {
    PastryNode bootNode = (PastryNode) pastryNodes.elementAt(environment.getRandomSource().nextInt(i));
    NodeHandle bootstrap = bootNode.getLocalHandle();

    DirectNodeHandle closest = null; // TODO: fix this, old code: (DirectNodeHandle)factory.getNearest(handle, bootstrap)[0];
    DirectNodeHandle realClosest = simulator.getClosest(handle);
    if (! closest.getNodeId().equals(realClosest.getNodeId())) {
      float cProx = simulator.proximity(closest, handle);
      float rProx = simulator.proximity(realClosest, handle);
      if (cProx == 0) {
        System.out.println("ERROR: factory.getNearest("+handle+") returned "+closest);
      }
      if (rProx < cProx) {
        incorrect++;
        sum += (cProx / rProx);

        System.out.println("ERROR: CLOSEST TO " + handle + " WAS " + closest.getNodeId()+":"+ cProx + " REAL CLOSEST: " + realClosest.getNodeId()+":"+rProx);
        System.out.println("SO FAR: " + incorrect + "/" + i + " PERCENTAGE: " + (sum/incorrect));

      }
    }
  }

  public boolean pass() {
    return incorrect < NUM_NODES/2;
  }

  /**
   * main
   */
  public static void main(String args[]) throws IOException {
    ClosestRegrTest pt = new ClosestRegrTest();
    pt.run();
    System.out.println("pass:"+pt.pass());
    pt.environment.destroy();
  }
}
