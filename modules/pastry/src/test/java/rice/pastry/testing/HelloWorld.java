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

import java.util.NoSuchElementException;
import java.util.Vector;

import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * A hello world example for pastry. This is the "direct" driver.
 *
 * @version $Id: HelloWorld.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Sitaram Iyer
 */
@SuppressWarnings("unchecked")
public class HelloWorld {
  private PastryNodeFactory factory;

  private NetworkSimulator<DirectNodeHandle, RawMessage> simulator;

  private Vector<PastryNode> pastryNodes;

  private Vector<Object> helloClients;

  private static int numnodes = 3;

  private static int nummsgs = 30; // total messages

  private static boolean simultaneous_joins = false;

  private static boolean simultaneous_msgs = false;

  Environment environment;
  /**
   * Constructor
   */
  public HelloWorld(Environment env) {
    environment = env;
    simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(environment), simulator, environment);

    pastryNodes = new Vector<PastryNode>();
    helloClients = new Vector<Object>();
  }

  /**
   * Get a handle to a bootstrap node. This is only a simulation, so we pick the
   * most recently created node.
   *
   * @return handle to bootstrap node, or null.
   */
  private NodeHandle getBootstrap() {
    NodeHandle bootstrap = null;
    try {
      PastryNode lastnode = (PastryNode) pastryNodes.lastElement();
      bootstrap = lastnode.getLocalHandle();
    } catch (NoSuchElementException e) {
    }
    return bootstrap;
  }

  /**
   * Create a Pastry node and add it to pastryNodes. Also create a client
   * application for this node.
   */
  public void makePastryNode(int num) {
    NodeHandle bootHandle = getBootstrap();
    PastryNode pn;
    try {
      pn = factory.newNode();
    } catch (java.io.IOException ioe) {
      throw new RuntimeException(ioe);
    }
    pn.boot(bootHandle);
    pastryNodes.addElement(pn);

    HelloWorldApp app = new HelloWorldApp(pn);
    helloClients.addElement(app);

    synchronized (pn) {
      while(!pn.isReady()) {
        try {
          pn.wait(300);
        } catch (InterruptedException ie) {}
      }
    }

    System.out.println("created "+num+" "+pn);
  }

  /**
   * Print leafsets of all nodes in pastryNodes.
   */
  private void printLeafSets() {
    for (int i = 0; i < pastryNodes.size(); i++) {
      PastryNode pn = (PastryNode) pastryNodes.get(i);
      System.out.println(pn.getLeafSet().toString());
    }
  }

  /**
   * Invoke a HelloWorldApp method called sendRndMsg. First choose a random
   * application from helloClients.
   */
  private void sendRandomMessage() {
    int n = helloClients.size();
    int client = environment.getRandomSource().nextInt(n);
    HelloWorldApp app = (HelloWorldApp) helloClients.get(client);
    app.sendRndMsg(environment.getRandomSource());
  }

  /**
   * Process one message.
   */
  private boolean simulate() {
    return false;
  }

  private static void doIinitstuff(String args[]) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-nodes") && i + 1 < args.length)
        numnodes = Integer.parseInt(args[i + 1]);

      if (args[i].equals("-msgs") && i + 1 < args.length)
        nummsgs = Integer.parseInt(args[i + 1]);

      if (args[i].equals("-simultaneous_joins"))
        simultaneous_joins = true;

      if (args[i].equals("-simultaneous_msgs"))
        simultaneous_msgs = true;

      if (args[i].equals("-help")) {
        System.out
            .println("Usage: HelloWorld [-msgs m] [-nodes n]");
        System.out
            .println("                  [-simultaneous_joins] [-simultaneous_msgs] [-help]");
        System.out
            .println("  Log levels are set in the SLF4J backend's configuration.");
        System.exit(1);
      }
    }
  }

  /**
   * Usage: HelloWorld [-msgs m] [-nodes n]
   * [-simultaneous_joins] [-simultaneous_msgs] [-help]
   */
  public static void main(String args[]) {
    Environment env = Environment.directEnvironment();

    doIinitstuff(args);

    HelloWorld driver = new HelloWorld(env);

    for (int i = 0; i < numnodes; i++) {
      driver.makePastryNode(i);
      if (simultaneous_joins == false)
        while (driver.simulate())
          ;
    }
    if (simultaneous_joins) {
      System.out.println("let the joins begin!");
      while (driver.simulate())
        ;
    }

    System.out.println(numnodes + " nodes constructed");

    driver.printLeafSets();

    for (int i = 0; i < nummsgs; i++) {
      driver.sendRandomMessage();
      if (simultaneous_msgs == false)
        while (driver.simulate())
          ;
    }

    if (simultaneous_msgs) {
      System.out.println("let the msgs begin!");
      while (driver.simulate())
        ;
    }
    try { Thread.sleep(5000); } catch (InterruptedException ie) {}
    env.destroy();
  }
}
