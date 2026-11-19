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
package org.mpisws.p2p.testing.transportlayer.replay;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.peerreview.replay.BasicEntryDeserializer;
import org.mpisws.p2p.transport.peerreview.replay.record.RecordLayer;
import org.mpisws.p2p.transport.simpleidentity.InetSocketAddressSerializer;
import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.params.Parameters;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.pastry.Id;
import rice.pastry.NodeHandleFactory;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.ProximityNeighborSelector;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.pastry.transport.NodeHandleAdapter;

/**
 * This tutorial shows how to use Scribe.
 *
 * @author Jeff Hoye
 */
public class Recorder implements MyEvents {

  /**
   * this will keep track of our Scribe applications
   */
  ArrayList<MyScribeClient> apps = new ArrayList<MyScribeClient>();

  final Map<Node, RecordLayer<InetSocketAddress>> recorders = new HashMap<Node, RecordLayer<InetSocketAddress>>();

  /**
   * Based on the rice.tutorial.lesson4.DistTutorial
   *
   * This constructor launches numNodes PastryNodes. They will bootstrap to an
   * existing ring if one exists at the specified location, otherwise it will
   * start a new ring.
   *
   * @param bindport the local port to bind to
   * @param bootaddress the IP:port of the node to boot from
   * @param numNodes the number of nodes to create in this JVM
   * @param env the Environment
   */
  public Recorder(int bindport, final InetSocketAddress bootaddress,
      int numNodes, Environment env) throws Exception {

    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    final SocketPastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env) {

      @Override
      public rice.pastry.NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory<?> nhf) {
        SocketNodeHandle ret = (SocketNodeHandle)super.getLocalHandle(pn, nhf);
        logger.atDebug().log(() -> "getLocalHandle():"+ret.toStringFull());
        return ret;
      }

      @Override
      protected RandomSource cloneRandomSource(Environment rootEnvironment, Id nodeId, LogManager lman) {
        long randSeed = rootEnvironment.getRandomSource().nextLong();
        logger.debug("RandSeed for {} {}", nodeId, randSeed);

        return new SimpleRandomSource(randSeed, lman);
      }

      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> buildWireTransportLayer(InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
        // record here

        RecordLayer<InetSocketAddress> ret = new RecordLayer<InetSocketAddress>(
            super.buildWireTransportLayer(innermostAddress, pn), "0x"+pn.getNodeId().toStringBare(),
            new InetSocketAddressSerializer(), pn.getEnvironment());
        recorders.put(pn, ret);
        return ret;
      }

      @Override
      @SuppressWarnings("unchecked")
      protected Bootstrapper<InetSocketAddress> getBootstrapper(final PastryNode pn, NodeHandleAdapter nodeHandleAdapter, NodeHandleFactory<?> handleFactory, ProximityNeighborSelector pns) {
        final Bootstrapper<InetSocketAddress> internal = super.getBootstrapper(pn, nodeHandleAdapter, handleFactory, pns);
        Bootstrapper<InetSocketAddress> ret = bootaddresses -> {
          try {
            recorders.get(pn).logEvent(EVT_BOOT);
          } catch (IOException ioe) {
            pn.getEnvironment().getLogManager().getLogger(Bootstrapper.class, null).warn("Error recording EVT_BOOT", ioe);
          }
          internal.boot(bootaddresses);
        };

        return ret;
      }
    };

    // loop to construct the nodes/apps
    for (int curNode = 0; curNode < numNodes; curNode++) {

      // construct a node; if no bootstrap node is alive, boot() makes it start its own ring
      final ArrayList<PastryNode> nodeContainer = new ArrayList<PastryNode>(1);
      env.getSelectorManager().invoke(() -> {
        PastryNode node = (PastryNode)factory.newNode();
        nodeContainer.add(node);
        // construct a new scribe application
        MyScribeClient app = new MyScribeClient(node);
        apps.add(app);

        node.getBootstrapper().boot(Collections.singleton(bootaddress));
      });

      // the node may require sending several messages to fully boot into the ring
      synchronized(nodeContainer) {
        while(nodeContainer.isEmpty()) {
          // delay so we don't busy-wait
          nodeContainer.wait(500);
        }
      }

      PastryNode node = nodeContainer.get(0);

      synchronized(node) {
        while (!node.isReady() && !node.joinFailed()) {
          // delay so we don't busy-wait
          node.wait(500);

          // abort if can't join
          if (node.joinFailed()) {
            throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason());
          }
        }
      }

      System.out.println("Finished creating new node: " + node);
    }

    // for the first app subscribe then start the publishtask
    Iterator<MyScribeClient> iterator = apps.iterator();
    MyScribeClient app = (MyScribeClient) iterator.next();
    env.getSelectorManager().invoke(new SubscribeInvokation(app));
    env.getSelectorManager().invoke(new PublishInvokation(app));
    // for all the rest just subscribe
    while (iterator.hasNext()) {
      app = (MyScribeClient) iterator.next();
      env.getSelectorManager().invoke(new SubscribeInvokation(app));
    }

    // now, print the tree
    env.getTimeSource().sleep(5000);
    printTree(apps);

    env.getTimeSource().sleep(15000);

    env.destroy();

    System.out.println("done recording");
    Thread.sleep(1000);
  }

  public abstract static class AppInvokation implements Runnable {
    MyScribeClient app;
    public AppInvokation(MyScribeClient app) {
      this.app = app;
    }

    public abstract void doIt() throws IOException;

    @Override
    public void run() {
      try {
        doIt();
      } catch (IOException ioe) {
        app.node.getEnvironment().getLogManager().getLogger(Bootstrapper.class, null).warn("Error recording event", ioe);
      }
    }
  }

  public class SubscribeInvokation extends AppInvokation {
    public SubscribeInvokation(MyScribeClient app) {
      super(app);
    }

    @Override
    public void doIt() throws IOException {
      recorders.get(app.node).logEvent(EVT_SUBSCRIBE);
      app.subscribe();
    }
  }

  public class PublishInvokation extends AppInvokation {
    public PublishInvokation(MyScribeClient app) {
      super(app);
    }

    @Override
    public void doIt() throws IOException {
      recorders.get(app.node).logEvent(EVT_PUBLISH);
      app.startPublishTask();
    }
  }

  public void printLog(String arg, Environment env) throws IOException {
    BasicEntryDeserializer.printLog(arg, new MyEntryDeserializer(new InetSocketAddressSerializer()), env);
  }

  /**
   * Note that this function only works because we have global knowledge. Doing
   * this in an actual distributed environment will take some more work.
   *
   * @param apps Vector of the applications.
   */
  public static void printTree(List<MyScribeClient> apps) {
    // build a hashtable of the apps, keyed by nodehandle
    Map<NodeHandle,MyScribeClient> appTable = new LinkedHashMap<NodeHandle,MyScribeClient>();
    for (MyScribeClient app : apps) {
      appTable.put(app.endpoint.getLocalNodeHandle(), app);
    }
    NodeHandle seed = ((MyScribeClient) apps.get(0)).endpoint
        .getLocalNodeHandle();

    // get the root
    NodeHandle root = getRoot(seed, appTable);

    // print the tree from the root down
    recursivelyPrintChildren(root, 0, appTable);
  }

  /**
   * Recursively crawl up the tree to find the root.
   */
  public static NodeHandle getRoot(NodeHandle seed, Map<NodeHandle,MyScribeClient> appTable) {
    MyScribeClient app = (MyScribeClient) appTable.get(seed);
    if (app.isRoot())
      return seed;
    NodeHandle nextSeed = app.getParent();
    return getRoot(nextSeed, appTable);
  }

  /**
   * Print's self, then children.
   */
  public static void recursivelyPrintChildren(NodeHandle curNode,
      int recursionDepth, Map<NodeHandle,MyScribeClient> appTable) {
    // print self at appropriate tab level
    String text = "";
    for (int numTabs = 0; numTabs < recursionDepth; numTabs++) {
      text += "  ";
    }
    text += curNode.getId().toString();
    System.out.println(text);

    // recursively print all children
    MyScribeClient app = (MyScribeClient) appTable.get(curNode);
    NodeHandle[] children = app.getChildren();
    for (NodeHandle nodeHandle : children) {
      recursivelyPrintChildren(nodeHandle, recursionDepth + 1, appTable);
    }
  }

  /**
   * Usage: java -cp target/freepastry-<version>-jar-with-dependencies.jar
   * org.mpisws.p2p.testing.transportlayer.replay.Recorder localbindport bootIP bootPort numNodes
   * example java org.mpisws.p2p.testing.transportlayer.replay.Recorder 9001 pokey.cs.almamater.edu 9001
   */
  public static void main(String[] args) throws Exception {
    Parameters params = new SimpleParameters(Environment.defaultParamFileArray,null);

    if (params.getBoolean("org.mpisws.p2p.testing.transportlayer.replay.Recorder_logtofile")) {
      System.setOut(new PrintStream("replay.txt"));
      System.setErr(System.out);
    }

    // Loads pastry configurations
    Environment env = RecordLayer.generateEnvironment();
    params = env.getParameters();


    params.setInt("pastry_socket_scm_max_open_sockets", params.getInt("org.mpisws.p2p.testing.transportlayer.replay_pastry_socket_scm_max_open_sockets"));

    // disable the UPnP setting (in case you are testing this on a NATted LAN)
    params.setString("nat_search_policy","never");

    try {
      // the port to use locally
      int bindport = Integer.parseInt(args[0]);

      // build the bootaddress from the command line args
      InetAddress bootaddr = InetAddress.getByName(args[1]);
      int bootport = Integer.parseInt(args[2]);
      InetSocketAddress bootaddress = new InetSocketAddress(bootaddr, bootport);

      // the port to use locally
      int numNodes = Integer.parseInt(args[3]);

      // launch our node!
      new Recorder(bindport, bootaddress, numNodes,
          env); // the constructor runs the whole scenario
    } catch (Exception e) {
      // remind user how to use
      System.out.println("Usage:");
      System.out
          .println("java [-cp FreePastry-<version>.jar] rice.tutorial.scribe.ScribeTutorial localbindport bootIP bootPort numNodes");
      System.out
          .println("example java rice.tutorial.scribe.ScribeTutorial 9001 pokey.cs.almamater.edu 9001 10");
      throw e;
    }
  }
}
