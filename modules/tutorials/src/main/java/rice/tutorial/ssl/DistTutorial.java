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
package rice.tutorial.ssl;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

import org.mpisws.p2p.pki.x509.CATool;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.leafset.LeafSet;
import rice.pastry.socket.CertifiedSocketPastryNodeFactory;
import rice.pastry.standard.CertifiedNodeIdFactory;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * This tutorial shows how to setup a FreePastry node using the Socket Protocol.
 *
 * @author Jeff Hoye
 */
public class DistTutorial {

  /**
   * This constructor sets up a PastryNode.  It will bootstrap to an
   * existing ring if it can find one at the specified location, otherwise
   * it will start a new ring.
   *
   * @param bindport the local port to bind to
   * @param bootaddress the IP:port of the node to boot from
   * @param env the environment for these nodes
   */
  public DistTutorial(int bindport, InetSocketAddress bootaddress, Environment env, Path keyStoreFile) throws Exception {

    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

    // create the keystore
    // written by "CATool -store" (PKCS12, blank password)
    final KeyStore store = KeyStore.getInstance(CATool.DEFAULT_KEYSTORE_TYPE);
    try (InputStream fis = Files.newInputStream(keyStoreFile)) {
      store.load(fis, "".toCharArray());
    }

    // the node id is the one the CA put in the node's certificate; this fails if the certificate
    // is not valid for the CA in the store, or does not belong to the node's key
    rice.pastry.Id id = CertifiedNodeIdFactory.fromKeyStore(store, "".toCharArray(), env).generateNodeId();

    // construct the PastryNodeFactory: it runs mutual TLS, takes the node id from the certificate, accepts only
    // certificate common names that are node ids, and admits a node into the routing state once its certificate was seen
    CertifiedSocketPastryNodeFactory factory = new CertifiedSocketPastryNodeFactory(store, "".toCharArray(), bindport, env);

    // construct a node with the id this time
    PastryNode node = factory.newNode(id);

    // construct a new MyApp
    MyApp app = new MyApp(node);

    // boot the node
    node.boot(bootaddress);

    // the node may require sending several messages to fully boot into the ring
    while(!node.isReady() && !node.joinFailed()) {
      // delay so we don't busy-wait
      node.awaitReadyOrJoinFailed(500, TimeUnit.MILLISECONDS);
    }

    // abort if can't join
    if (node.joinFailed()) {
      throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason());
    }

    System.out.println("Finished creating new node "+node);




    // wait 10 seconds
    env.getTimeSource().sleep(10000);


    // route 10 messages
    for (int i = 0; i < 10; i++) {
      // pick a key at random
      Id randId = nidFactory.generateNodeId();

      // send to that key
      app.routeMyMsg(randId);

      // wait a sec
      env.getTimeSource().sleep(1000);
    }

    // wait 10 seconds
    env.getTimeSource().sleep(10000);

    // send directly to my leafset
    LeafSet leafSet = node.getLeafSet();

    // this is a typical loop to cover your leafset.  Note that if the leafset
    // overlaps, then duplicate nodes will be sent to twice
    for (int i=-leafSet.ccwSize(); i<=leafSet.cwSize(); i++) {
      if (i != 0) { // don't send to self
        // select the item
        NodeHandle nh = leafSet.get(i);

        // send the message directly to the node
        app.routeMyMsgDirect(nh);

        // wait a sec
        env.getTimeSource().sleep(1000);
      }
    }
  }

  /**
   * Usage:
   * {@code java -cp modules/all/target/freepastry-2.1.0-jar-with-dependencies.jar rice.tutorial.ssl.DistTutorial localbindport bootIP bootPort}
   * example java rice.tutorial.ssl.DistTutorial 9001 pokey.cs.almamater.edu 9001
   */
  public static void main(String[] args) throws Exception {
    // Loads pastry settings
    Environment env = new Environment();

    // disable the UPnP setting (in case you are testing this on a NATted LAN)
    env.getParameters().setString("nat_search_policy","never");

    try {
      // the port to use locally
      int bindport = Integer.parseInt(args[0]);

      // build the bootaddress from the command line args
      InetAddress bootaddr = InetAddress.getByName(args[1]);
      int bootport = Integer.parseInt(args[2]);
      InetSocketAddress bootaddress = new InetSocketAddress(bootaddr,bootport);

      // get the keystore file
      String keystoreFileName = args[3];
      Path keystoreFile = Path.of(keystoreFileName);
      if (!Files.exists(keystoreFile)) throw new IllegalArgumentException("The file: "+keystoreFileName+" was not found.");

      // launch our node!
      new DistTutorial(bindport, bootaddress, env, keystoreFile);
    } catch (Exception e) {
      // remind user how to use
      System.out.println("Usage:");
      System.out.println("java -cp modules/all/target/freepastry-2.1.0-jar-with-dependencies.jar rice.tutorial.ssl.DistTutorial localbindport bootIP bootPort");
      System.out.println("example java rice.tutorial.DistTutorial 9001 pokey.cs.almamater.edu 9001");
      throw e;
    }
  }
}
