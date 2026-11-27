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
package rice.p2p.past.testing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import rice.Continuation;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastImpl;
import rice.p2p.past.PastStorageSerializer;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.persistence.CachedStorageImpl;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;
import rice.persistence.PersistentStorage;
import rice.persistence.Storage;

/**
 * @author jstewart
 */
@SuppressWarnings("unchecked")
public class DistPastTest {

    public DistPastTest(int bindPort, InetSocketAddress bootAddress, Environment env, int numNodes) throws Exception {

      // Generate the NodeIds Randomly
      NodeIdFactory nodeIdFactory = new RandomNodeIdFactory(env);
      Past localPast = null;
      Storage storage = null;

      // used for generating PastContent object Ids.
      // this implements the "hash function" for our DHT
      PastryIdFactory idFactory = new rice.pastry.commonapi.PastryIdFactory(env);

      // construct the PastryNodeFactory, this is how we use rice.pastry.socket
      PastryNodeFactory factory = new SocketPastryNodeFactory(nodeIdFactory, bindPort, env);

      // loop to construct the nodes/apps
      for (int nodeIndex = 0; nodeIndex < numNodes; nodeIndex++) {
        // construct a node and boot it; if there is no node at bootaddress (first loop), it starts its own ring
        PastryNode node = factory.newNode();
        node.boot(bootAddress);

        // the node may require sending several messages to fully boot into the ring
        while(!node.isReady()) {
          // delay so we don't busy-wait
          Thread.sleep(100);
        }

        System.out.println("Finished creating new node "+node);


        final PastImpl[] past = new PastImpl[1];
        storage = new PersistentStorage(idFactory, new PastStorageSerializer(() -> past[0]), ".",4*1024*1024,node.getEnvironment());
          localPast = past[0] = new PastImpl(node, new CachedStorageImpl(idFactory,storage,new LRUCache(new MemoryStorage(idFactory),512*1024,node.getEnvironment())), 3, "");
          localPast.setContentDeserializer((buf, endpoint, contentType) -> {
            if (contentType == DistPastTestContent.TYPE) {
              return new DistPastTestContent(buf, endpoint);
            }
            throw new IOException("Unknown content type: " + contentType);
          });
      }
      Thread.sleep(5000);

      String contentText = "test" + env.getRandomSource().nextInt();
      PastContent testContent = new DistPastTestContent(env,idFactory,contentText);
      System.out.println("Inserting "+testContent);
      Futures.forward(localPast.insert(testContent), new Continuation<Boolean[], Exception>() {
        @Override
        public void receiveResult(Boolean[] result) {
          Boolean[] results = result;
          for(int counter = 0; counter < results.length;counter++) {
            System.out.println("got "+results[counter].booleanValue());
          }

        }

        @Override
        public void receiveException(Exception result) {
          result.printStackTrace();
        }
      });

      Thread.sleep(5000);

      Futures.forward(localPast.lookup(testContent.getId()), new Continuation<PastContent, Exception>() {
        @Override
        public void receiveResult(PastContent result) {
          System.out.println("Got a "+result);
        }

        @Override
        public void receiveException(Exception result) {
          result.printStackTrace();
        }
      });
    }

    /**
     * Usage:
     * java -cp target/freepastry-<version>-jar-with-dependencies.jar rice.p2p.past.testing.DistPastTest localbindport bootIP bootPort numNodes
     * example java rice.p2p.past.testing.DistPastTest 9001 pokey.cs.almamater.edu 9001 10
     */
    public static void main(String[] args) throws Exception {
      // Loads pastry settings
      Environment env = new Environment();

      try {
        // the port to use locally
        int bindPort = Integer.parseInt(args[0]);

        // build the bootaddress from the command line args
        InetAddress bootInetAddress = InetAddress.getByName(args[1]);
        int bootPort = Integer.parseInt(args[2]);
        InetSocketAddress bootAddress = new InetSocketAddress(bootInetAddress,bootPort);

        // the port to use locally
        int numNodes = Integer.parseInt(args[3]);

        // launch our node!
        new DistPastTest(bindPort, bootAddress, env, numNodes);
      } catch (Exception e) {
        // remind user how to use
        System.out.println("Usage:");
        System.out.println("java [-cp FreePastry-<version>.jar] rice.p2p.past.testing.DistPastTest localbindport bootIP bootPort numNodes");
        System.out.println("example java rice.p2p.past.testing.DistPastTest 9001 pokey.cs.almamater.edu 9001 10");
        throw e;
      }
    }
  }
