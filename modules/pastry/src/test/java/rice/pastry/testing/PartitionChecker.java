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
/*
 * Created on Jul 5, 2005
 */
package rice.pastry.testing;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import rice.environment.Environment;
import rice.pastry.NodeHandle;
import rice.pastry.NodeSet;
import rice.pastry.leafset.LeafSet;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketPastryNodeFactory;

/**
 * Pass in a certificate which contains bootstrap nodes.
 *
 * Output is an ordered list of independent rings, and which bootstrap nodes belong
 * to each.
 *
 * @author Jeff Hoye
 */
public class PartitionChecker {

  protected final int MAX_THREADS = 100;

  int numThreads = 0;

  /**
   * Set of InetSocketAddress
   *
   * The set of bootstrap nodes that need to be checked.
   * Gained from the certificate.
   *
   * This list is reduced each time a node from this ring is identified
   */
  final HashSet<InetSocketAddress> unmatchedBootstraps = new HashSet<InetSocketAddress>();

  /**
   * of InetSocketAddress.  This list is kept around.
   */
  HashSet<InetSocketAddress> bootstraps;

  final PrintStream ps = new PrintStream(new FileOutputStream("response.txt"));


  /**
   * A list of Ring.  Increased whenever a new one is found.
   */
  ArrayList<Ring> rings;

  Environment environment;

  HashSet<NodeHandle> dead = new HashSet<NodeHandle>();

  public PartitionChecker(String ringIdString) throws Exception {
    environment = new Environment();
    buildBootstrapSetFromCert(ringIdString);
    rings = new ArrayList<Ring>();

    SocketPastryNodeFactory factory = new SocketPastryNodeFactory(null, 1, environment);


    while(unmatchedBootstraps.size() > 0) {
      rings.add(buildRing(factory, (InetSocketAddress) unmatchedBootstraps.iterator().next()));
    }
    Collections.sort(rings);
    for (Ring ring : rings) {
      System.out.println(ring);
    }
  }



  @SuppressWarnings("StringSplitter") // dropping trailing empty entries is intended
  protected void buildBootstrapSetFromCert(String ringIdString) throws Exception {
    unmatchedBootstraps.clear();
    bootstraps = new HashSet<InetSocketAddress>();



    BufferedReader in = new BufferedReader(new FileReader(ringIdString, StandardCharsets.UTF_8));

    String line;

    while(in.ready()) {
      line = in.readLine();
      String[] parts = line.split(":");

      int port = 10003;
      if (parts.length > 1) {
        port = Integer.parseInt(parts[1]);
      }
      InetSocketAddress addr = new InetSocketAddress(parts[0], port);

      unmatchedBootstraps.add(addr);
      bootstraps.add(addr);
    }
  }

  protected LeafSet getLeafSet(NodeHandle nh) throws IOException {
    // TODO: implement, need to make this an application and have a message to fetch this info

    return null;
  }

  protected Ring buildRing(final SocketPastryNodeFactory factory, InetSocketAddress bootstrap) throws Exception {
    unmatchedBootstraps.remove(bootstrap);
    numThreads = 0;
    final Ring ring = new Ring(bootstrap);

    final HashSet<NodeHandle> unseen = new HashSet<NodeHandle>();


    unseen.add(factory.getNodeHandle(bootstrap, 20000));

    synchronized (unseen) {
      while (true) {
        if (numThreads >= MAX_THREADS)
          unseen.wait();

        if (unseen.size() > 0) {
          numThreads++;

          final SocketNodeHandle handle = (SocketNodeHandle) unseen.iterator().next();
          if (handle == null) {
            break;
          }
            unseen.remove(handle);
            ring.addNode(handle);

            Thread t = new Thread() {
              @Override
              public void run() {
                try {
                  LeafSet ls = getLeafSet(handle);

                  ps.println(handle.getInetSocketAddress().getAddress().getHostAddress()+":"+
                      handle.getInetSocketAddress().getPort());


                  NodeSet ns = ls.neighborSet(Integer.MAX_VALUE);

                  if (! ns.get(0).equals(handle)) {
                    ring.addFailure(handle, new Exception("Node is now "+ns.get(0)));
                  }

                  synchronized (unseen) {
                    for (int i=1; i<ns.size(); i++)
                      if (! ring.contains(ns.get(i)) && ! dead.contains(ns.get(i)))
                        unseen.add(ns.get(i));
                  }

                } catch (java.net.ConnectException e) {
                  ring.addFailure(handle,e);
                  dead.add(handle);
                } catch (java.net.SocketTimeoutException e) {
                  ring.addFailure(handle,e);
                  dead.add(handle);
                } catch (IOException e) {
                  // any other failure: the finally block still accounts for the handle
                } finally {
                  synchronized (unseen) {
                    numThreads--;
                    unseen.notifyAll();
                  }
                }
              }
            };

            t.start();
          } else if (numThreads > 0) {
            unseen.wait();
          } else {
            break;
          }
        }
      }

    System.out.println("buildRing() complete:"+ring);
    return ring;
  }


  class Ring implements Comparable<Ring> {
    /**
     * of InetSocketAddress
     */
    final HashSet<InetSocketAddress> myBootstraps;
    /**
     * of SocketNodeHandle
     */
    final HashSet<SocketNodeHandle> nodes;

    String name;

    public Ring(InetSocketAddress bootAddr) {
      name = bootAddr.toString();
      myBootstraps = new HashSet<InetSocketAddress>();
      myBootstraps.add(bootAddr);
      nodes = new HashSet<SocketNodeHandle>();
    }

    public boolean contains(NodeHandle handle) {
      return nodes.contains(handle);
    }

    public String getName() {
      return name;
    }

    public void addFailure(SocketNodeHandle handle, Exception e) {
    }

    public synchronized void addNode(SocketNodeHandle snh) {

      InetSocketAddress newAddr = snh.getInetSocketAddress();
      synchronized(unmatchedBootstraps) {
        if (unmatchedBootstraps.contains(newAddr)) {
          unmatchedBootstraps.remove(newAddr);
        }
      }

      if (bootstraps.contains(newAddr)) {
        myBootstraps.add(newAddr);
      }
      nodes.add(snh);
    }

    @Override
    public String toString() {
      String s = nodes.size()+":"+myBootstraps.size()+": boots:";
      synchronized(myBootstraps) {
        Iterator<InetSocketAddress> i = myBootstraps.iterator();
        s+=i.next();
        while(i.hasNext()) {
          s+=","+i.next();
        }
      }
      s+=" non-boots:";
      synchronized(nodes) {
        for (SocketNodeHandle nxt : nodes) {
          if (!bootstraps.contains(nxt.getInetSocketAddress()))
            s+=","+nxt;
        }
      }
      return s;
    }

    @Override
    public int compareTo(Ring arg0) {
      Ring that = (Ring)arg0;
      return this.size() - that.size();
    }

    public int size() {
      return nodes.size();
    }
  }

  public static void main(String[] args) throws Exception {
    new PartitionChecker(args[0]);
    System.exit(0);
  }
}
