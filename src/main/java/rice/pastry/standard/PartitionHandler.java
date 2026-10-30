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
package rice.pastry.standard;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.pastry.NodeHandle;
import rice.pastry.NodeSetEventSource;
import rice.pastry.NodeSetListener;
import rice.pastry.PastryNode;
import rice.pastry.join.JoinRequest;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouteSet;
import rice.pastry.routing.RoutingTable;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.selector.TimerTask;

/**
 * The PartitionHandler does two things:  it collects a list of candidate nodes
 * that may be in another partition, and it periodically tries to reach these
 * nodes to heal a partition.  I can refactor the code to expose a method that
 * heals a partition tomorrow for you, but I'll paste the code below for the
 * moment.
 *
 * The basic theory of healing a partition is to send a join request via a node
 * that may not be in your partition.  Join requests get routed to your own key.
 * If the request comes back to you, then that node is in your partition and
 * nothing needs to be done.  If the request ends up at another node, it will
 * discover your existence and begin integrating the two rings, just as if you
 * were creating your local node for the first time.
 *
 * The way we look for candidate nodes is we choose a mix of the original
 * bootstraps (as a last resort fallback) and nodes that we once believed were
 * alive but are no longer in contact with.  The idea is we may have declared
 * those nodes dead due to losing connectivity to them but at some later point
 * we again may be able to contact them.
 *
 * The reason that we make the PartitionHandler a periodic process is that in
 * general you can't tell that a partition has happened.
 *
 * @author jstewart
 *
 */
public class PartitionHandler extends TimerTask implements NodeSetListener {

  private final PastryNode pastryNode;

  // maybe move this to a subclass
  private final InetSocketAddress[] bootstraps;

  private final SocketPastryNodeFactory factory;

  private final Logger logger;

  private final PartitionHandlerConfig config;
  private final double bootstrapRate;
  private final int maxGoneSize;
  private final int maxGoneAge;
  // map from Id's -> NodeHandle's to keep things unique per NodeId
  private final Map<rice.p2p.commonapi.Id,GoneSetEntry> gone;
  // guards gone; private, so that code holding a reference to this listener can't contend for it
  private final Object goneLock = new Object();

  private final Environment env;

  /**
   * Constructs a PartitionHandler.  This will register the handler with the
   * pastry node to begin collecting candidates nodes that may be in a
   * partition, but it will not initiate probing.  You must call the start()
   * method to do that.
   *
   * @param pn the local pastry node
   * @param factory the SocketPastryNodeFactory
   * @param bootstraps
   */
  public PartitionHandler(PastryNode pn, SocketPastryNodeFactory factory, InetSocketAddress[] bootstraps) {
    pastryNode = pn;
    this.factory = factory;
    this.bootstraps = bootstraps;
    env = pastryNode.getEnvironment();
    gone = new HashMap<rice.p2p.commonapi.Id, GoneSetEntry>();
    this.logger = pn.getEnvironment().getLogManager().getLogger(PartitionHandler.class,"");

    config = PartitionHandlerConfig.fromParameters(env.getParameters());
    maxGoneSize = config.maxHistorySize();
    maxGoneAge = config.maxHistoryAge();
    bootstrapRate = config.bootstrapCheckRate();

    pastryNode.getLeafSet().addNodeSetListener(this);
    pastryNode.getRoutingTable().addNodeSetListener(this);
  }

  private void doGoneMaintainence() {
    synchronized (goneLock) {
      Iterator<GoneSetEntry> it = gone.values().iterator();
      long now = env.getTimeSource().currentTimeMillis();

      logger.debug("Doing maintenance in PartitionHandler {}", now);

      logger.trace("gone size 1 is {} of {}", gone.size(), maxGoneSize);

      while (it.hasNext()) {
        GoneSetEntry g = (GoneSetEntry)it.next();
        if (now - g.timestamp > maxGoneAge) { // toss if too old
          logger.atTrace().log(() -> "Removing "+g+" from gone due to expiry");
          it.remove();
        } else if (g.nh.getLiveness() > NodeHandle.LIVENESS_DEAD) { // toss if epoch changed
          logger.atTrace().log(() -> "Removing "+g+" from gone due to death");
          it.remove();
        }
      }

      logger.trace("gone size 2 is {} of {}", gone.size(), maxGoneSize);

      while (gone.size() > maxGoneSize) {
        Object key = gone.keySet().iterator().next();
        gone.remove(key);
      }
      logger.trace("gone size 3 is {} of {}", gone.size(), maxGoneSize);
    }
  }

  private List<NodeHandle> getRoutingTableAsList() {
    RoutingTable rt = pastryNode.getRoutingTable();
    List<NodeHandle> rtHandles = new ArrayList<NodeHandle>(rt.numEntries());

    for (int r = 0; r < rt.numRows(); r++) {
      RouteSet[] row = rt.getRow(r);
      for (int c = 0; c < rt.numColumns(); c++) {
        RouteSet entry = row[c];
        if (entry != null) {
          for (int i = 0; i < entry.size(); i++) {
            NodeHandle nh = entry.get(i);
            if (!nh.equals(pastryNode.getLocalHandle())) {
              rtHandles.add(nh);
            }
          }
        }
      }
    }

    return rtHandles;
  }

  /**
   * This method randomly returns a node that was once in the LeafSet or in
   * the routing table but has since been removed.  The idea is that the node
   * may have been removed because it suffered a network outage that left it
   * in a partition of the ring.
   *
   * This method may also return a node from the current routing table if it
   * doesn't know of sufficient nodes that have left to pick a good one.
   *
   * @return a NodeHandle that may be in another partition, or null if no nodes
   * have left the routing table or LeafSet, and the routing table is empty.
   */
  public NodeHandle getCandidateNode() {
    synchronized (goneLock) {
      int which = env.getRandomSource().nextInt(maxGoneSize);
      logger.trace("getGone choosing node {} from gone or routing table", which);

      Iterator<GoneSetEntry> it = gone.values().iterator();
      while (which>0 && it.hasNext()) {
        which--;
        it.next();
      }

      if (it.hasNext()) {
        // assert which==0;
        logger.trace("getGone chose node from gone {}", which);
        return ((GoneSetEntry)it.next()).nh;
      }
    }

    List<NodeHandle> rtHandles = getRoutingTableAsList();

    if (rtHandles.isEmpty()) {
      logger.info("getGone returning null; routing table is empty!");
      return null;
    }

    int which = env.getRandomSource().nextInt(rtHandles.size());

    logger.trace("getGone choosing node {} from routing table", which);

    return rtHandles.get(which);

  }

  // possibly make this abstract
  private void getNodeHandleToProbe(Continuation<NodeHandle, Exception> c) {
    if (env.getRandomSource().nextDouble() > bootstrapRate) {
      NodeHandle nh = getCandidateNode();
      logger.trace("getGone chose {}", nh);
      if (nh != null) {
        c.receiveResult(nh);
        return;
      }
    }
    logger.trace("getNodeHandleToProbe choosing bootstrap");

    factory.getNodeHandle(bootstraps, c);
  }

  public void run() {
    logger.info("running partition handler");
    doGoneMaintainence();

    getNodeHandleToProbe(new Continuation<NodeHandle, Exception>() {

      public void receiveResult(NodeHandle result) {
        if (result != null) {
          rejoin((NodeHandle)result);
        } else {
          logger.info("getNodeHandleToProbe returned null");
        }
      }

      public void receiveException(Exception result) {
        // oh well
        logger.info("exception in PartitionHandler", result);
      }

    });
  }

  public void nodeSetUpdate(NodeSetEventSource nodeSetEventSource, NodeHandle handle, boolean added) {
    if (nodeSetEventSource.equals(pastryNode.getLeafSet())) {
      if (added) {
        synchronized (goneLock) {
          gone.remove(handle.getId());
        }
      }
    }
    if (!added) {
      synchronized (goneLock) {
        if (handle.getLiveness() == NodeHandle.LIVENESS_DEAD) {
          if (gone.containsKey(handle.getId())) {
            logger.trace("PartitionHandler updating node {}", handle);
            ((GoneSetEntry)gone.get(handle.getId())).nh = handle;
          } else {
            GoneSetEntry g = new GoneSetEntry(handle, env.getTimeSource().currentTimeMillis());
            logger.atTrace().log(() -> "PartitionHandler adding node "+g);
            gone.put(handle.getId(),g);
          }
        }
      }
    }
  }

  static private class GoneSetEntry {
    private NodeHandle nh;
    private final long timestamp;

    public GoneSetEntry(NodeHandle nh, long timestamp) {
      this.nh = nh;
      this.timestamp = timestamp;
    }

    public boolean equals(Object o) {
      if (!(o instanceof GoneSetEntry)) return false;
      GoneSetEntry other = (GoneSetEntry)o;
      return other.nh.getId().equals(nh.getId());
    }

    public int hashCode() {
      return nh.getId().hashCode();
    }

    public String toString() {
      return nh.toString() + " "+timestamp;
    }
  }

  /**
   * Manually kicks off a probe to a given target node.  This can be used to
   * manually heal a partition if you know via some external mechanism that one
   * occurred.
   *
   * @param target the node to rejoin through
   */
  public void rejoin(NodeHandle target) {
    JoinRequest jr = new JoinRequest(pastryNode.getLocalHandle(), pastryNode
        .getRoutingTable().baseBitLength());

    RouteMessage rm = new RouteMessage(pastryNode.getLocalHandle().getNodeId(),
        jr, null, null,
        pastryNode.getConfig().routeMsgVersion());

    rm.setPrevNode(pastryNode.getLocalHandle());
    rm.getOptions().setRerouteIfSuspected(false);
    NodeHandle nh = pastryNode.coalesce(target);
    pastryNode.send(nh, rm, null, null);
  }

}
