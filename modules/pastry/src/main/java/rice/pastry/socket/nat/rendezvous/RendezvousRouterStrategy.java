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
package rice.pastry.socket.nat.rendezvous;

import java.util.Iterator;
import java.util.Map;

import org.mpisws.p2p.transport.identity.IdentityLayerCore;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.pastry.NodeHandle;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouterStrategy;

public class RendezvousRouterStrategy implements RouterStrategy {
  /** routingQuality() of a dead node. */
  private static final int QUALITY_DEAD = 10;
  /** routingQuality() of a node we can neither contact directly nor are connected to. */
  private static final int QUALITY_NOT_CONTACTABLE = 5;
  /** Worst routingQuality() still used as a next hop. */
  private static final int MAX_ROUTABLE_QUALITY = 3;

  PriorityTransportLayer<MultiInetSocketAddress> priority;
  Logger logger;

  public RendezvousRouterStrategy(PriorityTransportLayer<MultiInetSocketAddress> priority, Environment env) {
    this.priority = priority;
    this.logger = env.getLogManager().getLogger(RendezvousRouterStrategy.class, null);
  }

  @Override
  public NodeHandle pickNextHop(RouteMessage msg, Iterator<NodeHandle> i) {
    if (!i.hasNext()) return null;
    NodeHandle best = i.next();
    int bestRating = routingQuality(best);
    logger.trace("Routing {}(0) {}:{}", msg, best, bestRating);
    if (bestRating == 0) return best;

    int counter = 1;
    while (i.hasNext()) {
      NodeHandle next = i.next();
      int nextRating = routingQuality(next);
      if (logger.isTraceEnabled()) logger.trace("Routing "+msg+"(" + counter++ + ") "+next+":"+nextRating);

      // found a perfect node?
      if (nextRating == 0) return next;

      // found a better node?
      if (nextRating < bestRating) {
        best = next;
        bestRating = nextRating;
      }

    }

    if (bestRating > MAX_ROUTABLE_QUALITY) {
      logger.info("Can't find route for {}", msg);

      /*
         This is needed to prevent problems before the leafset is built, or in the case that the entire leafset is not connectable.
         Just fail if there is no chance.
       */
      return null; // fail if node is unreachable
    }

    logger.trace("Routing {}returning {}:{}", msg, best, bestRating);
    return best;
  }

  /**
   * Returns the quality of the nh for routing 0 is optimal
   *
   * 0 if connected and alive
   * Suspected is a 1 (if connected or directly connectable)
   * Alive and connected/directly contactable = 0
   * not directly connectable = 5 (unless connected)
   * 10 if faulty
   *
   */
  protected int routingQuality(NodeHandle nodeHandle) {
    RendezvousSocketNodeHandle rnh = (RendezvousSocketNodeHandle)nodeHandle;
    if (nodeHandle.getLiveness() >= NodeHandle.LIVENESS_DEAD) {
      return QUALITY_DEAD;
    }
    int connectionStatus = priority.connectionStatus(rnh.getAddress());
    int liveness = nodeHandle.getLiveness();
    boolean contactDirect = rnh.canContactDirect();

    if (contactDirect) {
      // this code biases connected nodes
      int result = 2;

      // a point for being not suspected
      if (liveness == NodeHandle.LIVENESS_ALIVE) result--;
      // a point for being connected
      if (connectionStatus == PriorityTransportLayer.STATUS_CONNECTED) {
        result--;
      } else {
        priority.openPrimaryConnection(rnh.getAddress(), getOptions(rnh)); // the only option is the node handle, so the identity layer can find it
      }
      return result;

    }

    // !contactDirect
    if (connectionStatus > PriorityTransportLayer.STATUS_CONNECTING) {
      // connect if we can
      priority.openPrimaryConnection(rnh.getAddress(), getOptions(rnh)); // the only option is the node handle, so the identity layer can find it
    }

    if (connectionStatus == PriorityTransportLayer.STATUS_CONNECTED) {
      if (liveness == NodeHandle.LIVENESS_ALIVE) return 0;
      return 1; // suspected
    }

    // !contactDirect, !connected, don't really care about suspected or not
    return QUALITY_NOT_CONTACTABLE;
  }

  protected Map<String, Object> getOptions(NodeHandle nodeHandle) {
    return OptionsFactory.addOption(null, IdentityLayerCore.NODE_HANDLE_FROM_INDEX, nodeHandle);
  }

}
