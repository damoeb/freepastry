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
package rice.pastry.client;

import java.io.IOException;

import rice.pastry.Id;
import rice.pastry.IdRange;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.messaging.Message;
import rice.pastry.routing.RouteMessage;

/**
 * CommonAPIAppl is an abstract class that all new applications should extend.
 * It provides the common KBR API defined in
 *
 * "Towards a Common API for Structured Peer-to-Peer Overlays." Frank Dabek, Ben
 * Zhao, Peter Druschel, John Kubiatowicz and Ion Stoica. In Proceedings of the
 * 2nd International Workshop on Peer-to-peer Systems (IPTPS'03) , Berkeley, CA,
 * February 2003.
 *
 * @version $Id: CommonAPIAppl.java 3801 2007-07-18 15:04:40Z jeffh $
 *
 * @author Peter Druschel
 */

public abstract class CommonAPIAppl extends PastryAppl {

  /**
   * Constructor.
   *
   * @param pn the pastry node that the application attaches to.
   */
  public CommonAPIAppl(PastryNode pn) {
    super(pn);
  }

  // API methods to be invoked by applications

  /**
   * This method provides information about ranges of keys for which the node n
   * is currently a r-root. The operations returns null if the range could not
   * be determined. It is an error to query the range of a node not present in
   * the neighbor set as returned by the update upcall or the neighborSet call.
   *
   * Some implementations may have multiple, disjoint ranges of keys for which a
   * given node is responsible (Pastry has two). The parameter key allows the
   * caller to specify which range should be returned. If the node referenced by
   * n is the r-root for key, then the resulting range includes key. Otherwise,
   * the result is the nearest range clockwise from key for which n is
   * responsible.
   *
   * @param n nodeHandle of the node whose range is being queried
   * @param r the rank
   * @param key the key
   * @param cumulative if true, returns ranges for which n is an i-root for 0 &lt;i
   *          &lt;=r
   * @return the range of keys, or null if range could not be determined for the
   *         given node and rank
   */

  public IdRange range(NodeHandle n, int r, Id key, boolean cumulative) {

    if (cumulative)
      return getLeafSet().range(n, r);

    IdRange ccw = getLeafSet().range(n, r, false);
    IdRange cw = getLeafSet().range(n, r, true);

    if (cw == null || ccw.contains(key)
        || key.isBetween(cw.getCW(), ccw.getCCW()))
      return ccw;
    else
      return cw;

  }

  /*
   * upcall methods, to be overridden by the derived application object
   *
   * Applications process messages by executing code in upcall methods that are
   * invoked by the KBR routing system at nodes along a message's path and at
   * its root. To permit event-driven implementations, upcall handlers must not
   * block and should not perform long-running computations.
   */

  /**
   * Called by pastry when a message arrives for this application.
   *
   * @param msg the message that is arriving.
   */

  public abstract void deliver(Id key, Message msg);

  /**
   * Called by pastry when a message is enroute and is passing through this
   * node. If this method is not overridden, the default behaviour is to let the
   * message pass through.
   *
   * @param msg the route message that is passing through (its target and next hop are set).
   *
   */

  public void forward(RouteMessage msg) {
    return;
  }

  /**
   * Called by pastry when the neighbor set changes.
   *
   * @param nh the handle of the node that was added or removed.
   * @param joined true if the node was added, false if the node was removed.
   */

  public void update(NodeHandle nh, boolean joined) {
  }

  /**
   * Invoked when the Pastry node has joined the overlay network and is ready to
   * send and receive messages
   */

  public void notifyReady() {
  }

  /*
   * internal methods
   */

  // hide defunct methods from PastryAppl
  public final void messageForAppl(Message msg) {
  }

  /**
   * Called by pastry when the leaf set changes.
   *
   * @param nh the handle of the node that was added or removed.
   * @param wasAdded true if the node was added, false if the node was removed.
   */

  public final void leafSetChange(NodeHandle nh, boolean wasAdded) {
    update(nh, wasAdded);
  }

  /**
   * Called by pastry to deliver a message to this client. Not to be overridden.
   *
   * @param msg the message that is arriving.
   */

  public void receiveMessage(Message msg) {
    logger.trace("[{}] recv {}", thePastryNode, msg);

    if (msg instanceof RouteMessage) {
      RouteMessage rm = (RouteMessage) msg;

      // call application
      forward(rm);

      if (rm.getNextHop() != null) {
        NodeHandle nextHop = rm.getNextHop();

        // if the message is for the local node, deliver it here
        if (getNodeId().equals(nextHop.getNodeId())) {
          try {
            deliver(rm.getTarget(), rm.unwrap(deserializer));
          } catch (IOException ioe) {
            throw new RuntimeException("Error deserializing message "+rm,ioe);
          }
        } else {
          // route the message
          thePastryNode.getRouter().route(rm);
        }

      }
    } else {
      // if the message is not a RouteMessage, then it is for the local node and
      // was sent with a PastryAppl.routeMsgDirect(); we deliver it for backward
      // compatibility
      deliver(null, msg);
    }
  }

}
