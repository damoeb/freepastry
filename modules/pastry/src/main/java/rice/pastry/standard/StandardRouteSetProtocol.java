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

import java.io.IOException;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.client.PastryAppl;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.PMessageDeserializer;
import rice.pastry.routing.BroadcastRouteRow;
import rice.pastry.routing.InitiateRouteSetMaintenance;
import rice.pastry.routing.RequestRouteRow;
import rice.pastry.routing.RouteProtocolAddress;
import rice.pastry.routing.RouteSet;
import rice.pastry.routing.RouteSetMessageType;
import rice.pastry.routing.RouteSetProtocol;
import rice.pastry.routing.RouteSetProtocolMessage;
import rice.pastry.routing.RoutingTable;

/**
 * An implementation of a simple route set protocol.
 *
 * @version $Id: StandardRouteSetProtocol.java,v 1.15 2005/03/11 00:58:02 jeffh
 *          Exp $
 *
 * @author Andrew Ladd
 * @author Peter Druschel
 */

public class StandardRouteSetProtocol extends PastryAppl implements RouteSetProtocol {
  private final int maxTrials;

  private RoutingTable routeTable;

  private Environment environmet;

  private final Logger logger;

  static class SRSPDeserializer extends PMessageDeserializer {
    public SRSPDeserializer(PastryNode pn) {
      super(pn);
    }

    @Override
    public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
      return switch (RouteSetMessageType.find(type)) {
        case REQUEST_ROUTE_ROW -> new RequestRouteRow(sender,buf);
        case BROADCAST_ROUTE_ROW -> new BroadcastRouteRow(buf,pn,pn);
        case null -> null;
      };
    }
  }

  /**
   * Constructor.
   *
   * @param ln the local node
   * @param rt the routing table
   */
  public StandardRouteSetProtocol(PastryNode ln, RoutingTable rt) {
    this(ln, rt, null);
  }

  public StandardRouteSetProtocol(PastryNode ln, RoutingTable rt, MessageDeserializer md) {
    super(ln, null, RouteProtocolAddress.getCode(),  md == null ? new SRSPDeserializer(ln) : md);
    this.environmet = ln.getEnvironment();
    maxTrials = (1 << rt.baseBitLength()) / 2;
    routeTable = rt;
    logger = environmet.getLogManager().getLogger(getClass(), null);
  }

  /**
   * Receives a message.
   *
   * @param msg the message.
   */

  @Override
  public void messageForAppl(Message msg) {
    if (!(msg instanceof RouteSetProtocolMessage routeSetMessage)) {
      throw new Error(
          "StandardRouteSetProtocol: received message is of unknown type");
    }
    switch (routeSetMessage) {
      case BroadcastRouteRow brr -> {
        if (logger.isTraceEnabled()) logger.trace("Received "+brr.toStringFull());

        RouteSet[] row = brr.getRow();

        NodeHandle nh = brr.from();
        if (nh.getLiveness() < NodeHandle.LIVENESS_DEAD)
          routeTable.put(nh);

        for (RouteSet routeSet : row) {
          RouteSet rs = routeSet;

          for (int j = 0; rs != null && j < rs.size(); j++) {
            nh = rs.get(j);
            if (nh.getLiveness() >= NodeHandle.LIVENESS_DEAD)
              continue;
            routeTable.put(nh);
          }
        }
      }

      // a remote node request one of our routeTable rows
      case RequestRouteRow rrr -> {

        int reqRow = rrr.getRow();
        NodeHandle nh = rrr.returnHandle();

        RouteSet row[] = routeTable.getRow(reqRow);
        BroadcastRouteRow brr = new BroadcastRouteRow(thePastryNode.getLocalHandle(), row);
        if (logger.isTraceEnabled()) logger.trace("Responding to "+rrr+" with "+brr.toStringFull());
        thePastryNode.send(nh,brr,null,options);
      }

      // request for routing table maintenance
      case InitiateRouteSetMaintenance irsm -> {
        // perform routing table maintenance
        maintainRouteSet();
      }
    }

  }

  /**
   * performs periodic maintenance of the routing table for each populated row
   * of the routing table, it picks a random column and swaps routing table rows
   * with the closest entry in that column
   */

  private void maintainRouteSet() {

    logger.info("maintainRouteSet {}", thePastryNode.getLocalHandle().getNodeId());

    // for each populated row in our routing table
    for (short i = (short)(routeTable.numRows() - 1); i >= 0; i--) {
      RouteSet row[] = routeTable.getRow(i);
      BroadcastRouteRow brr = new BroadcastRouteRow(thePastryNode.getLocalHandle(), row);
      RequestRouteRow rrr = new RequestRouteRow(thePastryNode.getLocalHandle(), i);
      int myCol = thePastryNode.getLocalHandle().getNodeId().getDigit(i,
          routeTable.baseBitLength());
      int j;

      // try up to maxTrials times to find a column with live entries
      for (j = 0; j < maxTrials; j++) {
        // pick a random column
        int col = environmet.getRandomSource().nextInt(routeTable.numColumns());
        if (col == myCol)
          continue;

        RouteSet rs = row[col];

        // swap row with closest node only

        if (rs != null && rs.size() > 0) {
          NodeHandle nh;

          nh = rs.closestNode(10); // any liveness status will work

          // this logic is to make this work correctly in the simulator
          // if we find him not alive, then we would have routed to him and the liveness would have failed
          // thus the correct behavior is to break, not continue.  In other words, we waste this cycle
          // finding the node faulty and removing it rather than doing an actual swap
          // - Jeff Hoye,  Aug 9, 2006
          if (nh.getLiveness() >= NodeHandle.LIVENESS_DEAD) {
            logger.debug("found dead node in table:{}", nh);
            rs.remove(nh);
            break;
          }

          if (logger.isDebugEnabled()) logger.debug("swapping with "+(i+1)+"/"+routeTable.numRows()+" "+(j+1)+"/"+maxTrials+":"+nh);
          thePastryNode.send(nh,brr,null,options);
          thePastryNode.send(nh,rrr,null,options);
          break;
        }
      }

      // once we hit a row where we can't find a populated entry after numTrial
      // trials, we finish
      if (j == maxTrials)
        break;

    }

  }

  @Override
  public boolean deliverWhenNotReady() {
    return true;
  }

}
