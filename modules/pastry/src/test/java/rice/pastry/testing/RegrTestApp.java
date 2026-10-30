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

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.client.CommonAPIAppl;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.PRawMessage;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.SendOptions;

/**
 * RegrTestApp
 *
 * A regression test suite for pastry. This is the per-node app object.
 *
 * @version $Id: RegrTestApp.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author andrew ladd
 * @author peter druschel
 */

public class RegrTestApp extends CommonAPIAppl {

  private static class RTAddress {
    private static int myCode = 0x9219d6ff;

    static int getCode() {
      return myCode;
    }
  }

  private static int addr = RTAddress.getCode();

  private PastryRegrTest prg;

  public RegrTestApp(PastryNode pn, PastryRegrTest prg) {
    super(pn);
    this.prg = prg;
    // the only message this application sends is a raw RTMessage
    setDeserializer((buf, type, priority, sender) -> {
      if (type == RTMessage.TYPE) {
        return new RTMessage(addr, buf, thePastryNode);
      }
      throw new IOException("Unknown message type: " + type);
    });

  }

  @Override
  public int getAddress() {
    return addr;
  }

  public void sendMsg(Id nid) {
    routeMsg(nid, new RTMessage(addr, getNodeHandle(), nid),
        new SendOptions());
  }

  public void sendTrace(Id nid) {
    routeMsg(nid, new RTMessage(addr, getNodeHandle(), nid),
        new SendOptions());
  }

  /**
   * Makes sure the message was delivered to the correct node by cross-referencing the
   * sorted nodes list in the simulator.
   */
  @Override
  public void deliver(Id key, Message msg) {

    // check if numerically closest
    Id localId = getNodeId();

    if (localId != key) {
      int inBetween;
      if (localId.compareTo(key) < 0) {
        int i1 = prg.pastryNodesSortedReady.subMap(localId, key).size();
        int i2 = prg.pastryNodesSortedReady.tailMap(key).size()
            + prg.pastryNodesSortedReady.headMap(localId).size();

        inBetween = (i1 < i2) ? i1 : i2;
      } else {
        int i1 = prg.pastryNodesSortedReady.subMap(key, localId).size();
        int i2 = prg.pastryNodesSortedReady.tailMap(localId).size()
            + prg.pastryNodesSortedReady.headMap(key).size();

        inBetween = (i1 < i2) ? i1 : i2;
      }

      if (inBetween > 1) {
        System.out.println("messageForAppl failure, inBetween=" + inBetween);
        System.out.print(msg);
        System.out.println(" received at " + getNodeId());
        System.out.println(getLeafSet());
      }
    }
  }

  @Override
  public void forward(RouteMessage rm) {
    Message msg;
    try {
      msg = rm.unwrap(deserializer);
    } catch (IOException ioe) {
      throw new RuntimeException("Error deserializing message "+rm,ioe);
    }
    Id key = rm.getTarget();
    Id nextHop = rm.getNextHop().getNodeId();

    Id localId = getNodeId();
    Id.Distance dist = localId.distance(key);
    int base = getRoutingTable().baseBitLength();

    if (prg.lastMsg == msg) {
      int localIndex = localId.indexOfMSDD(key, base);
      int lastIndex = prg.lastNode.indexOfMSDD(key, base);

      if ((localIndex > lastIndex && nextHop != localId)
          || (localIndex == lastIndex && dist.compareTo(prg.lastDist) > 0))
        System.out.println("at... " + getNodeId()
            + " enrouteMessage failure with " + msg + " lastNode="
            + prg.lastNode + " lastDist=" + prg.lastDist + " dist=" + dist
            + " nextHop=" + nextHop + " loci=" + localIndex + " lasti="
            + lastIndex);

      prg.lastDist = dist;
    }
    prg.lastMsg = msg;
    prg.lastDist = dist;
    prg.lastNode = localId;

  }

  @Override
  public void update(NodeHandle nh, boolean wasAdded) {
    final Id nid = nh.getNodeId();

    if (!prg.pastryNodesSorted.containsKey(nid) && nh.getLiveness() < NodeHandle.LIVENESS_DEAD)
      System.out.println("at... " + getNodeId()
          + "leafSetChange failure 1 with " + nid);

    Id localId = thePastryNode.getNodeId();

    if (localId == nid)
      System.out.println("at... " + getNodeId()
          + "leafSetChange failure 2 with " + nid);

    int inBetween;

    if (localId.compareTo(nid) < 0) { // localId < nid?
      int i1 = prg.pastryNodesSorted.subMap(localId, nid).size();
      int i2 = prg.pastryNodesSorted.tailMap(nid).size()
          + prg.pastryNodesSorted.headMap(localId).size();

      inBetween = (i1 < i2) ? i1 : i2;
    } else {
      int i1 = prg.pastryNodesSorted.subMap(nid, localId).size();
      int i2 = prg.pastryNodesSorted.tailMap(localId).size()
          + prg.pastryNodesSorted.headMap(nid).size();

      inBetween = (i1 < i2) ? i1 : i2;
    }

    int lsSize = getLeafSet().maxSize() / 2;

    if ((inBetween > lsSize && wasAdded
        && !prg.pastryNodesLastAdded.contains(thePastryNode) && !prg.inConcJoin)
        || (inBetween <= lsSize && !wasAdded && !getLeafSet().member(nh))
        && prg.pastryNodesSorted.containsKey(nh.getNodeId())) {

      // "leafSetChange failure 3" is not reported: the ConsistentJoinProtocol can violate it.
      // Perhaps we need a more intricate mechanism to merge the leafsets.
    }
  }

  @Override
  public void routeSetChange(NodeHandle nh, boolean wasAdded) {
    Id nid = nh.getNodeId();

    if (!prg.pastryNodesSorted.containsKey(nid)) {
      if (nh.getLiveness() < NodeHandle.LIVENESS_DEAD || wasAdded)
        System.out.println("at... " + getNodeId()
            + "routeSetChange failure 1 with " + nid + " wasAdded=" + wasAdded);
    }

  }

  /**
   * Invoked when the Pastry node has joined the overlay network and is ready to
   * send and receive messages
   */

  @Override
  public void notifyReady() {
  }

  public PastryNode getPastryNode() {
    return thePastryNode;
  }

}

/**
 * DO NOT declare this inside PingClient; see HelloWorldApp for details.
 */

class RTMessage extends PRawMessage {
  static final short TYPE = 1;

  public NodeHandle sourceNode;

  public Id target;

  public RTMessage(int addr, NodeHandle src, Id tgt) {
    super(addr);
    sourceNode = src;
    target = tgt;
  }

  RTMessage(int addr, InputBuffer buf, PastryNode node) throws IOException {
    super(addr);
    sourceNode = node.readNodeHandle(buf);
    target = Id.build(buf);
  }

  @Override
  public short getType() {
    return TYPE;
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    sourceNode.serialize(buf);
    target.serialize(buf);
  }

  @Override
  public String toString() {
    String s = "";
    s += "RTMsg from " + sourceNode + " to " + target;
    return s;
  }
}
