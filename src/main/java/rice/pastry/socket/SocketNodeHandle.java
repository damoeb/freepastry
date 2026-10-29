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
package rice.pastry.socket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.dist.DistNodeHandle;
import rice.pastry.messaging.Message;

public class SocketNodeHandle extends DistNodeHandle<MultiInetSocketAddress> {

  public MultiInetSocketAddress eaddress;
  protected long epoch;
  
  protected SocketNodeHandle(MultiInetSocketAddress eisa, long epoch, Id id, PastryNode node) {
    super(id);
    this.eaddress = eisa;
    this.epoch = epoch;    
    setLocalNode(node);
  }


  public long getEpoch() {
    return epoch;
  }

  public void setLocalNode(PastryNode pn) {
    localnode = pn;
    this.logger = localnode.getEnvironment().getLogManager().getLogger(getClass(),null);
  }  

  public MultiInetSocketAddress getIdentifier() {
    return eaddress;
  }

  /**
   * Returns the last known liveness information about the Pastry node
   * associated with this handle. Invoking this method does not cause network
   * activity.
   *
   * @return true if the node is alive, false otherwise.
   */
  public int getLiveness() {
    if (isLocal()) {
      return LIVENESS_ALIVE;
    } else {
      return ((PastryNode)localnode).getLivenessProvider().getLiveness(this, null);
    }
  }
  
  /**
   * Returns the InetSocketAddress that should be used to contact the node
   * 
   * @return the address of the node
   */
  
  public MultiInetSocketAddress getAddress() {
    return eaddress;
  }
    
  public InetSocketAddress getInetSocketAddress() {
    return eaddress.getAddress(0);
  }
    
  /**
   * Method which FORCES a check of liveness of the remote node.  Note that
   * this method should ONLY be called by internal Pastry maintenance algorithms - 
   * this is NOT to be used by applications.  Doing so will likely cause a
   * blowup of liveness traffic.
   *
   * @return true if node is currently alive.
   */
  public boolean checkLiveness() {
    if (logger.level <= Logger.FINE) logger.log(this+".checkLiveness()");
    ((PastryNode)localnode).getLivenessProvider().checkLiveness(this, null);
    return getLiveness() < LIVENESS_DEAD;
  }

  /**
   * Method which returns whether or not this node handle is on its
   * home node.
   *
   * @return Whether or not this handle is local
   */
  public boolean isLocal() {
    assertLocalNode();
    return getLocalNode().getLocalHandle().equals(this);
  }
  
  /**
   * Called to send a message to the node corresponding to this handle.
   *
   * @deprecated use PastryNode.send(msg, nh)
   * @param msg Message to be delivered, may or may not be routeMessage.
   */
  @Deprecated
  public void receiveMessage(Message msg) {
    assertLocalNode();
    Map<String, Object> options = new HashMap<String, Object>(1);
    options.put(PriorityTransportLayer.OPTION_PRIORITY, msg.getPriority());
    getLocalNode().send(this, msg,null, options);
  }
  
  /**
   * Returns a String representation of this DistNodeHandle. This method is
   * designed to be called by clients using the node handle, and is provided in
   * order to ensure that the right node handle is being talked to.
   *
   * @return A String representation of the node handle.
   */
  public String toString() {
    return "[SNH: " + nodeId + "/" + eaddress + "]";
  }

  public String toStringFull() {
    return "[SNH: " + nodeId + "/" + eaddress + " " + epoch+"]";
  }

  /**
   * Equivalence relation for nodehandles. They are equal if and only if their
   * corresponding NodeIds are equal.
   *
   * @param obj the other nodehandle .
   * @return true if they are equal, false otherwise.
   */
  public boolean equals(Object obj) {
    if (! (obj instanceof SocketNodeHandle)) 
      return false;

    SocketNodeHandle other = (SocketNodeHandle) obj;
    
    
    boolean ret = (epoch == other.epoch && other.getNodeId().equals(getNodeId()) && other.eaddress.equals(eaddress));

    return ret;
  }

  /**
   * Hash codes for node handles. It is the hashcode of their corresponding
   * NodeId's.
   *
   * @return a hash code.
   */
  public int hashCode() {
    int hash = ((int)epoch) ^ getNodeId().hashCode() ^ eaddress.hashCode();
    return hash;
  }

  /**
   * Returns the last known proximity information about the Pastry node
   * associated with this handle. Invoking this method does not cause network
   * activity. Smaller values imply greater proximity. The exact nature and
   * interpretation of the proximity metric implementation-specific.
   *
   * @deprecated use PastryNode.proximity(nh)
   * @return the proximity metric value
   */
  @Deprecated
  public int proximity() {
    return ((PastryNode)localnode).getProxProvider().proximity(this, null);
  }

  /**
   * Ping the node. Refreshes the cached liveness status and proximity value of
   * the Pastry node associated with this. Invoking this method causes network
   * activity.
   *
   * @return true if node is currently alive.
   */
  public boolean ping() {
    if (localnode.getLocalHandle().equals(this)) return false;
    ((PastryNode)localnode).getLivenessProvider().checkLiveness(this, null);
    return getLiveness() < LIVENESS_DEAD;
  }  

  /**
   * @param o ignored
   * @param obj ignored
   * @deprecated does nothing; SocketNodeHandle is no longer an Observer
   */
  @Deprecated
  public void update(Observable o, Object obj) {
  }


/*********************** Serialization ***********************/  
  
  /**
   * Note, this SNH needs to be coalesced!!!
   * @param buf
   * @return
   */
  static SocketNodeHandle build(InputBuffer buf, PastryNode local) throws IOException {
//    NodeHandle (Version 0)
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    + Epoch InetSocketAddress                                       +
//    +                                                               +           
//    +                                                               +           
//    +                                                               +           
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    +   Id                                                          +
//    +                                                               +
//    +                                                               +
//    +                                                               +
//    +                                                               +
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    MultiInetSocketAddress eaddr = MultiInetSocketAddress.build(buf);
    long epoch = buf.readLong();
    Id nid = Id.build(buf);
    return new SocketNodeHandle(eaddr, epoch, nid, local);
  }

  public void serialize(OutputBuffer buf) throws IOException {
    eaddress.serialize(buf);
    buf.writeLong(epoch);
    nodeId.serialize(buf);
  }
}
