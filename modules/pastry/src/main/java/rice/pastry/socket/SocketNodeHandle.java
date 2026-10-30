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
import java.net.InetSocketAddress;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.PastryNode;
import rice.pastry.dist.DistNodeHandle;

public class SocketNodeHandle extends DistNodeHandle<MultiInetSocketAddress> {

  private final MultiInetSocketAddress eaddress;
  private final long epoch;

  protected SocketNodeHandle(MultiInetSocketAddress eisa, long epoch, Id id, PastryNode node) {
    super(id);
    this.eaddress = eisa;
    this.epoch = epoch;
    setLocalNode(node);
  }


  @Override
  public long getEpoch() {
    return epoch;
  }

  public void setLocalNode(PastryNode pn) {
    localnode = pn;
    this.logger = localnode.getEnvironment().getLogManager().getLogger(getClass(),null);
  }

  /**
   * Returns the last known liveness information about the Pastry node
   * associated with this handle. Invoking this method does not cause network
   * activity.
   *
   * @return true if the node is alive, false otherwise.
   */
  @Override
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

  @Override
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
  @Override
  public boolean checkLiveness() {
    logger.debug("{}.checkLiveness()", this);
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
   * Returns a String representation of this DistNodeHandle. This method is
   * designed to be called by clients using the node handle, and is provided in
   * order to ensure that the right node handle is being talked to.
   *
   * @return A String representation of the node handle.
   */
  @Override
  public String toString() {
    return "[SNH: " + getNodeId() + "/" + eaddress + "]";
  }

  public String toStringFull() {
    return "[SNH: " + getNodeId() + "/" + eaddress + " " + epoch+"]";
  }

  /**
   * Equivalence relation for nodehandles. They are equal if and only if their
   * corresponding NodeIds are equal.
   *
   * @param obj the other nodehandle .
   * @return true if they are equal, false otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (! (obj instanceof SocketNodeHandle other))
      return false;



    boolean ret = (epoch == other.epoch && other.getNodeId().equals(getNodeId()) && other.eaddress.equals(eaddress));

    return ret;
  }

  /**
   * Hash codes for node handles. It is the hashcode of their corresponding
   * NodeId's.
   *
   * @return a hash code.
   */
  @Override
  public int hashCode() {
    int hash = ((int)epoch) ^ getNodeId().hashCode() ^ eaddress.hashCode();
    return hash;
  }

  /**
   * Ping the node. Refreshes the cached liveness status and proximity value of
   * the Pastry node associated with this. Invoking this method causes network
   * activity.
   *
   * @return true if node is currently alive.
   */
  @Override
  public boolean ping() {
    if (localnode.getLocalHandle().equals(this)) return false;
    ((PastryNode)localnode).getLivenessProvider().checkLiveness(this, null);
    return getLiveness() < LIVENESS_DEAD;
  }


/*********************** Serialization ***********************/

  /**
   * Note, this SNH needs to be coalesced!!!
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

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    eaddress.serialize(buf);
    buf.writeLong(epoch);
    getNodeId().serialize(buf);
  }
}
