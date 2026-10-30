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
package rice.pastry;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Vector;

import rice.p2p.commonapi.NodeHandleSetType;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * Represents an ordered set of NodeHandles. *
 *
 * @version $Id: NodeSet.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Peter Druschel
 */

public class NodeSet implements NodeSetI {

  /**
   * of NodeHandle
   */
  private final Vector<NodeHandle> set;

  /**
   * Constructor.
   */
  @SuppressWarnings("JdkObsolete") // set is shared across threads; Vector's synchronization guards it
  public NodeSet() {
    set = new Vector<NodeHandle>();
  }

  /*
   * NodeSet methods
   */

  /**
   * Appends a member to the ordered set.
   *
   * @param handle the handle to put.
   * @return false if handle was already a member, true otherwise
   */

  public boolean put(NodeHandle handle) {
    if (set.contains(handle))
      return false;

    set.add(handle);
    return true;
  }

  /**
   * Gets the ith element in the set.
   *
   * @param i an index.
   * @return the handle, or null if the position is out of bounds
   */

  public NodeHandle get(int i) {
    NodeHandle h;

    try {
      h = (NodeHandle) set.elementAt(i);
    } catch (ArrayIndexOutOfBoundsException e) {
      return null;
    }

    return h;
  }

  /**
   * test membership using Id
   *
   * @param id the id to test
   * @return true of NodeHandle associated with id is a member, false otherwise
   */

  private boolean memberId(rice.p2p.commonapi.Id id) {
    return (getIndexId(id) >= 0);
  }

  /**
   * Removes a node id and its handle from the set.
   *
   * @param nid the node to remove.
   *
   * @return the node handle removed or null if nothing.
   */

  public NodeHandle remove(Id nid) {
    try {
      return (NodeHandle) set.remove(getIndex(nid));
    } catch (ArrayIndexOutOfBoundsException e) {
      return null;
    }
  }

  /**
   * Gets the number of elements.
   *
   * @return the size.
   */

  @Override
  public int size() {
    return set.size();
  }

  /**
   * Gets the index of the element with the given node id.
   *
   * @param nid the node id.
   * @return the index or -1 if no such element
   */

  public int getIndex(Id nid) {
    return getIndexId(nid);
  }

  private int getIndexId(rice.p2p.commonapi.Id nid) {
    NodeHandle h;

    for (int i = 0; i < set.size(); i++) {
      try {
        h = (NodeHandle) set.elementAt(i);
        if (h.getNodeId().equals(nid))
          return i;
      } catch (Exception ignored) {
        // an entry that cannot be compared is not a match
      }
    }

    return -1;
  }

  /**
   * test membership
   *
   * @param handle the handle to test
   * @return true of handle is a member, false otherwise
   */
  public boolean member(NodeHandle handle) {
    return set.contains(handle);
  }

  /**
   * Returns a string representation of the NodeSet
   */

  @Override
  public String toString() {
    String s = "NodeSet: ";
    for (int i = 0; i < size(); i++)
      s = s + get(i).getNodeId();

    return s;
  }

  // Common API Support

  /**
   * Puts a NodeHandle into the set.
   *
   * @param handle the handle to put.
   *
   * @return true if the put succeeded, false otherwise.
   */
  @Override
  public boolean putHandle(rice.p2p.commonapi.NodeHandle handle) {
    return put((NodeHandle) handle);
  }

  /**
   * Finds the NodeHandle associated with the NodeId.
   *
   * @param id a node id.
   * @return the handle associated with that id or null if no such handle is
   *         found.
   */
  @Override
  public rice.p2p.commonapi.NodeHandle getHandle(rice.p2p.commonapi.Id id) {
    synchronized(set) {
      for (NodeHandle nh : set) {
        if (nh.getId().equals(id)) return nh;
      }
    }
    return null;
  }

  /**
   * Gets the ith element in the set.
   *
   * @param i an index.
   * @return the handle associated with that id or null if no such handle is
   *         found.
   */
  @Override
  public rice.p2p.commonapi.NodeHandle getHandle(int i) {
    return get(i);
  }

  /**
   * Verifies if the set contains this particular id.
   *
   * @param id a node id.
   * @return true if that node id is in the set, false otherwise.
   */
  @Override
  public boolean memberHandle(rice.p2p.commonapi.Id id) {
    return memberId(id);
  }

  /**
   * Removes a node id and its handle from the set.
   *
   * @param id the id of the node to remove.
   *
   * @return the node handle removed or null if nothing.
   */
  @Override
  public rice.p2p.commonapi.NodeHandle removeHandle(rice.p2p.commonapi.Id id) {
    return remove((rice.pastry.Id) id);
  }

  /**
   * Gets the index of the element with the given node id.
   *
   * @param id the id.
   *
   * @return the index or throws a NoSuchElementException.
   */
  @Override
  public int getIndexHandle(rice.p2p.commonapi.Id id)
      throws NoSuchElementException {
    return getIndex((Id)id);
  }

  @SuppressWarnings("JdkObsolete") // set is shared across threads; Vector's synchronization guards it
  public NodeSet(InputBuffer buf, NodeHandleFactory<? extends NodeHandle> nhf) throws IOException {
    short numNodes = buf.readShort();
    set = new Vector<NodeHandle>(numNodes);

    for (int i = 0; i < numNodes; i++) {
      set.add(nhf.readNodeHandle(buf));
    }
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeShort((short)set.size());
    for (NodeHandle nh : set) {
      nh.serialize(buf);
    }
  }

  @Override
  public short getType() {
    return NodeHandleSetType.NODE_SET.id();
  }
}
