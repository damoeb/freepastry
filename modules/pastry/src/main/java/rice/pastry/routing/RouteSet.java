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
package rice.pastry.routing;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.NodeHandleSetType;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeHandleSet;

/**
 * A set of nodes typically stored in the routing table. The set contains a
 * bounded number of the closest node handles. Since proximity value can change
 * unpredictably, we don't keep the set in sorted order.
 *
 * @version $Id: RouteSet.java 4060 2007-12-21 17:49:36Z jeffh $
 *
 * @author Andrew Ladd
 * @author Peter Druschel
 */

public class RouteSet implements PastryNodeHandleSet,
    NodeHandleListener, Iterable<NodeHandle> {
  private NodeHandle[] nodes;

  private int theSize;

  private int closest;

  int row;
  int col;
  PastryNode localNode;

  /**
   * Constructor.
   *
   * @param maxSize the maximum number of nodes that fit in this set.
   */

  public RouteSet(int maxSize, int row, int column, PastryNode local, NodeHandle initialVal) {
    this(maxSize, row, column, local);
    nodes[theSize++] = initialVal;
    closest = 0;
  }

  public RouteSet(int maxSize, int row, int column, PastryNode local) {
    this.localNode = local;
    nodes = new NodeHandle[maxSize];
    theSize = 0;
    closest = -1;
    this.row = row;
    this.col = column;
  }

  @Override
  public String toString() {

    String description = "RS";
    if (col >= 0) {
      description+="("+row+","+Id.tran[col]+"):";
    } else {
      description+=":";
    }
    for (NodeHandle node : nodes) {
      description+=node+",";
    }
    return description;
  }

  /**
   * Puts a node into the set. The insertion succeeds either if the set is below
   * is maximal size or if the handle is closer than the most distant member in
   * the set.
   *
   * @param handle the handle to put.
   *
   * @return true if the put succeeded, false otherwise.
   */
  public boolean put(NodeHandle handle) {
    if (handle.getLiveness() >= NodeHandle.LIVENESS_DEAD) return false;
    int worstIndex = -1;
    int worstProximity = Integer.MIN_VALUE;

    // scan entries
    for (int i = 0; i < theSize; i++) {

      // if handle is already in the set, abort
      if (nodes[i].equals(handle))
        return false;

      if (nodes[i].getLiveness() >= NodeHandle.LIVENESS_DEAD) {
        // node is dead, replace immediately
        notifyTable(nodes[i], false);

        // in case we observe this handle, stop doing so
        nodes[i].removeNodeHandleListener(this);

        // insert new handle
        nodes[i] = handle;

        notifyTable(handle, true);
        handle.addNodeHandleListener(this);
        return true;
      }

      // find entry with worst proximity
      int proximity = localNode.proximity(nodes[i]);
      if (proximity >= worstProximity) {
        worstProximity = proximity;
        worstIndex = i;
      }
    }

    if (theSize < nodes.length) {
      nodes[theSize++] = handle;

      notifyTable(handle, true);

      // ping handles while the set is not full
      handle.ping();
      handle.addNodeHandleListener(this);

      return true;
    } else {
      if (localNode.proximity(handle) == Integer.MAX_VALUE) {
        // wait until the proximity value is available

        handle.ping(); // TODO: only ping handles that came from the deserializer
        handle.addNodeHandleListener(this);

        return false;
      } else if (localNode.proximity(handle) < worstProximity) {
        // remove handle with worst proximity
        notifyTable(nodes[worstIndex], false);

        // in case we observe this handle, stop doing so
        nodes[worstIndex].removeNodeHandleListener(this);

        // insert new handle
        nodes[worstIndex] = handle;

        notifyTable(handle, true);
        handle.addNodeHandleListener(this);

        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Is called whenever the liveness or proximity of a
   * registered node handle is changed.
   *
   * @param handle The node handle
   * @param arg the event type (PROXIMITY_CHANGE, DECLARED_LIVE, DECLARED_DEAD)
   */
  @Override
  public void nodeHandleUpdate(rice.p2p.commonapi.NodeHandle handle, Object arg) {
    // if the proximity is initialized for the time, insert the handle
    if (NodeHandle.PROXIMITY_CHANGED.equals(arg)) {
      put((NodeHandle) handle);
    } else if (NodeHandle.DECLARED_DEAD.equals(arg)) {
      // changed to remove dead handles - AM
      remove((NodeHandle) handle);
    }
  }

  /**
   * Removes a node from a set.
   *
   * @param nodeId the node id to remove.
   *
   * @return the removed handle or null.
   */
  public NodeHandle remove(Id nodeId) {
    for (int i = 0; i < theSize; i++) {
      if (nodes[i].getNodeId().equals(nodeId)) {
        NodeHandle handle = nodes[i];

        nodes[i] = nodes[--theSize];

        notifyTable(handle, false);

        // in case we observe this handle, stop doing so
        handle.removeNodeHandleListener(this);

        return handle;
      }
    }

    return null;
  }

  /**
   * Removes a node from a set.
   *
   * @param nodeHandle the node to remove.
   *
   * @return the removed handle or null.
   */
  public NodeHandle remove(NodeHandle nodeHandle) {
    for (int i = 0; i < theSize; i++) {
      if (nodes[i].equals(nodeHandle)) {
        NodeHandle handle = nodes[i];

        nodes[i] = nodes[--theSize];

        notifyTable(handle, false);

        // in case we observe this handle, stop doing so
        handle.removeNodeHandleListener(this);

        return handle;
      }
    }

    return null;
  }

  RoutingTable observer;

  private void notifyTable(NodeHandle handle, boolean added) {
    if (observer != null)
      observer.nodeSetUpdate(this, handle, added);
  }

  public void setRoutingTable(RoutingTable routingTable) {
    observer = routingTable;
  }

  /**
   * Membership test.
   *
   * @param nodeId the node id to membership of.
   *
   * @return true if it is a member, false otherwise.
   */
  public boolean contains(Id nodeId) {
    for (int i = 0; i < theSize; i++)
      if (nodes[i].getNodeId().equals(nodeId))
        return true;

    return false;
  }

  /**
   * Return the current size of the set.
   *
   * @return the size.
   */
  @Override
  public int size() {
    return theSize;
  }

  public int capacity() {
    return nodes.length;
  }

  /**
   * Return the closest live node in the set.
   *
   * @return the closest node, or null if no live node exists in the set.
   */
  public NodeHandle closestNode() {
    return closestNode(NodeHandle.LIVENESS_SUSPECTED);
  }

  /**
   * Return the closest live node in the set.
   *
   * @return the closest node, or null if no live node exists in the set.
   */
  public NodeHandle closestNode(int minLiveness) {
    int bestProximity = Integer.MAX_VALUE;
    NodeHandle bestNode = null;

    for (int i = 0; i < theSize; i++) {
      if (nodes[i].getLiveness() > minLiveness)
        continue;

      int proximity = localNode.proximity(nodes[i]);
      if (proximity <= bestProximity) {
        bestProximity = proximity;
        bestNode = nodes[i];
        closest = i;
      }
    }

    // If a backup node handle bubbles up to the top, ping it.
    if (bestNode != null && bestProximity == Integer.MAX_VALUE)
      bestNode.ping();

    return bestNode;
  }

  /**
   * Returns the node in the ith position in the set.
   *
   * @return the ith node.
   */
  public NodeHandle get(int i) {
    if (i < 0 || i >= theSize)
      throw new NoSuchElementException();

    return nodes[i];
  }

  /**
   * Get the index of the node id.
   *
   * @return the node.
   */
  public int getIndex(Id nodeId) {
    for (int i = 0; i < theSize; i++)
      if (nodes[i].getNodeId().equals(nodeId))
        return i;

    return -1;
  }

  /**
   * Get the index of the node id.
   *
   * @return the node.
   */
  public int getIndex(NodeHandle nodeHandle) {
    for (int i = 0; i < theSize; i++)
      if (nodes[i].equals(nodeHandle))
        return i;

    return -1;
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
   * Finds the NodeHandle associated with the Id.
   *
   * @param id a node id.
   * @return the handle associated with that id or null if no such handle is
   *         found.
   */
  @Override
  public rice.p2p.commonapi.NodeHandle getHandle(rice.p2p.commonapi.Id id) {
    int index = getIndex((Id) id);
    return index < 0 ? null : get(index);
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
  public boolean containsHandleFor(rice.p2p.commonapi.Id id) {
    return contains((Id) id);
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
    return remove((Id) id);
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
    return getIndex((Id) id);
  }

  @Override
  public short getType() {
    return NodeHandleSetType.ROUTE_SET.id();
  }

 /**
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  *   +    maxSize    +    theSize    +    closest    +
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  *   + NodeHandle 1st                                                +
  *                    ...
  *   +                                                               +
  *   +                                                               +
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  *                    ...
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  *   + NodeHandle theSize-th                                         +
  *                    ...
  *   +                                                               +
  *   +                                                               +
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  */
  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)nodes.length);
    buf.writeByte((byte)theSize);
    buf.writeByte((byte)closest);
    for (int i = 0; i < theSize; i++) {
      nodes[i].serialize(buf);
    }
  }

  public RouteSet(InputBuffer buf, NodeHandleFactory<?> nodeHandleFactory, PastryNode local) throws IOException {
    this.localNode = local;
    byte maxSize = buf.readByte();
    theSize = buf.readByte();
    closest = buf.readByte();
    if (maxSize < 0 || theSize < 0 || theSize > maxSize) {
      throw new IOException("Invalid route set sizes: max " + maxSize + ", size " + theSize);
    }
    nodes = new NodeHandle[maxSize];
    for (int i = 0; i < theSize; i++) {
      nodes[i] = nodeHandleFactory.readNodeHandle(buf);
    }
    row = -1;
    col = -1;
  }

  @Override
  public Iterator<NodeHandle> iterator() {
    return Arrays.asList(nodes).iterator();
  }

  public boolean isEmpty() {
    return theSize <= 0;
  }

}
