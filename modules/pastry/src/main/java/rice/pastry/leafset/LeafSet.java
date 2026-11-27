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

package rice.pastry.leafset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.IdDistance;
import rice.pastry.IdRange;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.NodeSet;
import rice.pastry.NodeSetListener;
import rice.pastry.routing.RoutingTable;

/**
 * A class for representing and manipulating the leaf set.
 *
 * The leafset is not strictly a set: when the ring is small, a node may appear in both the cw and the ccw half of the "set".
 *
 * @version $Id: LeafSet.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Andrew Ladd
 * @author Peter Druschel
 */
public class LeafSet implements Iterable<NodeHandle> {
  Id baseId;
  NodeHandle baseHandle;
  LeafSetHalf cwSet;
  LeafSetHalf ccwSet;

  boolean observe = true;
  private int theSize;

  // can get backup entries from this when we remove
  RoutingTable routingTable;

  /** certified mode: only handles it accepts become members; null admits every handle */
  private @Nullable Predicate<NodeHandle> admission;

  private LeafSet(LeafSet that) {
    this(that, true);
  }

  private LeafSet(LeafSet that, boolean observe) {
    this.observe = observe;
    this.baseId = that.baseId;
    this.baseHandle = that.baseHandle;
    this.ccwSet = that.ccwSet.copy(this);
    this.cwSet = that.cwSet.copy(this);
    this.theSize = that.theSize;
  }

  /**
   * Constructor.
   *
   * @param localNode the local node
   * @param size the size of the leaf set.
   * @param routingTable (to fall back on for more entries on delete operations)
   */
  public LeafSet(NodeHandle localNode, int size, RoutingTable routingTable) {
    this(localNode, size, true);
    this.routingTable = routingTable;
  }

  public LeafSet(NodeHandle localNode, int size, boolean observe) {
    this.observe = observe;

    baseHandle = localNode;
    baseId = localNode.getNodeId();
    theSize = size;

    cwSet = new LeafSetHalf(this, localNode, size/2, true);
    ccwSet = new LeafSetHalf(this, localNode, size/2, false);
  }

  /**
   * @param admission decides which handles may become members (it is asked for every handle that would be added);
   *     null admits every handle
   */
  public void setAdmission(@Nullable Predicate<NodeHandle> admission) {
    this.admission = admission;
  }

  /**
   * @return false if the handle would enter this half but the admission test refuses it; the test is asked only for
   *     a handle that would enter, as it may start verifying the handle
   */
  boolean admits(LeafSetHalf half, NodeHandle handle) {
    return admission == null || !half.test(handle) || admission.test(handle);
  }

  /**
   * Puts a NodeHandle into the set.
   *
   * @param handle the handle to put.
   * @return true if successful, false otherwise.
   */
  public boolean put(NodeHandle handle) {
    return put(handle, false);
  }

  public boolean put(NodeHandle handle, boolean suppressNotification)
  {
    Id nodeId = handle.getNodeId();
    if (nodeId.equals(baseId)) return false;
    if (member(handle)) return false;
    if (admission != null && !admission.test(handle)) return false;

    @SuppressWarnings("ShortCircuitBoolean") // both sets must get the handle
    boolean result = cwSet.put(handle, suppressNotification) | ccwSet.put(handle, suppressNotification);
    return result;
  }

  /**
   * Test if a put of the given NodeHandle would succeed.
   *
   * @param handle the handle to test.
   * @return true if a put would succeed, false otherwise.
   */
  public boolean test(NodeHandle handle)
  {
    Id nodeId = handle.getNodeId();
    if (nodeId.equals(baseId)) return false;
    if (member(handle)) return false;

    return cwSet.test(handle) || ccwSet.test(handle);
  }

  /**
   * Test if the leafset overlaps
   *
   * @return true if the most distant cw member appears in the ccw set or vice versa, false otherwise
   */
  public boolean overlaps() {
    if (size() > 0 && ( ccwSet.member(cwSet.get(cwSet.size()-1)) ||
                        cwSet.member(ccwSet.get(ccwSet.size()-1)) ) )
      return true;

    else return false;
  }

  public boolean isComplete() {
    if (size() == maxSize()) return true;
    if (overlaps()) return true;
    return false;
  }

  /**
   * There are 2 possible indexes (if the ring is small), the cw index and the ccw, this returns the nearest index, and if they are the same, the cw index.
   *
   * Note: previous to FP2.1a3, this always returned the cw index if it existed.
   *
   */
  public int getIndex(NodeHandle nodeHandle) throws NoSuchElementException {

    if (baseId.equals(nodeHandle.getId())) return 0;

    int cwIndex = cwSet.getIndex(nodeHandle);
    int ccwIndex = ccwSet.getIndex(nodeHandle);
    if (cwIndex >= 0 && ccwIndex >= 0) {
      //the ring overlaps


      if (cwIndex <= ccwIndex) {
        // cw is closer (or equal)
        return cwIndex + 1;
      }

      // ccw is closer
      return -ccwIndex - 1;
    }

    if (cwIndex >= 0) return cwIndex + 1;
    if (ccwIndex >= 0) return -ccwIndex - 1;

    throw new NoSuchElementException();
  }


  /**
   * Finds the NodeHandle at a given index.
   *
   * @param index an index.
   * @return the handle associated with that index.
   */
  public NodeHandle get(int index)
  {
    if (index == 0) return baseHandle;
    if (index >= 0) return cwSet.get(index - 1);
    else return ccwSet.get(- index - 1);
  }


  /**
   * Verifies if the set contains this particular handle.
   *
   * @param nodeHandle a NodeHandle.
   * @return true if that NodeHandle is in the set, false otherwise.
   */
  public boolean member(NodeHandle nodeHandle) {
    return cwSet.member(nodeHandle) || ccwSet.member(nodeHandle);
  }

  public boolean contains(NodeHandle nodeHandle) {
    return member(nodeHandle);
  }

  /**
   * Verifies if the set contains this particular id.
   *
   * @param nodeId a node id.
   * @return true if that node id is in the set, false otherwise.
   */
  public boolean member(Id nodeId)
  {
    return cwSet.member(nodeId) || ccwSet.member(nodeId);
  }


  /**
   * Removes a node id and its handle from the set.
   *
   * @param nodeHandle the node to remove.
   */
  public void remove(NodeHandle nodeHandle)
  {
    cwSet.remove(nodeHandle);
    ccwSet.remove(nodeHandle);

    // don't do this inside LeafSetHalf.remove() otherwise you may get an
    // add notification before a removal
    cwSet.findMoreEntriesFromRoutingTable();
    ccwSet.findMoreEntriesFromRoutingTable();
  }

  /**
   * Gets the maximal size of the leaf set.
   *
   * @return the size.
   */
  public int maxSize() { return theSize; }

  /**
   * Gets the current size of the leaf set.  Note that if the leafset overlaps,
   * there will be duplicates.  If you want the unique nodes, use getUniqueCount().
   *
   * @return the size.
   */
  public int size() {
    return clockwiseSize() + counterClockwiseSize();
  }

  /**
   * Gets the current clockwise size.
   *
   * @return the size.
   */
  public int clockwiseSize() { return cwSet.size(); }

  /**
   * Gets the current counterclockwise size.
   *
   * @return the size.
   */
  public int counterClockwiseSize() { return ccwSet.size(); }

  /**
   * complement - given an index of a node in the leafset, produces
   * the index of the same Id in the opposite half of the
   * leafset
   *
   * @param inx the index of the entry to complement
   * @return the index of the same node in the opposite half of the leafset, or inx if it does not exist there
   */
  int complement(int inx) {
    int result;

    if (inx == 0) return 0;
    if (inx < 0) {
      if (inx < -counterClockwiseSize()) return inx;
      result = cwSet.getIndex(ccwSet.get(-inx - 1)) + 1;
    }
    else {
      if (inx > clockwiseSize()) return inx;
      result = -ccwSet.getIndex(cwSet.get(inx - 1)) - 1;
    }

    if (result == 0) result = inx;
    return result;
  }

  /**
   * Numerically closest node to a given a node in the leaf set.
   *
   * @param nodeId a node id.
   * @return the index of the numerically closest node (0 if baseId is the closest).
   */
  public int mostSimilar(Id nodeId) {
    IdDistance cwMinDist;
    IdDistance ccwMinDist;
    int cwMS;
    int ccwMS;

    if (baseId.clockwise(nodeId)) {
      cwMS = cwSet.mostSimilar(nodeId);
      if (cwMS < cwSet.size()-1)
        return cwMS + 1;
      ccwMS = ccwSet.mostSimilar(nodeId);
    }
    else {
      ccwMS = ccwSet.mostSimilar(nodeId);
      if (ccwMS < ccwSet.size()-1)
        return -ccwMS - 1;
      cwMS = cwSet.mostSimilar(nodeId);
    }

    cwMinDist = cwSet.get(cwMS).getNodeId().distance(nodeId);
    ccwMinDist = ccwSet.get(ccwMS).getNodeId().distance(nodeId);

    int cmp = cwMinDist.compareTo(ccwMinDist);
    if (cmp < 0 || (cmp == 0 && nodeId.clockwise(cwSet.get(cwMS).getNodeId())) )
      return cwMS + 1;
    else
      return -ccwMS - 1;
  }

  /**
   * Computes an ordered set of nodes that are neighbors of this local node, in order of numerical closeness to the
   * local node, see {@link LeafSetReplicas}.
   *
   * @param max the maximal size of the set requested
   * @return the ordered set of nodehandles
   */
  public NodeSet neighborSet(int max) {
    return LeafSetReplicas.neighborSet(this, max);
  }

  /**
   * Computes an ordered set of nodes, in order of numerical closeness to a given key, see {@link LeafSetReplicas}.
   *
   * @param key the key
   * @param max the maximal size of the set requested
   * @return the ordered set of nodehandles
   */
  public NodeSet replicaSet(Id key, int max) {
    return LeafSetReplicas.replicaSet(this, key, max);
  }

  /**
   * Returns the number of unique nodes in the leafset
   *
   * @return the number of unique nodes in the leafset
   */
  public int getUniqueCount() {
    return getUniqueSet().size()+1; // the local node
  }

  /**
   * Unordered iterator, does not contain local node.  Contains each element only once.
   *
   * TODO: Make this in order from nearest neighbor to farthest neighbor, not by replica, but take cw[0], cc2[0], cw[1], ccw[1] etc...
   *
   */
  @Override
  public Iterator<NodeHandle> iterator() {
    return getUniqueSet().iterator();
  }

  /**
   * Set of nodes in the leafset, not the local node, each node only once.
   */
  public Collection<NodeHandle> getUniqueSet() {
    HashSet<NodeHandle> superset = new HashSet<NodeHandle>();
    superset.addAll(cwSet.getCollection());
    superset.addAll(ccwSet.getCollection());
    return superset;
  }



  /**
   * Computes the range of keys for which the node is an i-root, 0&lt;=i&lt;=rank, see {@link LeafSetRanges}.
   *
   * @param nodeHandle the nodehandle
   * @return the range of keys
   * @throws LeafSetRangeCannotBeDeterminedException if the node is not a member, or the leaf set does not know enough
   */
  public IdRange range(NodeHandle nodeHandle, int rank) {
    return LeafSetRanges.range(this, nodeHandle, rank);
  }

  /**
   * Computes the one of the two ranges of keys for which the node is a rank-root, see {@link LeafSetRanges}.
   *
   * @param nodeHandle the nodehandle
   * @param clockwise if true returns the clockwise range, else the counterclockwise range
   * @return the range of keys
   */
  public IdRange range(NodeHandle nodeHandle, int rank, boolean clockwise) {
    return LeafSetRanges.range(this, nodeHandle, rank, clockwise);
  }

  /**
   * Merge a remote leafset into this, see {@link LeafSetMerge}.
   *
   * @param remotels the remote leafset
   * @param from the node from which we received the leafset
   * @param routeTable the routing table
   * @param testOnly if true, do not change the leafset
   * @param insertedHandles if not null, a Set that contains, upon return of this method, the nodeHandles that would be inserted into this LeafSet if testOnly is true
   * @return true if the local leafset changed
   */
  public boolean merge(LeafSet remotels, NodeHandle from, RoutingTable routeTable,
                       boolean testOnly, Set<NodeHandle> insertedHandles) {
    return LeafSetMerge.merge(this, remotels, from, routeTable, testOnly, insertedHandles);
  }

  /**
   * Adds a listener to both halves of the leaf set.
   *
   * @param listener the listener to add.
   */
  public void addNodeSetListener(NodeSetListener listener) {
    cwSet.addNodeSetListener(listener);
    ccwSet.addNodeSetListener(listener);
  }

  /**
   * Removes a listener from both halves of the leaf set.
   *
   * @param listener the listener to remove.
   */
  public void removeNodeSetListener(NodeSetListener listener) {
    cwSet.removeNodeSetListener(listener);
    ccwSet.removeNodeSetListener(listener);
  }

  /**
   * Returns a string representation of the leaf set
   *
   */
  @Override
  public String toString()
  {
    String description = "leafset: ";
    for (int i=-ccwSet.size(); i<0; i++)
      description = description + get(i).getNodeId();
    description = description + " [ " + baseId + " ] ";
    for (int i=1; i<=cwSet.size(); i++)
      description = description + get(i).getNodeId();

    description+=" complete:"+isComplete();
    description+=" size:"+size();
    if (size() > 0)
    description+=" s1:"+ ccwSet.member(cwSet.get(cwSet.size()-1));
    description+=" s2:"+ cwSet.member(ccwSet.get(ccwSet.size()-1));

    return description;
  }

  protected boolean isProperlyRemoved(NodeHandle handle) {
    return !member(handle);
  }

  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  protected boolean testOtherSet(LeafSetHalf set, NodeHandle handle) {
    LeafSetHalf otherSet = ccwSet;
    if (otherSet == set) {
      otherSet = cwSet;
    }

    return otherSet.test(handle);
  }

  public boolean directTest(NodeHandle handle) {
    return cwSet.test(handle) || ccwSet.test(handle);
  }

  public LeafSet copy() {
    return new LeafSet(this);
  }

  /**
   * Reads a leaf set from its wire format, see {@link LeafSetWire}.
   */
  public static LeafSet build(InputBuffer buf, NodeHandleFactory<?> nodeHandleFactory) throws IOException {
    return LeafSetWire.read(buf, nodeHandleFactory);
  }

  public LeafSet(NodeHandle localNode, int size, boolean observe, NodeHandle[] cwTable, NodeHandle[] ccwTable) {
    this.observe = observe;

    baseHandle = localNode;
    baseId = localNode.getNodeId();
    theSize = size;

    cwSet = new LeafSetHalf(this, localNode, size/2, true, cwTable);
    ccwSet = new LeafSetHalf(this, localNode, size/2, false, ccwTable);
  }

  /**
   * Writes this leaf set in its wire format, see {@link LeafSetWire}.
   */
  public synchronized void serialize(OutputBuffer buf) throws IOException {
    LeafSetWire.write(this, buf);
  }

  /**
   *
   * If overlaps() a NodeHandle may show up twice.  Does not return self.
   *
   * @return list of NodeHandle
   */
  public synchronized List<NodeHandle> asList() {
    List<NodeHandle> handles = new ArrayList<NodeHandle>();
    for (int i=-counterClockwiseSize(); i<=clockwiseSize(); i++) {
      if (i != 0) {
        handles.add(get(i));
      }
    }
    return handles;
  }
}
