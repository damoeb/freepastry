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

import rice.pastry.IdRange;
import rice.pastry.NodeHandle;

/**
 * The key ranges of a {@link LeafSet}: for which keys a member is an i-root, computed from the members that the
 * leaf set knows.
 */
final class LeafSetRanges {
  private LeafSetRanges() {
  }

  /**
   * Perform (x mod y).  This is necessary because the java % operator
   * is actually remainder instead of mod.  Useless.
   *
   * @return x mod y
   */
  private static int mod(int x, int y) {
    if ((x < 0) ^ (y < 0))
      return y + (x % y);
    else
      return x % y;
  }

  /**
   * range
   * computes the range of keys for which node n is a i-root, 0&lt;=i&lt;=r
   * a node is the r-root for a key of the node becomes the numerically closest node to the key when
   * i-roots for the key fail, O&lt;=i&lt;r, where a key's 0-root is the numerically closest node to the key.
   *
   * @param nodeHandle the nodehandle
   * @return the range of keys, or null if n is not a member of the leafset, or if the range cannot be computed
   */
  static IdRange range(LeafSet leafSet, NodeHandle nodeHandle, int rank) {
   // first, we check the arguments
   if (rank < 0) throw new IllegalArgumentException("Range must be greater than or equal to zero. Attempted "+rank);
   if (! (leafSet.member(nodeHandle) || leafSet.baseHandle.equals(nodeHandle))) throw new LeafSetRangeCannotBeDeterminedException("Node "+nodeHandle+" is not in this leafset.",rank,Integer.MIN_VALUE, leafSet.getUniqueCount(), nodeHandle, leafSet);

   // get the position of the node and the number of nodes in the network
   int position = leafSet.getIndex(nodeHandle);
   int num = leafSet.getUniqueCount();
   NodeHandle cw, ccw; // these are the nodes in the Leafset that represent the extents of the range

   // if this leafset overlaps (or we are the only node in the network), then we have
   // global knowledge about the network and can determine any range.  Otherwise, we
   // need to determine whether or not we can determine the range
   if (leafSet.overlaps() || (num == 1)) {

     // if the rank is more than the number of nodes in the network, then we return
     // the whole range.
     if (rank+1 >= num) {
       return new IdRange(nodeHandle.getNodeId(), nodeHandle.getNodeId());
     }

     // we determine the locations if it's pair nodes
     // note that what we are doing is the following:
     //   we want the nodes at indices {pos-r, pos+r}, but these indices
     //   might be beyond the range of the leafset.  since the range of the unique
     //   leafset is [-ccwSize .. num-ccwSize-1], we shift the range up to
     //   [0 .. num-1] by adding ccwSize.  then, we want to mod p+-r
     //   in order to bring it within this range.  this is done by modding
     //   num (as this will result in the range [0 .. num-1] as
     //   desired.  Last, we shift the range back down by substracting
     //   ccwSize.
     ccw = leafSet.get(mod(position - rank - 1 + leafSet.ccwSet.size(), num) - leafSet.ccwSet.size());
     cw = leafSet.get(mod(position + rank + 1 + leafSet.ccwSet.size(), num) - leafSet.ccwSet.size());
    } else {
      // now, we need to find the pair nodes for this node range
      ccw = leafSet.get(position - rank - 1);
      cw = leafSet.get(position + rank + 1);
    }

   // if either of it's pair nodes are null, then we cannot determine the range
   if ((ccw == null) || (cw == null)) {
     throw new LeafSetRangeCannotBeDeterminedException("This leafset doesn't have enough information to provide the correct range.",rank,position, leafSet.getUniqueCount(), nodeHandle, leafSet);
   }

   // otherwise, we then construct the ranges which comprise the main range, and finally
   // return the overlap
   IdRange cwRange = new IdRange(nodeHandle.getNodeId(), cw.getNodeId()).counterClockwiseHalf();
   IdRange ccwRange = new IdRange(ccw.getNodeId(), nodeHandle.getNodeId()).clockwiseHalf();

   return ccwRange.merge(cwRange);
  }

  /**
   * range
   * computes the ranges of keys for which node n is a r-root
   * a node is the r-root for a key of the node becomes the numerically closest node to the key when
   * i-roots for the key fail, O&lt;=i&lt;r, where a key's 0-root is the numerically closest node to the key.
   * there can be two contiguous ranges of keys; the cw parameter selects which one is returned.
   *
   * @param nodeHandle the nodehandle
   * @param clockwise if true returns the clockwise range, else the counterclockwise range
   * @return the range of keys, or null if n is not a member of the leafset, or if the range cannot be computed
   */
  static IdRange range(LeafSet leafSet, NodeHandle nodeHandle, int rank, boolean clockwise) {
    IdRange range = leafSet.range(nodeHandle, rank);
    if (rank == 0) return range;
    IdRange rprev = null;
    try {
      rprev = leafSet.range(nodeHandle, rank-1);
    } catch (LeafSetRangeCannotBeDeterminedException ignored) {
      // keeps previous functionality now that we are throwing rcbde rather than returning null
    }
    if (range == null || rprev == null) return range;

    IdRange result;

    if (!clockwise) result = range.diff(rprev);
    else     result = rprev.diff(range);
    return result;
  }
}
