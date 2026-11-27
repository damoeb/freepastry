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

import java.util.HashSet;
import java.util.Set;

import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.routing.RoutingTable;

/**
 * Merges the leaf set of a remote node into a local {@link LeafSet}, nearest nodes first.
 */
final class LeafSetMerge {
  private LeafSetMerge() {
  }

  /**
   * Merge a remote leafset into the local one
   *
   * @param local the leaf set to merge into
   * @param remotels the remote leafset
   * @param from the node from which we received the leafset
   * @param routeTable the routing table
   * @param testOnly if true, do not change the leafset
   * @param insertedHandles if not null, a Set that contains, upon return of this method, the nodeHandles that would be inserted into this LeafSet if testOnly is true
   * @return true if the local leafset changed
   */
  static boolean merge(LeafSet local, LeafSet remotels, NodeHandle from, RoutingTable routeTable,
                       boolean testOnly, Set<NodeHandle> insertedHandles) {

    boolean changed, result = false;
    int cwSize = remotels.clockwiseSize();
    int ccwSize = remotels.counterClockwiseSize();

    // this is the only way to get the notification correct
    Set<NodeHandle> myInsertedHandles = new HashSet<NodeHandle>();

    // merge the received leaf set into our own
    // to minimize inserts/removes, we do this from nearest to farthest nodes

    // get indices of localId in the leafset
    int cw = remotels.cwSet.getIndex(local.baseId);
    int ccw = remotels.ccwSet.getIndex(local.baseId);

    if (cw < 0) {
      // localId not in cw set

      if (ccw < 0) {
        // localId not in received leafset, that means local node is joining

        if (remotels.size() < 2) {
          cw = ccw = 0;
        }
        else {

          // find the num. closest to localId in the remotels
          int closest = remotels.mostSimilar(local.baseId);
          Id closestId = remotels.get(closest).getNodeId();

          if (closest == -remotels.counterClockwiseSize() || closest == remotels.clockwiseSize()) {
            // doesn't hurt to merge it anyways
          }

          if (closest == 0) {
            // from is closest
            if (local.baseId.clockwise(closestId)) {
              cw = closest;
              ccw = remotels.complement(closest - 1);
            }
            else {
              cw = remotels.complement(closest + 1);
              ccw = closest;
            }
          }
          else if (closest < 0) {
            // from is cw
            if (local.baseId.clockwise(closestId)) {
              cw = closest;
              ccw = remotels.complement(closest - 1);
            }
            else {
              cw = closest + 1;
              ccw = remotels.complement(closest);
            }
          }
          else {
            // from is ccw
            if (local.baseId.clockwise(closestId)) {
              cw = remotels.complement(closest);
              ccw = closest - 1;
            }
            else {
              ccw = closest;
              cw = remotels.complement(closest + 1);
            }
          }

        }
      }
      else {
        // localId in ccw set

        ccw = -ccw  - 2;
        cw = ccw + 2;
      }
    }
    else {
      // localId in cw set

      if (ccw < 0) {
        cw = cw + 2;
        ccw = cw - 2;
      }
      else {
        // localId is in both halves
        int tmp = ccw;
        ccw = cw;
        cw = -tmp;
      }
    }

    for (int i=cw; i<=cwSize; i++) {
      NodeHandle currentHandle;

      if (i == 0) currentHandle = from;
      else currentHandle = remotels.get(i);

      if (currentHandle.getLiveness() >= NodeHandle.LIVENESS_DEAD) continue;

      if (testOnly) {
        // see it is missing
        changed = local.cwSet.test(currentHandle);
      }
      else {
        // merge into our cw leaf set half, if the admission test lets the handle in
        changed = local.admits(local.cwSet, currentHandle) && local.cwSet.put(currentHandle, true);

        if (changed) myInsertedHandles.add(currentHandle);

        // update RT as well
        routeTable.put(currentHandle);
      }

      result |= changed;
      if (insertedHandles != null && changed) insertedHandles.add(currentHandle);
    }

    for (int i=ccw; i>= -ccwSize; i--) {
      NodeHandle nh;

      if (i == 0) nh = from;
      else nh = remotels.get(i);

      if (nh.getLiveness() >= NodeHandle.LIVENESS_DEAD) continue;

      if (testOnly) {
        // see if it is missing
        changed = local.ccwSet.test(nh);
      }
      else {
        // merge into our leaf set, if the admission test lets the handle in
        changed = local.admits(local.ccwSet, nh) && local.ccwSet.put(nh, true);

        if (changed) myInsertedHandles.add(nh);

        // update RT as well
        routeTable.put(nh);
      }

      result |= changed;
      if (insertedHandles != null && changed) insertedHandles.add(nh);
    }

    // if there is overlap, insert nearest nodes regardless of orientation
    if (local.overlaps()) {
      for (int i=-ccwSize; i <= cwSize; i++) {
        NodeHandle nh;

        if (i == 0) nh = from;
        else nh = remotels.get(i);

        if (nh.getLiveness() >= NodeHandle.LIVENESS_DEAD) continue;

        if (testOnly) {
          // merge into our leaf set
          changed = local.test(nh);
        }
        else {
          // merge into our leaf set
          changed = local.put(nh);

          if (changed) myInsertedHandles.add(nh);

          // update RT as well
          routeTable.put(nh);
        }

        result |= changed;
        if (insertedHandles != null && changed) insertedHandles.add(nh);
      }
    }

    for (NodeHandle nodeHandle : myInsertedHandles) {
      local.cwSet.notifyListeners(nodeHandle, true);
    }

    return result;
  }

}
