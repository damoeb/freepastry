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

import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeSet;

/**
 * The replica and neighbor sets of a {@link LeafSet}: ordered sets of the members that are numerically closest to a
 * key, or to the local node.
 */
final class LeafSetReplicas {
  private LeafSetReplicas() {
  }

  /**
   * compute an ordered set of nodes that are neighbors of this local node,
   * in order of numerical closeness to the local node
   *
   * @param max the maximal size of the set requested
   * @return the ordered set of nodehandles
   */
  static NodeSet neighborSet(LeafSet leafSet, int max) {
    NodeSet set;
    int cwSize = leafSet.clockwiseSize();
    int ccwSize = leafSet.counterClockwiseSize();
    NodeHandle cwExtreme = leafSet.get(cwSize);
    NodeHandle ccwExtreme = leafSet.get(-ccwSize);
    set = replicaSet(leafSet, leafSet.baseId,max);
    if(!set.contains(cwExtreme) && !set.contains(ccwExtreme)) {
      // the value of max did not cause us to reach either end of the leafset
      // in the method replicaSet
      return set;
    }
    else {
      if(!set.contains(cwExtreme)) {
        // All the nodes in the ccwSet are already members
        for(int i=1; i <= cwSize; i++) {
          set.put(leafSet.get(i));
        }
      }
      if(!set.contains(ccwExtreme)) {
        // All the nodes in the cwSet are already members
        for(int i=1; i <= ccwSize; i++) {
          set.put(leafSet.get(-i));
        }
      }
      return set;
    }
  }

  /**
   * compute an ordered set of nodes, in order of numerical closeness to a given key
   *
   * @param key the key
   * @param max the maximal size of the set requested
   * @return the ordered set of nodehandles
   */
  static NodeSet replicaSet(LeafSet leafSet, Id key, int max) {
    NodeSet set = new NodeSet();
    if (max < 1) return set;

    if ( !leafSet.overlaps() && leafSet.size() > 0 &&
         !key.isBetween(leafSet.get(-leafSet.ccwSet.size()).getNodeId(), leafSet.get(leafSet.cwSet.size()).getNodeId()) &&
         !key.equals(leafSet.get(leafSet.cwSet.size()).getNodeId()) )
      // can't determine root of key, return empty set
      return set;

    // compute the nearest node
    int nearest = leafSet.mostSimilar(key);

    // add the key's root
    set.put(leafSet.get(nearest));

    int cw = nearest;
    int ccw = nearest;
    int wrapped = 0;

    while (set.size() < max && wrapped < 3) {
      int tmp;

      // determine next nearest node
      NodeHandle cwNode = leafSet.get(cw);
      NodeHandle ccwNode = leafSet.get(ccw);
      Id.Distance cwDist = cwNode.getNodeId().distance(key);
      Id.Distance ccwDist = ccwNode.getNodeId().distance(key);

      if (cwDist.compareTo(ccwDist) <= 0) {
        // cwNode is closer to key
        set.put(cwNode);

        tmp = cw;
        if (cw == leafSet.cwSet.size()) {
          if ((cw = leafSet.complement(cw)) == tmp) return set;
          wrapped++;
        }
        cw++;

      } else {
        // ccwNode is closer to key
        set.put(ccwNode);

        tmp = ccw;
        if (-ccw == leafSet.ccwSet.size()) {
          if ((ccw = leafSet.complement(ccw)) == tmp) return set;
          wrapped++;
        }
        ccw--;

      }
    }


    return set;
  }
}
