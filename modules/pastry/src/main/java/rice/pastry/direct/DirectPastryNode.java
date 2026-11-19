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
package rice.pastry.direct;


import rice.pastry.PastryNode;

/**
 * Direct pastry node. Subclasses PastryNode, and does about nothing else.
 *
 * @version $Id: DirectPastryNode.java 4221 2008-05-19 16:41:19Z jeffh $
 *
 * @author Sitaram Iyer
 */

public class DirectPastryNode {
  /**
   * Used for proximity calculation of DirectNodeHandle. This will probably go
   * away when we switch to a byte-level protocol.
   *
   * A ThreadLocal, so a thread that never clears its entry does not keep itself
   * and its node reachable after it ends.
   */
  static private final ThreadLocal<PastryNode> currentNode = new ThreadLocal<PastryNode>();

  /**
   * Returns the previous one.
   *
   * @param dpn the node to make current for this thread, null to clear it
   * @return the previous current node
   */
  public static PastryNode setCurrentNode(PastryNode dpn) {
    PastryNode result = currentNode.get();
    if (dpn == null) {
      currentNode.remove();
    } else {
      currentNode.set(dpn);
    }
    return result;
  }

  public static PastryNode getCurrentNode() {
    return currentNode.get();
  }
}
