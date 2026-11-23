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
import java.util.HashSet;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;

/**
 * The wire format of a {@link LeafSet}: reads a leaf set from a buffer and writes one.
 *
 * So that small LeafSets (who have overlapping nodes) don't waste bandwidth,
 * leafset first defines the NodeHandles to be loaded into an array, then
 * specifies their locations. We do this because
 * a NodeHandle takes up a lot more space than the index in the leafset, and
 * it may be in the leafset 1 or 2 times.
 *
 * <pre>
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   + byte theSize  +numUniqueHandls+ byte cwSize   + byte ccwSize  +
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   + NodeHandle baseHandle                                         +
 *                    ...
 *   +                                                               +
 *   +                                                               +
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   + NodeHandle 1st                                                +
 *                    ...
 *   +                                                               +
 *   +                                                               +
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *                    ...
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   + NodeHandle numUniqueHandls-th                                 +
 *                    ...
 *   +                                                               +
 *   +                                                               +
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   + byte cw 1st   +  cw  2nd      + ...           + ccw 1st       +
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   + ccw 2nd       +  ...          + ...           + ccw Nth       +
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * </pre>
 */
final class LeafSetWire {
  private LeafSetWire() {
  }

  /** Reads a leaf set. */
  static LeafSet read(InputBuffer buf, NodeHandleFactory<?> nodeHandleFactory) throws IOException {
    byte theSize = buf.readByte();
    byte numUniqueHandles = buf.readByte();
    byte cwSize = buf.readByte();
    byte ccwSize = buf.readByte();
    if (theSize < 0 || numUniqueHandles < 0 || cwSize < 0 || ccwSize < 0) {
      throw new IOException("Invalid leafset sizes: " + theSize + "/" + numUniqueHandles + "/" + cwSize + "/" + ccwSize);
    }
    NodeHandle baseHandle = nodeHandleFactory.readNodeHandle(buf);
    NodeHandle[] nhTable = new NodeHandle[numUniqueHandles];
    for (int i = 0; i < numUniqueHandles; i++) {
      nhTable[i] = nodeHandleFactory.readNodeHandle(buf);
    }

    NodeHandle[] cwTable = new NodeHandle[cwSize];
    NodeHandle[] ccwTable = new NodeHandle[ccwSize];

    for (int i = 0; i < cwSize; i++) {
      cwTable[i] = readTableEntry(buf, nhTable);
    }

    for (int i = 0; i < ccwSize; i++) {
      ccwTable[i] = readTableEntry(buf, nhTable);
    }

    return new LeafSet(baseHandle,theSize, true, cwTable, ccwTable);
  }

  private static NodeHandle readTableEntry(InputBuffer buf, NodeHandle[] nhTable) throws IOException {
    byte index = buf.readByte();
    if (index < 0 || index >= nhTable.length) {
      throw new IOException("Leafset handle index " + index + " out of range 0.." + (nhTable.length - 1));
    }
    return nhTable[index];
  }

  /**
   * Writes the leaf set.
   *
   * @param leafSet the leaf set; the caller holds its lock
   */
  static void write(LeafSet leafSet, OutputBuffer buf) throws IOException {
    HashSet<NodeHandle> superset = new HashSet<NodeHandle>();
    superset.addAll(leafSet.cwSet.getCollection());
    superset.addAll(leafSet.ccwSet.getCollection());
    ArrayList<NodeHandle> list = new ArrayList<NodeHandle>(superset);

    buf.writeByte((byte)leafSet.maxSize());
    buf.writeByte((byte)list.size());
    buf.writeByte((byte)leafSet.clockwiseSize());
    buf.writeByte((byte)leafSet.counterClockwiseSize());

    leafSet.baseHandle.serialize(buf);

    for (NodeHandle nodeHandle : list) {
      nodeHandle.serialize(buf);
    }

    for (int i = 0; i < leafSet.cwSet.size(); i++) {
      buf.writeByte((byte)list.indexOf(leafSet.cwSet.get(i)));
    }

    for (int i = 0; i < leafSet.ccwSet.size(); i++) {
      buf.writeByte((byte)list.indexOf(leafSet.ccwSet.get(i)));
    }
  }
}
