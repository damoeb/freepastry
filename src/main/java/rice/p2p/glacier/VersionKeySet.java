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
package rice.p2p.glacier;

import java.util.SortedMap;

import rice.p2p.commonapi.Id;
import rice.p2p.util.ids.SortedMapIdSet;

/**
 * A sorted set of VersionKeys.
 *
 * @version $Id: VersionKeySet.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author ahae
 */
public class VersionKeySet extends SortedMapIdSet<VersionKeySet> {

  /**
   * Constructor.
   */
  public VersionKeySet() {
    super();
  }

  /**
   * Creates a set whose members are the keys of the given map; the set is a view of the map.
   *
   * @param s the map whose keys are the members
   */
  protected VersionKeySet(SortedMap<Id, ?> s) {
    super(s);
  }

  protected VersionKeySet newSet(SortedMap<Id, ?> members) {
    return new VersionKeySet(members);
  }

  /**
   * Not supported.
   *
   * @return never returns
   * @throws RuntimeException always
   */
  public byte[] hash() {
    throw new RuntimeException("VersionKeySet.getHash() is not supported!");
  }

  /**
   * Returns a string without the members.
   *
   * @return "[ IdSet:  ]"
   */
  public String toString() {
    return "[ IdSet:  ]";
  }
}
