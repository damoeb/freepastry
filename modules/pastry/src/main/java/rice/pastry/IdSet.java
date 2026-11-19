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

import java.security.MessageDigest;
import java.util.Iterator;
import java.util.SortedMap;

import rice.p2p.util.HashUtils;
import rice.p2p.util.ids.SortedMapIdSet;

/**
 * Represents a set of Pastry ids.
 *
 * @version $Id: IdSet.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Peter Druschel
 */
public final class IdSet extends SortedMapIdSet<IdSet> {

  // a cache of the fingerprint hash, or null if it must be recomputed
  private byte[] cachedHash;

  // the set this one is a view of: changes made through the view change that set's members too
  private IdSet parent;

  /**
   * Constructor.
   */
  public IdSet() {
    super();
  }

  /**
   * Creates a set whose members are the keys of the given map; the set is a view of the map.
   *
   * @param sortedMap the map whose keys are the members
   */
  public IdSet(SortedMap<rice.p2p.commonapi.Id, ?> sortedMap) {
    super(sortedMap);
  }

  @Override
  protected IdSet newSet(SortedMap<rice.p2p.commonapi.Id, ?> members) {
    return new IdSet(members);
  }

  @Override
  public IdSet subSet(rice.p2p.commonapi.Id from, rice.p2p.commonapi.Id to) {
    IdSet view = super.subSet(from, to);
    view.parent = this;
    return view;
  }

  @Override
  protected void membersChanged() {
    cachedHash = null;
    if (parent != null) parent.membersChanged();
  }

  /**
   * compute a fingerprint of the members in this IdSet
   *
   * @return the 32-byte SHA-256 content hash of the members, in order
   */
  @Override
  public byte[] hash() {
    if (cachedHash != null) return cachedHash.clone();

    // recompute the hash
    MessageDigest digest = HashUtils.newDigest();

    Iterator<rice.p2p.commonapi.Id> iterator = getIterator();

    byte[] bytes = new byte[Id.IdBitLength / 8];

    while (iterator.hasNext()) {
      ((Id) iterator.next()).blit(bytes);
      digest.update(bytes);
    }

    cachedHash = digest.digest();

    return cachedHash.clone();
  }
}
