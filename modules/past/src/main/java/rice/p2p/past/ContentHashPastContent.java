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

package rice.p2p.past;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;

/**
 * An abstract class for content-hash objects stored in Past.
 *
 * Provided as a convenience.
 *
 * A content-hash object is stored under an id derived from its payload, so that anyone holding the id can check what
 * a node hands back. That only works if the check is made: subclasses say how the id follows from the payload by
 * implementing {@link #computeContentHash(IdFactory)}, and {@link PastImpl} refuses to insert, cache or return an
 * object whose id is not what that method computes.
 *
 * @version $Id: ContentHashPastContent.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author Peter Druschel
 * @author Alan Mislove
 */
public abstract class ContentHashPastContent implements PastContent {

  private final Id myId;

  // ----- PastCONTENT METHODS -----

  public ContentHashPastContent(Id myId) {
    this.myId = myId;
  }

  /**
   * Recomputes from the payload the id this object has to be stored under, normally by hashing the payload, for
   * example {@code factory.buildId(payload)}. Must not use {@link #getId()}: the id is what is being checked.
   *
   * @param factory the id factory of the node doing the check
   * @return the id the payload hashes to
   */
  public abstract Id computeContentHash(IdFactory factory);

  /**
   * Tells whether the id of this object is the hash of its payload.
   *
   * @param factory the id factory of the node doing the check
   * @return true if {@link #getId()} equals {@link #computeContentHash(IdFactory)}
   */
  public final boolean verifyContentHash(IdFactory factory) {
    return myId != null && myId.equals(computeContentHash(factory));
  }

  /**
   * Checks if a insert operation should be allowed.  Invoked when a
   * Past node receives an insert request and it is a replica root for
   * the id; invoked on the object to be inserted.  This method
   * determines the effect of an insert operation on an object that
   * already exists: it computes the new value of the stored object,
   * as a function of the new and the existing object.
   *
   * @param id the key identifying the object
   * @param existingContent the existing object stored on this node (null
   *        if no object associated with id is stored on this node)
   * @return null, if the operation is not allowed; else, the new
   *         object to be stored on the local node.
   */
  @Override
  public PastContent checkInsert(Id id, PastContent existingContent) throws PastException {
    // can't overwrite content hash objects
    if (existingContent != null) {
      throw new PastException("ContentHashPastContent: can't insert, object already exists");
    }

    // only allow the key the object claims; that it is the hash of the payload is checked by Past, which has the
    // id factory (see verifyContentHash)
    if (!id.equals(getId())) {
      throw new PastException("ContentHashPastContent: can't insert, content hash incorrect");
    }
    return this;
  }

  /**
   * Produces a handle for this content object. The handle is retrieved and returned to the
   * client as a result of the Past.lookupHandles() method.
   *
   * @param local The local past service
   * @return the handle
   */
  @Override
  public PastContentHandle getHandle(Past local) {
    return new ContentHashPastContentHandle(local.getLocalNodeHandle(), getId());
  }

  /**
   * Returns the Id under which this object is stored in Past.
   *
   * @return the id
   */
  @Override
  public Id getId() {
    return myId;
  }

  /**
   * States if this content object is mutable. Mutable objects are not subject to dynamic caching in Past.
   *
   * @return true if this object is mutable, else false
   */
  @Override
  public boolean isMutable() {
    return false;
  }
}
