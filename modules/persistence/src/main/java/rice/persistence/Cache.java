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

package rice.persistence;
/*
 * @(#) Cache.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 * @version $Id: Cache.java 4654 2009-01-08 16:33:07Z jeffh $
 */

import java.util.concurrent.CompletableFuture;

import rice.Continuation;
import rice.Futures;
import rice.p2p.commonapi.Id;

/**
 * This interface is the abstraction of something which provides a
 * caching service.  Implementations should take in parameters specific
 * to the cache algorithm.  Two implementations are provided,
 * the LRUCache and NoOpCache. This interface extends the Catalog
 * interface, as the cache provides a Catalog service.
 *
 * @version $Id: Cache.java 4654 2009-01-08 16:33:07Z jeffh $
 */
@SuppressWarnings("unchecked")
public interface Cache extends Catalog {

  /**
   * Caches an object in this storage. This method is non-blocking.
   * If the object has already been stored at the location id, this
   * method has the effect of calling <code>uncache(id)</code> followed
   * by <code>cache(id, obj)</code>. This method finishes by calling
   * receiveResult() on the provided continuation with whether or not
   * the object was cached.  Note that the object may not actually be
   * cached due to the cache replacement policy.
   *
   * Returns <code>True</code> if the cache actaully stores the object, else
   * <code>False</code> (through receiveResult on c).
   *
   * @param id The object's id.
   * @param metadata The object's metdatadata
   * @param obj The object to cache.
   * @param continuation The command to run once the operation is complete
   */
  public void cache(Id id, Object metadata, Object obj, Continuation<? super Boolean, Exception> continuation);

  /**
   * Caches an object under the given id; the cache may evict other objects for it.
   *
   * @param id The object's id.
   * @param metadata The object's metadata
   * @param obj The object to cache.
   * @return completes with whether the object was cached
   */
  default CompletableFuture<Boolean> cache(Id id, Object metadata, Object obj) {
    return Futures.call(continuation -> cache(id, metadata, obj, continuation));
  }

  /**
   * Removes the object from the list of cached objects. This method is
   * non-blocking. If the object was not in the cached list in the first place,
   * nothing happens and <code>False</code> is returned.
   *
   * Returns <code>True</code> if the action succeeds, else
   * <code>False</code>  (through receiveResult on c).
   *
   * @param id The object's id
   * @param continuation The command to run once the operation is complete
   */
  public void uncache(Id id, Continuation<? super Boolean, Exception> continuation);

  /**
   * Removes the object cached under the given id.
   *
   * @param id The object's id.
   * @return completes with whether an object was removed
   */
  default CompletableFuture<Boolean> uncache(Id id) {
    return Futures.call(continuation -> uncache(id, continuation));
  }

  /**
   * Returns the maximum size of the cache, in bytes.
   *
   * @return The maximum size, in bytes, of the cache.
   */
  public long getMaximumSize();

  /**
   * Sets the maximum size of the cache, in bytes. Setting this
   * value to a smaller value than the current value may result in
   * object being evicted from the cache.
   *
   * Returns the success or failure of the setSize operation
   * (through receiveResult on c).
   *
   * @param size The new maximum size, in bytes, of the cache.
   * @param continuation The command to run once the operation is complete
   */
  public void setMaximumSize(int size, Continuation<? super Boolean, Exception> continuation);

  /**
   * Sets the maximum size of the cache, evicting objects if it is smaller than the cache's contents.
   *
   * @param size The new maximum size, in bytes.
   * @return completes with <code>true</code> once the cache fits
   */
  default CompletableFuture<Boolean> setMaximumSize(int size) {
    return Futures.call(continuation -> setMaximumSize(size, continuation));
  }
}
