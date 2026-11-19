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
 * @(#) CachedStorage.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 *
 * @version $Id: CachedStorageImpl.java 4654 2009-01-08 16:33:07Z jeffh $
 */
import java.util.SortedMap;

import rice.Continuation;
import rice.Continuation.StandardContinuation;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;

/**
 * This class provides both persistent and caching services to
 * external applications. Building the CachedStorage requires a
 * Storage object, to provide the back-end storage, and a Cache
 * to serve as a cache.  Note that this implementation has separate
 * areas for the Cache and Storage, but the next version will allow
 * the cache to use the unused storage space.
 */
@SuppressWarnings("unchecked")
public class CachedStorageImpl implements CachedStorage {

  // the storage used by this manager
  private Storage storage;

  // the cache used by this manager
  private Cache cache;

  /**
   * Builds a CachedStorage given a Storage object to provide
   * storage services and a Cache object to provide caching
   * services.
   *
   * @param factory The factory to use for Id creation
   * @param storage The Storage object which will serve as the
   *        persistent storage.
   * @param cache The Cache object which will serve as the cache.
   */
  public CachedStorageImpl(IdFactory factory, Storage storage, Cache cache) {
    this.storage = storage;
    this.cache = cache;
  }

  /**
   * Returns the permantent storage object used by this CachedStorage
   *
   * @return The storage of this storage manager
   */
  @Override
  public Storage getStorage() {
    return storage;
  }

  /**
   * Returns the cache object used by this CachedStorage
   *
   * @return The cache of this storage manager
   */
  @Override
  public Cache getCache() {
    return cache;
  }

  /**
   * Returns whether or not an object is present in the location <code>id</code>.
   *
   * @param id The id of the object in question.
   * @return Whether or not an object is present at id.
   */
  @Override
  public boolean exists(Id id) {
    return storage.exists(id);
  }

  /**
   * Renames the given object to the new id.  This method is potentially faster
   * than store/cache and unstore/uncache.
   *
   * @param oldId The id of the object in question.
   * @param newId The new id of the object in question.
   * @param continuation The command to run once the operation is complete
   */
  @Override
  public void rename(final Id oldId, final Id newId, Continuation<? super Boolean, Exception> continuation) {
    cache.rename(oldId, newId, new StandardContinuation<Boolean, Boolean, Exception>(continuation) {
      @Override
      public void receiveResult(Boolean result) {
        storage.rename(oldId, newId, parent);
      }
      @Override
      public String toString() { return "rename of " + oldId + " to " + newId; }
    });
  }

  /**
   * Returns the object identified by the given id, or <code>null</code> if
   * there is no corresponding object (through receiveResult on c).
   *
   * @param id The id of the object in question.
   * @param continuation The command to run once the operation is complete
   */
  @Override
  public void getObject(final Id id, Continuation<? super Object, Exception> continuation) {
    cache.getObject(id, new StandardContinuation<Object, Object, Exception>(continuation) {
      @Override
      public void receiveResult(Object result) {
        if (result != null) {
          parent.receiveResult(result);
        } else {
          storage.getObject(id, parent);
        }
      }
      @Override
      public String toString() { return "getObject of " + id; }
    });
  }

  /**
   * Returns the metadata associated with the provided object, or null if
   * no metadata exists.  The metadata must be stored in memory, so this
   * operation is guaranteed to be fast and non-blocking.
   *
   * @param id The id for which the metadata is needed
   * @return The metadata, or null of non exists
   */
  @Override
  public Object getMetadata(Id id) {
    return storage.getMetadata(id);
  }

  /**
   * Updates the metadata stored under the given key to be the provided
   * value.  As this may require a disk access, the requestor must
   * also provide a continuation to return the result to.
   *
   * @param id The id for the metadata
   * @param metadata The metadata to store
   * @param command The command to run once the operation is complete
   */
  @Override
  public void setMetadata(final Id id, final Object metadata, Continuation<? super Boolean, Exception> command) {
    cache.setMetadata(id, metadata, new StandardContinuation<Boolean, Boolean, Exception>(command) {
      @Override
      public void receiveResult(Boolean result) {
        storage.setMetadata(id, metadata, parent);
      }
      @Override
      public String toString() { return "setMetadata of " + id; }
    });
  }

  /**
   * Return the objects identified by the given range of ids. The IdSet
   * returned contains the Ids of the stored objects. The range is
   * partially inclusive, the lower range is inclusive, and the upper
   * exclusive.
   *
   * NOTE: This method blocks so if the behavior of this method changes and
   * no longer stored in memory, this method may be deprecated.
   *
   * @param range The range to query
   * @return The idset containg the keys
   */
  @Override
  public IdSet scan(IdRange range){
    return storage.scan(range);
  }

  /**
   * Return all objects currently stored by this catalog
   *
   * NOTE: This method blocks so if the behavior of this method changes and
   * no longer stored in memory, this method may be deprecated.
   *
   * @return The idset containg the keys
   */
  @Override
  public IdSet scan() {
    return storage.scan();
  }

  /**
   * Returns a map which contains keys mapping ids to the associated
   * metadata.
   *
   * @param range The range to query
   * @return The map containg the keys
   */
  @Override
  public SortedMap<Id, Object> scanMetadata(IdRange range) {
    return storage.scanMetadata(range);
  }

  /**
   * Returns a map which contains keys mapping ids to the associated
   * metadata.
   *
   * @return The treemap mapping ids to metadata
   */
  @Override
  public SortedMap<Id, Object> scanMetadata() {
    return storage.scanMetadata();
  }

  /**
   * Returns the submapping of ids which have metadata less than the provided
   * value.
   *
   * @param value The maximal metadata value
   * @return The submapping
   */
  @Override
  public SortedMap<Id, Object> scanMetadataValuesHead(Object value) {
    return storage.scanMetadataValuesHead(value);
  }

  /**
   * Returns the submapping of ids which have metadata null
   *
   * @return The submapping
   */
  @Override
  public SortedMap<Id, Object> scanMetadataValuesNull() {
    return storage.scanMetadataValuesNull();
  }

  /**
   * Returns the total size of the stored data in bytes.
   *
   * @return The total size, in bytes, of data stored.
   */
  @Override
  public long getTotalSize() {
    return cache.getTotalSize() + storage.getTotalSize();
  }

  /**
   * Returns the number of Ids currently stored in the catalog
   *
   * @return The number of ids in the catalog
   */
  @Override
  public int getSize() {
    return storage.getSize();
  }

  /**
   * Stores an object in this storage. This method is non-blocking.
   * If the object has already been stored at the location id, this
   * method has the effect of calling <code>unstore(id)</code> followed
   * by <code>store(id, obj)</code>. This method finishes by calling
   * receiveResult() on the provided continuation with the success
   * or failure of the store.
   *
   * Returns <code>True</code> if the action succeeds, else
   * <code>False</code> (through receiveResult on c).
   *
   * @param id The object's id.
   * @param metadata The object's metadata
   * @param obj The object to store.
   * @param continuation The command to run once the operation is complete
   */
  @Override
  public void store(Id id, Object metadata, Object obj, Continuation<? super Boolean, Exception> continuation) {
    storage.store(id, metadata, obj, continuation);
  }

  /**
   * Removes the object from the list of stored objects. This method is
   * non-blocking. If the object was not in the stored list in the first place,
   * nothing happens and <code>False</code> is returned.
   *
   * Returns <code>True</code> if the action succeeds, else
   * <code>False</code>  (through receiveResult on c).
   *
   * @param id The object's persistence id
   * @param continuation The command to run once the operation is complete
   */
  @Override
  public void unstore(Id id, Continuation<? super Boolean, Exception> continuation) {
    storage.unstore(id, continuation);
  }

  /**
   * Caches an object in this storage. This method is non-blocking.
   * If the object has already been stored at the location id, this
   * method has the effect of calling <code>uncachr(id)</code> followed
   * by <code>cache(id, obj)</code>. This method finishes by calling
   * receiveResult() on the provided continuation with whether or not
   * the object was cached.  Note that the object may not actually be
   * cached due to the cache replacement policy.
   *
   * Returns <code>True</code> if the cache actaully stores the object, else
   * <code>False</code> (through receiveResult on c).
   *
   * @param id The object's id.
   * @param metadata The object's metadata
   * @param obj The object to cache.
   * @param continuation The command to run once the operation is complete
   */
  @Override
  public void cache(Id id, Object metadata, Object obj, Continuation<? super Boolean, Exception> continuation) {
    cache.cache(id, metadata, obj, continuation);
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
  @Override
  public void uncache(Id id, Continuation<? super Boolean, Exception> continuation) {
    cache.uncache(id, continuation);
  }

  /**
   * Returns the maximum size of the cache, in bytes.
   *
   * @return The maximum size, in bytes, of the cache.
   */
  @Override
  public long getMaximumSize() {
    return cache.getMaximumSize();
  }

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
  @Override
  public void setMaximumSize(int size, Continuation<? super Boolean, Exception> continuation) {
    cache.setMaximumSize(size, continuation);
  }

  /**
   * Method which is used to erase all data stored in the Catalog.
   * Use this method with care!
   *
   * @param continuation The command to run once done
   */
  @Override
  public void flush(Continuation<? super Boolean, Exception> continuation) {
    cache.flush(new StandardContinuation<Boolean, Boolean, Exception>(continuation) {
      @Override
      public void receiveResult(Boolean result) {
        storage.flush(parent);
      }
    });
  }
}
