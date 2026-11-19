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
 * @(#) LRUCache.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 *
 * @version $Id: LRUCache.java 4654 2009-01-08 16:33:07Z jeffh $
 */
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;

import org.slf4j.Logger;
import rice.Continuation;
import rice.Continuation.StandardContinuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;

/**
 * This class is an encapsulation of a least-recently-used (LRU)
 * cache.  It uses the provided storage service in order to
 * store the cached data.  If the Storage provides non-corruption
 * services, these services will also be provided by this cache.
 */
@SuppressWarnings("unchecked")
public class LRUCache implements Cache {

  // the maximum size of the cache
  private int maximumSize;

  // the back-end storage used by this cache
  private final Storage storage;

  // the keys, eldest (least recently used) first; access order, so a touch moves a key to the end; guarded by itself
  private final Map<Id, Boolean> order = new LinkedHashMap<>(16, 0.75f, true);

  private final Environment environment;

  /**
   * Builds a LRU cache given a storage object to store the cached
   * data in and a maximum cache size.
   *
   * @param storage The storage service to use as a back-end storage
   * @param maximumSize The maximum size, in bytes, of storage to use
   */
  public LRUCache(Storage storage, int maximumSize, Environment env) {
    this.environment = env;
    this.storage = storage;
    this.maximumSize = maximumSize;

    Iterator<Id> i = storage.scan().getIterator();

    while (i.hasNext())
      touch(i.next());
  }

  /** Marks the id as the most recently used one; a null id is accepted. */
  private void touch(Id id) {
    synchronized (order) {
      order.put(id, Boolean.TRUE);
    }
  }

  private void forget(Id id) {
    synchronized (order) {
      order.remove(id);
    }
  }

  /** The least recently used id, or null if the cache tracks none. */
  private Id eldest() {
    synchronized (order) {
      return order.isEmpty() ? null : order.keySet().iterator().next();
    }
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
    storage.rename(oldId, newId, new StandardContinuation<Boolean, Boolean, Exception>(continuation) {
      @Override
      public void receiveResult(Boolean result) {
        if (Boolean.TRUE.equals(result)) {
          forget(oldId);
          touch(newId);
        }
        parent.receiveResult(result);
      }
    });
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
  public void cache(final Id id, final Object metadata, final Object obj, final Continuation<? super Boolean, Exception> continuation) {
    final int size = getSize(obj);

    if (exists(id)) {
      touch(id);

      continuation.receiveResult(Boolean.valueOf(true));
      return;
    }

    if (size > maximumSize) {
      continuation.receiveResult(Boolean.valueOf(false));
      return;
    }


    final Continuation<Boolean, Exception> store = new Continuation<Boolean, Exception>() {
      @Override
      public void receiveResult(Boolean result) {
        touch(id);

        storage.store(id, metadata, obj, continuation);
      }

      @Override
      public void receiveException(Exception e) {
        continuation.receiveException(e);
      }
    };

    long totalSize = storage.getTotalSize();

    if (maximumSize - size < totalSize)
      resize(maximumSize - size, store);
    else
      store.receiveResult(Boolean.TRUE);
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
    forget(id);

    storage.unstore(id, continuation);
  }

  /**
   * Returns whether or not an object is cached in the location <code>id</code>.
   *
   * @param id The id of the object in question.
   * @return Whether or not an object is present at id.
   */
  @Override
  public boolean exists(Id id) {
    return storage.exists(id);
  }

  /**
   * Returns the object identified by the given id.
   *
   * @param id The id of the object in question.
   * @param continuation The command to run once the operation is complete; receives the object, or <code>null</code> if there is no corresponding object
   */
  @Override
  public void getObject(Id id, Continuation<? super Object, Exception> continuation) {
    if (! exists(id)) {
      continuation.receiveResult(null);
      return;
    }

    touch(id);

    storage.getObject(id, continuation);
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
  public void setMetadata(Id id, Object metadata, Continuation<? super Boolean, Exception> command) {
    storage.setMetadata(id, metadata, command);
  }

  /**
   * Return the objects identified by the given range of ids. The IdSet
   * returned contains the Ids of the stored objects. The range is
   * partially inclusive, the lower range is inclusive, and the upper
   * exclusive.
   *
   *
   * NOTE: This method blocks so if the behavior of this method changes and
   * uses the disk, this method may be deprecated.
   *
   * @param range The range to query
   * @return The idset containg the keys
   */
   @Override
   public synchronized IdSet scan(IdRange range){
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
  public synchronized IdSet scan() {
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
   * Returns the maximum size of the cache, in bytes.
   *
   * @return The maximum size, in bytes, of the cache.
   */
  @Override
  public long getMaximumSize() {
    return maximumSize;
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
   * Returns the total size of the stored data in bytes.
   *
   * @return The total size, in bytes, of data stored.
   */
  @Override
  public long getTotalSize() {
    return storage.getTotalSize();
  }

  /**
   * Sets the maximum size of the cache, in bytes. Setting this
   * value to a smaller value than the current value may result in
   * object being evicted from the cache.
   *
   * @param size The new maximum size, in bytes, of the cache.
   * @param continuation The command to run once the operation is complete; receives the success or failure of the setSize operation
   */
  @Override
  public void setMaximumSize(final int size, final Continuation<? super Boolean, Exception> continuation) {
    Continuation<Boolean, Exception> local = new Continuation<Boolean, Exception>() {
      @Override
      public void receiveResult(Boolean result) {
        maximumSize = size;

        continuation.receiveResult(Boolean.valueOf(true));
      }

      @Override
      public void receiveException(Exception e) {
        continuation.receiveException(e);
      }
    };

    if (size < maximumSize) {
      resize(size, local);
    } else {
      local.receiveResult(Boolean.valueOf(true));
    }
  }

  /**
   * Internal method which removes objects from the cache until the cache
   * is smaller than the specified size
   *
   * @param size The maximum number of bytes to make the cache
   * @param continuation The command to run once the operation is complete
   */
  private void resize(final int size, Continuation<? super Boolean, Exception> continuation) {
    final Continuation<Boolean, Exception> remove = new StandardContinuation<Boolean, Boolean, Exception>(continuation) {
      @Override
      public void receiveResult(Boolean result) {
        if (storage.getTotalSize() > size) {
          Id last = eldest();
          if (last == null) {
            parent.receiveException(new IllegalStateException("Cache exceeds its size limit, but holds no evictable objects"));
            return;
          }
          uncache(last, this);
        } else {
          parent.receiveResult(Boolean.valueOf(true));
        }
      }
    };

    remove.receiveResult(Boolean.TRUE);
  }

  /**
   * Returns the size of the given object, in bytes.
   *
   * @param obj The object to determine the size of
   * @return The size, in bytes
   */
  private int getSize(Object obj) {
    try {
      return ObjectSizes.sizeOf(obj);
    } catch (RuntimeException e) {
      Logger logger = environment.getLogManager().getLogger(LRUCache.class, null);
      logger.warn("", e);
      // returns maximum value here, so it won't be cached
      return Integer.MAX_VALUE;
    }
  }

  /**
   * Method which is used to erase all data stored in the Cache.
   * Use this method with care!
   *
   * @param continuation The command to run once done
   */
  @Override
  public void flush(Continuation<? super Boolean, Exception> continuation) {
    storage.flush(new StandardContinuation<Boolean, Boolean, Exception>(continuation) {
      @Override
      public void receiveResult(Boolean result) {
        synchronized (order) {
          order.clear();
        }
        parent.receiveResult(Boolean.TRUE);
      }
    });
  }
}
