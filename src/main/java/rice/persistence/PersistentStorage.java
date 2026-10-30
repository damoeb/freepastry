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
 * @(#) PersistenceManager.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 *
 * @version $Id: PersistentStorage.java 4654 2009-01-08 16:33:07Z jeffh $
 */
import java.io.File;
import java.io.IOException;
import java.util.SortedMap;

import org.slf4j.Logger;
import rice.Continuation;
import rice.Continuation.ListenerContinuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.util.ConcurrentRingMap;

/**
 * This class is an implementation of Storage which provides
 * persistent storage to disk.  This class also guarantees that
 * the data will be consistent, even after a crash.  This class
 * also provides these services in a non-blocking fashion by
 * submitting WorkRequests to the environment's blocking-IO thread
 * (Processor.processBlockingIO()), which writes the data to disk; the
 * results are delivered on the selector thread.
 *
 * This class was initially designed to support only Ids whose toString()
 * method returns a String of constant length.  It has been extended to
 * support variable-length toString()s, but we have the caveat that
 * not toString() can be a substring of another toString() - this
 * will cause undefined behavior.  <b>Additionally, the toString() method on the key Ids
 * *CANNOT* have the period ('.') or exclamation point ('!')
 * characters in them - these are used for internal purposes.</b>
 *
 * The objects and their metadata are written by the {@link StorageSerializer} the storage is built with,
 * normally with the raw serialization the wire uses, and stored gzipped; the format of the file is
 * described in {@link ObjectFileFormat}, that of the per-directory metadata cache in {@link MetadataIndex}.
 * Files in another format (version 2 held Java-serialized XML) are not read.
 *
 * Persistence also supports the metadata interface specified in the
 * Catalog interface.  All metadata is guaranteed to be stored
 * in memory, so fetching the metadata of a given key is an efficient
 * operation.
 */
@SuppressWarnings("unchecked")
public class PersistentStorage implements Storage {

  /**
   * Static variables defining the layout of the on-disk storage
   */
  public static final long PERSISTENCE_MAGIC_NUMBER = 8038844221L;
  /** Version 3: raw serialization through a {@link StorageSerializer} (version 2 was Java-serialized XML). */
  public static final long PERSISTENCE_VERSION_3 = 3L;
  public static final long PERSISTENCE_REVISION_3_0 = 0L;

  /**
   * Static variables which define the location of the storage root
   */
  public static final String BACKUP_DIRECTORY = "/FreePastry-Storage-Root/";
  public static final String LOST_AND_FOUND_DIRECTORY = "lost+found";
  public static final String METADATA_FILENAME = "metadata.cache";

  /**
   * The splitting factor, or the number of files in one directory
   */
  public static final int MAX_FILES = 256;

  /**
   * The maximum number of subdirectories in a directory before splitting
   */
  public static final int MAX_DIRECTORIES = 32;

  /**
   * The amount of time before re-writing the metadata file
   */
  public static final int METADATA_SYNC_TIME = 300000;

  /**
   * Special placeholder for the file whose name should be zero-length
   */
   public static final String ZERO_LENGTH_NAME = "!";

  private IdFactory factory;        // the factory used for creating ids

  private String name;              // the name of this instance

  private boolean index;            // whether or not we are indexing the objects
  private final StorageLayout layout; // the directory tree
  private final ObjectFiles objectFiles; // the object operations on disk
  private final BlockingIO blockingIO; // runs them on the blocking IO thread

  private MetadataIndex metadataIndex; // the in-memory cache of object metadata (if indexing)

  private String rootDir;           // rootDirectory

  private final StorageQuota quota; // the quota and the space in use
  private final StorageStatistics stats; // the request counts

  Environment environment;
  Logger logger;

 /**
  * Builds a PersistentStorage given a root directory in which to
  * persist the data. Uses a default instance name.
  *
  * @param factory The factory to use for creating Ids.
  * @param serializer writes the objects and metadata to disk and reads them back
  * @param rootDir The root directory of the persisted disk.
  * @param size the size of the storage in bytes, or -1 for unlimited
  */
  public PersistentStorage(IdFactory factory, StorageSerializer serializer, String rootDir, long size, Environment env) throws IOException {
    this(factory, serializer, "default", rootDir, size, env);
  }

 /**
  * Builds a PersistentStorage given and an instance name
  *  and a root directory in which to persist the data.
  *
  * @param factory The factory to use for creating Ids.
  * @param serializer writes the objects and metadata to disk and reads them back
  * @param name the name of this instance
  * @param rootDir The root directory of the persisted disk.
  * @param size the size of the storage in bytes, or -1 for unlimited
  */
  public PersistentStorage(IdFactory factory, StorageSerializer serializer, String name, String rootDir, long size, Environment env) throws IOException {
    this(factory, serializer, name, rootDir, size, true, env);
  }

  /**
   * Builds a PersistentStorage given and an instance name
   *  and a root directory in which to persist the data.
   *
   * @param factory The factory to use for creating Ids.
   * @param serializer writes the objects and metadata to disk and reads them back
   * @param name the name of this instance
   * @param rootDir The root directory of the persisted disk.
   * @param size the size of the storage in bytes, or -1 for unlimited
   * @param index Whether or not to index the objects
   */
  public PersistentStorage(IdFactory factory, StorageSerializer serializer, String name, String rootDir, long size, boolean index, Environment env) throws IOException {
    this.environment = env;
    logger = environment.getLogManager().getLogger(PersistentStorage.class, null);
    this.factory = factory;
    this.name = name;
    this.rootDir = rootDir;
    this.quota = new StorageQuota(size);
    this.index = index;
    this.stats = new StorageStatistics(name, environment.getTimeSource(), logger);
    if (index) {
      this.metadataIndex = new MetadataIndex(factory, serializer, logger);
    }

    this.layout = new StorageLayout(rootDir, name, factory, environment.getRandomSource(), metadataIndex, logger);

    logger.info("Launching persistent storage in {} with name {} spliting factor {}", rootDir, name, MAX_FILES);

    this.blockingIO = new BlockingIO(environment);
    ObjectFileFormat format = new ObjectFileFormat(factory, serializer, logger);
    this.objectFiles = new ObjectFiles(layout, format, metadataIndex, quota, stats, name, environment.getTimeSource(), logger);

    new StorageRecovery(layout, format, metadataIndex, quota, logger).init();
  }

  private void printStats() {
    stats.printStats(() -> (index ? "" + metadataIndex.keyCountUnlocked() : "?"), this::getTotalSize);
  }

  /**
   * Method which allows the persistence root to schedule an event which will tell it to
   * sync the metadata cached. Should be called exactly once after the persistence root is
   * created.
   *
   * @param timer The timer to use to schedule the events
   */
  public void setTimer(rice.selector.Timer timer) {
    if (index) {
      timer.scheduleAtFixedRate(new rice.selector.TimerTask() {
        public String toString() { return "persistence dirty purge enqueue"; }
        public void run() {
          blockingIO.run(() -> "persistence dirty purge", new ListenerContinuation<Boolean, Exception>("Enqueue of writeMetadataFile", environment), () -> {
            writeDirty();
            return Boolean.TRUE;
          });
        }
      }, environment.getRandomSource().nextInt(METADATA_SYNC_TIME), METADATA_SYNC_TIME);
    }
  }

  /**
   * Renames the given object to the new id.  This method is potentially faster
   * than store/cache and unstore/uncache.
   *
   * @param oldId The id of the object in question.
   * @param newId The new id of the object in question.
   * @param c The command to run once the operation is complete
   */
  public void rename(final Id oldId, final Id newId, Continuation<? super Boolean, Exception> c) {
    printStats();

    blockingIO.run(() -> "rename " + oldId + " " + newId, c, () -> objectFiles.rename(oldId, newId));
  }

  /**
   * Makes the object persistent to disk and stored permanantly
   *
   * If the object is already persistent, this method will
   * simply update the object's serialized image.
   *
   * This is implemented atomically so that this may succeed
   * and store the new object, or fail and leave the previous
   * object intact.
   *
   * This method completes by calling receiveResult() of the provided continuation
   * with the success or failure of the operation.
   *
   * @param obj The object to be made persistent.
   * @param id The object's id.
   * @param metadata The object's metadata
   * @param c The command to run once the operation is complete; receives <code>true</code> if the action succeeds, else <code>false</code>
   */
  public void store(final Id id, final Object metadata, final Object obj, Continuation<? super Boolean, Exception> c) {
    if (id == null || obj == null) {
      c.receiveResult(Boolean.valueOf(false));
      return;
    }

    printStats();

    blockingIO.run(() -> "store " + id, c, () -> objectFiles.store(id, metadata, obj));
  }

  /**
   * Request to remove the object from the list of persistend objects.
   * Delete the serialized image of the object from stable storage. If
   * necessary. If the object was not in the cached list in the first place,
   * nothing happens and <code>false</code> is returned.
   *
   * This method also guarantees that the data on disk will remain consistent,
   * even after a crash by performing the delete atomically.
   *
   * This method completes by calling receiveResult() of the provided continuation
   * with the success or failure of the operation.
   *
   * @param id The object's persistence id
   * @param c The command to run once the operation is complete; receives <code>true</code> if the action succeeds, else <code>false</code>
   */
  public void unstore(final Id id, Continuation<? super Boolean, Exception> c) {
    printStats();

    blockingIO.run(() -> "unstore " + id, c, () -> objectFiles.unstore(id));
  }

  /**
   * Returns whether or not an object is present in the location <code>id</code>.
   *
   * @param id The id of the object in question.
   * @return Whether or not an object is present at id.
   */
  public boolean exists(Id id) {
    if (index) {
      return metadataIndex.exists(id);
    } else {
      throw new UnsupportedOperationException("exists() not supported without indexing");
    }
  }

  /**
   * Returns the metadata associated with the provided object, or null if
   * no metadata exists.  The metadata must be stored in memory, so this
   * operation is guaranteed to be fast and non-blocking.
   *
   * @param id The id for which the metadata is needed
   * @return The metadata, or null of non exists
   */
  public Object getMetadata(Id id) {
    if (index) {
      return metadataIndex.get(id);
    } else {
      throw new UnsupportedOperationException("getMetadata() not supported without indexing");
    }
  }

  /**
   * Updates the metadata stored under the given key to be the provided
   * value.  As this may require a disk access, the requestor must
   * also provide a continuation to return the result to.
   *
   * @param id The id for the metadata
   * @param metadata The metadata to store
   * @param c The command to run once the operation is complete
   */
  public void setMetadata(final Id id, final Object metadata, Continuation<? super Boolean, Exception> c) {
    printStats();

    if (! exists(id)) {
      c.receiveResult(Boolean.valueOf(false));
    } else {
      blockingIO.run(() -> "setMetadata " + id, c, () -> objectFiles.setMetadata(id, metadata));
    }
  }

  /**
   * Returns the object identified by the given id.
   *
   * @param id The id of the object in question.
   * @param c The command to run once the operation is complete; receives the object, or <code>null</code> if there is no corresponding object
   */
  public void getObject(final Id id, Continuation<? super Object, Exception> c) {
    printStats();

    if (index && (! exists(id))) {
      c.receiveResult(null);
    } else {
      blockingIO.run(() -> "getObject " + id, c, () -> objectFiles.getObject(id));
    }
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
   * @return The idset containing the keys
   */
  public IdSet scan(IdRange range){
    if (index) {
      if (range.isEmpty())
        return factory.buildIdSet();
      else if (range.getCCWId().equals(range.getCWId()))
        return scan();
      else
        return factory.buildIdSet(metadataIndex.keySubMap(range.getCCWId(), range.getCWId()));
    } else {
      throw new UnsupportedOperationException("scan() not supported without indexing");
    }
  }

  /**
   * Return the objects identified by the given range of ids. The IdSet
   * returned contains the Ids of the stored objects. The range is
   * partially inclusive, the lower range is inclusive, and the upper
   * exclusive.
   *
   * NOTE: This method blocks so if the behavior of this method changes and
   * uses the disk, this method may be deprecated.
   *
   * @return The idset containing the keys
   */
  public IdSet scan() {
    if (index) {
      return factory.buildIdSet(metadataIndex.keyMap());
    } else {
      throw new UnsupportedOperationException("scan() not supported without indexing");
    }
  }

  /**
   * Returns a map which contains keys mapping ids to the associated
   * metadata.
   *
   * @param range The range to query
   * @return The map containing the keys
   */
  public SortedMap<Id, Object> scanMetadata(IdRange range) {
    if (index) {
      if (range.isEmpty())
        return new ConcurrentRingMap<Id, Object>();
      else if (range.getCCWId().equals(range.getCWId()))
        return scanMetadata();
      else
        return metadataIndex.keySubMap(range.getCCWId(), range.getCWId());
    } else {
      throw new UnsupportedOperationException("scanMetadata() not supported without indexing");
    }
  }

  /**
   * Returns a map which contains keys mapping ids to the associated
   * metadata.
   *
   * @return The treemap mapping ids to metadata
   */
  public SortedMap<Id, Object> scanMetadata() {
    if (index) {
      return metadataIndex.keyMapUnlocked();
    } else {
      throw new UnsupportedOperationException("scanMetadata() not supported without indexing");
    }
  }

  /**
   * Returns the submapping of ids which have metadata less than the provided
   * value.
   *
   * @param value The maximal metadata value
   * @return The submapping
   */
  public SortedMap<Id, Object> scanMetadataValuesHead(Object value) {
    if (index) {
      return metadataIndex.valueHeadMapUnlocked(value);
    } else {
      throw new UnsupportedOperationException("scanMetadataValuesHead() not supported without indexing");
    }
  }

  /**
   * Returns the submapping of ids which have metadata null
   *
   * @return The submapping
   */
  public SortedMap<Id, Object> scanMetadataValuesNull() {
    if (index) {
      return metadataIndex.valueNullMapUnlocked();
    } else {
      throw new UnsupportedOperationException("scanMetadataValuesNull() not supported without indexing");
    }
  }

  /**
   * Returns the total size of the stored data in bytes.
   *
   * @return The total size, in bytes, of data stored.
   */
  public long getTotalSize(){
    return quota.getUsedSpace();
  }

  /**
   * Returns the number of Ids currently stored in the catalog
   *
   * @return The number of ids in the catalog
   */
  public int getSize() {
    if (index) {
      return metadataIndex.sizeUnlocked();
    } else {
      throw new UnsupportedOperationException("getSize() not supported without indexing");
    }
  }

  /**
   * Method which is used to erase all data stored in the Catalog.
   * Use this method with care!
   *
   * @param c The command to run once done
   */
  public void flush(Continuation<? super Boolean, Exception> c) {
    blockingIO.run(() -> "flush", c, () -> objectFiles.flush());
  }

  /*****************************************************************/
  /* Helper functions for Directory Splitting Management           */
  /*****************************************************************/

  /*****************************************************************/
  /* Helper functions for File Management                          */
  /*****************************************************************/


  /*****************************************************************/
  /* Helper functions for Metadata Storage                         */
  /*****************************************************************/

  /**
   * Function which writes out all of the dirty metadata files and marks
   * them as clean.
   */
  protected void writeDirty() {
    metadataIndex.writeDirty(layout::getRangeForDirectory);
  }


  /*****************************************************************/
  /* Helper functions for Object Input/Output                      */
  /*****************************************************************/


  /**
   * Utility function which returns the range of keys which a directory
   * corresponds to.
   *
   * @param dir The directory
   */
  protected IdRange getRangeForDirectory(File dir) {
    return layout.getRangeForDirectory(dir);
  }

  /*****************************************************************/
  /* Functions for Configuration Management                        */
  /*****************************************************************/


  /**
   * Sets the root directory that the persistence Manager uses
   *
   * @param dir the String representing the directory to use
   * @return boolean, true if the operation succeeds false if it doesn't
   */
  public boolean setRoot(String dir) {
    // TODO: files stored under the old root are neither moved nor reloaded
    rootDir = dir;
    return true;
  }

  /**
   * gets the root directory that the persistence Manager uses
   *
   * @return String the directory for the root
   */
  public String getRoot() {
    return rootDir;
  }

  /**
   * gets the amount of storage that the persistence Manager uses
   *
   * @return int the amount of storage in bytes allocated for use
   */
  public long getStorageSize() {
    return quota.getStorageSize();
  }

  /**
   * Sets the amount of storage that the persistence Manager uses
   *
   * @param size the amount of storage available to use in bytes
   * @return boolean, true if the operation succeeds false if it doesn't
   */
  public boolean setStorageSize(long size) {
    return quota.setStorageSize(size);
  }

  /**
   * Gets the name of this instance
   *
   * @return String the name of the instance
   *
   */
  public String getName(){
    return name;
  }

  /*****************************************************************/
  /* Inner Classes for FileName filtering                          */
  /*****************************************************************/

}
