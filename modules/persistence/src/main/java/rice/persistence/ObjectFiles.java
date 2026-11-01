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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Id;

/**
 * The object operations of {@link PersistentStorage} on disk. They run on the blocking IO thread; a write
 * goes to a temporary file first that is then renamed, so that a crash leaves either the old or the new
 * file.
 */
final class ObjectFiles {

  private final StorageLayout layout;
  private final ObjectFileFormat format;
  private final MetadataIndex metadataIndex; // null without indexing
  private final StorageQuota quota;
  private final StorageStatistics stats;
  private final String name;
  private final TimeSource timeSource;
  private final Logger logger;

  ObjectFiles(StorageLayout layout, ObjectFileFormat format, MetadataIndex metadataIndex, StorageQuota quota, StorageStatistics stats, String name, TimeSource timeSource, Logger logger) {
    this.layout = layout;
    this.format = format;
    this.metadataIndex = metadataIndex;
    this.quota = quota;
    this.stats = stats;
    this.name = name;
    this.timeSource = timeSource;
    this.logger = logger;
  }

  Boolean rename(final Id oldId, final Id newId) throws Exception {
    stats.countRename();

    Path f = layout.getFile(oldId);

    if ((f != null) && Files.exists(f)) {
      Path g = layout.getFile(newId);
      StorageFiles.renameFile(f, g);

      layout.checkDirectory(g.getParent());

      if (metadataIndex != null) {
        metadataIndex.rename(oldId, newId);
      }

      return Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  Boolean store(final Id id, final Object metadata, final Object obj) throws Exception {
    stats.countWrite();

    logger.atTrace().log(() -> "Storing object " + obj + " under id " + id.toStringFull() + " in root " + layout.getAppDirectory());

    /* first, create a temporary file */
    Path objFile = layout.getFile(id);
    Path transcFile = layout.makeTemporaryFile(id);

    logger.trace("Writing object {} to temporary file {} and renaming to {}", obj, transcFile, objFile);

    /* next, write out the data to a new copy of the original file */
    try {
      format.writeObject(obj, metadata, id, timeSource.currentTimeMillis(), transcFile);
      logger.atTrace().log(() -> "Done writing object " + obj + " under id " + id.toStringFull() + " in root " + layout.getAppDirectory());

      /* abort if this will put us over quota */
      if (quota.getUsedSpace() + StorageFiles.getFileLength(transcFile) > quota.getStorageSize())
        throw new OutofDiskSpaceException();
    } catch (Exception e) {
      /* if an IOException is thrown, delete the temporary file and abort */
      logger.warn("", e);
      StorageFiles.deleteFile(transcFile);
      throw e;
    }

    logger.atTrace().log(() -> "COUNT: Storing data of class " + obj.getClass().getName() + " under " + id.toStringFull() + " of size " + StorageFiles.getFileLength(transcFile) + " in " + name);

    /* recalculate amount used */
    quota.decreaseUsedSpace(StorageFiles.getFileLength(objFile));
    quota.increaseUsedSpace(StorageFiles.getFileLength(transcFile));

    /* now, rename the temporary file to be the real file */
    StorageFiles.renameFile(transcFile, objFile);

    /* mark the metadata cache of this file to be dirty */
    if (metadataIndex != null) {
      metadataIndex.put(id, metadata, objFile.getParent());
    }

    /* finally, check to see if this directory needs to be split */
    layout.checkDirectory(objFile.getParent());

    return Boolean.TRUE;
  }

  Boolean unstore(final Id id) throws Exception {
    stats.countDelete();

    /* first get the file */
    Path objFile = layout.getFile(id);

    if (objFile == null)
      return Boolean.FALSE;

    logger.atTrace().log(() -> "COUNT: Unstoring data under " + id.toStringFull() + " of size " + StorageFiles.getFileLength(objFile) + " in " + name);

    /* remove id from stored list */
    if (metadataIndex != null) {
      metadataIndex.remove(id, objFile.getParent());
    }

    /* check to make sure file exists */
    if (! Files.exists(objFile))
      return Boolean.FALSE;

    /* record the space collected and delete the file */
    quota.decreaseUsedSpace(StorageFiles.getFileLength(objFile));
    StorageFiles.deleteFile(objFile);

    return Boolean.TRUE;
  }

  Boolean setMetadata(final Id id, final Object metadata) throws Exception {
    stats.countMetadataWrite();

    logger.atTrace().log(() -> "COUNT: Updating metadata for " + id.toStringFull() + " in " + name);

    /* write the metadata to the file */
    Path objFile = layout.getFile(id);
    format.writeMetadata(objFile, metadata);

    /* then update our cache */
    if (metadataIndex != null) {
      metadataIndex.put(id, metadata, objFile.getParent());
    }

    return Boolean.TRUE;
  }

  Object getObject(final Id id) throws Exception {
    stats.countRead();

    /* get the file */
    Path objFile = layout.getFile(id);

    try {
      /* and make sure that it exists */
      if ((objFile == null) || ! Files.exists(objFile))
        return null;

      logger.atTrace().log(() -> "COUNT: Fetching data under " + id.toStringFull() + " of size " + StorageFiles.getFileLength(objFile) + " in " + name);
      return format.readData(objFile);
    } catch (Exception e) {
      /* remove our index for this file */
      if (metadataIndex != null) {
        metadataIndex.remove(id, objFile.getParent());
      }

      /* if there's a problem, move the file to the lost+found */
      layout.moveToLost(objFile);

      throw e;
    }
  }

  Boolean flush() throws Exception {
    logger.trace("COUNT: Flushing all data in {}", name);

    flushDirectory(layout.getAppDirectory());
    return Boolean.TRUE;
  }

  /**
   * Method which recursively flushes a directory hierarchy, removing all data.  Be careful!
   *
   * @param dir The directory to flush
   */
  private void flushDirectory(Path dir) throws IOException {
    logger.debug("Flushing file {}", dir);

    if (! Files.isDirectory(dir)) {
      Id id = layout.readKey(dir);

      /* remove id from stored list */
      if (metadataIndex != null) {
        metadataIndex.remove(id);
      }

      /* record the space collected and delete the file */
      quota.decreaseUsedSpace(StorageFiles.getFileLength(dir));
      StorageFiles.deleteFile(dir);
    } else {
      Path[] dirs = StorageFiles.list(dir, entry -> true);

      /* remove all subdirectories */
      for (int i=0; i<dirs.length; i++) {
        flushDirectory(dirs[i]);

        /* update the metadata */
        layout.forgetDirectory(dirs[i]);

        /* delete the dir */
        StorageFiles.deleteFile(dirs[i]);
      }
    }
  }

  private static class PersistenceException extends Exception {
  }

  private static class OutofDiskSpaceException extends PersistenceException {
  }
}
