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

import static rice.persistence.PersistentStorage.METADATA_FILENAME;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import rice.p2p.commonapi.Id;

/**
 * Initialization and crash recovery of {@link PersistentStorage}: creates the directories, moves temporary
 * and broken files to lost+found, moves files that are in the wrong directory, resolves conflicts between
 * two versions of a file, rebuilds the metadata index and adds up the space in use.
 */
final class StorageRecovery {

  private final StorageLayout layout;
  private final MetadataIndex metadataIndex; // null without indexing
  private final StorageQuota quota;
  private final Logger logger;

  StorageRecovery(StorageLayout layout, MetadataIndex metadataIndex, StorageQuota quota, Logger logger) {
    this.layout = layout;
    this.metadataIndex = metadataIndex;
    this.quota = quota;
    this.logger = logger;
  }

  /**
   * Perform all the miscealanious house keeping that must be done
   * when we start up
   */
  void init() throws IOException {
    logger.info("Initing directories");
    layout.initDirectories();
    logger.info("Initing directory map");
    layout.initDirectoryMap(layout.getAppDirectory());
    logger.info("Initing files");
    initFiles(layout.getAppDirectory());
    logger.info("Initing file map");
    initFileMap(layout.getAppDirectory());
    logger.info("Syncing metadata");
    if (metadataIndex != null)
      metadataIndex.writeDirty(layout::getRangeForDirectory);
    logger.info("Done initing");
  }

  /**
   * Ensures that all files are in the correct directories, and moves files which
   * are in the wrong directory.  Also checks for any temporary files and resolves
   * any detected conflicts.
   *
   * @param dir The directory to start on
   */
  private void initFiles(File dir) throws IOException {
    String[] dirs = dir.list(new StorageLayout.DirectoryFilter());
    String[] files = dir.list(new StorageLayout.FileFilter());

    /* first, init any files, and relocate any files if there are dirs */
    for (int i=0; i<files.length; i++) {
      try {
        if (! initTemporaryFile(dir, files[i])) {

          /* if there are directories in the dir, then move the file */
          if (dirs.length > 0)
            moveFileToCorrectDirectory(dir, files[i]);
        }
      } catch (Exception e) {
        logger.warn("Got exception {} initting file {} - moving to lost+found.", e, files[i], e);
        layout.moveToLost(new File(dir, files[i]));
      }
    }

    /* next, recurse into any dirs */
    for (int i=0; i<dirs.length; i++)
      initFiles(new File(dir, dirs[i]));

    /* and delete the old metadata file, if it exists */
    if (dirs.length > 0)
      StorageFiles.deleteFile(new File(dir, METADATA_FILENAME));
  }

  /**
   * Method which initializes a temporary file by doing the following:
   *   1.  If this file is not a temporary file, simply returns the file
   *   2.  If so, it simply moves this file to the lost and found and returns null
   *
   * This is done since all temporary files are, by definition, temporary
   * until they are renamed to an actual ID name.  Thus, if we detect a
   * temporary file during start-up, it is likely that the file is corrupted
   * and should be deleted.
   *
   * @returns Whether or not the file was a temporary file
   */
  private boolean initTemporaryFile(File parent, String name) throws IOException {
    if (! layout.isTemporaryFile(name))
      return false;

    layout.moveToLost(new File(parent, name));
    return true;
  }

  /**
   * Inititializes the idSet data structure
   *
   * In doing this it must resolve conflicts and aborted
   * transactions. After this is run the most current stable
   * state should be restored.  Also record the total used space
   * for all files in the root.
   * Lastly, deletes any files which are of zero length.
   *
   */
  private void initFileMap(File dir) throws IOException {
    logger.debug("Initting directory {}", dir);

    /* first, see if this directory needs to be expanded */
    layout.checkDirectory(dir);

    /* make sure the directory was not pruned */
    if (! dir.exists())
      return;

    /* now, read the metadata file in this directory */
    long modified = 0;

    if (metadataIndex != null) {
      try {
        modified = metadataIndex.readMetadataFile(dir, layout::getRangeForDirectory, layout::getPrefix);
      } catch (IOException e) {
        logger.error("Got exception {} reading metadata file - regenerating", e, e);
      }
    }

    /* next, start processing by listing the number of files and going from there */
    File[] files = dir.listFiles(new StorageLayout.FileFilter());
    File[] dirs = dir.listFiles(new StorageLayout.DirectoryFilter());

    for (int i=0; i<files.length; i++) {
      try {
        Id id = layout.readKey(files[i]);
        long len = StorageFiles.getFileLength(files[i]);

        if (id == null)
          logger.info("READING {} RETURNED NULL!", files[i]);

        if (len > 0) {
          quota.increaseUsedSpace(len);

          /* if the file is newer than the metadata file, update the metadata
          if we don't have the metadata for this file, update it */
          if ((metadataIndex != null) && ((! metadataIndex.containsUnlocked(id)) || (files[i].lastModified() > modified))) {
            if (logger.isTraceEnabled()) logger.trace("Reading newer metadata out of file " + files[i] + " id " + id.toStringFull() + " " + files[i].lastModified() + " " + modified + " " + metadataIndex.containsUnlocked(id));
            metadataIndex.putUnlocked(id, ObjectFileFormat.readMetadata(files[i], logger));
            metadataIndex.markDirtyUnlocked(dir);
          }
        } else {
          layout.moveToLost(files[i]);

          if ((metadataIndex != null) && metadataIndex.containsUnlocked(id)) {
            metadataIndex.removeUnlocked(id);
            metadataIndex.markDirtyUnlocked(dir);
          }
        }
      } catch (Exception e) {
        logger.warn("ERROR: Received Exception {} while initing file {} - moving to lost+found.", e, files[i], e);
        layout.moveToLost(files[i]);
      }
    }

    /* now recurse and check all of the children */
    for (int i=0; i<dirs.length; i++)
      initFileMap(dirs[i]);

    /* and finally see if this directory needs to be pruned or expanded */
    layout.checkDirectory(dir);
  }

  /**
   * Resolves a conflict between the two provided files by picking the
   * newer one and renames it to the given output file.
   *
   * @param file1 The first file
   * @param file2 The second file
   * @param output The file to store the result in
   */
  private void resolveConflict(File file1, File file2, File output) throws IOException {
    if (! file2.exists()) {
      StorageFiles.renameFile(file1, output);
    } else if (! file1.exists()) {
      StorageFiles.renameFile(file2, output);
    } else if (file1.equals(file2)) {
      StorageFiles.renameFile(file1, output);
    } else {
      logger.debug("resolving conflict between {} and {}", file1, file2);

      if (ObjectFileFormat.readVersion(file1) < ObjectFileFormat.readVersion(file2)) {
        layout.moveToLost(file1);
        StorageFiles.renameFile(file2, output);
      } else {
        layout.moveToLost(file2);
        StorageFiles.renameFile(file1, output);
      }
    }
  }

  /**
   * Takes a file and moves it to the correct directory
   * this is used in conjunction with expand to move files to their correct
   * subdirectories below the expanded directory
   *
   * @param parent The directory the file is in
   * @param name The name of the file to be moved
   */
  private void moveFileToCorrectDirectory(File parent, String name) throws IOException {
    File file = new File(parent, name);
    Id id = ObjectFileFormat.readKeyFromFile(file);
    File dest = layout.getDirectoryForId(id);

    /* if it's in the wrong directory, then move it and resolve the conflict if necessary */
    if (! dest.equals(parent)) {
      logger.debug("moving file {} to correct directory {} from {}", file, dest, parent);
      File other = new File(dest, id.toStringFull().substring(layout.getPrefix(dest).length()));
      resolveConflict(file, other, other);
      layout.checkDirectory(dest);
    }
  }
}
