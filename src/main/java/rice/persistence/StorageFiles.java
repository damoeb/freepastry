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

import java.io.File;
import java.io.IOException;

/**
 * File system primitives of {@link PersistentStorage} that report failures as IOExceptions.
 */
final class StorageFiles {

  private StorageFiles() {
  }

  /**
   * Create a directory given its name
   *
   * @param directory The directory to be created
   * @throws IOException if the directory cannot be created
   */
  static void createDirectory(File directory) throws IOException {
    if ((directory == null) || (directory.exists() && directory.isFile()) || (! (directory.exists()) && (! directory.mkdirs())))
      throw new IOException("Creation of directory " + directory + " failed!");
  }

  /**
   * Removes an empty directory given its name
   *
   * @param directory The directory to be deleted
   * @throws IOException if the directory is not empty or cannot be deleted
   */
  static void deleteDirectory(File directory) throws IOException {
    if ((directory != null) && directory.exists()) {
      if (directory.listFiles().length > 0)
        throw new IOException("Cannot delete " + directory + " - directory is not empty!");

      if (! directory.delete())
        throw new IOException("Deletion of directory " + directory + " failed!");
    }
  }

  /**
   *
   * Returns the length of a file in bytes
   *
   * @param file file whose length to get
   */
  static long getFileLength(File file) {
    return (((file != null) && file.exists()) ? file.length() : 0);
  }

  /**
   * Renames a given file on disk.
   *
   * @param oldFile The old name
   * @param newFile The new name
   */
  static void renameFile(File oldFile, File newFile) throws IOException {
    if ((oldFile != null) && oldFile.exists() && (! oldFile.equals(newFile))) {
      deleteFile(newFile);

      if (! oldFile.renameTo(newFile))
        throw new IOException("Rename of " + oldFile + " to " + newFile + " failed!");
    }
  }

  /**
   * deletes a given file from disk
   *
   * @param file file to be deleted.
   *
   */
  static void deleteFile(File file) throws IOException {
    if ((file != null) && file.exists() && (! file.delete()))
      throw new IOException("Delete of " + file + " failed!");
  }
}
