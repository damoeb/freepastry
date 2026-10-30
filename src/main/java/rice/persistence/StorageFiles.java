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

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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
  static void createDirectory(Path directory) throws IOException {
    if ((directory == null) || (Files.exists(directory) && Files.isRegularFile(directory)))
      throw new IOException("Creation of directory " + directory + " failed!");

    if (! Files.exists(directory)) {
      try {
        Files.createDirectories(directory);
      } catch (IOException e) {
        throw new IOException("Creation of directory " + directory + " failed!", e);
      }
    }
  }

  /**
   * Removes an empty directory given its name
   *
   * @param directory The directory to be deleted
   * @throws IOException if the directory is not empty or cannot be deleted
   */
  static void deleteDirectory(Path directory) throws IOException {
    if ((directory != null) && Files.exists(directory)) {
      try (Stream<Path> entries = Files.list(directory)) {
        if (entries.findAny().isPresent())
          throw new IOException("Cannot delete " + directory + " - directory is not empty!");
      }

      try {
        Files.delete(directory);
      } catch (IOException e) {
        throw new IOException("Deletion of directory " + directory + " failed!", e);
      }
    }
  }

  /**
   *
   * Returns the length of a file in bytes
   *
   * @param file file whose length to get
   * @return the length, or 0 if the file does not exist or cannot be read, as File.length() did
   */
  static long getFileLength(Path file) {
    if ((file == null) || ! Files.exists(file))
      return 0;

    try {
      return Files.size(file);
    } catch (IOException e) {
      return 0;
    }
  }

  /**
   * Returns the time a file was last modified
   *
   * @param file the file
   * @return the time in millis, or 0 if the file does not exist or cannot be read, as File.lastModified() did
   */
  static long lastModified(Path file) {
    try {
      return Files.getLastModifiedTime(file).toMillis();
    } catch (IOException e) {
      return 0;
    }
  }

  /**
   * Renames a given file on disk.
   *
   * @param oldFile The old name
   * @param newFile The new name
   */
  static void renameFile(Path oldFile, Path newFile) throws IOException {
    if ((oldFile != null) && Files.exists(oldFile) && (! oldFile.equals(newFile))) {
      deleteFile(newFile);

      try {
        Files.move(oldFile, newFile, ATOMIC_MOVE);
      } catch (IOException e) {
        throw new IOException("Rename of " + oldFile + " to " + newFile + " failed!", e);
      }
    }
  }

  /**
   * deletes a given file from disk
   *
   * @param file file to be deleted.
   *
   */
  static void deleteFile(Path file) throws IOException {
    if (file == null)
      return;

    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      throw new IOException("Delete of " + file + " failed!", e);
    }
  }

  /**
   * Lists the entries of a directory that the filter accepts.
   *
   * @param dir the directory
   * @param filter the entries to return
   * @return the entries, resolved against dir
   * @throws IOException if the directory cannot be listed
   */
  static Path[] list(Path dir, DirectoryStream.Filter<Path> filter) throws IOException {
    List<Path> result = new ArrayList<Path>();
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir, filter)) {
      for (Path entry : entries)
        result.add(entry);
    }
    return result.toArray(new Path[0]);
  }

  /**
   * Lists the names of the entries of a directory that the filter accepts.
   *
   * @param dir the directory
   * @param filter the entries to return
   * @return the names of the entries
   * @throws IOException if the directory cannot be listed
   */
  static String[] listNames(Path dir, DirectoryStream.Filter<Path> filter) throws IOException {
    Path[] entries = list(dir, filter);
    String[] result = new String[entries.length];
    for (int i = 0; i < entries.length; i++)
      result[i] = entries[i].getFileName().toString();
    return result;
  }
}
