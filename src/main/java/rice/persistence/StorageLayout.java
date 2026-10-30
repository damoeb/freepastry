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

import static rice.persistence.PersistentStorage.BACKUP_DIRECTORY;
import static rice.persistence.PersistentStorage.LOST_AND_FOUND_DIRECTORY;
import static rice.persistence.PersistentStorage.MAX_DIRECTORIES;
import static rice.persistence.PersistentStorage.MAX_FILES;
import static rice.persistence.PersistentStorage.METADATA_FILENAME;
import static rice.persistence.PersistentStorage.ZERO_LENGTH_NAME;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;

/**
 * The directory layout of {@link PersistentStorage}: an object is stored in a file named after its key, in a
 * tree of directories named after key prefixes. A directory with more than {@code MAX_FILES} files is split
 * into subdirectories, one with more than {@code MAX_DIRECTORIES} subdirectories is reformatted, and an empty
 * one is pruned. The tree is cached in memory.
 */
final class StorageLayout {

  private final String rootDir;           // rootDirectory
  private final String name;              // the name of this instance
  private final IdFactory factory;        // the factory used for creating ids
  private final RandomSource randomSource;
  private final MetadataIndex metadataIndex; // marks the directories created by a split dirty; null without indexing
  private final Logger logger;

  private Path rootDirectory;       // root directory to store stuff in
  private Path backupDirectory;     // dir for storing persistent objs
  private Path appDirectory;        // dir for storing persistent objs
  private Path lostDirectory;       // dir for lost objects

  private final HashMap<Path, Path[]> directories;      // the in-memory map of directories (for efficiency)
  private final HashMap<Path, String> prefixes;         // an in-memory cache of the directory prefixes

  StorageLayout(String rootDir, String name, IdFactory factory, RandomSource randomSource, MetadataIndex metadataIndex, Logger logger) {
    this.rootDir = rootDir;
    this.name = name;
    this.factory = factory;
    this.randomSource = randomSource;
    this.metadataIndex = metadataIndex;
    this.logger = logger;
    this.directories = new HashMap<Path, Path[]>();
    this.prefixes = new HashMap<Path, String>();
  }

  /**
   * @return the directory below which the objects are stored
   */
  Path getAppDirectory() {
    return appDirectory;
  }

  /**
   * Forgets a directory that has been deleted.
   */
  void forgetDirectory(Path dir) {
    directories.remove(dir);
    prefixes.remove(dir);
  }

  /**
   * Verify that the directory name passed to the
   * PersistenceManagerImpl constructor is valid and
   * creates the necessary subdirectories.
   *
   * @throws IOException if a directory cannot be created
   */
  void initDirectories() throws IOException {
    rootDirectory = Path.of(rootDir);
    StorageFiles.createDirectory(rootDirectory);

    backupDirectory = rootDirectory.resolve(BACKUP_DIRECTORY);
    StorageFiles.createDirectory(backupDirectory);

    appDirectory = backupDirectory.resolve(name);
    StorageFiles.createDirectory(appDirectory);

    lostDirectory = backupDirectory.resolve(LOST_AND_FOUND_DIRECTORY);
    StorageFiles.createDirectory(lostDirectory);
  }

  /**
   * Reads in the in-memory map of directories for use.
   *
   * @param dir The directory to recurse
   */
  void initDirectoryMap(Path dir) throws IOException {
    Path[] files = StorageFiles.list(dir, DIRECTORIES);
    directories.put(dir, files);

    for (int i=0; i<files.length; i++)
      initDirectoryMap(files[i]);
  }

  /**
   * Moves a file to the lost and found directory for this instance
   *
   * @param file the file to be moved
   *
   */
  void moveToLost(Path file) throws IOException {
    StorageFiles.renameFile(file, lostDirectory.resolve(getPrefix(file.getParent()) + file.getFileName().toString()));
  }

  /**
   * Method which checks to see if a directory has too many files, and if so,
   * expands the directory in order to bring it to the correct size
   *
   * @param directory The directory to check
   * @return Whether or not the directory was modified
   */
  boolean checkDirectory(Path directory) throws IOException {
    int files = numFilesDir(directory);
    int dirs = numDirectoriesDir(directory);

    logger.debug("Checking directory {} for oversize {}/{}", directory, files, dirs);

    if (files > MAX_FILES) {
      expandDirectory(directory);
      return true;
    } else if (dirs > MAX_DIRECTORIES) {
      reformatDirectory(directory);
      return true;
    } else if ((files == 0) && (dirs == 0) && (! directory.equals(appDirectory))) {
      pruneDirectory(directory);
      return true;
    }

    return false;
  }

  /**
   * This method removes an empty directory from the storage root, and updates
   * all of the associated metadata.
   *
   * @param dir The directory to remove
   */
  private void pruneDirectory(Path dir) throws IOException {
    logger.debug("Pruning directory {} due to emptiness", dir);

    /* First delete the metadata file, if it exists */
    StorageFiles.deleteFile(dir.resolve(METADATA_FILENAME));

    /* Then remove the directory */
    StorageFiles.deleteDirectory(dir);

    /* Finally update the metadata */
    directories.remove(dir);
    prefixes.remove(dir);
    this.directories.put(dir.getParent(), StorageFiles.list(dir.getParent(), DIRECTORIES));
  }

  /**
   * This method expands the directory when there are too many subdirectories
   * in one directory.  Basically uses the same logic as expandDirectory(), but also
   * updates the metadata.
   *
   * This is used to keep less then NUM_FILES in any directory
   * at one time. This speeds up look up time in a particular directory
   * under certain circumstances.
   *
   * @param dir The directory to expand
   */
  private void reformatDirectory(Path dir) throws IOException {
    logger.debug("Expanding directory {} due to too many subdirectories", dir);
    /* first, determine what directories we should create (ignoring the ! directories) */
    String[] newDirNames = getDirectories(StorageFiles.listNames(dir, DIRECTORIES));
    reformatDirectory(dir, newDirNames);
    logger.debug("Done expanding directory {}", dir);
  }

  /**
   * This method performs a directory expansion, given the names of the
   * subdirectories to create.
   *
   * @param dir The directory to expand
   * @param newDirNames The array containing the names of the new directories
   */
  private void reformatDirectory(Path dir, String[] newDirNames) throws IOException {
    String[] dirNames = StorageFiles.listNames(dir, DIRECTORIES);
    Path[] newDirs = new Path[newDirNames.length];

    /* create the new directories, move the old ones */
    for (int i=0; i<newDirNames.length; i++) {
      newDirs[i] = dir.resolve(newDirNames[i]);
      StorageFiles.createDirectory(newDirs[i]);
      logger.debug("Creating directory {}", newDirNames[i]);

      /* now look through the original directory and move any matching dirs */
      String[] subDirNames = getMatchingDirectories(newDirNames[i], dirNames);
      Path[] newSubDirs = new Path[subDirNames.length];

      for (int j=0; j<subDirNames.length; j++) {
        /* move the directory */
        Path oldDir = dir.resolve(subDirNames[j]);
        newSubDirs[j] = newDirs[i].resolve(subDirNames[j].substring(newDirNames[i].length()));
        logger.debug("Moving the old direcotry {} to {}", oldDir, newSubDirs[j]);
        StorageFiles.renameFile(oldDir, newSubDirs[j]);

        /* remove the stale entry, add the new one */
        this.directories.remove(oldDir);
        this.directories.put(newSubDirs[j], new Path[0]);
      }

      this.directories.put(newDirs[i], newSubDirs);
    }

    /* lastly, update the root directory */
    this.directories.put(dir, newDirs);
  }

  /**
   * Returns the sublist of the provided list which starts with the
   * given prefix.
   *
   * @param prefix The prefix to look for
   * @param dirNames The list to search through
   * @return The sublist
   */
  private String[] getMatchingDirectories(String prefix, String[] dirNames) {
    List<String> result = new ArrayList<String>();

    for (int i=0; i<dirNames.length; i++)
      if (dirNames[i].startsWith(prefix))
        result.add(dirNames[i]);

    return (String[]) result.toArray(new String[0]);
  }

  /**
   * This method expands the directory in to a subdirectory
   * for each prefix, contained in the directory
   *
   * This is used to keep less then NUM_FILES in any directory
   * at one time. This speeds up look up time in a particular directory
   * under certain circumstances.
   *
   * @param dir The directory to expand
   */
  private void expandDirectory(Path dir) throws IOException {
    logger.debug("Expanding directory {} due to too many files", dir);
    /* first, determine what directories we should create */
    String[] fileNames = StorageFiles.listNames(dir, FILES);
    String[] dirNames = getDirectories(fileNames);
    Path[] dirs = new Path[dirNames.length];

    /* create the directories */
    for (int i=0; i<dirNames.length; i++) {
      dirs[i] = dir.resolve(dirNames[i]);
      directories.put(dirs[i], new Path[0]);

      if (Files.exists(dirs[i]) && Files.isRegularFile(dirs[i]))
        StorageFiles.renameFile(dirs[i], dir.resolve(dirs[i].getFileName().toString() + ZERO_LENGTH_NAME));

      StorageFiles.createDirectory(dirs[i]);
      logger.debug("Creating directory {}", dirNames[i]);

      /* mark this directory for metadata syncing */
      if (metadataIndex != null)
        metadataIndex.markDirtyUnlocked(dirs[i]);
    }

    /* add the list of directories to the map */
    directories.put(dir, dirs);

    /* last, move the files into the correct directory */
    Path[] files = StorageFiles.list(dir, FILES);
    for (int i = 0; i < files.length; i++) {
      for (int j = 0; j < dirs.length; j++) {
        if (files[i].getFileName().toString().startsWith(dirs[j].getFileName().toString())) {
          if (logger.isTraceEnabled()) logger.trace("Renaming file " + files[i] + " to " + dirs[j].resolve(files[i].getFileName().toString().substring(dirs[j].getFileName().toString().length())));
          StorageFiles.renameFile(files[i], dirs[j].resolve(files[i].getFileName().toString().substring(dirs[j].getFileName().toString().length())));
          break;
        }
      }
    }

    /* and remove the metadata file */
    StorageFiles.deleteFile(dir.resolve(METADATA_FILENAME));

    logger.debug("Done expanding directory {}", dir);
  }

  /**
   * This method takes in a list of the file names in a given directory which
   * needs to be expanded, and returns the lists of directories which should
   * be created for the expansion to happen.
   *
   * @param names The names to return the directories for
   * @return The directory names
   */
  private String[] getDirectories(String[] names) {
    int length = getPrefixLength(names);
    String prefix = names[0].substring(0, length);
    CharacterHashSet set = new CharacterHashSet();

    for (String name : names) {
      if (name.length() > length)
        set.put(name.charAt(length));
    }

    char[] splits = set.get();
    String[] result = new String[splits.length];

    for (int i=0; i<result.length; i++)
      result[i] = prefix + splits[i];

    return result;
  }

  /**
   * This method takes in the list of file names in a given directory and
   * returns the longest common prefix of all of the names.
   *
   * @param names The names to find the prefix of
   * @return The longest common prefix of all of the names
   */
  private int getPrefixLength(String[] names) {
    int length = names[0].length()-1;

    for (int i=0; i<names.length; i++)
      length = getPrefixLength(names[0], names[i], length);

    return length;
  }

  /**
   * Method which takes in two strings and returns the length of the
   * shared prefix, or a predefined maximum.
   *
   * @param a The first string
   * @param b The second string
   * @param max The maximum value to return
   */
  private int getPrefixLength(String a, String b, int max) {
    int i=0;

    for (; (i<a.length()-1) && (i<b.length()-1) && (i<max); i++)
      if (a.charAt(i) != b.charAt(i))
        return i;

    return i;
  }

  /**
   * Returns whether or not the given file is a temporary file.
   *
   * @param name The name of the file
   * @return Whether or not it is temporary
   */
  boolean isTemporaryFile(String name) {
    return (name.indexOf(".") >= 0);
  }

  /**
   * Generates a new file name to assign for a given id
   *
   * The name is the id's full string (without the directory prefix) followed by
   * a random suffix, which is redrawn while such a file exists.
   *
   * @param id the id to generate a name for
   * @return the new File
   */
  Path makeTemporaryFile(Id id) throws IOException {
    Path directory = getDirectoryForId(id);
    Path file = directory.resolve(id.toStringFull().substring(getPrefix(directory).length()) + "." + randomSource.nextInt() % 100);

    while (Files.exists(file))
      file = directory.resolve(id.toStringFull().substring(getPrefix(directory).length()) + "." + randomSource.nextInt() % 100);

    return file;
  }

  /**
   * Gets the file for a certain id from the disk
   *
   * @param id the id to get a file for
   * @return Path the file for the id
   */
  Path getFile(Id id) throws IOException {
    Path dir = getDirectoryForId(id);
    String name = id.toStringFull().substring(getPrefix(dir).length());
    if (name.equals("")) name = ZERO_LENGTH_NAME;

    // check for ! directory
    Path file = dir.resolve(name);
    if (Files.exists(file) && Files.isDirectory(file))
      file = file.resolve(ZERO_LENGTH_NAME);

    return file;
  }

  /**
   * Gets the directory an id should be stored in
   *
   * @param id the string representation of the id
   * @return Path the directory that should contain an id
   */
  Path getDirectoryForId(Id id) throws IOException {
    return getDirectoryForName(id.toStringFull());
  }

  /**
   * Gets the directory a given file should be stored in
   *
   * @param name The name of the file
   * @return Path the directory that should contain an id
   */
  private Path getDirectoryForName(String name) throws IOException {
    return getDirectoryForName(name, appDirectory);
  }

  /**
   * Gets the best directory for the given name
   *
   * @param name The name to serach for
   * @param dir The directory to start at
   * @return The directory the name should be stored in
   */
  private Path getDirectoryForName(String name, Path dir) throws IOException {
    Path[] subDirs = (Path[]) directories.get(dir);

    if (subDirs.length == 0) {
      return dir;
    } else {
      for (int i=0; i<subDirs.length; i++)
        if (name.startsWith(subDirs[i].getFileName().toString()))
          return getDirectoryForName(name.substring(subDirs[i].getFileName().toString().length()), subDirs[i]);
        else if ((name.length() == 0) && subDirs[i].getFileName().toString().equals(ZERO_LENGTH_NAME))
          return getDirectoryForName(name, subDirs[i]);

      /* here, we must create the appropriate directory */
      if ((name.length() >= subDirs[0].getFileName().toString().length()) || ((name.length() == 0) && (subDirs[0].getFileName().toString().length() == 1))) {
        Path newDir = dir.resolve((name.length() == 0 ? ZERO_LENGTH_NAME : name.substring(0, subDirs[0].getFileName().toString().length())));
        logger.debug("Necessarily creating dir {}", newDir.getFileName().toString());
        StorageFiles.createDirectory(newDir);
        this.directories.put(dir, append(subDirs, newDir));
        this.directories.put(newDir, new Path[0]);

        /* finally, we must check if this caused too many dirs in one dir.  If so, we
           simply rerun the algorithm which will reflect the new dir */
        if (checkDirectory(dir)) {
          return getDirectoryForName(name, dir);
        } else {
          return newDir;
        }
      } else {
        /* here, we have to handle a weird case where the filename is less than that of
        an existing directory.  To handle this, we pretend like we're doing a directory
        split, and create new subdirs so we can accommodate everything. */
        String[] dirs = new String[subDirs.length + 1];

        for (int i=0; i<subDirs.length; i++)
          dirs[i] = subDirs[i].getFileName().toString();
        dirs[subDirs.length] = (name.length() == 0 ? ZERO_LENGTH_NAME : name);

        /* now reformat the directory, creating an entry for this name */
        reformatDirectory(dir, getDirectories(dirs));

        /* and finally, recurse, which should find a directory for this name */
        return getDirectoryForName(name, dir);
      }
    }
  }

  /**
   * Internal method which returns the prefix of a given directory
   *
   * @param file The directory to return the prefix for
   * @return The prefix
   */
  String getPrefix(Path file) {
    if (prefixes.get(file) != null)
      return (String) prefixes.get(file);

    StringBuilder buffer = new StringBuilder();
    while (! file.equals(appDirectory)) {
      buffer.insert(0, file.getFileName().toString().replaceAll(ZERO_LENGTH_NAME, ""));
      file = file.getParent();
    }
    prefixes.put(file, buffer.toString());

    return getPrefix(file);
  }

  /**
   * Method which appends the given file to the list of files
   *
   * @param files The list of files
   * @param file The file
   * @return The new list
   */
  private Path[] append(Path[] files, Path file) {
    Path[] result = new Path[files.length + 1];

    for (int i=0; i<files.length; i++)
      result[i] = files[i];

    result[files.length] = file;
    return result;
  }

  /**
   * Gets the number of subdirectories in directory
   *
   * @param dir the directory to check
   * @return int the number of directories
   */
  private int numDirectoriesDir(Path dir) throws IOException {
    return StorageFiles.list(dir, DIRECTORIES).length;
  }

  /**
   * Gets the number of files in directory (excluding directories)
   *
   * @param dir the directory to check
   * @return int the number of files
   */
  private int numFilesDir(Path dir) throws IOException {
     return StorageFiles.list(dir, FILES).length;
  }

  /**
   * Utility function which returns the range of keys which a directory
   * corresponds to.
   *
   * @param dir The directory
   */
  IdRange getRangeForDirectory(Path dir) {
    String result = "";

    while (! dir.equals(appDirectory)) {
      result = dir.getFileName().toString() + result;
      dir = dir.getParent();
    }

    return factory.buildIdRangeFromPrefix(result);
  }

  /**
   * Reads the key of a file from its name and the names of its directories.
   *
   * @param file The file to create the key from.
   * @return The key that was read in
   */
  Id readKey(Path file) {
    String s = getPrefix(file.getParent()) + file.getFileName().toString().replaceAll(ZERO_LENGTH_NAME, "");

    if (s.indexOf(".") >= 0) {
      return factory.buildIdFromToString(s.toCharArray(), 0, s.indexOf("."));
    } else {
      return factory.buildIdFromToString(s.toCharArray(), 0, s.length());
    }
  }

  /**
   * Accepts only subdirectories.
   */
  static final DirectoryStream.Filter<Path> DIRECTORIES = Files::isDirectory;

  /**
   * Accepts only stored files: neither directories nor the metadata file.
   */
  static final DirectoryStream.Filter<Path> FILES = entry -> (! Files.isDirectory(entry)) && (! entry.getFileName().toString().equals(METADATA_FILENAME));

  /**
   * Class for a hashtable of primitive characters
   */
  private static class CharacterHashSet {
    private final boolean[] bitMap = new boolean[256];

    public void put(char a) {
      bitMap[(int) a] = true;
    }

    public char[] get() {
      int[] nums = getOffsets();
      char[] result = new char[nums.length];

      for (int i=0; i<result.length; i++)
        result[i] = (char) nums[i];

      return result;
    }

    private int[] getOffsets() {
      int[] result = new int[count()];
      int index = 0;

      for (int i=0; i<result.length; i++)
        result[i] = getOffset(i);

      return result;
    }

    private int getOffset(int index) {
      int location = 0;

      /* skip the first index-1 values */
      while (index > 0) {
        if (bitMap[location])
          index--;

        location++;
      }

      /* find the next true value */
      while (! bitMap[location])
        location++;

      return location;
    }

    private int count() {
      int total = 0;

      for (int i=0; i<bitMap.length; i++)
        if (bitMap[i])
          total++;

      return total;
    }
  }
}
