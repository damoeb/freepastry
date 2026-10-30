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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
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

  private File rootDirectory;       // root directory to store stuff in
  private File backupDirectory;     // dir for storing persistent objs
  private File appDirectory;        // dir for storing persistent objs
  private File lostDirectory;       // dir for lost objects

  private final HashMap<File, File[]> directories;      // the in-memory map of directories (for efficiency)
  private final HashMap<File, String> prefixes;         // an in-memory cache of the directory prefixes

  StorageLayout(String rootDir, String name, IdFactory factory, RandomSource randomSource, MetadataIndex metadataIndex, Logger logger) {
    this.rootDir = rootDir;
    this.name = name;
    this.factory = factory;
    this.randomSource = randomSource;
    this.metadataIndex = metadataIndex;
    this.logger = logger;
    this.directories = new HashMap<File, File[]>();
    this.prefixes = new HashMap<File, String>();
  }

  /**
   * @return the directory below which the objects are stored
   */
  File getAppDirectory() {
    return appDirectory;
  }

  /**
   * Forgets a directory that has been deleted.
   */
  void forgetDirectory(File dir) {
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
    rootDirectory = new File(rootDir);
    StorageFiles.createDirectory(rootDirectory);

    backupDirectory = new File(rootDirectory, BACKUP_DIRECTORY);
    StorageFiles.createDirectory(backupDirectory);

    appDirectory = new File(backupDirectory, name);
    StorageFiles.createDirectory(appDirectory);

    lostDirectory = new File(backupDirectory, LOST_AND_FOUND_DIRECTORY);
    StorageFiles.createDirectory(lostDirectory);
  }

  /**
   * Reads in the in-memory map of directories for use.
   *
   * @param dir The directory to recurse
   */
  void initDirectoryMap(File dir) {
    File[] files = dir.listFiles(new DirectoryFilter());
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
  void moveToLost(File file) throws IOException {
    StorageFiles.renameFile(file, new File(lostDirectory, getPrefix(file.getParentFile()) + file.getName()));
  }

  /**
   * Method which checks to see if a directory has too many files, and if so,
   * expands the directory in order to bring it to the correct size
   *
   * @param directory The directory to check
   * @return Whether or not the directory was modified
   */
  boolean checkDirectory(File directory) throws IOException {
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
  private void pruneDirectory(File dir) throws IOException {
    logger.debug("Pruning directory {} due to emptiness", dir);

    /* First delete the metadata file, if it exists */
    StorageFiles.deleteFile(new File(dir, METADATA_FILENAME));

    /* Then remove the directory */
    StorageFiles.deleteDirectory(dir);

    /* Finally update the metadata */
    directories.remove(dir);
    prefixes.remove(dir);
    this.directories.put(dir.getParentFile(), dir.getParentFile().listFiles(new DirectoryFilter()));
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
  private void reformatDirectory(File dir) throws IOException {
    logger.debug("Expanding directory {} due to too many subdirectories", dir);
    /* first, determine what directories we should create (ignoring the ! directories) */
    String[] newDirNames = getDirectories(dir.list(new DirectoryFilter()));
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
  private void reformatDirectory(File dir, String[] newDirNames) throws IOException {
    String[] dirNames = dir.list(new DirectoryFilter());
    File[] newDirs = new File[newDirNames.length];

    /* create the new directories, move the old ones */
    for (int i=0; i<newDirNames.length; i++) {
      newDirs[i] = new File(dir, newDirNames[i]);
      StorageFiles.createDirectory(newDirs[i]);
      logger.debug("Creating directory {}", newDirNames[i]);

      /* now look through the original directory and move any matching dirs */
      String[] subDirNames = getMatchingDirectories(newDirNames[i], dirNames);
      File[] newSubDirs = new File[subDirNames.length];

      for (int j=0; j<subDirNames.length; j++) {
        /* move the directory */
        File oldDir = new File(dir, subDirNames[j]);
        newSubDirs[j] = new File(newDirs[i], subDirNames[j].substring(newDirNames[i].length()));
        logger.debug("Moving the old direcotry {} to {}", oldDir, newSubDirs[j]);
        StorageFiles.renameFile(oldDir, newSubDirs[j]);

        /* remove the stale entry, add the new one */
        this.directories.remove(oldDir);
        this.directories.put(newSubDirs[j], new File[0]);
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
  private void expandDirectory(File dir) throws IOException {
    logger.debug("Expanding directory {} due to too many files", dir);
    /* first, determine what directories we should create */
    String[] fileNames = dir.list(new FileFilter());
    String[] dirNames = getDirectories(fileNames);
    File[] dirs = new File[dirNames.length];

    /* create the directories */
    for (int i=0; i<dirNames.length; i++) {
      dirs[i] = new File(dir, dirNames[i]);
      directories.put(dirs[i], new File[0]);

      if (dirs[i].exists() && dirs[i].isFile())
        StorageFiles.renameFile(dirs[i], new File(dir, dirs[i].getName() + ZERO_LENGTH_NAME));

      StorageFiles.createDirectory(dirs[i]);
      logger.debug("Creating directory {}", dirNames[i]);

      /* mark this directory for metadata syncing */
      if (metadataIndex != null)
        metadataIndex.markDirtyUnlocked(dirs[i]);
    }

    /* add the list of directories to the map */
    directories.put(dir, dirs);

    /* last, move the files into the correct directory */
    File[] files = dir.listFiles(new FileFilter());
    for (int i = 0; i < files.length; i++) {
      for (int j = 0; j < dirs.length; j++) {
        if (files[i].getName().startsWith(dirs[j].getName())) {
          if (logger.isTraceEnabled()) logger.trace("Renaming file " + files[i] + " to " + new File(dirs[j], files[i].getName().substring(dirs[j].getName().length())));
          StorageFiles.renameFile(files[i], new File(dirs[j], files[i].getName().substring(dirs[j].getName().length())));
          break;
        }
      }
    }

    /* and remove the metadata file */
    StorageFiles.deleteFile(new File(dir, METADATA_FILENAME));

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
  File makeTemporaryFile(Id id) throws IOException {
    File directory = getDirectoryForId(id);
    File file = new File(directory, id.toStringFull().substring(getPrefix(directory).length()) + "." + randomSource.nextInt() % 100);

    while (file.exists())
      file = new File(directory, id.toStringFull().substring(getPrefix(directory).length()) + "." + randomSource.nextInt() % 100);

    return file;
  }

  /**
   * Returns whether or not the provided file is an acestor of the other file.
   *
   * @param file The file to check
   * @param ancestor The potential ancestor
   * @return Whether or not the file is an ancestor
   */
  private boolean isAncestor(File file, File ancestor) {
    while ((file != null) && (! file.equals(ancestor))) {
      file = file.getParentFile();
    }

    return (file != null);
  }

  /**
   * Gets the file for a certain id from the disk
   *
   * @param id the id to get a file for
   * @return File the file for the id
   */
  File getFile(Id id) throws IOException {
    File dir = getDirectoryForId(id);
    String name = id.toStringFull().substring(getPrefix(dir).length());
    if (name.equals("")) name = ZERO_LENGTH_NAME;

    // check for ! directory
    File file = new File(dir, name);
    if (file.exists() && file.isDirectory())
      file = new File(file, ZERO_LENGTH_NAME);

    return file;
  }

  /**
   * Gets the directory an id should be stored in
   *
   * @param id the string representation of the id
   * @return File the directory that should contain an id
   */
  File getDirectoryForId(Id id) throws IOException {
    return getDirectoryForName(id.toStringFull());
  }

  /**
   * Gets the directory a given file should be stored in
   *
   * @param name The name of the file
   * @return File the directory that should contain an id
   */
  private File getDirectoryForName(String name) throws IOException {
    return getDirectoryForName(name, appDirectory);
  }

  /**
   * Gets the best directory for the given name
   *
   * @param name The name to serach for
   * @param dir The directory to start at
   * @return The directory the name should be stored in
   */
  private File getDirectoryForName(String name, File dir) throws IOException {
    File[] subDirs = (File[]) directories.get(dir);

    if (subDirs.length == 0) {
      return dir;
    } else {
      for (int i=0; i<subDirs.length; i++)
        if (name.startsWith(subDirs[i].getName()))
          return getDirectoryForName(name.substring(subDirs[i].getName().length()), subDirs[i]);
        else if ((name.length() == 0) && subDirs[i].getName().equals(ZERO_LENGTH_NAME))
          return getDirectoryForName(name, subDirs[i]);

      /* here, we must create the appropriate directory */
      if ((name.length() >= subDirs[0].getName().length()) || ((name.length() == 0) && (subDirs[0].getName().length() == 1))) {
        File newDir = new File(dir, (name.length() == 0 ? ZERO_LENGTH_NAME : name.substring(0, subDirs[0].getName().length())));
        logger.debug("Necessarily creating dir {}", newDir.getName());
        StorageFiles.createDirectory(newDir);
        this.directories.put(dir, append(subDirs, newDir));
        this.directories.put(newDir, new File[0]);

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
          dirs[i] = subDirs[i].getName();
        dirs[subDirs.length] = (name.length() == 0 ? ZERO_LENGTH_NAME : name);

        /* now reformat the directory, creating an entry for this name */
        reformatDirectory(dir, getDirectories(dirs));

        /* and finally, recurse, which should find a directory for this name */
        return getDirectoryForName(name, dir);
      }
    }
  }

  /**
   * Internal method which returns the postfix for a given id
   *
   * @param id The id to return the postfix for
   * @param file The directory the id will be stored in
   * @return The postfix
   */
  private String getPostfix(Id id, File file) {
    return id.toStringFull().substring(getPrefix(file).length());
  }

  /**
   * Internal method which returns the prefix of a given directory
   *
   * @param file The directory to return the prefix for
   * @return The prefix
   */
  String getPrefix(File file) {
    if (prefixes.get(file) != null)
      return (String) prefixes.get(file);

    StringBuilder buffer = new StringBuilder();
    while (! file.equals(appDirectory)) {
      buffer.insert(0, file.getName().replaceAll(ZERO_LENGTH_NAME, ""));
      file = file.getParentFile();
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
  private File[] append(File[] files, File file) {
    File[] result = new File[files.length + 1];

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
  private int numDirectoriesDir(File dir) {
    return dir.listFiles(new DirectoryFilter()).length;
  }

  /**
   * Gets the number of files in directory (excluding directories)
   *
   * @param dir the directory to check
   * @return int the number of files
   */
  private int numFilesDir(File dir) {
     return dir.listFiles(new FileFilter()).length;
  }

  /**
   * Returns whether or not the given filename represents a stored file
   *
   * @param parent The directory the file is in
   * @param name The name of the file
   * @return true if it is neither a directory nor the metadata file
   */
  private static boolean isFile(File parent, String name) {
    return ((! new File(parent, name).isDirectory()) && (! name.equals(METADATA_FILENAME)));
  }

  /**
   * Returns whether or not the given filename represents a directory
   *
   * @param parent The directory the file is in
   * @param name The name of the file
   * @return true if it is a directory
   */
  private static boolean isDirectory(File parent, String name) {
    return (new File(parent, name)).isDirectory();
  }

  /**
   * Returns whether a directory contains subdirectories
   *
   * @param dir the directory to check
   * @return boolean whether directory contains subdirectories
   *
   */
  private boolean containsDir(File dir) {
    return (dir.listFiles(new DirectoryFilter()).length != 0);
  }

  /**
   * Utility function which returns the range of keys which a directory
   * corresponds to.
   *
   * @param dir The directory
   */
  IdRange getRangeForDirectory(File dir) {
    String result = "";

    while (! dir.equals(appDirectory)) {
      result = dir.getName() + result;
      dir = dir.getParentFile();
    }

    return factory.buildIdRangeFromPrefix(result);
  }

  /**
   * Abstract over reading a single key from a file using Java
   * serialization.
   *
   * @param file The file to create the key from.
   * @return The key that was read in
   */
  Id readKey(File file) {
    String s = getPrefix(file.getParentFile()) + file.getName().replaceAll(ZERO_LENGTH_NAME, "");

    if (s.indexOf(".") >= 0) {
      return factory.buildIdFromToString(s.toCharArray(), 0, s.indexOf("."));
    } else {
      return factory.buildIdFromToString(s.toCharArray(), 0, s.length());
    }
  }

  /**
   * A class that filters out the files in a directory to return
   * only subdirectories.
   */
  static final class DirectoryFilter implements FilenameFilter {
    public boolean accept(File dir, String name) {
      return isDirectory(dir, name);
    }
  }

  /**
   * A class that filters out the files in a directory to return
   * only files ( no directories )
   */
  static final class FileFilter implements FilenameFilter {
    public boolean accept(File dir, String name) {
      return isFile(dir, name);
    }
  }

  /**
   * Class for a hashtable of primitive characters
   */
  private static class CharacterHashSet {
    private final boolean[] bitMap = new boolean[256];

    public void put(char a) {
      bitMap[(int) a] = true;
    }

    public boolean contains(char a) {
      return bitMap[(int) a];
    }

    public void remove(char a) {
      bitMap[(int) a] = false;
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
