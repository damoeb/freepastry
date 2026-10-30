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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.function.Function;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.util.ImmutableSortedMap;
import rice.p2p.util.JavaSerializationFilter;
import rice.p2p.util.ReverseTreeMap;
import rice.p2p.util.rawserialization.JavaSerializationException;

/**
 * The in-memory index of {@link PersistentStorage}: the metadata of every stored object, and the per-directory
 * {@code metadata.cache} files that persist it. Directories whose metadata changed are marked dirty and
 * written out by {@link #writeDirty(Function)}.
 * <p>
 * The methods without "Unlocked" in their name synchronize on the index, as the storage did before; the
 * "Unlocked" ones don't, and are used where the storage did not either (initialization, and the blocking IO
 * thread).
 */
final class MetadataIndex {

  private final ReverseTreeMap<Id, Serializable> metadata;  // the in-memory cache of object metadata
  private final HashSet<File> dirty;            // the list of directories which have dirty metadata
  private final Environment environment;
  private final Logger logger;

  MetadataIndex(Environment environment, Logger logger) {
    this.environment = environment;
    this.logger = logger;
    this.dirty = new HashSet<File>();
    this.metadata = new ReverseTreeMap<Id, Serializable>();
  }

  boolean exists(Id id) {
    synchronized (metadata) {
      return metadata.containsKey(id);
    }
  }

  Serializable get(Id id) {
    synchronized (metadata) {
      return (Serializable) metadata.get(id);
    }
  }

  /**
   * Records the metadata of an object stored in {@code dir}, and marks that directory dirty.
   */
  void put(Id id, Serializable value, File dir) {
    synchronized (metadata) {
      metadata.put(id, value);
      dirty.add(dir);
    }
  }

  /**
   * Forgets an object stored in {@code dir}, and marks that directory dirty.
   */
  void remove(Id id, File dir) {
    synchronized (metadata) {
      metadata.remove(id);
      dirty.add(dir);
    }
  }

  /**
   * Forgets an object, without marking its directory dirty.
   */
  void remove(Id id) {
    synchronized (metadata) {
      metadata.remove(id);
    }
  }

  void rename(Id oldId, Id newId) {
    synchronized (metadata) {
      metadata.put(newId, metadata.get(oldId));
      metadata.remove(oldId);
    }
  }

  /**
   * @return a snapshot of all keys and their metadata
   */
  SortedMap<Id, Serializable> keyMap() {
    synchronized (metadata) {
      return new ImmutableSortedMap<Id, Serializable>(metadata.keyMap());
    }
  }

  /**
   * @return a snapshot of the keys from {@code ccw} (inclusive) to {@code cw} (exclusive) and their metadata
   */
  SortedMap<Id, Serializable> keySubMap(Id ccw, Id cw) {
    synchronized (metadata) {
      return new ImmutableSortedMap<Id, Serializable>(metadata.keySubMap(ccw, cw));
    }
  }

  SortedMap<Id, Serializable> keyMapUnlocked() {
    return new ImmutableSortedMap<Id, Serializable>(metadata.keyMap());
  }

  SortedMap<Id, Serializable> valueHeadMapUnlocked(Serializable value) {
    return new ImmutableSortedMap<Id, Serializable>(metadata.valueHeadMap(value));
  }

  SortedMap<Id, Serializable> valueNullMapUnlocked() {
    return new ImmutableSortedMap<Id, Serializable>(metadata.valueNullMap());
  }

  int sizeUnlocked() {
    return metadata.size();
  }

  int keyCountUnlocked() {
    return metadata.keySet().size();
  }

  boolean containsUnlocked(Id id) {
    return metadata.containsKey(id);
  }

  void putUnlocked(Id id, Serializable value) {
    metadata.put(id, value);
  }

  void removeUnlocked(Id id) {
    metadata.remove(id);
  }

  void markDirtyUnlocked(File dir) {
    dirty.add(dir);
  }

  /**
   * Function which writes out all of the dirty metadata files and marks
   * them as clean.
   *
   * @param rangeForDirectory the range of keys a directory holds
   */
  void writeDirty(Function<File, IdRange> rangeForDirectory) {
    File[] files = (File[]) dirty.toArray(new File[0]);

    for (int i=0; i<files.length; i++) {
      HashMap<Id, Serializable> map = new HashMap<Id, Serializable>();
      IdRange range = rangeForDirectory.apply(files[i]);
      Iterator<Id> keys = null;

      if (range.getCCWId().compareTo(range.getCWId()) <= 0)
        keys = metadata.keySubMap(range.getCCWId(), range.getCWId()).keySet().iterator();
      else
        keys = metadata.keyTailMap(range.getCCWId()).keySet().iterator();

      while (keys.hasNext()) {
        Id next = keys.next();
        map.put(next, (Serializable) metadata.get(next));
      }

      try {
        writeMetadataFile(files[i], map);

        synchronized (metadata) {
          dirty.remove(files[i]);
        }
      } catch (FileNotFoundException f) {
        try {
          synchronized (metadata) {
            dirty.remove(files[i]);
          }

          logger.warn("ERROR: Could not find directory while writing out metadata in '{}' - removing from dirty list and continuing!", files[i].getCanonicalPath(), f);
        } catch (IOException g) {
          logger.error("PANIC: Got IOException {} trying to detail FNF exception {} while writing out file {}", g, f, files[i], g);
        }
      } catch (IOException e) {
        try {
          logger.warn("ERROR: Got error {} while writing out metadata in '{}' - aborting!", e, files[i].getCanonicalPath(), e);
        } catch (IOException f) {
          logger.error("PANIC: Got IOException {} trying to detail exception {} while writing out file {}", f, e, files[i], f);
        }
      }
    }
  }

  /**
   * Utility function which reads the metadata file off of disk and stores the
   * result into the memory cache.
   *
   * @param file The directory to read the metadata file for
   * @param rangeForDirectory the range of keys a directory holds
   * @param prefixOf the key prefix a directory stands for
   * @return the time the metadata file was last modified, 0 if it was broken and -1 if there is none
   */
  long readMetadataFile(File file, Function<File, IdRange> rangeForDirectory, Function<File, String> prefixOf) throws IOException {
    File metadata = new File(file, METADATA_FILENAME);

    if (! metadata.exists())
      return -1L;

    FileInputStream fin = null;

    try {
      fin = new FileInputStream(metadata);
      ObjectInputStream objin = new ObjectInputStream(new BufferedInputStream(fin));
      // defense in depth against a planted metadata file: only admit allowlisted classes
      JavaSerializationFilter.install(objin, environment);

      IdRange range = rangeForDirectory.apply(file);

      try {
        HashMap<?, ?> map = (HashMap<?, ?>) objin.readObject();
        Iterator<?> keys = map.keySet().iterator();

        while (keys.hasNext()) {
          Id id = (Id) keys.next();

          if ((range.containsId(id)) && (new File(file, id.toStringFull().substring(prefixOf.apply(file).length())).exists()))
            this.metadata.put(id, (Serializable)map.get(id));
          else
            dirty.add(file);
        }

        return metadata.lastModified();
      } catch (ClassNotFoundException e) {
        logger.warn("ERROR: Got exception {} while reading metadata file {} - rebuilding file", e, metadata, e);
        StorageFiles.deleteFile(metadata);
        return 0L;
      } catch (IOException e) {
        logger.warn("ERROR: Got exception {} while reading metadata file {} - rebuilding file", e, metadata, e);
        StorageFiles.deleteFile(metadata);
        return 0L;
      }
    } finally {
      fin.close();
    }
  }

  /**
   * Utility function which writes the metadata file out to disk.
   *
   * @param file The directory to write the file to
   * @param map The data to write
   */
  private static void writeMetadataFile(File file, HashMap<Id, Serializable> map) throws IOException {
    FileOutputStream fout = null;

    try {
      fout = new FileOutputStream(new File(file, METADATA_FILENAME));
      ObjectOutputStream objout = new ObjectOutputStream(new BufferedOutputStream(fout));
      objout.writeObject(map);
      objout.close();
    } catch (IOException ioe) {
      throw new JavaSerializationException(map, ioe);
    } finally {
      if (fout != null)
        fout.close();
    }
  }
}
