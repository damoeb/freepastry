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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import org.slf4j.Logger;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.PrivateFiles;
import rice.p2p.util.ReverseTreeMap;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

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

  private final ReverseTreeMap<Id, Object> metadata;  // the in-memory cache of object metadata
  private final HashSet<Path> dirty;            // the list of directories which have dirty metadata
  private final IdFactory factory;
  private final StorageSerializer serializer;
  private final Logger logger;

  /** The first long of a metadata cache file. */
  static final long METADATA_CACHE_MAGIC = 0x46504D4443616368L; // "FPMDCach"
  /** The metadata cache format version. */
  static final int METADATA_CACHE_VERSION = 1;

  MetadataIndex(IdFactory factory, StorageSerializer serializer, Logger logger) {
    this.factory = factory;
    this.serializer = serializer;
    this.logger = logger;
    this.dirty = new HashSet<Path>();
    this.metadata = new ReverseTreeMap<Id, Object>();
  }

  boolean exists(Id id) {
    synchronized (metadata) {
      return metadata.containsKey(id);
    }
  }

  Object get(Id id) {
    synchronized (metadata) {
      return metadata.get(id);
    }
  }

  /**
   * Records the metadata of an object stored in {@code dir}, and marks that directory dirty.
   */
  void put(Id id, Object value, Path dir) {
    synchronized (metadata) {
      metadata.put(id, value);
      dirty.add(dir);
    }
  }

  /**
   * Forgets an object stored in {@code dir}, and marks that directory dirty.
   */
  void remove(Id id, Path dir) {
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
  SortedMap<Id, Object> keyMap() {
    synchronized (metadata) {
      return Collections.unmodifiableSortedMap(new TreeMap<Id, Object>(metadata.keyMap()));
    }
  }

  /**
   * @return a snapshot of the keys from {@code ccw} (inclusive) to {@code cw} (exclusive) and their metadata
   */
  SortedMap<Id, Object> keySubMap(Id ccw, Id cw) {
    synchronized (metadata) {
      return Collections.unmodifiableSortedMap(new TreeMap<Id, Object>(metadata.keySubMap(ccw, cw)));
    }
  }

  SortedMap<Id, Object> keyMapUnlocked() {
    return Collections.unmodifiableSortedMap(metadata.keyMap());
  }

  SortedMap<Id, Object> valueHeadMapUnlocked(Object value) {
    return Collections.unmodifiableSortedMap(metadata.valueHeadMap(value));
  }

  SortedMap<Id, Object> valueNullMapUnlocked() {
    return Collections.unmodifiableSortedMap(metadata.valueNullMap());
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

  void putUnlocked(Id id, Object value) {
    metadata.put(id, value);
  }

  void removeUnlocked(Id id) {
    metadata.remove(id);
  }

  void markDirtyUnlocked(Path dir) {
    dirty.add(dir);
  }

  /**
   * Function which writes out all of the dirty metadata files and marks
   * them as clean.
   *
   * @param rangeForDirectory the range of keys a directory holds
   */
  void writeDirty(Function<Path, IdRange> rangeForDirectory) {
    Path[] files = dirty.toArray(new Path[0]);

    for (int i=0; i<files.length; i++) {
      HashMap<Id, Object> map = new HashMap<Id, Object>();
      IdRange range = rangeForDirectory.apply(files[i]);

      if (range.getCounterClockwiseBound().compareTo(range.getClockwiseBound()) <= 0) {
        map.putAll(metadata.keySubMap(range.getCounterClockwiseBound(), range.getClockwiseBound()));
      } else {
        /* the range wraps around: it holds the keys from ccw up and the keys below cw */
        map.putAll(metadata.keyTailMap(range.getCounterClockwiseBound()));
        map.putAll(metadata.keyMap().headMap(range.getClockwiseBound()));
      }

      try {
        writeMetadataFile(files[i], map);

        synchronized (metadata) {
          dirty.remove(files[i]);
        }
      } catch (FileSystemException e) {
        // the file could not be opened, as FileOutputStream reported with a FileNotFoundException
        synchronized (metadata) {
          dirty.remove(files[i]);
        }

        logger.warn("ERROR: Could not find directory while writing out metadata in '{}' - removing from dirty list and continuing!", files[i].toAbsolutePath().normalize(), e);
      } catch (IOException e) {
        logger.warn("ERROR: Got error {} while writing out metadata in '{}' - aborting!", e, files[i].toAbsolutePath().normalize(), e);
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
  long readMetadataFile(Path file, Function<Path, IdRange> rangeForDirectory, Function<Path, String> prefixOf) throws IOException {
    Path metadata = file.resolve(METADATA_FILENAME);

    if (! Files.exists(metadata))
      return -1L;

    IdRange range = rangeForDirectory.apply(file);

    try {
      InputBuffer buf = new SimpleInputBuffer(Files.readAllBytes(metadata));
      if (buf.readLong() != METADATA_CACHE_MAGIC) {
        throw new IOException("Not a metadata cache");
      }
      int version = buf.readInt();
      if (version != METADATA_CACHE_VERSION) {
        throw new IOException("Unknown metadata cache version " + version);
      }

      int count = buf.readInt();
      for (int i = 0; i < count; i++) {
        Id id = factory.buildIdFromString(buf.readUTF());
        Object value = buf.readBoolean() ? serializer.readMetadata(buf) : null;

        if (range.containsId(id) && Files.exists(file.resolve(id.toStringFull().substring(prefixOf.apply(file).length()))))
          this.metadata.put(id, value);
        else
          dirty.add(file);
      }

      return StorageFiles.lastModified(metadata);
    } catch (IOException | RuntimeException e) {
      logger.warn("ERROR: Got exception {} while reading metadata file {} - rebuilding file", e, metadata, e);
      StorageFiles.deleteFile(metadata);
      return 0L;
    }
  }

  /**
   * Utility function which writes the metadata file out to disk: a magic number, the format version, the
   * number of entries, and per entry the key (UTF of {@link Id#toStringFull()}), whether there is metadata
   * and the metadata itself, written by the {@link StorageSerializer}.
   *
   * @param file The directory to write the file to
   * @param map The data to write
   */
  private void writeMetadataFile(Path file, Map<Id, Object> map) throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    buf.writeLong(METADATA_CACHE_MAGIC);
    buf.writeInt(METADATA_CACHE_VERSION);
    buf.writeInt(map.size());
    for (Map.Entry<Id, Object> entry : map.entrySet()) {
      buf.writeUTF(entry.getKey().toStringFull());
      buf.writeBoolean(entry.getValue() != null);
      if (entry.getValue() != null) {
        serializer.writeMetadata(entry.getValue(), buf);
      }
    }

    try (OutputStream output = PrivateFiles.newOutputStream(file.resolve(METADATA_FILENAME))) {
      output.write(buf.getBytes(), 0, buf.getWritten());
    }
  }
}
