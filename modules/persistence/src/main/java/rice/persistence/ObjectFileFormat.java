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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * The format of the object files of {@link PersistentStorage}, version 3:
 *
 * <pre>
 * object section, gzipped:
 *   [key, UTF of Id.toStringFull()]
 *   [version (the time it was stored), long]
 *   [has object, boolean] [object, written by the StorageSerializer]
 * metadata section, gzipped (rewritten in place by setMetadata):
 *   [has metadata, boolean] [metadata, written by the StorageSerializer]
 * trailer:
 *   [persistence magic number, long]
 *   [persistence version, long]
 *   [persistence revision, long]
 *   [metadata section length, long]
 * </pre>
 *
 * Files of other versions (version 2 held Java-serialized XML) are not read; a file without a valid trailer
 * is reported as broken.
 */
final class ObjectFileFormat {

  /** The length of the trailer in bytes. */
  static final int TRAILER_LENGTH = 32;

  private final IdFactory factory;
  private final StorageSerializer serializer;
  private final Logger logger;

  ObjectFileFormat(IdFactory factory, StorageSerializer serializer, Logger logger) {
    this.factory = factory;
    this.serializer = serializer;
    this.logger = logger;
  }

  /**
   * Reads the object stored in the file.
   *
   * @param file The file to read the object from.
   * @return The object that was read in, which may be null
   */
  Object readData(Path file) throws IOException {
    InputBuffer buf = readObjectSection(file);
    buf.readUTF(); // key
    buf.readLong(); // version
    return buf.readBoolean() ? serializer.readObject(buf) : null;
  }

  /**
   * Reads the key from the file.
   *
   * @param file The file to read the key from.
   * @return The key that was read in
   */
  Id readKeyFromFile(Path file) throws IOException {
    return factory.buildIdFromToString(readObjectSection(file).readUTF());
  }

  /**
   * Reads the version (the time the object was stored) from the file.
   *
   * @param file The file to read the version from.
   * @return The version that was read in
   */
  long readVersion(Path file) throws IOException {
    InputBuffer buf = readObjectSection(file);
    buf.readUTF(); // key
    return buf.readLong();
  }

  /**
   * Reads the metadata from the provided file, or returns null if no metadata was found.
   *
   * @param file The file which should be read for the metadata
   */
  Object readMetadata(Path file) throws IOException {
    try (RandomAccessFile ras = new RandomAccessFile(file.toFile(), "r")) {
      long length = metadataLength(ras, file);
      if (length < 0) {
        return null;
      }

      byte[] section = new byte[(int) length];
      ras.seek(ras.length() - TRAILER_LENGTH - length);
      ras.readFully(section);
      InputBuffer buf = gunzip(section);
      return buf.readBoolean() ? serializer.readMetadata(buf) : null;
    }
  }

  /**
   * Writes an object, its key, version and metadata to a new file.
   *
   * @param obj The object to be written
   * @param metadata The object's metadata
   * @param key The object's key
   * @param version The version (the time it is stored)
   * @param file The file to write to
   * @return The file's disk space usage
   */
  long writeObject(Object obj, Object metadata, Id key, long version, Path file) throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    buf.writeUTF(key.toStringFull());
    buf.writeLong(version);
    buf.writeBoolean(obj != null);
    if (obj != null) {
      serializer.writeObject(obj, buf);
    }

    try (OutputStream fout = Files.newOutputStream(file)) {
      fout.write(gzip(buf));
    }

    appendMetadata(file, metadata);
    return StorageFiles.getFileLength(file);
  }

  /**
   * Re-writes the metadata stored in the provided file.
   *
   * @param file The file to which the metadata should be written
   * @param metadata The metadata to write
   */
  void writeMetadata(Path file, Object metadata) throws IOException {
    try (RandomAccessFile ras = new RandomAccessFile(file.toFile(), "rw")) {
      long length = metadataLength(ras, file);
      if (length < 0) {
        throw new IOException("Not a version " + PersistentStorage.PERSISTENCE_VERSION_3 + " object file: " + file);
      }
      ras.setLength(ras.length() - TRAILER_LENGTH - length);
    }

    appendMetadata(file, metadata);
  }

  /**
   * Appends the metadata section and the trailer.
   */
  private void appendMetadata(Path file, Object metadata) throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    buf.writeBoolean(metadata != null);
    if (metadata != null) {
      serializer.writeMetadata(metadata, buf);
    }
    byte[] section = gzip(buf);

    SimpleOutputBuffer trailer = new SimpleOutputBuffer(TRAILER_LENGTH);
    trailer.writeLong(PersistentStorage.PERSISTENCE_MAGIC_NUMBER);
    trailer.writeLong(PersistentStorage.PERSISTENCE_VERSION_3);
    trailer.writeLong(PersistentStorage.PERSISTENCE_REVISION_3_0);
    trailer.writeLong(section.length);

    try (OutputStream fout = Files.newOutputStream(file, CREATE, APPEND)) {
      fout.write(section);
      fout.write(trailer.getBytes(), 0, trailer.getWritten());
    }
  }

  /**
   * @return the object section of the file, unzipped
   */
  private InputBuffer readObjectSection(Path file) throws IOException {
    try (RandomAccessFile ras = new RandomAccessFile(file.toFile(), "r")) {
      long length = metadataLength(ras, file);
      if (length < 0) {
        throw new IOException("Not a version " + PersistentStorage.PERSISTENCE_VERSION_3 + " object file: " + file);
      }

      byte[] section = new byte[(int) (ras.length() - TRAILER_LENGTH - length)];
      ras.seek(0);
      ras.readFully(section);
      return gunzip(section);
    }
  }

  /**
   * Checks the trailer.
   *
   * @return the length of the metadata section, or -1 if the file has no valid version 3 trailer
   */
  private long metadataLength(RandomAccessFile ras, Path file) throws IOException {
    long fileLength = ras.length();
    if (fileLength < TRAILER_LENGTH) {
      return -1;
    }

    ras.seek(fileLength - TRAILER_LENGTH);
    if (ras.readLong() != PersistentStorage.PERSISTENCE_MAGIC_NUMBER) {
      return -1;
    }
    if (ras.readLong() != PersistentStorage.PERSISTENCE_VERSION_3) {
      logger.warn("Persistence version of {} did not match - ignoring it", file);
      return -1;
    }
    if (ras.readLong() > PersistentStorage.PERSISTENCE_REVISION_3_0) {
      logger.warn("Persistence revision of {} did not match - ignoring it", file);
      return -1;
    }

    long length = ras.readLong();
    if ((length < 0) || (length > fileLength - TRAILER_LENGTH)) {
      return -1;
    }
    return length;
  }

  private static byte[] gzip(SimpleOutputBuffer buf) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream out = new GZIPOutputStream(bytes)) {
      out.write(buf.getBytes(), 0, buf.getWritten());
    }
    return bytes.toByteArray();
  }

  private static InputBuffer gunzip(byte[] section) throws IOException {
    try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(section))) {
      return new SimpleInputBuffer(in.readAllBytes());
    }
  }
}
