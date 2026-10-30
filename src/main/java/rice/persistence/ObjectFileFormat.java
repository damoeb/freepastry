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

import static rice.persistence.PersistentStorage.PERSISTENCE_MAGIC_NUMBER;
import static rice.persistence.PersistentStorage.PERSISTENCE_REVISION_2_1;
import static rice.persistence.PersistentStorage.PERSISTENCE_VERSION_2;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import rice.p2p.commonapi.Id;
import rice.p2p.util.XMLObjectInputStream;
import rice.p2p.util.XMLObjectOutputStream;

/**
 * The format of the object files of {@link PersistentStorage}:
 *
 * <pre>
 * [Key, Object, Version, Gzipped XML]
 * [Metadata, Gzipped XML]
 * [persistence magic number, long]
 * [persistence version, long]
 * [persistence revision, long]
 * [metadata length, long]
 * </pre>
 */
final class ObjectFileFormat {

  private ObjectFileFormat() {
  }

  /**
   * Basic function to read an object from the persisted file.
   *
   * @param file the file to read from
   * @param offset the offset to read from
   * @return Serializable the data stored at the offset in the file
   *
   */
  private static Serializable readObject(File file, int offset) throws IOException {
    FileInputStream fin = null;

    try {
      try {
        fin = new FileInputStream(file);
        ObjectInputStream objin = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(fin)));

        /* skip objects */
        for (int i=0; i<offset; i++)
          objin.readObject();

        return (Serializable) objin.readObject();
      } catch (ClassNotFoundException e) {
        throw new IOException(e.getMessage());
      }
    } finally {
      fin.close();
    }
  }

  /**
   * Abstract over reading a single object to a file using Java
   * serialization.
   *
   * @param file The file to create the object from.
   * @return The object that was read in
   */
  static Serializable readData(File file) throws IOException {
     return readObject(file, 1);
  }

  /**
   * Reads in the metadata from the provided file, or returns null if
   * no metadata was found
   *
   * @param file The file which should be read for the metadata
   * @param logger where to report a version mismatch
   */
  static Serializable readMetadata(File file, Logger logger) throws IOException {
    if (file.length() < 32)
      return null;

    RandomAccessFile ras = null;

    try {
      ras = new RandomAccessFile(file, "r");
      ras.seek(file.length() - 32);

      if (ras.readLong() != PERSISTENCE_MAGIC_NUMBER) {
        return null;
      } else if (ras.readLong() != PERSISTENCE_VERSION_2) {
        logger.warn("Persistence version did not match - exiting!");
        return null;
      } else if (ras.readLong() > PERSISTENCE_REVISION_2_1) {
        logger.warn("Persistence revision did not match - exiting!");
        return null;
      }

      long length = ras.readLong();
      ras.seek(file.length() - 32 - length);

      FileInputStream fis = null;

      try {
        fis = new FileInputStream(ras.getFD());
        ObjectInputStream objin = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(fis)));

        try {
          return (Serializable) objin.readObject();
        } catch (ClassNotFoundException e) {
          throw new IOException(e.getMessage());
        }
      } finally {
        fis.close();
      }
    } finally {
      ras.close();
    }
  }

  /**
   * Abstract over reading a single key from a file using Java
   * serialization.
   *
   * @param file The file to create the key from.
   * @return The key that was read in
   */
  static Id readKeyFromFile(File file) throws IOException {
    return (Id) readObject(file, 0);
  }

  /**
   * Abstract over reading a version from a file using Java
   * serialization.
   *
   * @param file The file to create the version from.
   * @return The key that was read in
   */
  static long readVersion(File file) throws IOException {
    Long temp = (Long) readObject(file, 2);

    return (temp == null ? 0 : temp.longValue());
  }

  /**
   * Abstract over writing a single object to a file using Java
   * serialization.
   *
   * @param obj The object to be written
   * @param file The file to serialize the object to.
   * @return The object's disk space usage
   */
  static long writeObject(Serializable obj, Serializable metadata, Id key, long version, File file) throws IOException {
    FileOutputStream fout = null;

    try {
      fout = new FileOutputStream(file);
      ObjectOutputStream objout = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(fout)));
      objout.writeObject(key);
      objout.writeObject(obj);
      objout.writeObject(Long.valueOf(version));
      objout.close();
    } finally {
      if (fout != null)
        fout.close();
    }

    long len1 = file.length();

    try {
      fout = new FileOutputStream(file, true);
      ObjectOutputStream objout = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(fout)));
      objout.writeObject(metadata);
      objout.close();
    } finally {
      fout.close();
    }

    long len2 = file.length();

    writeTrailer(file, len2-len1);

    return file.length();
  }

  /**
   * Re-writes the metadata stored in the provided file.
   *
   * @param file The file to which the metadata should be written
   * @param metadata The metadata to write
   */
  static void writeMetadata(File file, Serializable metadata) throws IOException {
    RandomAccessFile ras = null;
    FileOutputStream fout = null;

    if (file.length() > 32) {
      try {
        ras = new RandomAccessFile(file, "rw");
        ras.seek(file.length() - 32);

        if ((ras.readLong() == PERSISTENCE_MAGIC_NUMBER) &&
            (ras.readLong() == PERSISTENCE_VERSION_2) &&
            (ras.readLong() <= PERSISTENCE_REVISION_2_1)) {
          long length = ras.readLong();
          ras.setLength(file.length() - 32 - length);
        }
      } finally {
        ras.close();
      }
    }

    long len1 = file.length();

    try {
      fout = new FileOutputStream(file, true);
      ObjectOutputStream objout = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(fout)));
      objout.writeObject(metadata);
      objout.close();
    } finally {
      fout.close();
    }

    long len2 = file.length();

    writeTrailer(file, len2-len1);
  }

  /**
   * Appends the trailer: magic number, version, revision and the length of the metadata.
   */
  private static void writeTrailer(File file, long metadataLength) throws IOException {
    FileOutputStream fout = null;

    try {
      fout = new FileOutputStream(file, true);
      DataOutputStream dos = new DataOutputStream(fout);
      dos.writeLong(PERSISTENCE_MAGIC_NUMBER);
      dos.writeLong(PERSISTENCE_VERSION_2);
      dos.writeLong(PERSISTENCE_REVISION_2_1);
      dos.writeLong(metadataLength);
      dos.close();
    } finally {
      // the callers' streams are closed already, so a failure to open this one propagates as before
      if (fout != null)
        fout.close();
    }
  }
}
