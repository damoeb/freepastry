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

package rice.p2p.util;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivateKey;
import java.util.Arrays;
import org.jspecify.annotations.Nullable;

/**
*
 *
 * Reads what {@link EncryptedOutputStream} writes, decrypting the AES key with the private key, and decrypting
 * and authenticating each chunk.  A chunk that was changed, dropped, repeated or moved makes the read throw a
 * {@link SecurityException}.  The format:
 *
 * 4 bytes - length(e(k))
 * e(k)
 * chunks of upper-level data
 *
 * where each chunk is length(c) [4 bytes] followed by c, see {@link EncryptedOutputStream}.  The stream ends
 * cleanly only after the authenticated final chunk; if the underlying stream ends earlier, reading throws a
 * {@link SecurityException}.
 *
 * @version $Id: EncryptedInputStream.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class EncryptedInputStream extends InputStream {

  // the private key
  private final PrivateKey privateKey;

  // the decrypted symmetric key
  private byte[] key;

  // the actual underlying stream
  private final DataInputStream stream;

  // any buffered data from the stream
  private byte @Nullable [] buffer;

  // the amount of buffered data which has been returned
  private int bufferLength;

  // the number of chunks read so far
  private long chunks;

  // whether the authenticated final chunk has been read
  private boolean finished;

  /**
   * Builds an encrypted inputstream given a private key to
   * decrypt things under
   *
   * @param privateKey The key
   * @param stream The underlying stream
   * @throws IOException if reading the encrypted symmetric key fails
   */
  public EncryptedInputStream(PrivateKey privateKey, InputStream stream) throws IOException {
    this.privateKey = privateKey;
    this.stream = new DataInputStream(stream);

    readHeader();
  }

  /**
    * Utility method which reads the header information
   */
  private void readHeader() throws IOException {
    byte[] enckey = new byte[stream.readInt()];
    stream.readFully(enckey);

    this.key = SecurityUtils.decryptAsymmetric(enckey, privateKey);
  }

  /**
   * Reads the next byte of data from the input stream. T
   *
   * @return the next byte of data, or <code>-1</code> if the end of the
   *         stream is reached.
   */
  @Override
  public int read() throws IOException {
    byte[] data = fill();
    if (data == null) {
      return -1;
    }
    return data[bufferLength++] & 0xff;
  }

  /**
   * Reads up to <code>len</code> bytes of data from the input stream into
   * an array of bytes.  An attempt is made to read as many as
   * <code>len</code> bytes, but a smaller number may be read, possibly
   * zero. The number of bytes actually read is returned as an integer.
   *
   * @param      b     the buffer into which the data is read.
   * @param      off   the start offset in array <code>b</code>
   *                   at which the data is written.
   * @param      length   the maximum number of bytes to read.
   * @return     the total number of bytes read into the buffer, or
   *             <code>-1</code> if there is no more data because the end of
   *             the stream has been reached.
   */
  @Override
  public int read(byte b[], int off, int length) throws IOException {
    java.util.Objects.checkFromIndexSize(off, length, b.length);
    if (length == 0) {
      return 0;
    }
    byte[] data = fill();
    if (data == null) {
      return -1;
    }
    int l = Math.min(length, available());
    System.arraycopy(data, bufferLength, b, off, l);
    bufferLength += l;

    return l;
  }

  /**
   * Makes sure there is unread buffered data, reading chunks as needed.
   *
   * @return the buffer holding the next unread data, or null if the authenticated end of the stream was reached
   */
  private byte @Nullable [] fill() throws IOException {
    while (available() == 0) {
      if (finished) {
        return null;
      }
      readBuffer();
    }
    return buffer;
  }

  /**
   * Internal method which reads in the next chunk of buffered data
   *
   * @throws SecurityException if the stream ends before the final chunk, or a chunk fails authentication
   */
  protected void readBuffer() throws IOException {
    byte[] encdata;
    try {
      encdata = new byte[stream.readInt()];
      stream.readFully(encdata);
    } catch (EOFException e) {
      throw new SecurityException("Encrypted stream truncated after " + chunks + " chunks", e);
    }

    // the final chunk is authenticated with a 1 as the last byte; try the data variant first
    byte[] aad = Arrays.copyOf(MathUtils.longToByteArray(chunks), Long.BYTES + 1);
    byte[] plain;
    try {
      plain = SecurityUtils.decryptSymmetric(encdata, key, aad);
    } catch (SecurityException notData) {
      aad[Long.BYTES] = 1;
      plain = SecurityUtils.decryptSymmetric(encdata, key, aad);
      finished = true;
    }
    chunks++;
    this.buffer = plain;
    this.bufferLength = 0;
  }

  /**
   * Returns the number of bytes that can be read (or skipped over) from
   * this input stream without blocking by the next caller of a method for
   * this input stream.  The next caller might be the same thread or or
   * another thread.
   *
   * @return     the number of bytes that can be read from this input stream
   *             without blocking.
   * @throws  IOException  if an I/O error occurs.
   */
  @Override
  public int available() throws IOException {
    if (buffer == null)
      return 0;
    else
      return buffer.length - bufferLength;
  }

  /**
   * Closes this input stream and releases any system resources associated
   * with the stream.
   *
   * @throws  IOException  if an I/O error occurs.
   */
  @Override
  public void close() throws IOException {
    stream.close();
  }
}
