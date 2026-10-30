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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the format of {@link EncryptedOutputStream}: an RSA-OAEP-wrapped AES key, then AES-GCM chunks bound to
 * their position.
 */
public class EncryptedStreamTest {

  static final byte[] FIRST = "first chunk".getBytes(StandardCharsets.UTF_8);
  static final byte[] SECOND = "second chunk".getBytes(StandardCharsets.UTF_8);

  static KeyPair pair;
  static KeyPair otherPair;

  @BeforeAll
  public static void generateKeyPairs() {
    pair = SecurityUtils.generateKeyAsymmetric();
    otherPair = SecurityUtils.generateKeyAsymmetric();
  }

  /** Writes FIRST and SECOND as one chunk each. */
  static byte[] twoChunks() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    EncryptedOutputStream eos = new EncryptedOutputStream(pair.getPublic(), out, 1024);
    eos.write(FIRST);
    eos.flush();
    eos.write(SECOND);
    eos.close();
    return out.toByteArray();
  }

  /** Splits a stream into its length-prefixed records: the wrapped key, then the chunks. */
  static List<byte[]> records(byte[] stream) {
    List<byte[]> result = new ArrayList<byte[]>();
    ByteBuffer buf = ByteBuffer.wrap(stream);
    while (buf.hasRemaining()) {
      byte[] record = new byte[buf.getInt()];
      buf.get(record);
      result.add(record);
    }
    return result;
  }

  static byte[] join(List<byte[]> records) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] record : records) {
      out.writeBytes(ByteBuffer.allocate(4).putInt(record.length).array());
      out.writeBytes(record);
    }
    return out.toByteArray();
  }

  static byte[] read(byte[] stream, KeyPair keys, int length) throws IOException {
    DataInputStream in = new DataInputStream(new EncryptedInputStream(keys.getPrivate(), new ByteArrayInputStream(stream)));
    byte[] result = new byte[length];
    in.readFully(result);
    return result;
  }

  @Test
  public void roundTrip() throws IOException {
    byte[] expected = new byte[FIRST.length + SECOND.length];
    System.arraycopy(FIRST, 0, expected, 0, FIRST.length);
    System.arraycopy(SECOND, 0, expected, FIRST.length, SECOND.length);
    assertArrayEquals(expected, read(twoChunks(), pair, expected.length));
  }

  @Test
  public void headerIsAnOaepWrappedAesKeyAndChunksAreGcm() throws IOException {
    List<byte[]> records = records(twoChunks());
    assertEquals(3, records.size());
    byte[] key = SecurityUtils.decryptAsymmetric(records.get(0), pair.getPrivate());
    assertEquals(32, key.length);
    assertEquals(12 + FIRST.length + 16, records.get(1).length);
    assertArrayEquals(FIRST, SecurityUtils.decryptSymmetric(records.get(1), key, MathUtils.longToByteArray(0)));
    assertArrayEquals(SECOND, SecurityUtils.decryptSymmetric(records.get(2), key, MathUtils.longToByteArray(1)));
  }

  @Test
  public void readingWithTheWrongPrivateKeyThrows() throws IOException {
    byte[] stream = twoChunks();
    assertThrows(SecurityException.class, () -> read(stream, otherPair, FIRST.length));
  }

  @Test
  public void aTamperedChunkThrows() throws IOException {
    byte[] stream = twoChunks();
    stream[stream.length - 1] ^= 0x01; // the tag of the last chunk
    assertThrows(SecurityException.class, () -> read(stream, pair, FIRST.length + SECOND.length));
  }

  @Test
  public void reorderedChunksThrow() throws IOException {
    List<byte[]> records = records(twoChunks());
    byte[] first = records.get(1);
    records.set(1, records.get(2));
    records.set(2, first);
    byte[] stream = join(records);
    assertThrows(SecurityException.class, () -> read(stream, pair, 1));
  }

  @Test
  public void aDroppedChunkThrows() throws IOException {
    List<byte[]> records = records(twoChunks());
    records.remove(1);
    byte[] stream = join(records);
    assertThrows(SecurityException.class, () -> read(stream, pair, 1));
  }
}
