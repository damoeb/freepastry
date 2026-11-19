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
package rice.persistence.testing;

import java.io.IOException;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.persistence.StorageSerializer;

/**
 * Persists the values the storage tests use: String and byte[] objects, Integer metadata. Each value starts
 * with a tag byte naming its class.
 */
public class TestStorageSerializer implements StorageSerializer {

  private static final byte STRING = 1;
  private static final byte BYTES = 2;
  private static final byte INTEGER = 3;

  @Override
  public void writeObject(Object obj, OutputBuffer buf) throws IOException {
    write(obj, buf);
  }

  @Override
  public Object readObject(InputBuffer buf) throws IOException {
    return read(buf);
  }

  @Override
  public void writeMetadata(Object metadata, OutputBuffer buf) throws IOException {
    write(metadata, buf);
  }

  @Override
  public Object readMetadata(InputBuffer buf) throws IOException {
    return read(buf);
  }

  private static void write(Object value, OutputBuffer buf) throws IOException {
    if (value instanceof String s) {
      buf.writeByte(STRING);
      buf.writeUTF(s);
    } else if (value instanceof byte[] bytes) {
      buf.writeByte(BYTES);
      buf.writeInt(bytes.length);
      buf.write(bytes, 0, bytes.length);
    } else if (value instanceof Integer i) {
      buf.writeByte(INTEGER);
      buf.writeInt(i);
    } else {
      throw new IOException("Cannot persist " + value + " of " + value.getClass().getName());
    }
  }

  private static Object read(InputBuffer buf) throws IOException {
    byte tag = buf.readByte();
    switch (tag) {
      case STRING:
        return buf.readUTF();
      case BYTES:
        byte[] bytes = new byte[buf.readInt()];
        buf.read(bytes, 0, bytes.length);
        return bytes;
      case INTEGER:
        return buf.readInt();
      default:
        throw new IOException("Unknown tag " + tag);
    }
  }
}
