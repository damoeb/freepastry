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

import java.io.IOException;
import java.io.UncheckedIOException;

import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Measures the stored objects for the size accounting of {@link MemoryStorage} and {@link LRUCache}: a
 * {@link RawSerializable} (Past and Scribe content, Glacier fragments) counts with its raw serialized size,
 * byte arrays, strings and boxed primitives with their natural size.
 */
public final class ObjectSizes {

  private ObjectSizes() {
  }

  /**
   * @param obj the object to measure, may be null
   * @return its size in bytes
   * @throws IllegalArgumentException if the object is of a kind that cannot be measured
   */
  public static int sizeOf(Object obj) {
    if (obj == null) {
      return 0;
    }
    if (obj instanceof byte[] bytes) {
      return bytes.length;
    }
    if (obj instanceof Long || obj instanceof Double) {
      return 8;
    }
    if (obj instanceof Integer || obj instanceof Float) {
      return 4;
    }
    if (obj instanceof Short || obj instanceof Character) {
      return 2;
    }
    if (obj instanceof Byte || obj instanceof Boolean) {
      return 1;
    }

    try {
      SimpleOutputBuffer buf = new SimpleOutputBuffer();
      if (obj instanceof String s) {
        buf.writeUTF(s);
      } else if (obj instanceof RawSerializable raw) {
        raw.serialize(buf);
      } else {
        throw new IllegalArgumentException("Cannot measure " + obj + " of " + obj.getClass().getName());
      }
      return buf.getWritten();
    } catch (IOException e) {
      throw new UncheckedIOException("Object " + obj + " was not serialized correctly!", e);
    }
  }
}
