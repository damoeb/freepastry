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
package rice.p2p.glacier.v2;

import java.io.IOException;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.persistence.StorageSerializer;

/**
 * Persists what {@link GlacierImpl} stores: {@link FragmentAndManifest}s with {@link FragmentMetadata} in the
 * fragment storage and the trash, and the {@link Long} timestamps of the neighbor storage. Each object starts
 * with a tag byte naming its class.
 */
public class GlacierStorageSerializer implements StorageSerializer {

  private static final byte FRAGMENT_AND_MANIFEST = 1;
  private static final byte TIMESTAMP = 2;

  @Override
  public void writeObject(Object obj, OutputBuffer buf) throws IOException {
    if (obj instanceof FragmentAndManifest fragmentAndManifest) {
      buf.writeByte(FRAGMENT_AND_MANIFEST);
      fragmentAndManifest.serialize(buf);
    } else if (obj instanceof Long timestamp) {
      buf.writeByte(TIMESTAMP);
      buf.writeLong(timestamp);
    } else {
      throw new IOException("Cannot persist " + obj + " of " + obj.getClass().getName());
    }
  }

  @Override
  public Object readObject(InputBuffer buf) throws IOException {
    byte tag = buf.readByte();
    switch (tag) {
      case FRAGMENT_AND_MANIFEST:
        return new FragmentAndManifest(buf);
      case TIMESTAMP:
        return buf.readLong();
      default:
        throw new IOException("Unknown Glacier object tag " + tag);
    }
  }

  @Override
  public void writeMetadata(Object metadata, OutputBuffer buf) throws IOException {
    if (!(metadata instanceof FragmentMetadata fragmentMetadata)) {
      throw new IOException("Cannot persist metadata " + metadata + " of " + metadata.getClass().getName());
    }
    fragmentMetadata.serialize(buf);
  }

  @Override
  public Object readMetadata(InputBuffer buf) throws IOException {
    return new FragmentMetadata(buf);
  }
}
