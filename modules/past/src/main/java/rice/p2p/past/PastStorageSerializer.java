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
package rice.p2p.past;

import java.io.IOException;
import java.util.function.Supplier;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.gc.ExpiringPastMetadata;
import rice.persistence.StorageSerializer;

/**
 * Persists what a {@link PastImpl} stores: {@link PastContent} objects, written with their raw type id and
 * {@link PastContent#serialize(OutputBuffer)} as on the wire and read back with the Past's
 * {@link PastImpl#getContentDeserializer() content deserializer}, and {@link ExpiringPastMetadata} metadata.
 * <p>
 * The storage is built before the Past that uses it, so the Past is looked up only when an object is read.
 */
public class PastStorageSerializer implements StorageSerializer {

  private final Supplier<? extends PastImpl> past;

  /**
   * @param past the Past whose content is stored; called when an object is read, not before
   */
  public PastStorageSerializer(Supplier<? extends PastImpl> past) {
    this.past = past;
  }

  @Override
  public void writeObject(Object obj, OutputBuffer buf) throws IOException {
    PastContent content = (PastContent) obj;
    buf.writeShort(content.getType());
    content.serialize(buf);
  }

  @Override
  public Object readObject(InputBuffer buf) throws IOException {
    PastImpl p = past.get();
    if (p == null) {
      throw new IOException("No Past to read the stored content with");
    }
    return p.getContentDeserializer().deserializePastContent(buf, p.getEndpoint(), buf.readShort());
  }

  @Override
  public void writeMetadata(Object metadata, OutputBuffer buf) throws IOException {
    if (!(metadata instanceof ExpiringPastMetadata gcPastMetadata)) {
      throw new IOException("Cannot persist metadata " + metadata + " of " + metadata.getClass().getName());
    }
    gcPastMetadata.serialize(buf);
  }

  @Override
  public Object readMetadata(InputBuffer buf) throws IOException {
    return new ExpiringPastMetadata(buf);
  }
}
