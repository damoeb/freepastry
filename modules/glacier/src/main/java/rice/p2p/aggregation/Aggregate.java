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
package rice.p2p.aggregation;

import java.io.IOException;

import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.PastException;
import rice.p2p.past.gc.GCPast;
import rice.p2p.past.gc.GCPastContent;
import rice.p2p.past.gc.GCPastContentHandle;
import rice.p2p.past.gc.GCPastMetadata;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.util.HashUtils;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * An aggregate: a group of small objects stored in Past as one, plus the pointers to the aggregates it
 * refers to.
 */
public class Aggregate implements GCPastContent {

  protected GCPastContent[] components;
  protected Id[] pointers;
  protected Id myId;

  public Aggregate(GCPastContent[] components, Id[] pointers) {
    this.components = components;
    this.myId = null;
    this.pointers = pointers;
  }

  /**
   * Deserializing constructor, the counterpart of {@link #serialize(OutputBuffer)}.
   */
  public Aggregate(InputBuffer buf, Endpoint endpoint, PastContentDeserializer contentDeserializer) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        myId = endpoint.readId(buf, buf.readShort());
        pointers = new Id[buf.readShort()];
        for (int i = 0; i < pointers.length; i++) {
          pointers[i] = endpoint.readId(buf, buf.readShort());
        }
        components = new GCPastContent[buf.readShort()];
        for (int i = 0; i < components.length; i++) {
          components[i] = (GCPastContent) contentDeserializer.deserializePastContent(buf, endpoint, buf.readShort());
        }
        break;
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }

  public void setId(Id myId) {
    this.myId = myId;
  }

  @Override
  public Id getId() {
    return myId;
  }

  public Id[] getPointers() {
    return pointers;
  }

  public int numComponents() {
    return components.length;
  }

  public GCPastContent getComponent(int index) {
    return components[index];
  }

  @Override
  public long getVersion() {
    return 0;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public PastContent checkInsert(rice.p2p.commonapi.Id id, PastContent existingContent) throws PastException {
    if (existingContent == null) {
      return this;
    } else {
      return existingContent;
    }
  }

  /**
   * @return the hash of the pointers and components, or null if they cannot be serialized
   */
  public byte[] getContentHash() {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    try {
      serializeHelper(buf);
    } catch (IOException ioe) {
      return null;
    }

    return HashUtils.hash(buf.getBytes(), 0, buf.getWritten());
  }

  @Override
  public PastContentHandle getHandle(Past local) {
    return new AggregateHandle(local.getLocalNodeHandle(), myId, getVersion(), GCPast.INFINITY_EXPIRATION);
  }

  @Override
  public GCPastContentHandle getHandle(GCPast local, long expiration) {
    return new AggregateHandle(local.getLocalNodeHandle(), myId, getVersion(), expiration);
  }

  @Override
  public GCPastMetadata getMetadata(long expiration) {
    return new GCPastMetadata(expiration);
  }

  @Override
  public short getType() {
    return AggregationContentType.AGGREGATE.id();
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version
    buf.writeShort(myId.getType());
    myId.serialize(buf);
    serializeHelper(buf);
  }

  private void serializeHelper(OutputBuffer buf) throws IOException {
    buf.writeShort((short)pointers.length);
    for (Id pointer : pointers) {
      buf.writeShort(pointer.getType());
      pointer.serialize(buf);
    }

    buf.writeShort((short)components.length);
    for (GCPastContent component : components) {
      buf.writeShort(component.getType());
      component.serialize(buf);
    }
  }
}
