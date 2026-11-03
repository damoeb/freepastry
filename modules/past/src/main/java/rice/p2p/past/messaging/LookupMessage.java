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

package rice.p2p.past.messaging;

import java.io.IOException;

import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.PastContent;

/**
 * This class is the representation of a lookup request (by Id) in Past.
 *
 * response should be PastContent
 *
 * @version $Id: LookupMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
public final class LookupMessage extends ContinuationMessage {
  // the id to fetch
  private Id id;

  // whether or not this message has been cached
  private boolean cached = false;

  /**
   * Constructor
   *
   * @param uid The unique id
   * @param id The location to be stored
   * @param source The source address
   * @param dest The destination address
   */
  public LookupMessage(int uid, Id id, NodeHandle source, Id dest) {
    super(uid, source, dest);

    this.id = id;
  }

  /**
   * Method which returns the id
   *
   * @return The contained id
   */
  public Id getId() {
    return id;
  }

  /**
    * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  @Override
  public String toString() {
    return "[LookupMessage for " + id + " data " + response + "]";
  }

  /***************** Raw Serialization ***************************************/
  @Override
  public short getType() {
    return PastMessageType.LOOKUP.id();
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version
    if (response != null && response instanceof PastContent) {
      super.serialize(buf, true);
      PastContent rpc = (PastContent)response;
      buf.writeShort(rpc.getType());
      rpc.serialize(buf);
    } else {
      super.serialize(buf, false);
    }

    buf.writeShort(id.getType());
    id.serialize(buf);
    buf.writeBoolean(cached);
  }

  public static LookupMessage build(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new LookupMessage(buf, endpoint, pcd);
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }

  private LookupMessage(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    super(buf, endpoint);
    if (serType == S_SUB) {
      short contentType = buf.readShort();
      response = pcd.deserializePastContent(buf, endpoint, contentType);
    }
    try {
      id = endpoint.readId(buf, buf.readShort());
    } catch (IllegalArgumentException iae) {
      endpoint.getEnvironment().getLogManager().getLogger(LookupMessage.class, null).atWarn().log(() -> iae+" "+this+" serType:"+serType+" UID:"+getUID()+" d:"+getDestination()+" s:"+getSource());
      throw iae;
    }
    cached = buf.readBoolean();
  }
}
