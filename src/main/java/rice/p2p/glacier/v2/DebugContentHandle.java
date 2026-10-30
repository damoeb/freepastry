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

import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.gc.GCPastContentHandle;

public class DebugContentHandle implements GCPastContentHandle {
  public static final short TYPE = 3;

  private final Id myId;
  private final rice.p2p.commonapi.NodeHandle myNodeHandle;
  private final long myExpiration;
  private final long myVersion;

  DebugContentHandle(Id id, long version, long expiration, NodeHandle nodeHandle) {
    myId = id;
    myNodeHandle = nodeHandle;
    myExpiration = expiration;
    myVersion = version;
  }

  public Id getId() {
    return myId;
  }

  public NodeHandle getNodeHandle() {
    return myNodeHandle;
  }

  public long getVersion() {
    return myVersion;
  }

  public long getExpiration() {
    return myExpiration;
  }

  /**
   * Deserializing constructor, the counterpart of {@link #serialize(OutputBuffer)}.
   */
  public DebugContentHandle(InputBuffer buf, Endpoint endpoint) throws IOException {
    myId = endpoint.readId(buf, buf.readShort());
    myVersion = buf.readLong();
    myExpiration = buf.readLong();
    myNodeHandle = endpoint.readNodeHandle(buf);
  }

  public short getType() {
    return TYPE;
  }

  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeShort(myId.getType());
    myId.serialize(buf);
    buf.writeLong(myVersion);
    buf.writeLong(myExpiration);
    myNodeHandle.serialize(buf);
  }
}
