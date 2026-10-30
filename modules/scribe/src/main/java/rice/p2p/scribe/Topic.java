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

package rice.p2p.scribe;

import java.io.IOException;

import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * This class represents a specific topic in the Scribe system.
 *
 * @param id The Id to which this topic is mapped
 * @author Alan Mislove
 */
public record Topic(Id id) implements Comparable<Topic> {

  /**
   * Constructor which takes a name for this topic
   *
   * @param factory The factory to use when creating the id
   * @param name The name for this topic
   */
  public Topic(IdFactory factory, String name) {
    this(getId(factory, name));
  }

  /**
   * Reads a topic written by {@link #serialize(OutputBuffer)}.
   */
  public Topic(InputBuffer buf, Endpoint endpoint) throws IOException {
    this(endpoint.readId(buf, buf.readShort()));
  }

  /**
   * Returns the Id to which the string is mapped
   *
   * @param factory The factory to use when creating the id
   * @param name The string to map
   * @return The id to which this string is mapped
   */
  public static Id getId(IdFactory factory, String name) {
    return factory.buildId(name);
  }

  /**
   * Returns a String representation of this topic
   *
   * @return A String representation of this topic
   */
  @Override
  public String toString() {
    return "[TOPIC " + id + "]";
  }

  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeShort(id.getType());
    id.serialize(buf);
  }

  @Override
  public int compareTo(Topic that) {
    return this.id.compareTo(that.id);
  }
}
