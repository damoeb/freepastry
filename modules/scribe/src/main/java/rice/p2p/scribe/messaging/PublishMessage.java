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

package rice.p2p.scribe.messaging;

import java.io.IOException;

import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.Topic;
import rice.p2p.scribe.rawserialization.ScribeContentDeserializer;

/**
 * The publish message.
 *
 * @version $Id: PublishMessage.java 3850 2007-09-10 15:53:23Z jeffh $
 *
 * @author Alan Mislove
 */
public final class PublishMessage extends ScribeMessage {
  /** Published content is sent this much ahead of other Scribe messages (lower is more urgent). */
  private static final int PRIORITY_BOOST = 10;

  // the content of this message
  private final ScribeContent content;

  /**
   * Constructor
   *
   * @param source The source address
   * @param topic The topic to publish to
   * @param content The content to publish
   */
  public PublishMessage(NodeHandle source, Topic topic, ScribeContent content) {
    super(source, topic);

    this.content = content;
  }

  /**
   * Returns the content
   *
   * @return The content
   */
  public ScribeContent getContent() {
    return content;
  }

  public String toString() {
    return "PublishMessage"+topic+":"+content;
  }

  @Override
  public int getPriority() {
    return super.getPriority()-PRIORITY_BOOST;
  }

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return ScribeMessageType.PUBLISH.id();
  }

  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version
    super.serialize(buf);

    buf.writeShort(content.getType());
    content.serialize(buf);
  }

  public static PublishMessage build(InputBuffer buf, Endpoint endpoint, ScribeContentDeserializer scd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new PublishMessage(buf, endpoint, scd);
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }

  /**
   * Private because it should only be called from build(), if you need to extend this,
   * make sure to build a serializeHelper() like in AnycastMessage/SubscribeMessage, and properly handle the
   * version number.
   */
  private PublishMessage(InputBuffer buf, Endpoint endpoint, ScribeContentDeserializer cd) throws IOException {
    super(buf, endpoint);

    // this can be done lazilly to be more efficient, must cache remaining bits, endpoint, cd, and implement own InputBuffer
    short contentType = buf.readShort();
    content = cd.deserializeScribeContent(buf, endpoint, contentType);

  }
}
