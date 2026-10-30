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
import java.util.function.Supplier;

import org.slf4j.Logger;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.scribe.messaging.AnycastFailureMessage;
import rice.p2p.scribe.messaging.AnycastMessage;
import rice.p2p.scribe.messaging.DropMessage;
import rice.p2p.scribe.messaging.PublishMessage;
import rice.p2p.scribe.messaging.PublishRequestMessage;
import rice.p2p.scribe.messaging.ScribeMessageType;
import rice.p2p.scribe.messaging.SubscribeAckMessage;
import rice.p2p.scribe.messaging.SubscribeFailedMessage;
import rice.p2p.scribe.messaging.SubscribeMessage;
import rice.p2p.scribe.messaging.UnsubscribeMessage;
import rice.p2p.scribe.rawserialization.ScribeContentDeserializer;

/**
 * Turns Scribe's wire messages back into objects. The content deserializer is looked up on every
 * message, because the application may replace it after Scribe was built.
 */
final class ScribeMessageDeserializer implements MessageDeserializer {

  private final Endpoint endpoint;
  private final Supplier<ScribeContentDeserializer> contentDeserializer;
  private final String instance;
  private final Logger logger;

  ScribeMessageDeserializer(Endpoint endpoint, Supplier<ScribeContentDeserializer> contentDeserializer, String instance, Logger logger) {
    this.endpoint = endpoint;
    this.contentDeserializer = contentDeserializer;
    this.instance = instance;
    this.logger = logger;
  }

  public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
    ScribeContentDeserializer cd = contentDeserializer.get();
    try {
      return switch (ScribeMessageType.of(type)) {
        case ANYCAST -> AnycastMessage.build(buf, endpoint, cd);
        case SUBSCRIBE -> SubscribeMessage.buildSM(buf, endpoint, cd);
        case SUBSCRIBE_ACK -> SubscribeAckMessage.build(buf, endpoint);
        case SUBSCRIBE_FAILED -> SubscribeFailedMessage.build(buf, endpoint);
        case DROP -> DropMessage.build(buf, endpoint);
        case PUBLISH -> PublishMessage.build(buf, endpoint, cd);
        case PUBLISH_REQUEST -> PublishRequestMessage.build(buf, endpoint, cd);
        case UNSUBSCRIBE -> UnsubscribeMessage.build(buf, endpoint);
        case ANYCAST_FAILURE -> AnycastFailureMessage.build(buf, endpoint, cd);
      };
    } catch (IOException e) {
      logger.atError().log(() -> "Exception in deserializer in "+endpoint.toString()+":"+instance+" "+cd+" "+e);
      throw e;
    }
  }
}
