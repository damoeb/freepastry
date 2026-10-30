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

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.PastException;

/**
 * This class represents a reminder to Past that an outstanding message exists,
 * and that the waiting continuation should be informed if the message is lost.
 *
 * @version $Id: MessageLostMessage.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
public final class MessageLostMessage extends PastMessage {
  // the id the message was sent to
  private final Id id;

  // the hint the message was sent to
  private final NodeHandle hint;

  // the message
  private final String messageString;

  /**
   * Constructor which takes a unique integer Id and the local id
   *
   * @param uid The unique id
   * @param local The local nodehandle
   * @param id The id the lost message was sent to
   * @param message The lost message (only its string form is kept)
   * @param hint The hint the lost message was sent to
   */
  public MessageLostMessage(int uid, NodeHandle local, Id id, Message message, NodeHandle hint) {
    super(uid, local, local.getId());

    setResponse();
    this.hint = hint;
    if (message != null) {
      this.messageString = message.toString();
    } else {
      this.messageString = "";
    }
    this.id = id;
  }

  /**
   * Method by which this message is supposed to return it's response -
   * in this case, it lets the continuation know that a the message was
   * lost via the receiveException method.
   *
   * @param c The continuation to return the reponse to.
   */
  @Override
  public void returnResponse(Continuation<Object, Exception> c, Environment env, String instance) {
    Logger logger = env.getLogManager().getLogger(getClass(), instance);
    Exception e = new PastException("Outgoing message '" + messageString + "' to " + id + "/" + hint + " was lost - please try again.");
    logger.warn("ERROR: Outgoing PAST message {} with UID {} was lost", messageString, getUID(), e);
    c.receiveException(e);
  }

  /**
  * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  @Override
  public String toString() {
    return "[MessageLostMessage]";
  }

  @Override
  public short getType() {
    return PastMessageType.MESSAGE_LOST.id();
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    throw new RuntimeException("serialize() not supported in MessageLostMessage");
  }
}
