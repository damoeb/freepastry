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

package rice.pastry.messaging;

import java.util.HashMap;

import org.slf4j.Logger;
import rice.Destructable;
import rice.pastry.PastryNode;
import rice.pastry.client.PastryAppl;
import rice.pastry.transport.Deserializer;

/**
 * An object which remembers the mapping from names to MessageReceivers
 * and dispatches messages by request.
 *
 * Nothing is buffered: a message whose address has no registered receiver is
 * dropped. Whether an application gets messages before the node is ready is
 * decided later, by the receiver (e.g. PastryEndpoint.receiveMessage(), which
 * also drops them).
 *
 * TODO:  We need to make it explicit which apps can receive messages before
 * PastryNode.isReady().
 *
 * @version $Id: MessageDispatch.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Jeff Hoye
 * @author Andrew Ladd
 */

public class MessageDispatch implements Destructable {

  // have modified from HashMap to HashMap to use the internal representation
  // of a LocalAddress.  Otherwise remote node cannot get its message delivered
  // because objects constructed differently are not mapped to the same value
  private HashMap<Integer,PastryAppl> addressBook;

  private final PastryNode localNode;

  private final Logger logger;

  /**
   * Also held by the transport layer to allow it to deserialize the messages.
   */
  private final Deserializer deserializer;

  /**
   * Constructor.
   */
  public MessageDispatch(PastryNode pn, Deserializer deserializer) {
    this.deserializer = deserializer;
    addressBook = new HashMap<Integer, PastryAppl>();
    this.localNode = pn;
    this.logger = pn.getEnvironment().getLogManager().getLogger(getClass(), null);
  }

  /**
   * Registers a receiver with the mail service.
   *
   * @param address the address of the receiver.
   * @param receiver the receiver.
   */
  public void registerReceiver(int address, PastryAppl receiver) {
    // the stack trace is to figure out who registered for what, it is not an error
    logger.debug("Registering {} for address {}", receiver, address);
    if (logger.isTraceEnabled()) logger.trace("Registering receiver for address {}", address, new Exception("stack trace"));
    if (addressBook.get(Integer.valueOf(address)) != null) {
      throw new IllegalArgumentException("Registering receiver for already-registered address " + address);
    }

    deserializer.setDeserializer(address, receiver.getDeserializer());
    addressBook.put(Integer.valueOf(address), receiver);
  }

  public PastryAppl getDestination(Message msg) {
    return getDestinationByAddress(msg.getDestination());
  }

  public PastryAppl getDestinationByAddress(int addr) {
    PastryAppl mr = (PastryAppl) addressBook.get(Integer.valueOf(addr));
    return mr;
  }

  /**
   * Dispatches a message to the appropriate receiver.
   *
   * Returns false and drops the message if no receiver is registered for its address.
   *
   * @param msg the message.
   *
   * @return true if message could be dispatched, false otherwise.
   */
  public boolean dispatchMessage(Message msg) {
    if (msg.getDestination() == 0) {
      Logger logger = localNode.getEnvironment().getLogManager().getLogger(MessageDispatch.class, null);
      logger.warn("Message {},{} has no destination.", msg, msg.getClass().getName(), new Exception("Stack Trace"));
      return false;
    }
    // NOTE: There is no safety issue with calling localNode.isReady() because this is on the
    // selector thread, and a node is only set ready on the selector thread too.
    PastryAppl mr = (PastryAppl) addressBook.get(Integer.valueOf(msg.getDestination()));

    if (mr == null) {
      if ((logger.isDebugEnabled()) ||
          (localNode.isReady() && (logger.isInfoEnabled()))) {
        logger.info("Dropping message " + msg + " because the application address " + msg.getDestination() + " is unknown.");
      }
      return false;
    } else {
      mr.receiveMessage(msg);
      return true;
    }
  }

  public void destroy() {
    for (PastryAppl mr : addressBook.values()) {
      logger.info("Destroying {}", mr);
      mr.destroy();
    }
    addressBook.clear();
  }
}
