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
package rice.pastry.routing;

import java.io.IOException;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryEndpointMessage;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.PastryMessageDeserializer;
import rice.pastry.messaging.PastryRawMessage;

/**
 * A route message contains a pastry message that has been wrapped to be sent to
 * another pastry node.
 *
 * @version $Id: RouteMessage.java 4279 2008-07-15 11:13:33Z jeffh $
 *
 * @author Andrew Ladd
 */

public class RouteMessage extends PastryRawMessage implements rice.p2p.commonapi.RouteMessage {
  private Id target;
  private NodeHandle destinationHandle;
  private byte version;

  private NodeHandle prevNode;

  /** the node the transport received this message from; null if the message was created on this node */
  private @Nullable NodeHandle receivedFrom;

  private SendOptions opts;

  private int auxAddress;

  private NodeHandle nextHop;

  private Message internalMsg;
  // optimization to not use instanceof in the normal new case
  private PastryRawMessage rawInternalMsg = null;
  private InputBuffer serializedMsg;
  private PastryNode pn;

  boolean hasSender;
  byte internalPriority;
  short internalType;

  /**
   * This is used by the Rerouter to keep track of how many attempted reroutes of the message.
   */
  private int numRetries = 0;

  private RMDeserializer endpointDeserializer = new RMDeserializer();

  /**
   * Constructor.
   *
   * @param target this is id of the node the message will be routed to.
   * @param msg the wrapped message.
   * @param serializeVersion the serialization version of the message
   */

  public RouteMessage(Id target, PastryRawMessage msg, byte serializeVersion) {
    this(target, msg, null, null, serializeVersion);
  }

  /**
   * Constructor.
   *
   * @param target this is id of the node the message will be routed to.
   * @param msg the wrapped message.
   * @param opts the send options for the message.
   */

  public RouteMessage(Id target, PastryRawMessage msg, SendOptions opts, byte serializeVersion) {
    this(target, msg, null, opts, serializeVersion);
  }

  /**
   * Constructor.
   *
   * @param target this is id of the node the message will be routed to.
   * @param msg the wrapped message.
   * @param firstHop the nodeHandle of the first hop destination
   * @param serializeVersion the serialization version of the message
   */
  public RouteMessage(Id target, PastryRawMessage msg, NodeHandle firstHop, byte serializeVersion) {
    this(target, msg, firstHop, null, serializeVersion);
  }

  /**
   * Constructor.
   *
   * @param target this is id of the node the message will be routed to.
   * @param msg the wrapped message.
   * @param firstHop the nodeHandle of the first hop destination
   * @param opts the send options for the message.
   * @param serializeVersion the serialization version of the message
   */
  public RouteMessage(Id target, PastryRawMessage msg, NodeHandle firstHop, SendOptions opts, byte serializeVersion) {
    super(RouterAddress.getCode());
    this.version = serializeVersion;
    this.target = (Id) target;
    internalMsg = msg;
    rawInternalMsg = msg;
    if (msg != null) internalType = msg.getType();
    nextHop = firstHop;
    this.opts = opts;
    if (this.opts == null) this.opts = new SendOptions();
    if (msg != null) // can be null on the deserialization, but that ctor properly sets auxAddress
      auxAddress = msg.getDestination();
  }




  /**
   * Gets the target node id of this message.
   *
   * @return the target node id.
   */

  public Id getTarget() {
    return target;
  }

  public NodeHandle getPrevNode() {
    return prevNode;
  }

  public void setPrevNode(NodeHandle nodeHandle) {
    prevNode = nodeHandle;
  }

  /**
   * The previous hop as the transport saw it: the node whose connection this message arrived on. Unlike
   * {@link #getPrevNode()}, which the router overwrites with the local handle before it hands the message to an
   * application, this stays as received. In certified mode it is the node whose id {@code PastryEndpoint} checks.
   * The sender named inside the wrapped message is something else: the originator's claim, not authenticated.
   *
   * @return the node this message was received from, or null if it was created on this node
   */
  public @Nullable NodeHandle getReceivedFrom() {
    return receivedFrom;
  }

  /**
   * Records the node this message arrived from, for a transport that hands over the message object itself instead
   * of bytes to build it from (the direct simulator): there the same object travels from hop to hop, so each
   * receipt overwrites the hop of the one before.
   *
   * @param nextHop the node the message was received from
   */
  public void setReceivedFrom(NodeHandle nextHop) {
    receivedFrom = nextHop;
  }

  public NodeHandle getNextHop() {
    return nextHop;
  }

  public void setNextHop(NodeHandle nodeHandle) {
    nextHop = nodeHandle;
  }

  /**
   * Get priority
   *
   * @return the priority of this message.
   */

  @Override
  public int getPriority() {
    if (internalMsg != null)
      return internalMsg.getPriority();
    return internalPriority;
  }

  /**
   * Get transmission options.
   *
   * @return the options.
   */

  public SendOptions getOptions() {
    if (opts == null) {
      opts = new SendOptions();
    }
    return opts;
  }

  @Override
  public String toString() {
    if (internalMsg == null) {
      return "R[serialized{"+auxAddress+","+internalType+"}]";
    }
    String str = "R[" + internalMsg + "]";

    return str;
  }

  // Common API Support

  @Override
  public rice.p2p.commonapi.Id getDestinationId() {
    return getTarget();
  }

  @Override
  public rice.p2p.commonapi.NodeHandle getNextHopHandle() {
    return nextHop;
  }

  @Override
  public rice.p2p.commonapi.Message getMessage(MessageDeserializer deserializer) throws IOException {
    endpointDeserializer.setSubDeserializer(deserializer);
    return ((PastryEndpointMessage) unwrap(endpointDeserializer)).getMessage();
  }

  @Override
  public void setDestinationId(rice.p2p.commonapi.Id id) {
    target = (Id) id;
  }

  @Override
  public void setNextHopHandle(rice.p2p.commonapi.NodeHandle nextHop) {
    this.nextHop = (NodeHandle) nextHop;
  }

  /**
   * Replaces the application's message; the message must have been read with
   * {@link #getMessage(MessageDeserializer)} first.
   */
  @Override
  public void setMessage(RawMessage message) {
    if (internalMsg == null) {
      throw new IllegalStateException("The message of "+this+" is still serialized, read it with getMessage() first");
    }
    ((PastryEndpointMessage) internalMsg).setMessage(message);
  }

  /**
   * version 1:
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            int auxAddress                                     +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + bool hasHndle +        // if it has a destinationHandle instead of an Id
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            Id    target                                       +
   *   +  (only exists if the hasHandle boolean is false              +
   *   +                                                               +
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            NodeHandle destinationHandle                       +
   *   +  (used if the RouteMessage is intended for a specific node)   +
   *   +       (only exists if the hasHandle boolean is true)          +
   *        ...
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            NodeHandle prev                                    +
   *   +  (used to repair routing table during routing)                +
   *   +                                                               +
   *        ...
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            Internal Message                                   +
   *   +  (see below)                                                  +
   *   +                                                               +
   *
   * version 0:
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            int auxAddress                                     +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            Id    target                                       +
   *   +                                                               +
   *   +                                                               +
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            NodeHandle prev                                    +
   *   +  (used to repair routing table during routing)                +
   *   +                                                               +
   *        ...
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            Internal Message                                   +
   *   +  (see below)                                                  +
   *   +                                                               +
   */
  public static RouteMessage build(InputBuffer buf, byte priority, PastryNode pn, NodeHandle prev, byte outputVersion) throws IOException {

    byte version = buf.readByte();
    switch(version) {
    case 0:
      {
        int auxAddress = buf.readInt();
        Id target = Id.build(buf);
        return new RouteMessage(target, auxAddress, prev, buf, priority, pn, null, outputVersion);
      }
    case 1:
      {
        int auxAddress = buf.readInt();
        NodeHandle destHandle = null;
        Id target = null;
        boolean hasDestHandle = buf.readBoolean();
        if (hasDestHandle) { // destHandle exists
          destHandle = pn.readNodeHandle(buf);
          target = destHandle.getNodeId();
        } else {
          target = Id.build(buf);
        }
        return new RouteMessage(target, auxAddress, prev, buf, priority, pn, destHandle, outputVersion);
      }
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }

  public RouteMessage(Id target, int auxAddress, NodeHandle prev, InputBuffer buf, byte priority, PastryNode pn, NodeHandle destinationHandle, byte serializeVersion) throws IOException {
    this(target, (PastryRawMessage) null, null, null, serializeVersion);
    hasSender = buf.readBoolean();
    internalPriority = priority;
    internalType = buf.readShort();
    prevNode = prev;
    receivedFrom = prev;
    serializedMsg = buf;
    this.destinationHandle = destinationHandle;
    this.pn = pn;
    this.auxAddress = auxAddress;

  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte(version); // version (deserialized in build())
    buf.writeInt(auxAddress); // (deserialized in build())
    switch (version) {
    case 0:
      target.serialize(buf); // (deserialized in build())
      break;
    case 1:
      buf.writeBoolean(destinationHandle != null); // (deserialized in build())
      if (destinationHandle != null) {
        destinationHandle.serialize(buf); // (deserialized in build())
      } else {
        target.serialize(buf); // (deserialized in build())
      }
    } // switch

    if (serializedMsg != null) { // pri, sdr
      // fixed Fabio's bug from Nov 2006 (these were deserialized in the constructor above, but not added back into the internal stream.)
      buf.writeBoolean(hasSender);

      buf.writeShort(internalType);

      // optimize this, possibly by extending InternalBuffer interface to access the raw underlieing bytes
      byte[] bytes = new byte[serializedMsg.bytesRemaining()];
      serializedMsg.read(bytes);
      buf.write(bytes,0,bytes.length);
      serializedMsg = new SimpleInputBuffer(bytes);
    } else {
//    address was already peeled off as the auxAddress
//    different wire to deserialize the Address and eliminate unneeded junk
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    +bool hasSender +   Priority    +  Type (Application specific)   + // zero is reserved
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
//    optional
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    +            NodeHandle sender                                  +
//    +                                                               +
//                      ...  flexible size
//    +                                                               +
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      if (rawInternalMsg == null) {
        throw new IOException("Cannot serialize " + this + ": its message " + internalMsg + " is not a raw message");
      }
      NodeHandle sender = rawInternalMsg.getSender();
      boolean hasSender = (sender != null);
      if (hasSender) {
        buf.writeBoolean(true);
      } else {
        buf.writeBoolean(false);
      }

      // range check priority
      int priority = rawInternalMsg.getPriority();
      if (priority > Byte.MAX_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+rawInternalMsg+" was "+priority+".");
      if (priority < Byte.MIN_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+rawInternalMsg+" was "+priority+".");

      short type = rawInternalMsg.getType();
      buf.writeShort(type);

      if (hasSender) {
        sender.serialize(buf);
      }

      rawInternalMsg.serialize(buf);
    }
  }

  public Message unwrap(MessageDeserializer deserializer) throws IOException {
    if (internalMsg != null) {
      return internalMsg;
    }

    // deserialize using md

//  address was already peeled off as the auxAddress
//  different wire to deserialize the Address and eliminate unneeded junk
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  +bool hasSender +   Priority    +  Type (Application specific)   + // zero is reserved
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
//  optional
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  +            NodeHandle sender                                  +
//  +                                                               +
//                    ...  flexible size
//  +                                                               +
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    NodeHandle internalSender = null;
    if (hasSender) {
      internalSender = pn.readNodeHandle(serializedMsg);
    }

    internalMsg = (Message)deserializer.deserialize(serializedMsg, internalType, internalPriority, internalSender);
    // the deserializers build the message at its default priority; forwarding must keep the one it was sent with
    internalMsg.setPriority(internalPriority);
    // serialize() writes the message from here on, when it is routed on to the next hop
    rawInternalMsg = internalMsg instanceof PastryRawMessage rawMessage ? rawMessage : null;

    // the serializedMsg is now dirty, because the unwrapper may change the internal message
    serializedMsg = null;
    pn = null;

    return internalMsg;
  }

  @Override
  public short getType() {
    return RouterMessageType.ROUTE.id();
  }

  class RMDeserializer extends PastryMessageDeserializer {
    MessageDeserializer sub;

    public RMDeserializer() {
      super(null);
    }

    public void setSubDeserializer(MessageDeserializer deserializer) {
      sub = deserializer;
    }

    @Override
    public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
      return new PastryEndpointMessage(auxAddress, buf, sub, type, priority, sender);
    }

  }

  public int getAuxAddress() {
    return auxAddress;
  }

  public void setDestinationHandle(NodeHandle handle) {
    destinationHandle = handle;
  }

  public NodeHandle getDestinationHandle() {
    return destinationHandle;
  }

  private Map<String, Object> options;
  public Map<String, Object> getTransportLayerOptions() {
    return options;
  }

  public void setTransportLayerOptions(Map<String, Object> options) {
    this.options = options;
  }

  // synchronization problem...
  // This is so the RouteMessage can be cancelled
  Cancellable tlCancellable;
  public void setTransportLayerCancellable(Cancellable cancellable) {
    tlCancellable = cancellable;
  }

  public boolean cancel() {
    return tlCancellable.cancel();
  }

  RouteMessageNotification notifyMe;

  public void setRouteMessageNotification(RouteMessageNotification notification) {
    notifyMe = notification;
  }

  public void sendSuccess(NodeHandle nextHop) {
    if (notifyMe != null) notifyMe.sendSuccess(this, nextHop);
  }

  /**
   * Return true if it notified a higher layer.
   *
   */
  public boolean sendFailed(Exception e) {
    if (notifyMe != null) notifyMe.sendFailed(this, e);
    return notifyMe != null;
  }

  public Message getInternalMessage() {
    return internalMsg;
  }

  public void incrementNumRetries() {
    numRetries++;
  }

  public int getNumRetries() {
    return numRetries;
  }
}
