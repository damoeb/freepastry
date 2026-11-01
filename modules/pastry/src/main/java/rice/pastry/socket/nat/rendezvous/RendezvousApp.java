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
package rice.pastry.socket.nat.rendezvous;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessTypes;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.rendezvous.RendezvousStrategy;
import org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayer;
import org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import rice.Continuation;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.util.AttachableCancellable;
import rice.p2p.util.rawserialization.WireLengths;
import rice.pastry.PastryNode;
import rice.pastry.client.PastryAppl;
import rice.pastry.messaging.Message;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouteMessageNotification;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;
import rice.selector.SelectorManager;


/**
 * @author Jeff Hoye
 *
 */
public class RendezvousApp extends PastryAppl implements RendezvousStrategy<RendezvousSocketNodeHandle> {
  private final SelectorManager selectorManager;
  private RendezvousTransportLayer<RendezvousSocketNodeHandle> tl;


  public RendezvousApp(PastryNode pn) {
    super(pn,null,0,null);
    setDeserializer(new MessageDeserializer() {

      @Override
      public rice.p2p.commonapi.Message deserialize(InputBuffer buf, short type,
          int priority, NodeHandle sender) throws IOException {
        byte version;
        switch (RendezvousMessageType.find(type)) {
        case null:
          throw new IOException("Unknown type: "+type);
        case BYTE_BUFFER:
          version = buf.readByte();
          if (version == 0) { // version 0
            NodeHandle originalSender = thePastryNode.readNodeHandle(buf);
            byte[] msg = WireLengths.readBytes(buf);
            return new ByteBufferMsg(ByteBuffer.wrap(msg),originalSender,priority,getAddress());
          } else {
            throw new IOException("Unknown version for ByteBufferMsg: "+version);
          }
        case PILOT_FORWARD:
          version = buf.readByte();
          if (version == 0) { // version 0
            RendezvousSocketNodeHandle target = (RendezvousSocketNodeHandle)thePastryNode.readNodeHandle(buf);
            ByteBufferMsg subMsg = (ByteBufferMsg)deserialize(buf, RendezvousMessageType.BYTE_BUFFER.id(), priority, sender);
            return new PilotForwardMsg(getAddress(), subMsg, target);
          } else {
            throw new IOException("Unknown version for PilotForwardMsg: "+version);
          }
        case OPEN_CHANNEL:
          version = buf.readByte();
          if (version == 0) { // version 0
            RendezvousSocketNodeHandle rendezvous = (RendezvousSocketNodeHandle)thePastryNode.readNodeHandle(buf);
            RendezvousSocketNodeHandle source = (RendezvousSocketNodeHandle)thePastryNode.readNodeHandle(buf);
            int uid = buf.readInt();
            return new OpenChannelMsg(getAddress(), rendezvous, source, uid);
          } else {
            throw new IOException("Unknown version for OpenChannelMsg: "+version);
          }
        }
      }
    }); // this constructor doesn't auto-register
    selectorManager = pn.getEnvironment().getSelectorManager();
  }

  @Override
  public void messageForAppl(Message msg) {
    if (!(msg instanceof RendezvousMessage rendezvousMessage)) {
      return;
    }
    switch (rendezvousMessage) {
      case ByteBufferMsg bbm -> {
        logger.debug("messageForAppl({})", bbm);
        try {
          tl.messageReceivedFromOverlay((RendezvousSocketNodeHandle)bbm.getOriginalSender(), bbm.buffer, null);
        } catch (IOException ioe) {
          logger.warn("dropping {}", bbm, ioe);
        }
      }
      case PilotForwardMsg pfm -> {
        logger.trace("Forwarding message {}", pfm);
        thePastryNode.send(pfm.getTarget(), pfm.getBBMsg(), null, null);
      }
      case OpenChannelMsg ocm -> {
        // we're a NATted node who needs to open a channel
        tl.openChannel(ocm.getSource(),  ocm.getRendezvous(), ocm.getUid());
      }
    }
  }

  @Override
  public boolean deliverWhenNotReady() {
    return true;
  }

  @Override
  public Cancellable openChannel(final RendezvousSocketNodeHandle target,
      final RendezvousSocketNodeHandle rendezvous,
      final RendezvousSocketNodeHandle source,
      final int uid,
      final Continuation<Integer, Exception> deliverAckToMe,
      final Map<String, Object> options) {

    logger.atInfo().log(() -> "openChannel()"+source+"->"+target+" via "+rendezvous+" uid:"+uid+","+deliverAckToMe+","+options);

    if (target.getLiveness() > LivenessTypes.LIVENESS_DEAD) {
      // if he's dead forever (consider changing this to dead... but think of implications)
      logger.atInfo().log(() -> "openChannel() attempted to open to dead_forever target. Dropping."+source+"->"+target+" via "+rendezvous+" uid:"+uid+","+deliverAckToMe+","+options);
      if (deliverAckToMe != null) deliverAckToMe.receiveException(new NodeIsFaultyException(target));
      return null;
    }

    if (target.canContactDirect()) {
      // this is a bug
      throw new IllegalArgumentException("Target must be firewalled.  Target:"+target);
    }

    // we don't want state changing, so this can only be called on the selector
    if (!selectorManager.isSelectorThread()) {
      final AttachableCancellable ret = new AttachableCancellable();
      selectorManager.invoke(() -> ret.attach(openChannel(target, rendezvous, source, uid, deliverAckToMe, options)));
      return ret;
    }

    OpenChannelMsg msg = new OpenChannelMsg(getAddress(), rendezvous, source, uid);
    logger.debug("routing {} to {}", msg, target);
    final RouteMessage rm =
      new RouteMessage(
          target.getNodeId(),
          msg,
        thePastryNode.getConfig().routeMsgVersion());
    rm.setDestinationHandle(target);

    logger.atTrace().log(() -> "openChannel("+target+","+rendezvous+","+source+","+uid+","+deliverAckToMe+","+options+") sending via "+rm);

    // TODO: make PastryNode have a router that does this properly, rather than receiveMessage
    final Cancellable ret = () -> {
      logger.atDebug().log(() -> "openChannel("+target+","+rendezvous+","+source+","+uid+","+deliverAckToMe+","+options+").cancel()");
      return rm.cancel();
    };

    // NOTE: Installing this anyway if the LogLevel is high enough is kind of wild, but really useful for debugging
    if ((deliverAckToMe != null) || logger.isInfoEnabled()) {
      rm.setRouteMessageNotification(new RouteMessageNotification() {
        @Override
        public void sendSuccess(rice.pastry.routing.RouteMessage message, rice.pastry.NodeHandle nextHop) {
          logger.atTrace().log(() -> "openChannel("+target+","+rendezvous+","+source+","+uid+","+deliverAckToMe+","+options+").sendSuccess():"+nextHop);
          if (deliverAckToMe != null) deliverAckToMe.receiveResult(uid);
        }
        @Override
        public void sendFailed(rice.pastry.routing.RouteMessage message, Exception e) {
          logger.atDebug().log(() -> "openChannel("+target+","+rendezvous+","+source+","+uid+","+deliverAckToMe+","+options+").sendFailed("+e+")");
          if (deliverAckToMe != null) deliverAckToMe.receiveException(e);
        }
      });
    }

    rm.setTLOptions(options);

    thePastryNode.getRouter().route(rm);

    return ret;
  }

  @Override
  public MessageRequestHandle<RendezvousSocketNodeHandle, ByteBuffer> sendMessage(
      final RendezvousSocketNodeHandle i,
      final ByteBuffer m,
      final MessageCallback<RendezvousSocketNodeHandle, ByteBuffer> deliverAckToMe,
      Map<String, Object> ops) {
    logger.debug("sendMessage({},{},{},{})", i, m, deliverAckToMe, ops);
    // TODO: use the new method in PastryAppl

    // pull USE_UDP, because that's not going to happen
    final Map<String, Object> options = OptionsFactory.removeOption(ops, WireTransportLayer.OPTION_TRANSPORT_TYPE);

    int priority = 0;
    if (options.containsKey(PriorityTransportLayer.OPTION_PRIORITY)) {
      priority = ((Integer)options.get(PriorityTransportLayer.OPTION_PRIORITY));
    }
    ByteBufferMsg msg = new ByteBufferMsg(m, thePastryNode.getLocalHandle(), priority, getAddress());

    if (options.containsKey(RendezvousTransportLayerImpl.OPTION_USE_PILOT)) {
      RendezvousSocketNodeHandle pilot = (RendezvousSocketNodeHandle)options.get(RendezvousTransportLayerImpl.OPTION_USE_PILOT);
      logger.atTrace().log(() -> "sendMessage("+i+","+m+","+deliverAckToMe+","+options+") sending via "+pilot);
      final MessageRequestHandleImpl<RendezvousSocketNodeHandle, ByteBuffer> ret =
        new MessageRequestHandleImpl<RendezvousSocketNodeHandle, ByteBuffer>(i,m,options);
      ret.setSubCancellable(thePastryNode.send(pilot, new PilotForwardMsg(getAddress(),msg,i), new PMessageNotification(){
        @Override
        public void sent(PMessageReceipt msg) {
          if (deliverAckToMe != null) deliverAckToMe.ack(ret);
        }
        @Override
        public void sendFailed(PMessageReceipt msg, Exception reason) {
          if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, reason);
        }
      }, null));
      return ret;
    } else {

      final RouteMessage rm =
        new RouteMessage(
            i.getNodeId(),
            msg,
          thePastryNode.getConfig().routeMsgVersion());
      rm.setDestinationHandle(i);

      rm.setTLOptions(options);

      logger.atTrace().log(() -> "sendMessage("+i+","+m+","+deliverAckToMe+","+options+") sending via "+rm);

      // TODO: make PastryNode have a router that does this properly, rather than receiveMessage
      final MessageRequestHandle<RendezvousSocketNodeHandle, ByteBuffer> ret = new MessageRequestHandle<RendezvousSocketNodeHandle, ByteBuffer>() {

        @Override
        public boolean cancel() {
          logger.debug("sendMessage({},{},{},{}).cancel()", i, m, deliverAckToMe, options);
          return rm.cancel();
        }

        @Override
        public ByteBuffer getMessage() {
          return m;
        }

        @Override
        public RendezvousSocketNodeHandle getIdentifier() {
          return i;
        }

        @Override
        public Map<String, Object> getOptions() {
          return options;
        }
      };

      // NOTE: Installing this anyway if the LogLevel is high enough is kind of wild, but really useful for debugging
      if ((deliverAckToMe != null) || logger.isDebugEnabled()) {
        rm.setRouteMessageNotification(new RouteMessageNotification() {
          @Override
          public void sendSuccess(rice.pastry.routing.RouteMessage message, rice.pastry.NodeHandle nextHop) {
            logger.atTrace().log(() -> "sendMessage("+i+","+m+","+deliverAckToMe+","+options+").sendSuccess():"+nextHop);
            if (deliverAckToMe != null) deliverAckToMe.ack(ret);
          }
          @Override
          public void sendFailed(rice.pastry.routing.RouteMessage message, Exception e) {
            logger.atDebug().log(() -> "sendMessage("+i+","+m+","+deliverAckToMe+","+options+").sendFailed("+e+")");
            if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, e);
          }
        });
      }

      rm.setTLOptions(options);

      thePastryNode.getRouter().route(rm);

      return ret;
    }
  }

  @Override
  public String toString() {
    return "RendezvousApp{"+thePastryNode+"}";
  }

  @Override
  public void setTransportLayer(
      RendezvousTransportLayer<RendezvousSocketNodeHandle> tl) {
    this.tl = tl;
  }





}
