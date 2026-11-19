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
package rice.pastry.standard;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.priority.QueueOverflowException;
import rice.p2p.commonapi.Cancellable;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.messaging.MessageDispatch;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouterStrategy;
import rice.pastry.routing.SendOptions;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;

import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_SUSPECTED;

/**
 * The superclass makes the routing decisions.  This class handles the rapid-rerouting.
 *
 * @author Jeff Hoye
 *
 */
public class RapidRerouter extends StandardRouter implements LivenessListener<NodeHandle> {

  /**
   * The max times to try to reroute a message.
   */
  public static final int MAX_RETRIES = 10;

  /**
   * These messages should be rapidly rerouted if the node goes suspected.
   */
  final Map<NodeHandle, Collection<RouterNotification>> pending;

  public RapidRerouter(PastryNode thePastryNode, MessageDispatch dispatch, RouterStrategy strategy) {
    super(thePastryNode, dispatch, strategy);
    pending = new HashMap<NodeHandle, Collection<RouterNotification>>();

    thePastryNode.addLivenessListener(this);
  }


  @Override
  protected void sendTheMessage(RouteMessage rm, NodeHandle handle) {
    if (rm.getOptions().areMultipleHopsAllowed() && rm.getOptions().rerouteIfSuspected()) {
      // can be rapidly rerouted
      if (handle.getLiveness() >= LIVENESS_SUSPECTED) {
        super.sendTheMessage(rm, handle);
        return;
      }

      RouterNotification notifyMe = new RouterNotification(rm, handle);
      addToPending(notifyMe, handle);
      rm.setTransportLayerCancellable(notifyMe);
      notifyMe.setCancellable(thePastryNode.send(handle, rm, notifyMe, rm.getTransportLayerOptions()));
    } else {
      super.sendTheMessage(rm, handle);
    }
  }

  protected void rerouteMe(final RouteMessage rm, NodeHandle oldDest, Exception ioe) {
    logger.debug("rerouteMe({} oldDest:{})", rm, oldDest);

    rm.incrementNumRetries();
    if (rm.getNumRetries() > MAX_RETRIES) {
      // TODO: notify an error handler, not just the message's own callback
      boolean dontPrint = false;
      if (ioe == null) {
        dontPrint = rm.sendFailed(new TooManyRouteAttempts(rm, MAX_RETRIES));
      } else {
        dontPrint = rm.sendFailed(ioe);
      }
      if (dontPrint) {
        logger.info("rerouteMe() dropping {} after {} attempts to (re)route.", rm, rm.getNumRetries());
      } else {
        logger.warn("rerouteMe() dropping {} after {} attempts to (re)route.", rm, rm.getNumRetries());
      }
      return;
    }

    // give the selector a chance to do some IO before trying to schedule again
    thePastryNode.getEnvironment().getSelectorManager().invoke(() -> {
      // this is going to make forward() be called again, can prevent this with a check in getPrevNode().equals(localNode)
      rm.getOptions().setRerouteIfSuspected(SendOptions.defaultRerouteIfSuspected);
      route(rm);
    });
  }

  private void addToPending(RouterNotification notifyMe, NodeHandle handle) {
    logger.debug("addToPending({} to:{})", notifyMe, handle);
    synchronized(pending) {
      Collection<RouterNotification> c = pending.get(handle);
      if (c == null) {
        c = new HashSet<RouterNotification>();
        pending.put(handle, c);
      }
      c.add(notifyMe);
    }
  }

  /**
   * Return true if it was still pending.
   *
   * @return true if still pending
   */
  private boolean removeFromPending(RouterNotification notifyMe, NodeHandle handle) {
    synchronized(pending) {
      Collection<RouterNotification> c = pending.get(handle);
      if (c == null) {
        logger.debug("removeFromPending({},{}) had no pending messages for handle.", notifyMe, handle);
        return false;
      }
      boolean ret = c.remove(notifyMe);
      if (c.isEmpty()) {
        pending.remove(handle);
      }
      if (!ret) {
        logger.debug("removeFromPending({},{}) msg was not there.", notifyMe, handle);
      }
      return ret;
    }
  }

  @Override
  public void livenessChanged(NodeHandle i, int val, Map<String, Object> options) {
    if (val >= LIVENESS_SUSPECTED) {
      Collection<RouterNotification> rerouteMe;
      synchronized(pending) {
        rerouteMe = pending.remove(i);
      }
      if (rerouteMe != null) {
        logger.debug("removing all messages to:{}", i);
        for (RouterNotification rn : rerouteMe) {
          rn.cancel();
          rerouteMe(rn.rm, rn.dest, null);
        }
      }
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    thePastryNode.removeLivenessListener(this);
  }

  class RouterNotification implements Cancellable, PMessageNotification {
    RouteMessage rm;
    NodeHandle dest;
    PMessageReceipt cancellable;

    public RouterNotification(RouteMessage rm, NodeHandle handle) {
      this.rm = rm;
      this.dest = handle;
      logger.debug("RN.ctor() {} to:{}", rm, dest);
    }

    public void setCancellable(PMessageReceipt receipt) {
      if (receipt == null) logger.warn("{}.setCancellable(null)", this, new Exception("Stack Trace"));

      this.cancellable = receipt;
    }

    @Override
    public void sendFailed(PMessageReceipt msg, Exception reason) {
      // what to do..., rapidly reroute?
      failed = true;
      cancellable = null;
      rm.setTransportLayerCancellable(null);
      if (reason instanceof QueueOverflowException) {
        // todo, reroute this to a node w/o a full queue
        removeFromPending(this, dest);
        if (rm.sendFailed(reason)) {
          logger.info("sendFailed({})=>{}", msg.getMessage(), msg.getIdentifier(), reason);
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("sendFailed("+msg.getMessage()+")=>"+msg.getIdentifier(), reason);
          } else {
            if (logger.isInfoEnabled()) {
              if (msg.getIdentifier() == null) {
                logger.info("sendFailed("+msg.getMessage()+")=>"+msg.getIdentifier()+" "+reason+" identifier was null!!!", new Exception("Stack Trace"));
              } else {
                logger.info("sendFailed("+msg.getMessage()+")=>"+msg.getIdentifier()+" "+reason);
              }
            }
          }
        }
        return;
      }

      if (reason instanceof NodeIsFaultyException) {
        if (msg.getIdentifier().getLiveness() < NodeHandle.LIVENESS_DEAD) {
          if (logger.isWarnEnabled()) {
            logger.warn("Threw NodeIsFaultyException, and node is alive.  Node:"+msg.getIdentifier()+" Liveness:"+msg.getIdentifier().getLiveness(), reason);
            logger.warn("RRTrace", new Exception("Stack Trace"));
          }
        }
      }

      if (removeFromPending(this, dest)) {
        logger.info("Send failed on message {} to {} rerouting.{}", rm, dest, msg, reason);
        rerouteMe(rm, dest, reason);
      } else {
        rm.sendFailed(reason);
        logger.info("sendFailed({})=>{}", msg.getMessage(), msg.getIdentifier(), reason);
      }
    }

    boolean failed = false;
    boolean sent = false;
    boolean cancelled = false;

    @Override
    public void sent(PMessageReceipt msg) {
      logger.debug("Send success {} to:{} {}", rm, dest, msg);
      sent = true;
      cancellable = null;
      rm.setTransportLayerCancellable(null);
      removeFromPending(this, dest);
      rm.sendSuccess(dest);
    }

    @Override
    public boolean cancel() {
      logger.debug("cancelling {}", this);
      if (cancellable == null && logger.isWarnEnabled()) logger.warn("cancellable = null c:"+cancelled+" s:"+sent+" f:"+failed);
      cancelled = true;
      if (cancellable != null) return cancellable.cancel();
      return true;
    }

    @Override
    public String toString() {
      return "RN{"+rm+"->"+dest+"}";
    }
  }


}
