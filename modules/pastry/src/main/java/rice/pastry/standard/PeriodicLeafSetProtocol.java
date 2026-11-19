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

import java.io.IOException;
import java.util.Map;

import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.TimerWeakHashMap;
import rice.pastry.NodeHandle;
import rice.pastry.NodeSet;
import rice.pastry.NodeSetEventSource;
import rice.pastry.NodeSetListener;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeListener;
import rice.pastry.ReadyStrategy;
import rice.pastry.ScheduledMessage;
import rice.pastry.client.PastryAppl;
import rice.pastry.leafset.BroadcastLeafSet;
import rice.pastry.leafset.InitiateLeafSetMaintenance;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetMessageType;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.leafset.LeafSetProtocolAddress;
import rice.pastry.leafset.RequestLeafSet;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.PastryMessageDeserializer;
import rice.pastry.routing.RoutingTable;
import rice.selector.TimerTask;

/**
 * An implementation of a periodic-style leafset protocol
 *
 * @version $Id: PeriodicLeafSetProtocol.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
public class PeriodicLeafSetProtocol extends PastryAppl implements ReadyStrategy, NodeSetListener, NodeHandleListener, PastryNodeListener, LeafSetProtocol {
  /** Listener priority on leaf set members' node handles. */
  private static final int NODE_HANDLE_LISTENER_PRIORITY = 50;
  /** How long the last send and receive times of a leaf set broadcast are kept per node. */
  private static final int BLS_TIME_HARD_LINK_MILLIS = 300000;

  private final NodeHandle localHandle;

  private final PastryNode localNode;

  private final LeafSet leafSet;

  private final RoutingTable routeTable;

  /**
   * NodeHandle -> Long remembers the TIME when we received a BLS from that
   * NodeHandle
   */
  private final Map<NodeHandle, Long> lastTimeReceivedBLS; // the leases you have
  private final Map<NodeHandle, Long> lastTimeSentBLS; // the leases you have issued

  /**
   * Related to rapidly determining direct neighbor liveness.
   */
  public final int PING_NEIGHBOR_PERIOD; // 20 sec

  public final int LEASE_PERIOD; // 30 sec

  public final int BLS_THROTTLE; // 10 sec

  ScheduledMessage pingNeighborMessage;

  RandomSource random;

  TimeSource timeSource;

  public static class PLSPMessageDeserializer extends PastryMessageDeserializer {

    public PLSPMessageDeserializer(PastryNode pn) {
      super(pn);
    }

    @Override
    public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
      return switch (LeafSetMessageType.find(type)) {
        case REQUEST_LEAF_SET -> new RequestLeafSet(sender, buf);
        case BROADCAST_LEAF_SET -> new BroadcastLeafSet(buf, pn);
        case null -> throw new IOException("Unknown message type " + type + " for " + getClass().getName());
      };
    }

  }

  /**
   * Builds a periodic leafset protocol
   *
   */
  public PeriodicLeafSetProtocol(PastryNode node, NodeHandle local,
      LeafSet leafSet, RoutingTable routingTable) {
    super(node, null, LeafSetProtocolAddress.getCode(), new PLSPMessageDeserializer(node));
    this.localNode = node;
    node.addPastryNodeListener(this); // to call start() if we are the seed node

    PeriodicLeafSetConfig config = PeriodicLeafSetConfig.fromParameters(node.getEnvironment().getParameters());
    if (config.useOwnRandom()) {
      if (config.randomSeed().isPresent()) {
        this.random = new SimpleRandomSource(config.randomSeed().getAsLong(), node.getEnvironment().getLogManager(),
            "periodic_leaf_set");
      } else {
        this.random = new SimpleRandomSource(node.getEnvironment().getLogManager(), "periodic_leaf_set");
      }
    } else {
      this.random = node.getEnvironment().getRandomSource();
    }

    this.timeSource = node.getEnvironment().getTimeSource();
    this.localHandle = local;

    // make sure to register all the existing leafset entries
    this.leafSet = leafSet;
    for (NodeHandle nodeHandle : this.leafSet.asList()) {
      nodeHandle.addNodeHandleListener(this, NODE_HANDLE_LISTENER_PRIORITY);
    }

    this.routeTable = routingTable;
    this.lastTimeReceivedBLS = new TimerWeakHashMap<NodeHandle, Long>(node.getEnvironment().getSelectorManager().getTimer(), BLS_TIME_HARD_LINK_MILLIS);
    this.lastTimeSentBLS = new TimerWeakHashMap<NodeHandle, Long>(node.getEnvironment().getSelectorManager().getTimer(), BLS_TIME_HARD_LINK_MILLIS);
    PING_NEIGHBOR_PERIOD = config.pingNeighborPeriod();
    LEASE_PERIOD = config.leasePeriod();
    BLS_THROTTLE = config.requestLeaseThrottle();
    this.lastTimeRenewedLease = new TimerWeakHashMap<NodeHandle, Long>(node.getEnvironment().getSelectorManager().getTimer(), LEASE_PERIOD*2);

    pingNeighborMessage = localNode.scheduleMessageAtFixedRate(
        new InitiatePingNeighbor(), PING_NEIGHBOR_PERIOD, PING_NEIGHBOR_PERIOD);
  }

  private void updateRecBLS(NodeHandle from, long time) {
    if (time == 0) return;
    Long oldTime = (Long) lastTimeReceivedBLS.get(from);
    if ((oldTime == null) || (oldTime.longValue() < time)) {
      lastTimeReceivedBLS.put(from, Long.valueOf(time));
      long leaseTime = time+LEASE_PERIOD-timeSource.currentTimeMillis();
      if (logger.isDebugEnabled()) {
        logger.debug("PLSP.updateRecBLS("+from+","+time+"):"+leaseTime);
      } else {
        if (leaseTime < 10 && logger.isInfoEnabled()) {
          logger.info("PLSP.updateRecBLS("+from+","+time+"):"+leaseTime);
        }
      }

      // need to do this so that nodes are notified
      if (hasSetStrategy)
        isReady();
    }
  }

  /**
   * Receives messages.
   *
   * @param msg the message.
   */
  @Override
  @SuppressWarnings("ReferenceEquality") // the local node handle is a single instance
  public void receiveMessage(Message msg) {
    assert thePastryNode.getEnvironment().getSelectorManager().isSelectorThread() : "the leaf set is modified on the selector thread only";
    if (msg instanceof BroadcastLeafSet broadcastLeafSet) {
      // receive a leafset from another node

      // if we have now successfully joined the ring, set the local node ready
      if (broadcastLeafSet.type() == BroadcastLeafSet.JoinInitial) {
        // merge the received leaf set into our own
        leafSet.merge(broadcastLeafSet.leafSet(), broadcastLeafSet.from(), routeTable, false,
            null);

        broadcastAll();
      } else {
        // first check for missing entries in their leafset
        NodeSet set = leafSet.neighborSet(Integer.MAX_VALUE);

        // don't need to remove any nodes that we already found faulty, because leafset.merge does not accept them

        // if we find any missing entries, check their liveness
        for (int i = 0; i < set.size(); i++)
          if (broadcastLeafSet.leafSet().test(set.get(i)))
            set.get(i).checkLiveness();

        // now check for assumed-dead entries in our leafset
        set = broadcastLeafSet.leafSet().neighborSet(Integer.MAX_VALUE);

        // if we find any missing entries, check their liveness
        for (int i = 0; i < set.size(); i++)
          if (set.get(i).getLiveness() >= NodeHandle.LIVENESS_DEAD)
            set.get(i).checkLiveness();

        // merge the received leaf set into our own
        leafSet.merge(broadcastLeafSet.leafSet(), broadcastLeafSet.from(), routeTable, false,
            null);
      }
      // do this only if you are his proper neighbor !!!
      if ((broadcastLeafSet.leafSet().get(1) == localHandle) ||
          (broadcastLeafSet.leafSet().get(-1) == localHandle)) {
        updateRecBLS(broadcastLeafSet.from(), broadcastLeafSet.getTimeStamp());
      }

    } else if (msg instanceof RequestLeafSet requestLeafSet) {
      // request for leaf set from a remote node

      if (requestLeafSet.getTimeStamp() > 0) { // without a timestamp, this is just a normal request, not a lease request
        // remember that we gave out a lease, and go unReady() if the node goes faulty
        lastTimeRenewedLease.put(requestLeafSet.returnHandle(),Long.valueOf(timeSource.currentTimeMillis()));

        // it's important to put the node in the leafset so that we don't accept messages for a node that
        // we issued a lease to
        // it's also important to record issuing the lease before putting into the leafset, so we can
        // call removeFromLeafsetIfPossible() in leafSetChange()
        leafSet.put(requestLeafSet.returnHandle());

        // nodes were never coming back alive when we called them faulty incorrectly
        if (requestLeafSet.returnHandle().getLiveness() >= NodeHandle.LIVENESS_DEAD) {
          logger.atInfo().log(() -> "Issued lease to dead node:"+requestLeafSet.returnHandle()+" initiating checkLiveness()");
          requestLeafSet.returnHandle().checkLiveness();

        }
      }

      // I don't like that we're sending the message after setting lastTimeRenewedLease, because there
      // could be a delay between here and above, but this should still be fine, because with
      // the assumption that the clocks are both advancing normally, we still should not get inconsistency
      // because the other node will stop receiving 30 seconds after he requested the lease, and we
      // won't receive until 30 seconds after the lastTimeRenewedLease.put() above
      thePastryNode.send(requestLeafSet.returnHandle(),
          new BroadcastLeafSet(localHandle, leafSet, BroadcastLeafSet.Update, requestLeafSet.getTimeStamp()),null, options);

    } else if (msg instanceof InitiateLeafSetMaintenance) {
      // perform leafset maintenance
      NodeSet set = leafSet.neighborSet(Integer.MAX_VALUE);

      if (set.size() > 1) {
        NodeHandle handle = set.get(random.nextInt(set.size() - 1) + 1);
        thePastryNode.send(handle,
            new RequestLeafSet(localHandle, timeSource.currentTimeMillis()),null, options);
        thePastryNode.send(handle,
            new BroadcastLeafSet(localHandle, leafSet, BroadcastLeafSet.Update, 0),null, options);

        NodeHandle check = set.get(random
            .nextInt(set.size() - 1) + 1);
        check.checkLiveness();
      }
    } else if (msg instanceof InitiatePingNeighbor) {
      // IPN every 20 seconds
      NodeHandle left = leafSet.get(-1);
      NodeHandle right = leafSet.get(1);

      // send BLS to left/right neighbor
      // ping if we don't currently have a lease
      if (left != null) {
        sendBLS(left, !hasLease(left));
      }
      if (right != null) {
        sendBLS(right, !hasLease(right));
      }
    }
  }

  /**
   * Broadcast the leaf set to all members of the local leaf set.
   *
   */
  protected void broadcastAll() {
    BroadcastLeafSet broadcastLeafSet = new BroadcastLeafSet(localHandle, leafSet,
        BroadcastLeafSet.JoinAdvertise, 0);
    NodeSet set = leafSet.neighborSet(Integer.MAX_VALUE);

    for (int i = 1; i < set.size(); i++)
      thePastryNode.send(set.get(i), broadcastLeafSet,null, options);
  }

  // Ready Strategy
  boolean hasSetStrategy = false;

  @Override
  public void start() {
    if (!hasSetStrategy) {
      logger.info("PLSP.start(): Setting self as ReadyStrategy");
      localNode.setReadyStrategy(this);
      hasSetStrategy = true;
      localNode.addLeafSetListener(this);
      // to notify listeners now if we have proper leases
      isReady();
    }
  }

  @Override
  public void stop() {
    if (hasSetStrategy) {
      logger.info("PLSP.start(): Removing self as ReadyStrategy");
      hasSetStrategy = false;
      localNode.removeLeafSetListener(this);
    }
  }

  /**
   * Called when the leafset changes
   */
  NodeHandle lastLeft;
  NodeHandle lastRight;
  @Override
  @SuppressWarnings("ReferenceEquality") // node handles are coalesced, so a new instance is a new neighbor
  public void nodeSetUpdate(NodeSetEventSource nodeSetEventSource, NodeHandle handle, boolean added) {
    NodeHandle newLeft = leafSet.get(-1);
    if (newLeft != null && (lastLeft != newLeft)) {
      lastLeft = newLeft;
      sendBLS(lastLeft,true);
    }
    NodeHandle newRight = leafSet.get(1);
    if (newRight != null && (lastRight != newRight)) {
      lastRight = newRight;
      sendBLS(lastRight,true);
    }
  }

  boolean ready = false;
  @Override
  public void setReady(boolean r) {
    if (ready != r) {
      synchronized(thePastryNode) {
        ready = r;
      }
      thePastryNode.notifyReadyObservers();
    }
  }

  /**
   *
   *
   */
  @Override
  public boolean isReady() {
    // check to see if we've heard from the left/right neighbors recently enough
    boolean shouldBeReady = shouldBeReady(); // temp

    if (shouldBeReady != ready) {
      thePastryNode.setReady(shouldBeReady); // will call back in to setReady() and notify the observers
    }

    return shouldBeReady;
  }

  public boolean shouldBeReady() {

    NodeHandle left = leafSet.get(-1);
    NodeHandle right = leafSet.get(1);

    // do it this way so we get going on both leases if need be
    boolean result = true;

    // see if received BLS within past LEASE_PERIOD seconds from left neighbor
    if (!hasLease(left)) {
      result = false;
      sendBLS(left, true);
    }

    // see if received BLS within past LEASE_PERIOD seconds from right neighbor
    if (!hasLease(right)) {
      result = false;
      sendBLS(right, true);
    }

    return result;
  }

  /**
   * Do we have a lease from this node?
   *
   * Returns true if nh is null.
   *
   * @param nodeHandle the NodeHandle we are interested if we have a lease from
   * @return if we have a lease from the NodeHandle
   */
  public boolean hasLease(NodeHandle nodeHandle) {
    long curTime = timeSource.currentTimeMillis();
    long leaseOffset = curTime-LEASE_PERIOD;

    if (nodeHandle != null) {
      Long time = (Long) lastTimeReceivedBLS.get(nodeHandle);
      if (time == null
          || (time.longValue() < leaseOffset)) {
        // we don't have a lease
        return false;
      }
    }
    return true;
  }

  /**
   *
   * @return true if we sent it, false if we didn't because of throttled
   */
  private boolean sendBLS(NodeHandle sendTo, boolean checkLiveness) {
    Long time = (Long) lastTimeSentBLS.get(sendTo);
    long currentTime = timeSource.currentTimeMillis();
    if (time == null
        || (time.longValue() < (currentTime - BLS_THROTTLE))) {
      logger.debug("PeriodicLeafSetProtocol: Checking liveness on neighbor:{} {} cl:{}", sendTo, time, checkLiveness); // only log if not throttled
      lastTimeSentBLS.put(sendTo, Long.valueOf(currentTime));

      thePastryNode.send(sendTo, new BroadcastLeafSet(localHandle, leafSet, BroadcastLeafSet.Update, 0), null, options);
      thePastryNode.send(sendTo, new RequestLeafSet(localHandle, currentTime), null, options);
      if (checkLiveness) {
        sendTo.checkLiveness();
      }
      return true;
    }
    return false;
  }

  /**
   * NodeHandle -> time
   *
   * Leases we have issued.  We cannot remove the node from the leafset until this expires.
   *
   * If this node is found faulty (and you took over the leafset), must go non-ready until lease expires
   */
  Map<NodeHandle, Long> lastTimeRenewedLease;

  /**
   * Should not be called because we are overriding the receiveMessage()
   * interface anyway.
   */
  @Override
  public void messageForApplication(Message msg) {
    throw new RuntimeException("Should not be called.");
  }

  /**
   * We always want to receive messages.
   */
  @Override
  public boolean deliverWhenNotReady() {
    return true;
  }

  boolean destroyed = false;
  @Override
  public void destroy() {
    logger.info("PLSP: destroy() called");
    destroyed = true;
    if (pingNeighborMessage != null)
      pingNeighborMessage.cancel();
    pingNeighborMessage = null;
    lastLeft = null;
    lastRight = null;
    lastTimeReceivedBLS.clear();
    lastTimeRenewedLease.clear();
    lastTimeSentBLS.clear();
  }

  @Override
  public void leafSetChange(NodeHandle nodeHandle, boolean wasAdded) {
    super.leafSetChange(nodeHandle, wasAdded);
    if (wasAdded) {
      nodeHandle.addNodeHandleListener(this, NODE_HANDLE_LISTENER_PRIORITY);
      if (nodeHandle.getLiveness() >= NodeHandle.LIVENESS_DEAD) {
        removeFromLeafsetIfPossible(nodeHandle);
      }
    } else {
      logger.debug("Removed {} from the LeafSet.", nodeHandle);
      nodeHandle.removeNodeHandleListener(this);
    }
  }

  /**
   * Only remove the item if you did not give a lease.
   */
  @Override
  @SuppressWarnings("ReferenceEquality") // DECLARED_DEAD is a sentinel instance
  public void nodeHandleUpdate(final rice.p2p.commonapi.NodeHandle handle, final Object arg) {
    if (destroyed) return;
    if (handle instanceof NodeHandle nodeHandle) {
      if (arg == NodeHandle.DECLARED_DEAD) {
        removeFromLeafsetIfPossible(nodeHandle);
      }
    }
  }

  /**
   * this is if we are the "seed" node
   */
  @Override
  public void pastryNodeReady(PastryNode node, boolean ready) {
    if (destroyed) return;
    if (ready) {
      localNode.removePastryNodeListener(this);
      start();
    }
  }

  public void removeFromLeafsetIfPossible(final NodeHandle nodeHandle) {
    assert thePastryNode.getEnvironment().getSelectorManager().isSelectorThread() : "the leaf set is modified on the selector thread only";
    if (nodeHandle.getLiveness() < NodeHandle.LIVENESS_DEAD) return;
    Long l_time = (Long)lastTimeRenewedLease.get(nodeHandle);
    if (l_time == null) {
      // there is no lease on record
      leafSet.remove(nodeHandle);
    } else {
      // verify doesn't have a current lease
      long leaseExpiration = l_time.longValue()+LEASE_PERIOD;
      long nowMillis = timeSource.currentTimeMillis();
      if (leaseExpiration > nowMillis) {
        logger.atInfo().log(() -> "Removing "+nodeHandle+" from leafset later."+(leaseExpiration-nowMillis));
        // remove it later when lease expires
        thePastryNode.getEnvironment().getSelectorManager().getTimer().schedule(new TimerTask() {
          @Override
          public void run() {
            logger.debug("removeFromLeafsetIfPossible({})", nodeHandle);
            // do this recursively in case we issue a new lease
            removeFromLeafsetIfPossible(nodeHandle);
          }
        }, leaseExpiration-nowMillis);
      } else {
        // lease has expired
        leafSet.remove(nodeHandle);
      }
    }
  }

}
