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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.mpisws.p2p.transport.liveness.LivenessTypes;
import rice.Continuation;
import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.util.TimerWeakHashMap;
import rice.pastry.ExponentialBackoffScheduledMessage;
import rice.pastry.Id;
import rice.pastry.JoinFailedException;
import rice.pastry.NodeHandle;
import rice.pastry.NodeSetEventSource;
import rice.pastry.NodeSetListener;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeListener;
import rice.pastry.ReadyStrategy;
import rice.pastry.ScheduledMessage;
import rice.pastry.client.PastryAppl;
import rice.pastry.join.InitiateJoin;
import rice.pastry.join.JoinAddress;
import rice.pastry.join.JoinMessageType;
import rice.pastry.join.JoinProtocol;
import rice.pastry.join.JoinRequest;
import rice.pastry.leafset.BroadcastLeafSet;
import rice.pastry.leafset.LeafSet;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.PastryMessageDeserializer;
import rice.pastry.routing.BroadcastRouteRow;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouteMessageNotification;
import rice.pastry.routing.RouteSet;
import rice.pastry.routing.RoutingTable;
import rice.pastry.transport.PastryMessageReceipt;
import rice.pastry.transport.PastryMessageSendListener;
import rice.selector.LoopObserver;
import rice.selector.TimerTask;

/**
 * The join protocol. It routes a join request to the joiner's id, then does not go ready until it
 * contacted its entire leafset, which gossips new members.
 *
 * The join:
 * initiateJoin() causes InitiateJoinMsg to be sent periodically on the joiner
 * causes handleInitiateJoin() constructs a RouteMessage(JoinRequest) and sends it to the bootstrap
 * causes handleIntermediateHop() on each node along the route
 * causes respondToJoiner() on last hop
 * causes completeJoin() on joiner
 * causes setReady(), which probes the leafset with ConsistentJoinMsgs before the node goes ready
 *
 * Provides consistency as long as checkLiveness() never incorrectly reports a node faulty.
 *
 * Based on MSR-TR-2003-94.  The difference is that our assumption that checkLiveness() is much stronger
 * because we are using DSR rather than checking ourself.
 *
 * Another difference is that we are unwilling to pull nodes from our leafset without checkingLiveness() ourself.
 *
 * @author Peter Druschel
 * @author Andrew Ladd
 * @author Rongmei Zhang
 * @author Y. Charlie Hu
 * @author Jeff Hoye
 */
public class ConsistentJoinProtocol extends PastryAppl implements JoinProtocol, NodeHandleListener, PastryNodeListener, NodeSetListener, LoopObserver {
  /** The join message is resent after 2 s, doubling up to 60 s. */
  private static final long JOIN_RETRY_INITIAL_MILLIS = 2000;
  private static final double JOIN_RETRY_BACKOFF_FACTOR = 2;
  private static final long JOIN_RETRY_MAX_MILLIS = 60000;

  protected NodeHandle localHandle;

  private final RoutingTable routeTable;

  protected final LeafSet leafSet;

  /** Listener priority on the node handles we wait to hear from. */
  private static final int NODE_HANDLE_LISTENER_PRIORITY = 40;
  /** How long a node that answered our join is remembered. */
  private static final int GOT_RESPONSE_HARD_LINK_MILLIS = 300000;

  /**
   * This variable is set to prevent the process from going to sleep or not
   * being scheduled for too long.
   */
  private final int MAX_TIME_TO_BE_SCHEDULED;

  /**
   * The number of nodes needed to hear from to go live, can be up to the LeafSet.maxSize()
   */
  private final int MAX_NUM_TO_HEAR_FROM = 8;

  /**
   * Suppresses sendTheMessage() if we are not ready to do this part of the join process,
   * or we are already done.
   */
  private boolean tryingToGoReady = false;

  /**
   * Set of NodeHandles that know about us. -> Object
   */
  Map<NodeHandle, Object> gotResponse;

  /**
   * Nodes that we think are dead.
   *
   * NodeHandle -> FailedTime
   */
  final HashMap<NodeHandle, FailedTime> failed;
  TimerTask cleanupTask; // cleans up failed
  static class FailedTime implements Comparable<FailedTime> {
    long time;
    NodeHandle handle;
    public FailedTime(NodeHandle handle, long time) {
      this.time = time;
      this.handle = handle;
    }
    @Override
    public int compareTo(FailedTime arg0) {
      FailedTime failedTime = (FailedTime)arg0;
      // note this is backwards, because we want them sorted in reverse order
      return Long.compare(failedTime.time, this.time);
    }

    @Override
    public String toString() {
      return "FT:"+handle+" "+time;
    }
  }


  /**
   * how long a node should remain in failed
   *
   * acquired from pastry_protocol_consistentJoin_failedRetentionTime
   */
  int failedNodeExpirationTime;
  /**
   * maximum number of failed entries to gossip
   * sends only the most recent failures
   *
   * acquired from pastry_protocol_consistentJoin_maxFailedToSend
   */
  int maxFailedEntries;

  /**
   * NodeHandles I'm observing
   */
  HashSet<NodeHandle> observing;

  /**
   * Will retry sending ConsistentJoinMessage to all neighbors who have not responded
   * on this interval.  Only necessary if somehow the message was dropped.
   */
  public final int RETRY_INTERVAL;

  /**
   * Reads the messages of the join protocol. A subclass with message types of its own extends it.
   */
  public static class CJPDeserializer extends PastryMessageDeserializer {
    public CJPDeserializer(PastryNode pn) {
      super(pn);
    }

    @Override
    public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
      return switch (JoinMessageType.find(type)) {
        case JOIN_REQUEST -> new JoinRequest(buf,pn, (NodeHandle)sender, pn);
        case CONSISTENT_JOIN -> new ConsistentJoinMessage(buf,pn,(NodeHandle)sender);
        // read by the subclass's deserializer
        case RENDEZVOUS_JOIN_REQUEST -> null;
        case null -> throw new IOException("Unknown message type " + type + " for " + getClass().getName());
      };
    }
  }

  public ConsistentJoinProtocol(PastryNode localNode, NodeHandle localHandle,
      RoutingTable routingTable, LeafSet leafSet, ReadyStrategy nextReadyStrategy) {
    this(localNode, localHandle, routingTable, leafSet, nextReadyStrategy, null);
  }

  /**
   * Constructor takes in the usual suspects.
   *
   * @param nextReadyStrategy if non-null, will call nextRenderStrategy.start() when complete
   */
  public ConsistentJoinProtocol(PastryNode localNode, NodeHandle localHandle,
      RoutingTable routingTable, LeafSet leafSet, ReadyStrategy nextReadyStrategy,
      MessageDeserializer deserializer) {
    this(localNode, localHandle, routingTable, leafSet, nextReadyStrategy, deserializer, ConsistentJoinConfig.fromParameters(localNode.getEnvironment().getParameters()));
  }

  /**
   * As above, with the parameters given instead of read from the environment.
   */
  public ConsistentJoinProtocol(PastryNode localNode, NodeHandle lh,
      RoutingTable routingTable, LeafSet ls, ReadyStrategy nextReadyStrategy,
      MessageDeserializer deserializer, ConsistentJoinConfig config) {
    super(localNode, null, JoinAddress.getCode(), deserializer != null ? deserializer : new CJPDeserializer(localNode));
    localHandle = lh;
    routeTable = routingTable;
    leafSet = ls;
    gotResponse = new TimerWeakHashMap<NodeHandle, Object>(localNode.getEnvironment().getSelectorManager().getTimer(), GOT_RESPONSE_HARD_LINK_MILLIS);
    failed = new HashMap<NodeHandle, FailedTime>();
    observing = new HashSet<NodeHandle>();
    this.nextReadyStrategy = nextReadyStrategy;
    ls.addNodeSetListener(this);
    localNode.addPastryNodeListener(this);
    MAX_TIME_TO_BE_SCHEDULED = config.maxTimeToBeScheduled();
    RETRY_INTERVAL = config.retryInterval();
    failedNodeExpirationTime = config.failedRetentionTime();
    maxFailedEntries = config.maxFailedToSend();
    int cleanupInterval = config.cleanupInterval();

    localNode.getEnvironment().getSelectorManager().addLoopObserver(this);

    cleanupTask = new TimerTask() {
      @Override
      public void run() {
        logger.debug("CJP: Cleanup task.");
        synchronized(failed) {
          long nowMillis = thePastryNode.getEnvironment().getTimeSource().currentTimeMillis();
          long expiration = nowMillis-failedNodeExpirationTime;
          Iterator<FailedTime> i = failed.values().iterator();
          while(i.hasNext()) {
            FailedTime failedTime = (FailedTime)i.next();
            if (failedTime.time < expiration) {
              logger.debug("CJP: Removing {} from failed set.", failedTime.handle);
              i.remove();
              failedTime.handle.removeNodeHandleListener(ConsistentJoinProtocol.this);
              observing.remove(failedTime.handle);
            } else {
              logger.atTrace().log(() -> "CJP: Not Removing "+failedTime.handle+" from failed set until "+(failedTime.time+failedNodeExpirationTime)+" which is another "+(failedTime.time+failedNodeExpirationTime-nowMillis)+" millis.");
            }
          }
        }
      }

      @Override
      public String toString() {
        return "CJP$cleanupTask{"+thePastryNode+"}"+cancelled;
      }
    };
    localNode.getEnvironment().getSelectorManager().schedule(cleanupTask, cleanupInterval, cleanupInterval);
  }

  /**
   * Get address.
   *
   * @return gets the address.
   */
  @Override
  public int getAddress() {
    return JoinAddress.getCode();
  }

  /** Retransmits the join request while this node is joining; {@code null} otherwise. */
  private ScheduledMessage joinEvent;

  @Override
  public void initiateJoin(Collection<NodeHandle> bootstrap) {
    logger.info("initiateJoin({})", bootstrap);
    if (bootstrap == null || bootstrap.isEmpty()) {
      // no bootstrap node, so ready immediately
      thePastryNode.setReady();
    } else {
      // schedule (re-)transmission of the join message at an exponential backoff
      joinEvent = new ExponentialBackoffScheduledMessage(
          thePastryNode, new InitiateJoin(bootstrap),
          thePastryNode.getEnvironment().getSelectorManager().getTimer(),
          0, JOIN_RETRY_INITIAL_MILLIS, JOIN_RETRY_BACKOFF_FACTOR, JOIN_RETRY_MAX_MILLIS);
    }
  }

  private void handleInitiateJoin(InitiateJoin initiateJoin) {
    final NodeHandle nodeHandle = initiateJoin.getHandle();

    if (nodeHandle == null) {
      logger.error("ERROR: Cannot join ring.  All bootstraps are faulty.{}", initiateJoin);
      thePastryNode.joinFailed(new JoinFailedException("Cannot join ring.  All bootstraps are faulty."+initiateJoin));
    } else {
      logger.info("InitiateJoin attempting to join:{} liveness:{}", nodeHandle, nodeHandle.getLiveness());
        getJoinRequest(nodeHandle, new Continuation<JoinRequest, Exception>() {

          @Override
          public void receiveResult(JoinRequest joinRequest) {
            RouteMessage routeMessage = new RouteMessage(localHandle.getNodeId(), joinRequest, null, null,
                thePastryNode.getConfig().routeMsgVersion());

            routeMessage.getOptions().setRerouteIfSuspected(false);
            routeMessage.setPrevNode(localHandle);
            thePastryNode.send(nodeHandle, routeMessage, null, getOptions(joinRequest, options));
          }

          @Override
          public void receiveException(Exception exception) {

          }

        });

    }
  }

  protected void getJoinRequest(NodeHandle bootstrap, Continuation<JoinRequest, Exception> deliverJRToMe) {
    JoinRequest joinRequest = new JoinRequest(localHandle, thePastryNode
        .getRoutingTable().baseBitLength(), thePastryNode.getEnvironment().getTimeSource().currentTimeMillis());
    deliverJRToMe.receiveResult(joinRequest);
  }

  private void handleIntermediateHop(RouteMessage routeMessage) {
    try {
      JoinRequest joinRequest = (JoinRequest) routeMessage.unwrap(deserializer);

      Id localId = localHandle.getNodeId();
      NodeHandle joinHandle = joinRequest.getHandle();
      Id nodeId = joinHandle.getNodeId();

      if (!joinHandle.equals(localHandle)) {
        int base = thePastryNode.getRoutingTable().baseBitLength();

        int msdd = localId.indexOfMostSignificantDifferingDigit(nodeId, base);
        int last = joinRequest.lastRow();

        for (int i = last - 1; msdd > 0 && i >= msdd; i--) {
          RouteSet[] row = routeTable.getRow(i);

          joinRequest.pushRow(row);
        }

        routeMessage.setRouteMessageNotification(new RouteMessageNotification(){
          @Override
          public void sendSuccess(RouteMessage message, NodeHandle nextHop) {
            logger.info("sendSuccess({}):{}", message, nextHop);
          }

          @Override
          public void sendFailed(RouteMessage message, Exception e) {
            logger.info("sendFailed({}) ", message, e);
          }
        });

        routeMessage.setTransportLayerOptions(getOptions(joinRequest, routeMessage.getTransportLayerOptions()));

        logger.info("Routing {}", routeMessage);
        thePastryNode.getRouter().route(routeMessage);
      }
    } catch (IOException ioe) {
      logger.error("ConsistentJoinProtocol.handleIntermediateHop()", ioe);
    }
  }

  private void handleJoinRequest(final JoinRequest joinRequest) {
    if (joinRequest.accepted() == false) {
      respondToJoiner(joinRequest);
    } else if (joinEvent == null || thePastryNode.isReady()) {
      // We are not joining (any more): a late answer to a retransmitted request, or a forged one. Completing the
      // join again would let any peer push its leaf set and routing table rows into a ready node.
      logger.info("Ignoring accepted JoinRequest {}, this node is not joining", joinRequest);
    } else { // this is the node that initiated the join request in the first place
      completeJoin(joinRequest);
    }
  }

  private void respondToJoiner(final JoinRequest joinRequest) {
    NodeHandle joiner = joinRequest.getHandle();

    // this is the terminal node on the request path
    if (thePastryNode.isReady()) {
      logger.info("acceptJoin {}", joinRequest);
      joinRequest.acceptJoin(localHandle, leafSet);
      thePastryNode.send(joiner,joinRequest,new PastryMessageSendListener(){
        @Override
        public void sent(PastryMessageReceipt msg) {
          logger.info("acceptJoin.sent({}):{}", msg, joinRequest);
        }
        @Override
        public void sendFailed(PastryMessageReceipt msg, Exception reason2) {
          Throwable reason = reason2;
          if (logger.isInfoEnabled()) {
            logger.info("acceptJoin.sendFailed("+msg+"):"+joinRequest, reason);
            while (reason.getCause() != null) {
              reason = reason.getCause();
              logger.info("because", reason);
            }
          }
        }
      }, getOptions(joinRequest, options));
    } else {
      logger.info("NOTE: Dropping incoming JoinRequest {} because local node is not ready!", joinRequest);
    }
  }

  /**
   * The transport options for sending {@code jr}; a subclass can add to {@code existing}.
   */
  protected Map<String, Object> getOptions(JoinRequest joinRequest, Map<String, Object> existing) {
    return existing;
  }

  /** called on the joiner */
  private void completeJoin(JoinRequest joinRequest) {
    NodeHandle joinHandle = joinRequest.getJoinHandle(); // the node we joined to.

    if (joinHandle.getId().equals(localHandle.getId()) && !joinHandle.equals(localHandle)) {
      logger.warn("NodeId collision, unable to join: {}:{}", localHandle, joinHandle);
    } else if (joinHandle.getLiveness() < NodeHandle.LIVENESS_DEAD) { // the join handle is alive
      routeTable.put(joinHandle);
      // add the num. closest node to the routing table

      // now update the local leaf set
      BroadcastLeafSet broadcastLeafSet = new BroadcastLeafSet(joinHandle, joinRequest.getLeafSet(),
          BroadcastLeafSet.JoinInitial, 0);
      thePastryNode.receiveMessage(broadcastLeafSet);

      // update local RT, then broadcast rows to our peers
      broadcastRows(joinRequest);

      // we have now successfully joined the ring, set the local node ready
      setReady();
    }
  }

  /**
   * Called once the join completed: probes the leaf set, and goes ready when all of it answered.
   */
  private void setReady() {
    if (joinEvent != null) {
      logger.info("cancelling {}", joinEvent);
      joinEvent.cancel();
      joinEvent = null;
    }

    if (tryingToGoReady) return;
    tryingToGoReady = true;
    logger.info("ConsistentJonProtocol.setReady()");
    gotResponse.clear();

    // send a probe to everyone in the leafset
    for (NodeHandle nodeHandle : whoDoWeNeedAResponseFrom()) {
      sendTheMessage(nodeHandle, false);
    }

    if (retryTask == null)
      retryTask = thePastryNode.scheduleMessage(new RequestFromEveryoneMsg(getAddress()), RETRY_INTERVAL, RETRY_INTERVAL);
  }

  /**
   * Observes all NodeHandles added to LeafSet
   * @param nodeHandle the nodeHandle to add
   */
  public void addToLeafSet(NodeHandle nodeHandle) {
    leafSet.put(nodeHandle);
    if (!observing.contains(nodeHandle)) {
      logger.debug("CJP observing {}", nodeHandle);
      nodeHandle.addNodeHandleListener(this, NODE_HANDLE_LISTENER_PRIORITY);
      observing.add(nodeHandle);
    }
  }

  /**
   * Used to trigger timer events.
   * @author Jeff Hoye
   */
  static class RequestFromEveryoneMsg extends Message {
    public RequestFromEveryoneMsg(int address) {
      super(address);
    }
  }

  TimerTask retryTask;

  public void requestFromEveryoneWeHaventHeardFrom() {
    if (thePastryNode.isReady()) {
      retryTask.cancel();
      retryTask = null;
      return;
    }

    Collection<NodeHandle> handles = whoDoWeNeedAResponseFrom();
    logger.info("CJP: timeout1, still waiting to hear from {} nodes.", handles.size());

    for (NodeHandle nodeHandle : handles) {
      logger.debug("CJP: timeout2, still waiting to hear from {}", nodeHandle);
      try {
        sendTheMessage(nodeHandle, false);
      } catch (NullPointerException exception) {
        logger.warn("npe, nh = {} c:{}", nodeHandle, handles);
        throw exception;
      }
    }
  }

  /**
   * Call this if there is an event such that you may have
   * not received messages for long enough for other nodes
   * to call you faulty.
   *
   * This method will call PastryNode.setReady(false) which will
   * stop delivering messages, and then via notifyReadyObservers()
   * will call this.update(PN, FALSE) which will call setReady()
   * which will begin the join process again.
   *
   */
  public void otherNodesMaySuspectFaulty(int timeNotScheduled) {
    logger.warn("WARNING: CJP.otherNodesMaySuspectFaulty({})", timeNotScheduled);
    // need to recover ownership of the readiness process
    nextReadyStrategy.stop();

    thePastryNode.setReadyStrategy(thePastryNode.getDefaultReadyStrategy());
    tryingToGoReady = false;

    thePastryNode.notifyReadyObservers();

    // restart self
    setReady();
  }

  /**
   * Returns all members of the leafset that are not in gotResponse
   */
  public Collection<NodeHandle> whoDoWeNeedAResponseFrom() {
    HashSet<NodeHandle> result = new HashSet<NodeHandle>();
    int leftIndex = leafSet.counterClockwiseSize();
    if (leftIndex > MAX_NUM_TO_HEAR_FROM/2) leftIndex = MAX_NUM_TO_HEAR_FROM/2;
    int rightIndex = leafSet.clockwiseSize();
    if (rightIndex > MAX_NUM_TO_HEAR_FROM/2) rightIndex = MAX_NUM_TO_HEAR_FROM/2;
    for (int i=-leftIndex; i<=rightIndex; i++) {
      if (i != 0) {
        NodeHandle nodeHandle = leafSet.get(i);
        if (gotResponse.get(nodeHandle) == null) {
          result.add(nodeHandle);
        }
      }
    }
    return result;
  }

  /**
   * Receives a message from the outside world.
   *
   * @param msg the message that was received.
   */
  @Override
  public void receiveMessage(Message msg) {
    assert thePastryNode.getEnvironment().getSelectorManager().isSelectorThread() : "join messages are received on the selector thread";
    logger.trace("CJP: receiveMessage({})", msg);
    if (msg instanceof ConsistentJoinMessage joinMessage) {
      handleConsistentJoinMsg(joinMessage);
    } else if (msg instanceof RequestFromEveryoneMsg) {
      requestFromEveryoneWeHaventHeardFrom();
    } else if (msg instanceof JoinRequest joinRequest) {
      handleJoinRequest(joinRequest);
    } else if (msg instanceof RouteMessage routeMessage) { // a join request message at an intermediate node
      handleIntermediateHop(routeMessage);
    } else if (msg instanceof InitiateJoin initiateJoin) { // request from the local node to join
      handleInitiateJoin(initiateJoin);
    }
  }

  /**
   * Handle the CJM as in the MSR-TR
   */
  private void handleConsistentJoinMsg(ConsistentJoinMessage joinMessage) {
    // identify node j, the sender of the message
    NodeHandle j = joinMessage.ls.get(0);

    if (j.getLiveness() > LivenessTypes.LIVENESS_SUSPECTED) {
      logger.info("got message from dead node:{}", joinMessage);
      j.checkLiveness(); // we got a message from a dead node...
    }

    // failed_i := failed_i - {j}
    failed.remove(j);

    // L_i.add(j);
    addToLeafSet(j);

    // for each n in L_i and failed do {probe}
    // rather than removing everyone in the remote failedset,
    // checkLiveness on them all
    for (NodeHandle nodeHandle : joinMessage.failed) {
      if (leafSet.member(nodeHandle)) {
        if (nodeHandle.getLiveness() == NodeHandle.LIVENESS_DEAD) {
          // if we already found them dead, don't bother
          // hopefully this is redundant with the leafset protocol

          // Note, can't remove from leafset, this is to only be done by the LeafSetProtocol
        } else {
          logger.debug("CJP: checking liveness2 on {}", nodeHandle);
          nodeHandle.checkLiveness();
        }
      }
    }

    // we don't do the L_i.remove() because we don't trust this info

    // L' stuff
    // this is so we don't probe too many dudez.  For example:
    // my leafset is not complete, so I will add anybody, but then keep
    // getting closer and closer, each time knocking out the next guy
    LeafSet lprime = leafSet.copy();
    for (int i=-joinMessage.ls.counterClockwiseSize(); i<=joinMessage.ls.clockwiseSize(); i++) {
      NodeHandle currentHandle = joinMessage.ls.get(i);
      if (!failed.containsKey(currentHandle) && currentHandle.getLiveness() < NodeHandle.LIVENESS_DEAD) {
        lprime.put(currentHandle);
      }
    }

    HashSet<NodeHandle> addThese = new HashSet<NodeHandle>();
    for (int i=-lprime.counterClockwiseSize(); i<=lprime.clockwiseSize(); i++) {
      if (i != 0) {
        NodeHandle nh = lprime.get(i);
        if (!leafSet.member(nh)) {
          addThese.add(nh);
        }
      }
    }

    for (NodeHandle nh : addThese) {
      // he's not a member, but he could be
      if (!failed.containsKey(nh) && nh.getLiveness() < NodeHandle.LIVENESS_DEAD) {
        addToLeafSet(nh);
        // probe
        sendTheMessage(nh, false);
      }
    }

    if (joinMessage.request) {
      // send reply
      sendTheMessage(j, true);
    }
    // done_probing:

    // mark that he knows about us
    gotResponse.put(j, new Object());
    if (tryingToGoReady)
      doneProbing();
  }

  ReadyStrategy nextReadyStrategy;

  /**
   * Similar to the MSR-TR
   *
   */
  void doneProbing() {
    int leftIndex = leafSet.counterClockwiseSize();
    int rightIndex = leafSet.clockwiseSize();
    if (leftIndex > MAX_NUM_TO_HEAR_FROM/2) leftIndex = MAX_NUM_TO_HEAR_FROM/2;
    if (rightIndex > MAX_NUM_TO_HEAR_FROM/2) rightIndex = MAX_NUM_TO_HEAR_FROM/2;
    if (leafSet.isComplete() || ((leftIndex == MAX_NUM_TO_HEAR_FROM/2) && (rightIndex == MAX_NUM_TO_HEAR_FROM/2))) {
      // here is where we see if we can go active
      ArrayList<NodeHandle> toHearFrom = new ArrayList<NodeHandle>();
      HashSet<NodeHandle> seen = new HashSet<NodeHandle>();

      for (int i=-leftIndex; i<=rightIndex; i++) {
        if (i != 0) {
          NodeHandle nodeHandle = leafSet.get(i);
          if (!seen.contains(nodeHandle) && (gotResponse.get(nodeHandle) == null)) {
            toHearFrom.add(nodeHandle);
          }
          seen.add(nodeHandle);
        }
      }

      if (toHearFrom.size() == 0) {
        if (!thePastryNode.isReady()) {
          // active_i = true;
          if (nextReadyStrategy == null) {
            thePastryNode.setReady();
          } else {
            nextReadyStrategy.start();
          }
          if (retryTask != null) {
            retryTask.cancel();
            retryTask = null;
          }
          tryingToGoReady = false;
        }
        // failed_i = {} is done by the cleanup task, not here
      } else {
        if (logger.isDebugEnabled()) {
          String toHearFromStr = "";
          for (NodeHandle currentHandle : toHearFrom) {
            toHearFromStr+=currentHandle+":"+currentHandle.getLiveness()+",";
          }
          logger.debug("CJP: still need to hear from:"+toHearFromStr);
        }
      }
    } else {
      logger.debug("CJP: LS is not complete: {}", leafSet);
      // sendTheMessage to leftmost and rightmost

      NodeHandle left = null;
      NodeHandle right = null;

      synchronized(leafSet) {
        int index = -leafSet.counterClockwiseSize();
        if ((index != 0) && index != -leafSet.maxSize()/2) {
          left = leafSet.get(index);
        }
        index = leafSet.clockwiseSize();
        if ((index != 0) && index != leafSet.maxSize()/2) {
          right = leafSet.get(index);
        }
      }
      if (left != null) sendTheMessage(left, true);
      if (right != null) sendTheMessage(right, true);
    }
  }

  /**
   * Sends a consistent join protocol message.
   * @param reply if the reason we are sending this message is just as a response
   */
  public void sendTheMessage(NodeHandle nodeHandle, boolean reply) {
    // if we're not trying to go ready, no need to send out requests
    if (!reply) {
      if (!tryingToGoReady) return;
    }
    logger.debug("CJP:  sendTheMessage({},{})", nodeHandle, reply);

    // TODO: repeat this message while the node is alive, so a rare message drop cannot stall going ready
    HashSet<NodeHandle> toSend;
    if (failed.size() < maxFailedEntries) {
      toSend = new HashSet<NodeHandle>(failed.keySet());
    } else {
      ArrayList<FailedTime> failedTimes = new ArrayList<FailedTime>(failed.values());
      Collections.sort(failedTimes);
      toSend = new HashSet<NodeHandle>();
      for (int i = 0; i < maxFailedEntries; i++) {
        FailedTime failedTime = (FailedTime)failedTimes.get(i);
        toSend.add(failedTime.handle);
      }
    }
    thePastryNode.send(nodeHandle,new ConsistentJoinMessage(leafSet,toSend,!reply),null,options);
  }

  @Override
  public void nodeSetUpdate(NodeSetEventSource set, NodeHandle handle, boolean added) {
    if (thePastryNode.isReady()) return;
    if (added) {
      if (gotResponse.get(handle) == null) {
        sendTheMessage(handle,false);
      }
    } else {
      doneProbing();
    }

  }

  /**
   * PastryNode updates are only logged.
   */
  @Override
  public void pastryNodeReady(PastryNode node, boolean ready) {
    update((Object)node, Boolean.valueOf(ready));
  }

  /**
   * PastryNode updates are only logged.
   */
  @Override
  public void pastryNodeJoinFailed(PastryNode node, JoinFailedException reason) {
    update((Object)node, reason);
  }

  /**
   * NodeHandle updates.
   */
  @Override
  public void nodeHandleUpdate(rice.p2p.commonapi.NodeHandle handle, Object arg) {
    update((Object)handle, arg);
  }

  private void update(Object arg0, Object arg) {
    logger.trace("CJP: update({},{}){}", arg0, arg, arg.getClass().getName());

    if (arg0 instanceof NodeHandle nodeHandle) {

        // assume it's a NodeHandle, cause we
        // want to throw the exception if it is something we don't recognize
      if (NodeHandle.DECLARED_DEAD.equals(arg)) {
        logger.debug("CJP:{} declared dead", arg0);
        if (!failed.containsKey(nodeHandle)) {
          failed.put(nodeHandle, new FailedTime(nodeHandle, thePastryNode.getEnvironment().getTimeSource().currentTimeMillis()));
        }

        // Note, can't remove from the leafset, this is the LeafSetProtocol's job
        doneProbing();
      }

      if (NodeHandle.DECLARED_LIVE.equals(arg)) {
        failed.remove(nodeHandle);
        if (!thePastryNode.isReady()) {
          if (leafSet.test(nodeHandle)) {
            leafSet.put(nodeHandle);
            sendTheMessage(nodeHandle, false);
          }
        }
      }
    } else {
      if (logger.isTraceEnabled()) logger.trace("update()", new Exception("Stack Trace"));
    }
  }

  /**
   * Part of the LoopObserver interface.  Used to detect if
   * we may have been found faulty by other nodes.
   *
   * @return the minimum loop time we are interested in being notified about.
   */
  @Override
  public int delayInterest() {
    return MAX_TIME_TO_BE_SCHEDULED;
  }


  /**
   * If it took longer than the time to detect faultiness, then other nodes
   * may believe we are faulty.  So we best rejoin.
   *
   * @param loopTime the time it took to do a single selection loop.
   */
  @Override
  public void loopTime(int loopTime) {
    // may want to do a full rejoin if this one trips... but make it longer
    // partition handler may handle this anyway
    otherNodesMaySuspectFaulty(loopTime);
  }

  @Override
  public void destroy() {
    logger.info("CJP: destroy() called");
    thePastryNode.getEnvironment().getSelectorManager().removeLoopObserver(this);
    cleanupTask.cancel();
    Iterator<NodeHandle> it2 = observing.iterator();
    while(it2.hasNext()) {
      NodeHandle nodeHandle = (NodeHandle)it2.next();
      nodeHandle.removeNodeHandleListener(this);
      it2.remove();
    }
    observing.clear();
    observing = null;
  }
  /**
   * Broadcasts the route table rows.
   *
   * @param joinRequest the join row.
   */

  public void broadcastRows(JoinRequest joinRequest) {
    int n = joinRequest.numRows();

    // send the rows to the RouteSetProtocol on the local node
    for (int i = joinRequest.lastRow(); i < n; i++) {
      RouteSet row[] = joinRequest.getRow(i);

      if (row != null) {
        BroadcastRouteRow broadcastRouteRow = new BroadcastRouteRow(localHandle, row);

        thePastryNode.receiveMessage(broadcastRouteRow);
      }
    }

    // now broadcast the rows to our peers in each row

    for (int i = joinRequest.lastRow(); i < n; i++) {
      RouteSet row[] = joinRequest.getRow(i);
      if (row == null) {
        // a hop on the path had no such row (or a peer left it out), as in the loop above
        continue;
      }

      BroadcastRouteRow brr = new BroadcastRouteRow(localHandle, row);

      for (RouteSet routeSet : row) {
        RouteSet rs = routeSet;
        if (rs == null)
          continue;

        // send to closest nodes only

        NodeHandle nodeHandle = rs.closestNode();
        if (nodeHandle != null)
          thePastryNode.send(nodeHandle,brr, null, options);
      }
    }
  }

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
}
