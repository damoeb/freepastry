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
/*
 * Created on Apr 13, 2005
 */
package rice.pastry.standard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Observable;

import org.mpisws.p2p.transport.liveness.LivenessListener;
import rice.environment.params.Parameters;
import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.util.TimerWeakHashMap;
import rice.pastry.JoinFailedException;
import rice.pastry.NodeHandle;
import rice.pastry.NodeSetEventSource;
import rice.pastry.NodeSetListener;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeListener;
import rice.pastry.ReadyStrategy;
import rice.pastry.leafset.LeafSet;
import rice.pastry.messaging.Message;
import rice.pastry.routing.RoutingTable;
import rice.selector.LoopObserver;
import rice.selector.TimerTask;

/**
 * Does not setReady until contacting entire leafset which gossips new members.
 *
 * Provides consistency as long as checkLiveness() never incorrectly reports a node faulty.
 *
 * Based on MSR-TR-2003-94.  The difference is that our assumption that checkLiveness() is much stronger
 * because we are using DSR rather than checking ourself.
 *
 * Another difference is that we are unwilling to pull nodes from our leafset without checkingLiveness() ourself.
 *
 * @author Jeff Hoye
 */
public class ConsistentJoinProtocol extends StandardJoinProtocol implements NodeHandleListener, PastryNodeListener, NodeSetListener, LoopObserver /*, ReadyStrategy */{

  /**
   * This variable is set to prevent the process from going to sleep or not
   * being scheduled for too long.
   */
  protected final int MAX_TIME_TO_BE_SCHEDULED;

  /**
   * The number of nodes needed to hear from to go live, can be up to the LeafSet.maxSize()
   */
  protected final int MAX_NUM_TO_HEAR_FROM = 8;

  /**
   * Suppresses sendTheMessage() if we are not ready to do this part of the join process,
   * or we are already done.
   */
  protected boolean tryingToGoReady = false;

  /**
   * Set of NodeHandles that know about us. -> Object
   */
  Map<NodeHandle, Object> gotResponse;

  /**
   * Nodes that we think are dead.
   *
   * NodeHandle -> FailedTime
   */
  HashMap<NodeHandle, FailedTime> failed;
  TimerTask cleanupTask; // cleans up failed
  static class FailedTime implements Comparable<FailedTime> {
    long time;
    NodeHandle handle;
    public FailedTime(NodeHandle handle, long time) {
      this.time = time;
      this.handle = handle;
    }
    public int compareTo(FailedTime arg0) {
      FailedTime ft = (FailedTime)arg0;
      // note this is backwards, because we want them sorted in reverse order
      return (int)(ft.time-this.time);
    }

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
   * Will retry sending ConsistentJoinMsg to all neighbors who have not responded
   * on this interval.  Only necessary if somehow the message was dropped.
   */
  public final int RETRY_INTERVAL;

  public static class CJPDeserializer extends SJPDeserializer {
    public CJPDeserializer(PastryNode pn) {
      super(pn);
    }

    @Override
    public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
      switch(type) {
        case ConsistentJoinMsg.TYPE:
          return new ConsistentJoinMsg(buf,pn,(NodeHandle)sender);
      }
      return super.deserialize(buf, type, priority, sender);
    }
  }

  public ConsistentJoinProtocol(PastryNode ln, NodeHandle lh,
      RoutingTable rt, LeafSet ls, ReadyStrategy nextReadyStrategy) {
    this(ln, lh, rt, ls, nextReadyStrategy, null);
  }

  /**
   * Constructor takes in the usual suspects.
   *
   * @param ln
   * @param lh
   * @param rt
   * @param ls
   * @param nextReadyStrategy if non-null, will call nextRenderStrategy.start() when complete
   * @param md
   */
  public ConsistentJoinProtocol(PastryNode ln, NodeHandle lh,
      RoutingTable rt, LeafSet ls, ReadyStrategy nextReadyStrategy,
      MessageDeserializer md) {
    super(ln, lh, rt, ls, md != null ? md : new CJPDeserializer(ln));
    gotResponse = new TimerWeakHashMap<NodeHandle, Object>(ln.getEnvironment().getSelectorManager().getTimer(), 300000);
    failed = new HashMap<NodeHandle, FailedTime>();
    observing = new HashSet<NodeHandle>();
    this.nextReadyStrategy = nextReadyStrategy;
    ls.addNodeSetListener(this);
    ln.addPastryNodeListener(this);
    Parameters p = ln.getEnvironment().getParameters();
    MAX_TIME_TO_BE_SCHEDULED = p.getInt("pastry_protocol_consistentJoin_max_time_to_be_scheduled"); // 15000
    RETRY_INTERVAL = p.getInt("pastry_protocol_consistentJoin_retry_interval");
    failedNodeExpirationTime = p.getInt("pastry_protocol_consistentJoin_failedRetentionTime"); // 90000
    maxFailedEntries = p.getInt("pastry_protocol_consistentJoin_maxFailedToSend"); // 20
    int cleanupInterval = p.getInt("pastry_protocol_consistentJoin_cleanup_interval"); // 300000

    ln.getEnvironment().getSelectorManager().addLoopObserver(this);

    cleanupTask = new TimerTask() {
      public void run() {
        logger.debug("CJP: Cleanup task.");
        synchronized(failed) {
          long now = thePastryNode.getEnvironment().getTimeSource().currentTimeMillis();
          long expiration = now-failedNodeExpirationTime;
          Iterator<FailedTime> i = failed.values().iterator();
          while(i.hasNext()) {
            FailedTime ft = (FailedTime)i.next();
            if (ft.time < expiration) {
              logger.debug("CJP: Removing {} from failed set.", ft.handle);
              i.remove();
              ft.handle.removeNodeHandleListener(ConsistentJoinProtocol.this);
              observing.remove(ft.handle);
            } else {
              logger.atTrace().log(() -> "CJP: Not Removing "+ft.handle+" from failed set until "+(ft.time+failedNodeExpirationTime)+" which is another "+(ft.time+failedNodeExpirationTime-now)+" millis.");
            }
          }
        }
      }

      public String toString() {
        return "CJP$cleanupTask{"+thePastryNode+"}"+cancelled;
      }
    };
    ln.getEnvironment().getSelectorManager().schedule(cleanupTask, cleanupInterval, cleanupInterval);
  }

  /**
   * This is where we start out, when the StandardJoinProtocol would call
   * setReady();
   */
  protected void setReady() {
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
    Iterator<NodeHandle> i = whoDoWeNeedAResponseFrom().iterator();
    while(i.hasNext()) {
      NodeHandle nh = i.next();
      sendTheMessage(nh, false);
    }

    if (retryTask == null)
      retryTask = thePastryNode.scheduleMsg(new RequestFromEveryoneMsg(getAddress()), RETRY_INTERVAL, RETRY_INTERVAL);
  }

  /**
   * Observes all NodeHandles added to LeafSet
   * @param nh the nodeHandle to add
   */
  public void addToLeafSet(NodeHandle nh) {
    leafSet.put(nh);
    if (!observing.contains(nh)) {
      logger.debug("CJP observing {}", nh);
      nh.addNodeHandleListener(this, 40);
      observing.add(nh);
    }
  }

  /**
   * Used to trigger timer events.
   * @author Jeff Hoye
   */
  class RequestFromEveryoneMsg extends Message {
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

    Collection<NodeHandle> c = whoDoWeNeedAResponseFrom();
    logger.info("CJP: timeout1, still waiting to hear from {} nodes.", c.size());

    Iterator<NodeHandle> i = c.iterator();
    while(i.hasNext()) {
      NodeHandle nh = (NodeHandle)i.next();
      logger.debug("CJP: timeout2, still waiting to hear from {}", nh);
      try {
        sendTheMessage(nh, false);
      } catch (NullPointerException npe) {
        logger.warn("npe, nh = {} c:{}", nh, c);
        throw npe;
      }
    }
  }

  /**
   * Call this if there is an event such that you may have
   * not received messages for long enough for other nodes
   * to call you faulty.
   *
   * This method will call PastryNode.setReady(false) which will
   * stop delivering messages, and then via the observer pattern
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
   * @return
   */
  public Collection<NodeHandle> whoDoWeNeedAResponseFrom() {
    HashSet<NodeHandle> ret = new HashSet<NodeHandle>();
    int leftIndex = leafSet.ccwSize();
    if (leftIndex > MAX_NUM_TO_HEAR_FROM/2) leftIndex = MAX_NUM_TO_HEAR_FROM/2;
    int rightIndex = leafSet.ccwSize();
    if (rightIndex > MAX_NUM_TO_HEAR_FROM/2) rightIndex = MAX_NUM_TO_HEAR_FROM/2;
    for (int i=-leftIndex; i<=rightIndex; i++) {
      if (i != 0) {
        NodeHandle nh = leafSet.get(i);
        if (gotResponse.get(nh) == null) {
          ret.add(nh);
        }
      }
    }
    return ret;
  }

  /**
   * Handle the CJM as in the MSR-TR
   */
  public void receiveMessage(Message msg) {
    logger.trace("CJP: receiveMessage({})", msg);
    if (msg instanceof ConsistentJoinMsg) {
      ConsistentJoinMsg cjm = (ConsistentJoinMsg)msg;
      // identify node j, the sender of the message
      NodeHandle j = cjm.ls.get(0);

      if (j.getLiveness() > LivenessListener.LIVENESS_SUSPECTED) {
        logger.info("got message from dead node:{}", msg);
        j.checkLiveness(); // we got a message from a dead node...
      }

      // failed_i := failed_i - {j}
      failed.remove(j);

      // L_i.add(j);
      addToLeafSet(j);

      // for each n in L_i and failed do {probe}
      // rather than removing everyone in the remote failedset,
      // checkLiveness on them all
      Iterator<NodeHandle> it = cjm.failed.iterator();
      while(it.hasNext()) {
        NodeHandle nh = (NodeHandle)it.next();
        if (leafSet.member(nh)) {
          if (nh.getLiveness() == NodeHandle.LIVENESS_DEAD) {
            // if we already found them dead, don't bother
            // hopefully this is redundant with the leafset protocol

            // Note, can't remove from leafset, this is to only be done by the LeafSetProtocol
          } else {
            logger.debug("CJP: checking liveness2 on {}", nh);
            nh.checkLiveness();
          }
        }
      }

      // we don't do the L_i.remove() because we don't trust this info

      // L' stuff
      // this is so we don't probe too many dudez.  For example:
      // my leafset is not complete, so I will add anybody, but then keep
      // getting closer and closer, each time knocking out the next guy
      LeafSet lprime = leafSet.copy();
      for (int i=-cjm.ls.ccwSize(); i<=cjm.ls.cwSize(); i++) {
        NodeHandle nh = cjm.ls.get(i);
        if (!failed.containsKey(nh) && nh.getLiveness() < NodeHandle.LIVENESS_DEAD) {
          lprime.put(nh);
        }
      }

      HashSet<NodeHandle> addThese = new HashSet<NodeHandle>();
      for (int i=-lprime.ccwSize(); i<=lprime.cwSize(); i++) {
        if (i != 0) {
          NodeHandle nh = lprime.get(i);
          if (!leafSet.member(nh)) {
            addThese.add(nh);
          }
        }
      }

      Iterator<NodeHandle> it2 = addThese.iterator();
      while(it2.hasNext()) {
        NodeHandle nh = (NodeHandle)it2.next();
        // he's not a member, but he could be
        if (!failed.containsKey(nh) && nh.getLiveness() < NodeHandle.LIVENESS_DEAD) {
          addToLeafSet(nh);
          // probe
          sendTheMessage(nh, false);
        }
      }

      if (cjm.request) {
        // send reply
        sendTheMessage(j, true);
      }
        // done_probing:

        // mark that he knows about us
        gotResponse.put(j, new Object());
        if (tryingToGoReady)
          doneProbing();
    } else if (msg instanceof RequestFromEveryoneMsg) {
      requestFromEveryoneWeHaventHeardFrom();
    } else {
      super.receiveMessage(msg);
    }
  }

  ReadyStrategy nextReadyStrategy;

  /**
   * Similar to the MSR-TR
   *
   */
  void doneProbing() {
    int leftIndex = leafSet.ccwSize();
    int rightIndex = leafSet.cwSize();
    if (leftIndex > MAX_NUM_TO_HEAR_FROM/2) leftIndex = MAX_NUM_TO_HEAR_FROM/2;
    if (rightIndex > MAX_NUM_TO_HEAR_FROM/2) rightIndex = MAX_NUM_TO_HEAR_FROM/2;
    if (leafSet.isComplete() || ((leftIndex == MAX_NUM_TO_HEAR_FROM/2) && (rightIndex == MAX_NUM_TO_HEAR_FROM/2))) {
      // here is where we see if we can go active
      ArrayList<NodeHandle> toHearFrom = new ArrayList<NodeHandle>();
      HashSet<NodeHandle> seen = new HashSet<NodeHandle>();

      for (int i=-leftIndex; i<=rightIndex; i++) {
        if (i != 0) {
          NodeHandle nh = leafSet.get(i);
          if (!seen.contains(nh) && (gotResponse.get(nh) == null)) {
            toHearFrom.add(nh);
          }
          seen.add(nh);
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
          Iterator<NodeHandle> i = toHearFrom.iterator();
          while(i.hasNext()) {
            NodeHandle nh = (NodeHandle)i.next();
            toHearFromStr+=nh+":"+nh.getLiveness()+",";
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
        int index = -leafSet.ccwSize();
        if ((index != 0) && index != -leafSet.maxSize()/2) {
          left = leafSet.get(index);
        }
        index = leafSet.cwSize();
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
   * @param nh
   * @param reply if the reason we are sending this message is just as a response
   */
  public void sendTheMessage(NodeHandle nh, boolean reply) {
    // if we're not trying to go ready, no need to send out requests
    if (!reply) {
      if (!tryingToGoReady) return;
    }
    logger.debug("CJP:  sendTheMessage({},{})", nh, reply);

    // TODO: repeat this message while the node is alive, so a rare message drop cannot stall going ready
    HashSet<NodeHandle> toSend;
    if (failed.size() < maxFailedEntries) {
      toSend = new HashSet<NodeHandle>(failed.keySet());
    } else {
      ArrayList<FailedTime> l = new ArrayList<FailedTime>(failed.values());
      Collections.sort(l);
      toSend = new HashSet<NodeHandle>();
      for (int i = 0; i < maxFailedEntries; i++) {
        FailedTime tf = (FailedTime)l.get(i);
        toSend.add(tf.handle);
      }
    }
    thePastryNode.send(nh,new ConsistentJoinMsg(leafSet,toSend,!reply),null,options);
  }

  public void nodeSetUpdate(NodeSetEventSource set, NodeHandle handle, boolean added) {
    if (thePastryNode.isReady()) return;
    if (added) {
      if (gotResponse.get(handle) == null) {
        sendTheMessage(handle,false);
      }
    } else {
      doneProbing();
    }

    return;
  }

  /**
   * Can be PastryNode updates, leafset updates, or nodehandle updates.
   *
   * @deprecated this class is no longer an Observer, see {@link #nodeHandleUpdate} and {@link PastryNodeListener}
   */
  @Deprecated
  public void update(Observable arg0, Object arg) {
    update((Object)arg0, arg);
  }

  /**
   * PastryNode updates are only logged.
   */
  public void pastryNodeReady(PastryNode node, boolean ready) {
    update((Object)node, Boolean.valueOf(ready));
  }

  /**
   * PastryNode updates are only logged.
   */
  public void pastryNodeJoinFailed(PastryNode node, JoinFailedException reason) {
    update((Object)node, reason);
  }

  /**
   * NodeHandle updates.
   */
  public void nodeHandleUpdate(rice.p2p.commonapi.NodeHandle handle, Object arg) {
    update((Object)handle, arg);
  }

  private void update(Object arg0, Object arg) {
    logger.trace("CJP: update({},{}){}", arg0, arg, arg.getClass().getName());

    if (arg0 instanceof NodeHandle) {

        // assume it's a NodeHandle, cause we
        // want to throw the exception if it is something we don't recognize
      NodeHandle nh = (NodeHandle)arg0;
      if (((Integer) arg) == NodeHandle.DECLARED_DEAD) {
        logger.debug("CJP:{} declared dead", arg0);
        if (!failed.containsKey(nh)) {
          failed.put(nh, new FailedTime(nh, thePastryNode.getEnvironment().getTimeSource().currentTimeMillis()));
        }

        // Note, can't remove from the leafset, this is the LeafSetProtocol's job
        doneProbing();
      }

      if (((Integer) arg) == NodeHandle.DECLARED_LIVE) {
        failed.remove(nh);
        if (!thePastryNode.isReady()) {
          if (leafSet.test(nh)) {
            leafSet.put(nh);
            sendTheMessage(nh, false);
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
  public int delayInterest() {
    return MAX_TIME_TO_BE_SCHEDULED;
  }


  /**
   * If it took longer than the time to detect faultiness, then other nodes
   * may believe we are faulty.  So we best rejoin.
   *
   * @param loopTime the time it took to do a single selection loop.
   */
  public void loopTime(int loopTime) {
    // may want to do a full rejoin if this one trips... but make it longer
    // partition handler may handle this anyway
    otherNodesMaySuspectFaulty(loopTime);
  }

  public void destroy() {
    logger.info("CJP: destroy() called");
    thePastryNode.getEnvironment().getSelectorManager().removeLoopObserver(this);
    cleanupTask.cancel();
    Iterator<NodeHandle> it2 = observing.iterator();
    while(it2.hasNext()) {
      NodeHandle nh = (NodeHandle)it2.next();
      nh.removeNodeHandleListener(this);
      it2.remove();
    }
    observing.clear();
    observing = null;
  }
}
