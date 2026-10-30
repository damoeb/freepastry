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
 * Created on Nov 8, 2005
 */
package rice.pastry.direct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.direct.Delivery;
import org.mpisws.p2p.transport.direct.DirectTransportLayer;
import org.mpisws.p2p.transport.direct.EventSimulator;
import org.mpisws.p2p.transport.direct.GenericNetworkSimulator;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessTypes;
import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;
import rice.selector.TimerTask;


public class BasicNetworkSimulator<Identifier, MessageType> extends EventSimulator implements
    GenericNetworkSimulator<Identifier, MessageType> {
  static class Tupel<Identifier, MessageType> {
    DirectTransportLayer<Identifier, MessageType> tl;
    NodeRecord record;

    public Tupel(Identifier i, DirectTransportLayer<Identifier, MessageType> tl, NodeRecord record) {
      super();
      this.tl = tl;
      this.record = record;
    }
  }

  /**
   * This maps to the next highest transport layer
   */
  Map<Identifier, Tupel<Identifier, MessageType>> nodes = Collections.synchronizedMap(new HashMap<Identifier, Tupel<Identifier, MessageType>>());


  // to notify of send/receive
  NetworkSimulator<Identifier, MessageType> sim;


  public BasicNetworkSimulator(Environment env, RandomSource random, NetworkSimulator<Identifier, MessageType> sim,
      NetworkSimulatorConfig config) {
    super(env, random, env.getLogManager().getLogger(BasicNetworkSimulator.class, null));
    this.sim = sim;
    manager.useLoopListeners(false);
    start();
  }

  private void addTask(TimerTask dtt) {
    logger.debug("addTask({})", dtt);
    manager.getTimer().schedule(dtt);
  }

  @Override
  public CancellableTask enqueueDelivery(Delivery d, int delay) {
    long time = timeSource.currentTimeMillis()+delay;
    logger.debug("BNS: enqueueDelivery {}:{}", d, time);
    DeliveryTimerTask dtt = new DeliveryTimerTask(d, time, d.getSeq());
    addTask(dtt);
    return dtt;
  }

  @Override
  public Cancellable deliverMessage(MessageType msg, Identifier node, Identifier from,
      int delay) {
    if (delay > 0) {
      sim.notifySimulatorListenersSent(msg, from, node, delay);
    }
    return deliverMessage(msg, node, from, delay, 0);
  }

  void notifySimulatorListenersReceived(MessageType m, Identifier from, Identifier to) {
    sim.notifySimulatorListenersReceived(m, from, to);
  }

  public Cancellable deliverMessage(MessageType msg, Identifier node, Identifier from, int delay, int period) {
    logger.debug("BNS: deliver {} to {}", msg, node);

    DirectTimerTask dtt = null;

    if (from == null || isAlive(from)) {
      MessageDelivery<Identifier, MessageType> md = new MessageDelivery<Identifier, MessageType>(msg, node, from, null, this);
      dtt = new DirectTimerTask(md, timeSource.currentTimeMillis()+ delay,period);
      addTask(dtt);
    }
    return dtt;
  }


  public void registerIdentifier(Identifier i, DirectTransportLayer<Identifier, MessageType> dtl, NodeRecord record) {
    nodes.put(i, new Tupel<>(i, dtl, record));
  }

  @Override
  public void remove(Identifier i) {
    if (!environment.getSelectorManager().isSelectorThread()) throw new IllegalStateException("Operation not permitted on non-selector thread.");
    nodes.remove(i);
    notifyLivenessListeners(i, LivenessTypes.LIVENESS_DEAD, null);
  }

  @Override
  public Environment getEnvironment(Identifier i) {
    return nodes.get(i).tl.getEnvironment();
  }

  @Override
  public boolean isAlive(Identifier i) {
    return nodes.containsKey(i);
  }

  @Override
  public DirectTransportLayer<Identifier, MessageType> getTL(Identifier i) {
    Tupel<Identifier, MessageType> t = nodes.get(i);
    if (t == null) return null;
    return t.tl;
  }

  /**
   * computes the one-way distance between two NodeIds
   *
   * @param a the first NodeId
   * @param b the second NodeId
   * @return the proximity between the two input NodeIds
   */
  @Override
  public float networkDelay(Identifier a, Identifier b) {
    Tupel<Identifier, MessageType> ta = nodes.get(a);
    Tupel<Identifier, MessageType> tb = nodes.get(b);
    if (ta == null) {
      throw new RuntimeException("asking about node proximity for unknown node "+a);
    }

    if (tb == null) {
      throw new RuntimeException("asking about node proximity for unknown node "+b);
    }

    NodeRecord nra = ta.record;
    NodeRecord nrb = tb.record;

    return nra.networkDelay(nrb);
  }

  /**
   * computes the rtt between two NodeIds
   *
   * @param a the first NodeId
   * @param b the second NodeId
   * @return the proximity between the two input NodeIds
   */
  public float proximity(Identifier a, Identifier b) {
    Tupel<Identifier, MessageType> ta = nodes.get(a);
    Tupel<Identifier, MessageType> tb = nodes.get(b);
    if (ta == null) {
      throw new RuntimeException("asking about node proximity for unknown node "+a+" "+b);
    }

    if (tb == null) {
      throw new RuntimeException("asking about node proximity for unknown node "+b+" "+a);
    }

    NodeRecord nra = ta.record;
    NodeRecord nrb = tb.record;

    return nra.proximity(nrb);
  }

  public NodeRecord getNodeRecord(DirectNodeHandle handle) {
    Tupel<Identifier, MessageType> t = nodes.get(handle);
    if (t == null) return null;
    return t.record;
  }

  final List<LivenessListener<Identifier>> livenessListeners = new ArrayList<LivenessListener<Identifier>>();
  @Override
  public void addLivenessListener(LivenessListener<Identifier> name) {
    synchronized(livenessListeners) {
      livenessListeners.add(name);
    }
  }

  @Override
  public boolean removeLivenessListener(LivenessListener<Identifier> name) {
    synchronized(livenessListeners) {
      return livenessListeners.remove(name);
    }
  }

  private void notifyLivenessListeners(Identifier i, int liveness, Map<String, Object> options) {
    logger.atTrace().log(() -> "notifyLivenessListeners("+i+","+liveness+"):"+livenessListeners.get(0));
    List<LivenessListener<Identifier>> temp;
    synchronized(livenessListeners) {
      temp = new ArrayList<LivenessListener<Identifier>>(livenessListeners);
    }
    for (LivenessListener<Identifier> listener : temp) {
      listener.livenessChanged(i, liveness, options);
    }
  }

  @Override
  public boolean checkLiveness(Identifier i, Map<String, Object> options) {
    return false;
  }

  @Override
  public int getLiveness(Identifier i, Map<String, Object> options) {
    if (nodes.containsKey(i)) {
      return LivenessTypes.LIVENESS_ALIVE;
    }
    return LivenessTypes.LIVENESS_DEAD;
  }

  @Override
  public void clearState(Identifier i) {
    throw new IllegalStateException("not implemented");
  }
}
