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
package org.mpisws.p2p.transport.proximity;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.liveness.PingListener;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.environment.time.TimeSource;

public class MinRTTProximityProvider<Identifier> implements ProximityProvider<Identifier>, PingListener<Identifier> {

   /**
    * Holds only pending DeadCheckers
    */
   final Map<Identifier, PeerMessageWriter> managers;

   Pinger<Identifier> tl;

   Logger logger;

   TimeSource time;

   int pingThrottle = 5000; // TODO: Make configurable

  public MinRTTProximityProvider(Pinger<Identifier> pinger, Environment env) {
    this.tl = pinger;
    this.logger = env.getLogManager().getLogger(MinRTTProximityProvider.class, null);
    this.time = env.getTimeSource();
    pinger.addPingListener(this);
    this.managers = new HashMap<Identifier, PeerMessageWriter>();
  }

  @Override
  public int proximity(Identifier identifier, @Nullable Map<String, Object> options) {
    PeerMessageWriter manager = getManager(identifier);
    int ret = manager.proximity;
    if (ret == DEFAULT_PROXIMITY) {
      manager.ping(options);
    }
    return ret;
  }

  @Override
  public void pingResponse(Identifier identifier, int rtt, @Nullable Map<String, Object> options) {
    getManager(identifier).markProximity(rtt, options);
  }

  @Override
  public void pingReceived(Identifier identifier, @Nullable Map<String, Object> options) {

  }

  @Override
  public void clearState(Identifier identifier) {
    synchronized(managers) {
      managers.remove(identifier);
    }
  }

  public PeerMessageWriter getManager(Identifier identifier) {
    synchronized(managers) {
      PeerMessageWriter manager = managers.get(identifier);
      if (manager == null) {
        manager = new PeerMessageWriter(identifier);
        logger.trace("Creating EM for {}", identifier);
        managers.put(identifier,manager);
      }
      return manager;
    }
  }

  /**
   * Internal class which is charges with managing the remote connection via
   * a specific route
   */
  public class PeerMessageWriter {

    // the remote route of this manager
    private final Identifier identifier;

    // the current best-known proximity of this route
    private int proximity;

    private long lastPingTime = Integer.MIN_VALUE; // we don't want underflow, but we don't want this to be zero either
    /**
     * Constructor - builds a manager for the given identifier
     *
     * @param route The remote identifier
     */
    public PeerMessageWriter(Identifier route) {
      if (route == null) throw new IllegalArgumentException("route is null");
      this.identifier = route;
      proximity = DEFAULT_PROXIMITY;
    }

    public void ping(@Nullable Map<String, Object> options) {
      long now = time.currentTimeMillis();
      if ((now - lastPingTime) < pingThrottle) {
        logger.atDebug().log(() -> "Dropping ping because pingThrottle."+(pingThrottle - (now - lastPingTime)));
        return;
      }
      lastPingTime = now;
      tl.ping(identifier, options);
    }

    /**
     * This method should be called when this route has its proximity updated
     *
     * @param proximity The proximity
     */
    protected void markProximity(int proximity, @Nullable Map<String, Object> options) {
      if (proximity < 0) throw new IllegalArgumentException("proximity must be >= 0, was:"+proximity);
      logger.trace("{}.markProximity({})", this, proximity);
      if (this.proximity > proximity) {
        logger.debug("{} updating proximity to {}", this, proximity);
        this.proximity = proximity;
        notifyProximityListeners(identifier, proximity, options);
      }
    }

    @Override
    public String toString() {
      return identifier.toString();
    }
  }

  final Collection<ProximityListener<Identifier>> listeners = new CopyOnWriteArrayList<ProximityListener<Identifier>>();
  @Override
  public void addProximityListener(ProximityListener<Identifier> listener) {
    listeners.add(listener);
  }

  @Override
  public boolean removeProximityListener(ProximityListener<Identifier> listener) {
    return listeners.remove(listener);
  }

  public void notifyProximityListeners(Identifier identifier, int prox, @Nullable Map<String, Object> options) {
    for (ProximityListener<Identifier> proximityListener : listeners) {
      proximityListener.proximityChanged(identifier, prox, options);
    }
  }
}
