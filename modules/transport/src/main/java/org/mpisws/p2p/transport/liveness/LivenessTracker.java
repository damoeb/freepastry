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
package org.mpisws.p2p.transport.liveness;

import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_ALIVE;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD_FOREVER;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_SUSPECTED;
/**
 * Internal class which is charged with managing the liveness of a remote
 * identifier
 */
public class LivenessTracker<Identifier> {
  /** How many pings sent on this route are remembered until their pong arrives. */
  static final int MAX_OUTSTANDING_PINGS = 8;

  final LivenessTransportLayer<Identifier> layer;
  private final Logger logger;


  /**
   * Retransmission Time Out
   */
  int RTO;

  /**
   * Average RTT
   */
  double RTT = 0;

  /**
   * Standard deviation RTT
   */
  double standardD;  // RFC1122 recommends choose value to target RTO = 3 seconds


  // the remote route of this manager
  final WeakReference<Identifier> identifier;

  // the current liveness of this route
  int liveness;

  // the last time the liveness information was updated
  long updated;

  // whether or not a check dead is currently being carried out on this route
  private LivenessTransportLayer<Identifier>.@Nullable DeadChecker pendingDeadchecker;

  final Set<LivenessCheckingSocket<Identifier>> sockets;

  /** when the layer last looked this tracker up; the layer forgets the least recently used idle trackers */
  long lastUsed;

  /** A ping sent on this route that no pong has answered yet. */
  private record OutstandingPing(long sentTime, long nonce) {
  }

  /** the last pings sent on this route that no pong has answered yet, oldest first */
  private final ArrayDeque<OutstandingPing> outstandingPings = new ArrayDeque<>();

  /** when the last ping was sent on this route; meaningless while no ping is outstanding */
  private long lastPingSent = 0L;

  /**
   * Constructor - builds a manager for the given identifier
   *
   * @param identifier the remote entity, held weakly
   */
  public LivenessTracker(LivenessTransportLayer<Identifier> layer, Identifier identifier) {
    this.layer = layer;
    this.logger = layer.logger;
    this.RTO = layer.DEFAULT_RTO;
    this.standardD = RTO/4.0;
    if (identifier == null) throw new IllegalArgumentException("identifier is null");
    this.identifier = new WeakReference<Identifier>(identifier);
    this.liveness = LIVENESS_SUSPECTED;

    this.pendingDeadchecker = null;
    this.updated = 0L;
    this.lastUsed = layer.time.currentTimeMillis();
    sockets = new HashSet<LivenessCheckingSocket<Identifier>>();
  }

  /** Remembers a ping sent on this route at {@code sentTime} with {@code nonce}, so that its pong is recognised. */
  synchronized void pingSent(long sentTime, long nonce) {
    if (outstandingPings.size() >= MAX_OUTSTANDING_PINGS) {
      outstandingPings.removeFirst();
    }
    outstandingPings.addLast(new OutstandingPing(sentTime, nonce));
    lastPingSent = sentTime;
  }

  /**
   * Matches a pong against the pings sent on this route.
   *
   * @param sentTime the time stamp the pong echoes
   * @param nonce the nonce the pong echoes: whoever does not know it cannot answer the ping
   * @return true if a ping with this time stamp and nonce was outstanding (it no longer is); false for a pong this
   *     node never asked for
   */
  synchronized boolean pongReceived(long sentTime, long nonce) {
    return outstandingPings.removeFirstOccurrence(new OutstandingPing(sentTime, nonce));
  }

  /**
   * @return true while this tracker is checking its route or carries sockets, or a ping sent less than the
   *     maximal RTO ago may still be answered: the layer does not forget such a tracker to make room for others
   */
  boolean inUse(long now) {
    synchronized (this) {
      if (pendingDeadchecker != null || (!outstandingPings.isEmpty() && now - lastPingSent < layer.RTO_UBOUND)) {
        return true;
      }
    }
    synchronized (sockets) {
      return !sockets.isEmpty();
    }
  }

  public LivenessTransportLayer<Identifier>.@Nullable DeadChecker getPending() {
    return pendingDeadchecker;
  }
  public void setPending(LivenessTransportLayer<Identifier>.@Nullable DeadChecker livenessTransportLayer) {
    pendingDeadchecker = livenessTransportLayer;
  }

  public void removeSocket(LivenessCheckingSocket<Identifier> socket) {
    synchronized(sockets) {
      sockets.remove(socket);
    }
  }

  public int retransmissionTimeoutMillis() {
    return (int)RTO;
  }

  /**
   * This method should be called when this route is declared
   * alive.
   */
  protected void markAlive(@Nullable Map<String, Object> options) {
    boolean notify = false;
    if (liveness != LIVENESS_ALIVE) notify = true;
    this.liveness = LIVENESS_ALIVE;
    if (notify) {
      Identifier temp = identifier.get();
      if (temp != null) {
        layer.notifyLivenessListeners(temp, liveness, options);
      }
    }
  }

  /**
   * This method should be called when this route is declared
   * suspected.
   */
  protected void markSuspected(@Nullable Map<String, Object> options) {
    // note, can only go from alive -> suspected, can't go from dead->suspected
    if (liveness > LIVENESS_SUSPECTED) return;

    boolean notify = false;
    if (liveness != LIVENESS_SUSPECTED) notify = true;
    this.liveness = LIVENESS_SUSPECTED;
    if (notify) {
      logger.debug("{}.markSuspected() notify = true", this);
      Identifier temp = identifier.get();
      if (temp != null) {
        layer.notifyLivenessListeners(temp, liveness, options);
      }
    }
  }

  /**
   * This method should be called when this route is declared
   * dead.
   */
  protected void markDead(@Nullable Map<String, Object> options) {
    boolean notify = false;
    if (liveness < LIVENESS_DEAD) notify = true;
    logger.trace("{}.markDead() notify:{}", this, notify);
    markDeadHelper(LIVENESS_DEAD, options, notify);
  }

  protected void markDeadForever(@Nullable Map<String, Object> options) {
    boolean notify = false;
    if (liveness < LIVENESS_DEAD_FOREVER) notify = true;
    logger.trace("{}.markDeadForever() notify:{}", this, notify);
    markDeadHelper(LIVENESS_DEAD_FOREVER, options, notify);
  }

  protected void markDeadHelper(int liveness, @Nullable Map<String, Object> options, boolean notify) {
    this.liveness = liveness;
    if (getPending() != null) {
      getPending().cancel(); // sets to null too
    }

    // it's very important to notify before closing the sockets,
    // otherwise the higher layers may fight you here by trying to open
    // sockets that this layer is closing
    if (notify) {
      Identifier temp = identifier.get();
      if (temp != null) {
        layer.notifyLivenessListeners(temp, liveness, options);
      } else {
        logger.warn("markDeadHelper({},{},{}) temp == null!  Can't notify listeners!", liveness, options, notify);
      }
    }

    ArrayList<LivenessCheckingSocket<Identifier>> temp;
    synchronized(sockets) {
      // the close() operation can cause a ConcurrentModificationException
      temp = new ArrayList<LivenessCheckingSocket<Identifier>>(sockets);
      sockets.clear();
    }
    for (LivenessCheckingSocket<Identifier> livenessCheckingSocket : temp) {
      logger.info("closing {}", livenessCheckingSocket);
      livenessCheckingSocket.close();
    }
  }


  /**
   * Adds a new round trip time datapoint to our RTT estimate, and
   * updates RTO and standardD accordingly.
   *
   * @param m new RTT
   */
  void updateRTO(long m) {
    if (m < 0) throw new IllegalArgumentException("rtt must be >= 0, was:"+m);

    // rfc 1122
    double err = m-RTT;
    double absErr = err;
    if (absErr < 0) {
      absErr *= -1;
    }
    RTT = RTT+layer.gainG*err;
    standardD = standardD + layer.gainH*(absErr-standardD);
    RTO = (int)(RTT+(4.0*standardD));
    if (RTO > layer.RTO_UBOUND) {
      RTO = layer.RTO_UBOUND;
    }
    if (RTO < layer.RTO_LBOUND) {
      RTO = layer.RTO_LBOUND;
    }
  }

  /**
   * Method which checks to see this route is dead.  If this address has
   * been checked within the past CHECK_DEAD_THROTTLE millis, then
   * this method does not actually do a check.
   *
   * @return true if there will be an update (either a ping, or a change in liveness)
   */
  protected boolean checkLiveness(final @Nullable Map<String, Object> options) {
    logger.trace("{}.checkLiveness()", this);

    boolean ret = false;
    int rto = layer.DEFAULT_RTO;
    synchronized (this) {
      if (this.getPending() != null) {
        if (this.liveness < LIVENESS_DEAD) {
          return true;
        } else {
          return false; // probably won't change
        }
      }

      long now = layer.time.currentTimeMillis();
      if ((this.liveness < LIVENESS_DEAD) ||
          (this.updated < now - layer.CHECK_DEAD_THROTTLE)) {
        this.updated = now;
        rto = retransmissionTimeoutMillis();
        this.setPending(layer.new DeadChecker(this, layer.NUM_PING_TRIES, rto, options));
        ret = true;
      } else {
        logger.atTrace().log(() -> this+".checkLiveness() not checking "+identifier.get()+" checked to recently, can't check for "+((updated+layer.CHECK_DEAD_THROTTLE)-now)+" millis.");
      }
    }
    if (ret) {
      final int theRTO = rto;

      Runnable task = new Runnable() {
        @Override
        public void run() {
          if (getPending() == null) return;  // could have been set to null in the meantime
          layer.timer.schedule(getPending(), theRTO);
          Identifier temp = identifier.get();
          if (temp != null) {
            layer.ping(temp, options);
          }
        }

        @Override
        public String toString() {
          return LivenessTracker.this.toString();
        }
      };
      if (layer.environment.getSelectorManager().isSelectorThread()) {
        task.run();
      } else {
        layer.environment.getSelectorManager().invoke(task);
      }
    }

    if (this.liveness >= LIVENESS_DEAD) return false; // probably won't change

    return ret;
  }

  @Override
  public String toString() {
    Identifier temp = identifier.get();
    if (temp == null) return "null";
    return temp.toString();
  }

  public void destroy() {
    if (getPending() != null) getPending().cancel();
  }
}
