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
package rice.p2p.glacier.v2;

import java.util.Iterator;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.glacier.v2.messaging.GlacierNeighborRequestMessage;
import rice.p2p.glacier.v2.messaging.GlacierNeighborResponseMessage;
import rice.persistence.CachedStorage;

/**
 * The neighbor protocol: nodes exchange which neighbors they know and when they last saw them, which
 * keeps {@link GlacierNeighborhood} up to date.
 */
final class GlacierNeighborProtocol {
  /** Asks neighborSet() for as many neighbors as it knows. */
  private static final int ALL_NEIGHBORS = 999;

  private final GlacierConfig config;
  private final Endpoint endpoint;
  private final GlacierMessageSender router;
  private final GlacierNeighborhood neighborhood;
  private final CachedStorage neighborStorage;
  private final IdFactory factory;
  private final Environment environment;
  private final Logger logger;

  GlacierNeighborProtocol(GlacierConfig config, Endpoint endpoint, GlacierMessageSender router, GlacierNeighborhood neighborhood, CachedStorage neighborStorage, IdFactory factory, Environment environment, Logger logger) {
    this.config = config;
    this.endpoint = endpoint;
    this.router = router;
    this.neighborhood = neighborhood;
    this.neighborStorage = neighborStorage;
    this.factory = factory;
    this.environment = environment;
    this.logger = logger;
  }

  /**
   * @return the periodic task that asks the leaf set about their neighbors once after joining, and then
   *         regularly expires the neighbors that have not been seen for too long
   */
  GlacierContinuation newNeighborTask() {
    return new GlacierContinuation() {
      long nextTimeout;

      @Override
      public String toString() {
        return "Neighbor continuation";
      }
      @Override
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.expireNeighborsDelayAfterJoin();

        NodeHandleSet leafSet = endpoint.neighborSet(ALL_NEIGHBORS);
        NodeHandle localHandle = router.getLocalNodeHandle();
        NodeHandle cwExtreme = localHandle;
        NodeHandle ccwExtreme = localHandle;

        for (int i=0; i<leafSet.size(); i++) {
          NodeHandle thisHandle = leafSet.getHandle(i);
          if (localHandle.getId().clockwise(thisHandle.getId())) {
            if (cwExtreme.getId().clockwise(thisHandle.getId()))
              cwExtreme = thisHandle;
          } else {
            if (ccwExtreme.getId().clockwise(thisHandle.getId()))
              ccwExtreme = thisHandle;
          }
        }

        IdRange leafRange = factory.buildIdRange(ccwExtreme.getId(), cwExtreme.getId());

        for (int k=0; k<leafSet.size(); k++) {
          if (!leafSet.getHandle(k).getId().equals(router.getLocalNodeHandle().getId())) {
            neighborhood.neighborSeen(leafSet.getHandle(k).getId(), environment.getTimeSource().currentTimeMillis());
            if (logger.isInfoEnabled()) logger.info("Asking "+leafSet.getHandle(k).getId()+" about neighbors in "+leafRange);
            router.sendMessage(
              null,
              new GlacierNeighborRequestMessage(getMyUID(), leafRange, router.getLocalNodeHandle(), leafSet.getHandle(k).getId(), GlacierTags.NEIGHBOR),
              leafSet.getHandle(k)
            );
          }
        }
      }
      @Override
      public void receiveResult(Object o) {
        if (o instanceof GlacierNeighborResponseMessage gnrm) {
          logger.atDebug().log(() -> "NeighborResponse from "+gnrm.getSource()+" with "+gnrm.numNeighbors()+" neighbors");
          for (int i=0; i<gnrm.numNeighbors(); i++)
            neighborhood.neighborSeen(gnrm.getNeighbor(i), gnrm.getLastSeen(i));
        } else {
          logger.warn("Unknown response in neighbor continuation: {} -- discarded", o);
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Exception in neighbor continuation: ", e);
        terminate();
      }
      @Override
      public void timeoutExpired() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.expireNeighborsInterval();

        neighborhood.expireNeighbors(endpoint.neighborSet(ALL_NEIGHBORS));
      }
      @Override
      public long getTimeout() {
        return nextTimeout;
      }
    };
  }

  /**
   * Answers a neighbor request with the neighbors in the requested range, and when each was last seen.
   */
  void handleNeighborRequest(final GlacierNeighborRequestMessage gnrm) {
    final IdSet requestedNeighbors = neighborStorage.scan(gnrm.getRequestedRange());
    final int numRequested = requestedNeighbors.numElements();

    if (numRequested < 1) {
      logger.debug("No neighbors in that range -- canceled");
      return;
    }

    logger.info("Neighbor request for {}, found {} neighbors", gnrm.getRequestedRange(), numRequested);

    final Id[] neighbors = new Id[numRequested];
    final long[] lastSeen = new long[numRequested];

    Iterator<Id> iter = requestedNeighbors.getIterator();
    for (int i=0; i<numRequested; i++)
      neighbors[i] = (Id) iter.next();

    neighborStorage.getObject(neighbors[0], new Continuation<Object, Exception>() {
      int currentLookup = 0;

      @Override
      public void receiveResult(Object o) {
        logger.debug("Continue: NeighborRequest from {} for range {}", gnrm.getSource().getId(), gnrm.getRequestedRange());

        if (o == null) {
          logger.warn("Problem while retrieving neighbors -- canceled");
          return;
        }

        if (o instanceof Long l) {
          logger.debug("Retr: Neighbor {} was last seen at {}", neighbors[currentLookup], o);
          lastSeen[currentLookup] = l.longValue();
          currentLookup ++;
          if (currentLookup < numRequested) {
            neighborStorage.getObject(neighbors[currentLookup], this);
          } else {
            logger.debug("Sending neighbor response...");
            router.sendMessage(
              null,
              new GlacierNeighborResponseMessage(gnrm.getUID(), neighbors, lastSeen, router.getLocalNodeHandle(), gnrm.getSource().getId(), gnrm.getTag()),
              gnrm.getSource()
            );
          }
        }
      }

      @Override
      public void receiveException(Exception e) {
        logger.warn("Problem while retrieving neighbors in range {} for {} -- canceled", gnrm.getRequestedRange(), gnrm.getSource(), e);
      }
    });
  }
}
