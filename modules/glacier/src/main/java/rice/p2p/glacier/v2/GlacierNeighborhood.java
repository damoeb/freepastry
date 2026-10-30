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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.NodeHandleSet;
import rice.persistence.StorageManager;

/**
 * Tracks when each neighbor was last seen (in the neighbor storage), and derives the part of the ring
 * this node is responsible for from those neighbors.
 */
final class GlacierNeighborhood {
  private final StorageManager neighborStorage;
  private final IdFactory factory;
  private final Id localId;
  private final Environment environment;
  private final Logger logger;
  private IdRange responsibleRange;
  private long neighborTimeout;

  GlacierNeighborhood(StorageManager neighborStorage, IdFactory factory, Id localId, long neighborTimeout, Environment environment, Logger logger) {
    this.neighborStorage = neighborStorage;
    this.factory = factory;
    this.localId = localId;
    this.neighborTimeout = neighborTimeout;
    this.environment = environment;
    this.logger = logger;
    this.responsibleRange = null;
  }

  IdRange getResponsibleRange() {
    return responsibleRange;
  }

  long getNeighborTimeout() {
    return neighborTimeout;
  }

  void determineResponsibleRange() {
    Id cwPeer = null, ccwPeer = null, xcwPeer = null, xccwPeer = null, myNodeId = localId;

    logger.debug("Determining responsible range");

    Iterator<Id> iter = neighborStorage.scan().getIterator();
    while (iter.hasNext()) {
      Id thisNeighbor = (Id) iter.next();
      logger.trace("Considering neighbor: {}", thisNeighbor);
      if (myNodeId.clockwise(thisNeighbor)) {
        if ((cwPeer == null) || thisNeighbor.isBetween(myNodeId, cwPeer))
          cwPeer = thisNeighbor;
        if ((xcwPeer == null) || xcwPeer.clockwise(thisNeighbor))
          xcwPeer = thisNeighbor;
      } else {
        if ((ccwPeer == null) || thisNeighbor.isBetween(ccwPeer, myNodeId))
          ccwPeer = thisNeighbor;
        if ((xccwPeer == null) || !xccwPeer.clockwise(thisNeighbor))
          xccwPeer = thisNeighbor;
      }
    }

    if (ccwPeer == null)
      ccwPeer = xcwPeer;
    if (cwPeer == null)
      cwPeer = xccwPeer;

    if (logger.isDebugEnabled()) logger.debug("XCCW: "+xccwPeer+" CCW: "+ccwPeer+" ME: "+myNodeId+" CW: "+cwPeer+" XCW: "+xcwPeer);

    if ((ccwPeer == null) || (cwPeer == null)) {
      responsibleRange = factory.buildIdRange(myNodeId, myNodeId);
      return;
    }

    Id.Distance ccwHalfDistance;
    if (!myNodeId.clockwise(ccwPeer))
      ccwHalfDistance = ccwPeer.distanceFromId(myNodeId).shiftDistance(1,0);
    else
      ccwHalfDistance = ccwPeer.longDistanceFromId(myNodeId).shiftDistance(1,0);

    Id.Distance cwHalfDistance;
    if (myNodeId.clockwise(cwPeer))
      cwHalfDistance = cwPeer.distanceFromId(myNodeId).shiftDistance(1,0);
    else
      cwHalfDistance = cwPeer.longDistanceFromId(myNodeId).shiftDistance(1,0);

    responsibleRange = factory.buildIdRange(
      ccwPeer.addToId(ccwHalfDistance),
      myNodeId.addToId(cwHalfDistance)
    );

    logger.info("New range: {}", responsibleRange);
  }

  void neighborSeen(Id nodeId, long when) {

    if (nodeId.equals(localId))
      return;

    logger.debug("Neighbor {} was seen at {}", nodeId, when);

    if (when > environment.getTimeSource().currentTimeMillis()) {
      if (logger.isWarnEnabled()) logger.warn("Neighbor: "+when+" is in the future (now="+environment.getTimeSource().currentTimeMillis()+")");
      when = environment.getTimeSource().currentTimeMillis();
    }

    final Id fNodeId = nodeId;
    final long fWhen = when;

    neighborStorage.getObject(nodeId,
      new Continuation<Object, Exception>() {
        public void receiveResult(Object o) {
          logger.debug("Continue: neighborSeen ({}, {}) after getObject", fNodeId, fWhen);

          final long previousWhen = (o!=null) ? ((Long)o).longValue() : 0;
          logger.debug("Neighbor: {} previously seen at {}", fNodeId, previousWhen);
          if (previousWhen >= fWhen) {
            logger.debug("Neighbor: No update needed (new TS={})", fWhen);
            return;
          }

          storeLastSeen(fNodeId, fWhen, previousWhen);
        }
        public void receiveException(Exception e) {
          logger.warn("receiveException({}) while retrieving a neighbor ({})", e, fNodeId);
        }
      }
    );
  }

  Id[][] getNeighborRanges() {
    Iterator<Id> iter = neighborStorage.scan().getIterator();
    List<Id> ccwIDs = new ArrayList<Id>();
    List<Id> cwIDs = new ArrayList<Id>();
    Id myID = localId;

    while (iter.hasNext()) {
      Id thisNeighbor = (Id)iter.next();
      if (myID.clockwise(thisNeighbor))
        cwIDs.add(thisNeighbor);
      else
        ccwIDs.add(thisNeighbor);
    }

    for (int j=0; j<2; j++) {
      List<Id> v = (j==0) ? cwIDs : ccwIDs;
      boolean madeProgress = true;
      while (madeProgress) {
        madeProgress = false;
        for (int i=0; i<(v.size()-1); i++) {
          if (v.get(i+1).clockwise(v.get(i))) {
            Id h = v.get(i);
            v.set(i, v.get(i+1));
            v.set(i+1, h);
            madeProgress = true;
          }
        }
      }
    }

    List<Id> allIDs = new ArrayList<Id>();
    allIDs.addAll(ccwIDs);
    allIDs.add(myID);
    allIDs.addAll(cwIDs);

    Id[][] result = new Id[allIDs.size()][3];
    for (int i=0; i<allIDs.size(); i++) {
      Id currentElement = allIDs.get(i);
      Id cwId, ccwId;

      if (i>0) {
        Id previousElement = allIDs.get(i-1);
        ccwId = previousElement.addToId(previousElement.distanceFromId(currentElement).shiftDistance(1,0));
      } else {
        ccwId = currentElement;
      }

      if (i<(allIDs.size()-1)) {
        Id nextElement = allIDs.get(i+1);
        cwId = currentElement.addToId(currentElement.distanceFromId(nextElement).shiftDistance(1,0));
      } else {
        cwId = currentElement;
      }

      result[i][0] = ccwId;
      result[i][1] = currentElement;
      result[i][2] = cwId;
    }

    return result;
  }

  /**
   * Refreshes the neighbors that are still in the leaf set, removes the stored neighbors that have not
   * been seen for longer than the neighbor timeout, and recomputes the responsible range.
   */
  void expireNeighbors(NodeHandleSet leafSet) {
    final long earliestAcceptableDate = environment.getTimeSource().currentTimeMillis() - neighborTimeout;
    IdSet allNeighbors = neighborStorage.scan();
    Iterator<Id> iter = allNeighbors.getIterator();

    logger.info("Checking neighborhood for expired certificates...");

    while (iter.hasNext()) {
      final Id thisNeighbor = (Id) iter.next();
      if (leafSet.memberHandle(thisNeighbor)) {
        logger.debug("CNE: Refreshing current neighbor: {}", thisNeighbor);
        neighborSeen(thisNeighbor, environment.getTimeSource().currentTimeMillis());
      } else {
        logger.debug("CNE: Retrieving {}", thisNeighbor);
        expireIfStale(thisNeighbor, earliestAcceptableDate);
      }
    }

    determineResponsibleRange();
  }

  private void storeLastSeen(final Id fNodeId, final long fWhen, final long previousWhen) {
    neighborStorage.store(fNodeId, null, Long.valueOf(fWhen),
      new Continuation<Object, Exception>() {
        public void receiveResult(Object o) {
          logger.debug("Continue: neighborSeen ({}, {}) after store", fNodeId, fWhen);
          logger.debug("Neighbor: Updated {} from {} to {}", fNodeId, previousWhen, fWhen);
          determineResponsibleRange();
        }
        public void receiveException(Exception e) {
          logger.warn("receiveException({}) while storing a neighbor ({})", e, fNodeId);
        }
      }
    );
  }

  /**
   * Removes a stored neighbor that was last seen before earliestAcceptableDate.
   */
  private void expireIfStale(final Id thisNeighbor, final long earliestAcceptableDate) {
    neighborStorage.getObject(thisNeighbor, new Continuation<Object, Exception>() {
      public void receiveResult(Object o) {
        if (o==null) {
          logger.warn("CNE: Cannot retrieve neighbor {}", thisNeighbor);
          return;
        }

        long lastSeen = ((Long)o).longValue();
        if (lastSeen < earliestAcceptableDate) {
          logger.info("CNE: Removing expired neighbor {} ({}<{})", thisNeighbor, lastSeen, earliestAcceptableDate);
          neighborStorage.unstore(thisNeighbor, new Continuation<Object, Exception>() {
            public void receiveResult(Object o) {
              logger.debug("CNE unstore successful: {}, returned {}", thisNeighbor, o);
            }
            public void receiveException(Exception e) {
              logger.atWarn().log(() -> "CNE unstore failed: "+thisNeighbor+", returned "+e);
            }
          });
        } else {
          logger.info("CNE: Neighbor {} still active, last seen {}", thisNeighbor, lastSeen);
        }
      }
      public void receiveException(Exception e) {
        logger.atWarn().log(() -> "CNE: Exception while retrieving neighbor "+thisNeighbor+", e="+e);
      }
    });
  }
}
