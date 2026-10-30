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

import org.slf4j.Logger;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.p2p.glacier.v2.messaging.GlacierMessage;
import rice.p2p.glacier.v2.messaging.GlacierNeighborRequestMessage;
import rice.p2p.glacier.v2.messaging.GlacierNeighborResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeForwardMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshCompleteMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshPatchMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshProbeMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierSyncMessage;
import rice.p2p.glacier.v2.messaging.GlacierTimeoutMessage;

/**
 * Dispatches the messages Glacier receives: responses go to the {@link GlacierContinuation} that waits
 * for them, requests to the collaborator that answers them, and timeouts to the {@link GlacierScheduler}.
 */
final class GlacierMessageDispatcher {
  private final GlacierScheduler scheduler;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierRequestHandler requestHandler;
  private final GlacierNeighborProtocol neighborProtocol;
  private final GlacierSyncProtocol syncProtocol;
  private final GlacierBulkRefresh bulkRefresh;
  private final Logger logger;

  GlacierMessageDispatcher(GlacierScheduler scheduler, GlacierTrafficShaper trafficShaper, GlacierRequestHandler requestHandler, GlacierNeighborProtocol neighborProtocol, GlacierSyncProtocol syncProtocol, GlacierBulkRefresh bulkRefresh, Logger logger) {
    this.scheduler = scheduler;
    this.trafficShaper = trafficShaper;
    this.requestHandler = requestHandler;
    this.neighborProtocol = neighborProtocol;
    this.syncProtocol = syncProtocol;
    this.bulkRefresh = bulkRefresh;
    this.logger = logger;
  }

  /**
   * @return the deserializer for all the message types Glacier sends
   */
  static MessageDeserializer deserializer(final Endpoint endpoint) {
    return (buf, type, priority, sender) -> {
      switch(type) {
        case GlacierDataMessage.TYPE:
          return GlacierDataMessage.build(buf, endpoint);
        case GlacierFetchMessage.TYPE:
          return GlacierFetchMessage.build(buf, endpoint);
        case GlacierNeighborRequestMessage.TYPE:
          return GlacierNeighborRequestMessage.build(buf, endpoint);
        case GlacierNeighborResponseMessage.TYPE:
          return GlacierNeighborResponseMessage.build(buf, endpoint);
        case GlacierQueryMessage.TYPE:
          return GlacierQueryMessage.build(buf, endpoint);
        case GlacierRangeForwardMessage.TYPE:
          return GlacierRangeForwardMessage.build(buf, endpoint);
        case GlacierRangeQueryMessage.TYPE:
          return GlacierRangeQueryMessage.build(buf, endpoint);
        case GlacierRangeResponseMessage.TYPE:
          return GlacierRangeResponseMessage.build(buf, endpoint);
        case GlacierRefreshCompleteMessage.TYPE:
          return GlacierRefreshCompleteMessage.build(buf, endpoint);
        case GlacierRefreshPatchMessage.TYPE:
          return GlacierRefreshPatchMessage.build(buf, endpoint);
        case GlacierRefreshProbeMessage.TYPE:
          return GlacierRefreshProbeMessage.build(buf, endpoint);
        case GlacierRefreshResponseMessage.TYPE:
          return GlacierRefreshResponseMessage.build(buf, endpoint);
        case GlacierResponseMessage.TYPE:
          return GlacierResponseMessage.build(buf, endpoint);
        case GlacierSyncMessage.TYPE:
          return GlacierSyncMessage.build(buf, endpoint);
      }
      throw new IllegalArgumentException("Unknown type:"+type);
    };
  }

  void deliver(Id id, Message message) {

    final GlacierMessage msg = (GlacierMessage) message;
    logger.debug("Received message {} with destination {} from {}", msg, id, msg.getSource().getId());

    if (msg instanceof GlacierDataMessage)
      trafficShaper.charge((GlacierDataMessage) msg);

    if (msg.isResponse()) {
      GlacierContinuation gc = scheduler.get(msg.getUID());

      if (gc != null) {
        if (!gc.hasTerminated()) {
          logger.debug("Resuming [{}]", gc);
          gc.syncReceiveResult(msg);
          logger.debug("---");
        } else {
          logger.debug("Message UID#{} is response, but continuation has already terminated", msg.getUID());
        }
      } else {
        logger.debug("Unusual: Message UID#{} is response, but continuation not found", msg.getUID());
      }

      return;
    }

    if (msg instanceof GlacierQueryMessage) {
      requestHandler.handleQuery((GlacierQueryMessage) msg);
    } else if (msg instanceof GlacierNeighborRequestMessage) {
      neighborProtocol.handleNeighborRequest((GlacierNeighborRequestMessage) msg);
    } else if (msg instanceof GlacierSyncMessage) {
      syncProtocol.handleSyncRequest((GlacierSyncMessage) msg);
    } else if (msg instanceof GlacierRefreshProbeMessage) {
      bulkRefresh.handleProbe((GlacierRefreshProbeMessage) msg);
    } else if (msg instanceof GlacierRefreshPatchMessage) {
      bulkRefresh.handlePatch((GlacierRefreshPatchMessage) msg);
    } else if (msg instanceof GlacierRangeQueryMessage) {
      syncProtocol.handleRangeQuery((GlacierRangeQueryMessage) msg);
    } else if (msg instanceof GlacierRangeForwardMessage) {
      syncProtocol.handleRangeForward((GlacierRangeForwardMessage) msg);
    } else if (msg instanceof GlacierFetchMessage) {
      requestHandler.handleFetch((GlacierFetchMessage) msg);
    } else if (msg instanceof GlacierDataMessage) {
      requestHandler.handleData((GlacierDataMessage) msg);
    } else if (msg instanceof GlacierTimeoutMessage) {

      /* TimeoutMessages are generated by the local node when a
         timeout expires. */

      scheduler.timerExpired();
    } else {
      panic("GLACIER ERROR - Received message " + msg + " of unknown type.");
    }
  }

  /**
   * This method is called when Glacier encounters a fatal error
   *
   * @param s Message describing the error, which is logged
   * @exception Error always
   */
  private void panic(String s) throws Error {
    logger.error("PANIC: {}", s);
    throw new Error("Panic");
  }
}
