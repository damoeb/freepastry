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

import java.io.IOException;
import org.slf4j.Logger;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.p2p.glacier.v2.messaging.GlacierMessage;
import rice.p2p.glacier.v2.messaging.GlacierMessageType;
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
 * for them, requests to the collaborator that answers them, and timeouts to the {@link GlacierContinuationRegistry}.
 */
final class GlacierMessageDispatcher {
  private final GlacierContinuationRegistry scheduler;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierRequestHandler requestHandler;
  private final GlacierNeighborProtocol neighborProtocol;
  private final GlacierSyncProtocol syncProtocol;
  private final GlacierBulkRefresh bulkRefresh;
  private final GlacierHandoffProtocol handoffProtocol;
  private final Logger logger;

  GlacierMessageDispatcher(GlacierContinuationRegistry scheduler, GlacierTrafficShaper trafficShaper, GlacierRequestHandler requestHandler, GlacierNeighborProtocol neighborProtocol, GlacierSyncProtocol syncProtocol, GlacierBulkRefresh bulkRefresh, GlacierHandoffProtocol handoffProtocol, Logger logger) {
    this.scheduler = scheduler;
    this.trafficShaper = trafficShaper;
    this.requestHandler = requestHandler;
    this.neighborProtocol = neighborProtocol;
    this.syncProtocol = syncProtocol;
    this.bulkRefresh = bulkRefresh;
    this.handoffProtocol = handoffProtocol;
    this.logger = logger;
  }

  /**
   * @return the deserializer for all the message types Glacier sends
   */
  static MessageDeserializer deserializer(final Endpoint endpoint) {
    return (buf, type, priority, sender) -> switch (GlacierMessageType.find(type)) {
      case DATA -> GlacierDataMessage.build(buf, endpoint);
      case FETCH -> GlacierFetchMessage.build(buf, endpoint);
      case NEIGHBOR_REQUEST -> GlacierNeighborRequestMessage.build(buf, endpoint);
      case NEIGHBOR_RESPONSE -> GlacierNeighborResponseMessage.build(buf, endpoint);
      case QUERY -> GlacierQueryMessage.build(buf, endpoint);
      case RANGE_FORWARD -> GlacierRangeForwardMessage.build(buf, endpoint);
      case RANGE_QUERY -> GlacierRangeQueryMessage.build(buf, endpoint);
      case RANGE_RESPONSE -> GlacierRangeResponseMessage.build(buf, endpoint);
      case REFRESH_COMPLETE -> GlacierRefreshCompleteMessage.build(buf, endpoint);
      case REFRESH_PATCH -> GlacierRefreshPatchMessage.build(buf, endpoint);
      case REFRESH_PROBE -> GlacierRefreshProbeMessage.build(buf, endpoint);
      case REFRESH_RESPONSE -> GlacierRefreshResponseMessage.build(buf, endpoint);
      case RESPONSE -> GlacierResponseMessage.build(buf, endpoint);
      case SYNC -> GlacierSyncMessage.build(buf, endpoint);
      // local timer message, never sent
      case TIMEOUT -> throw new IOException("Unknown type:"+type);
      case null -> throw new IOException("Unknown type:"+type);
    };
  }

  void deliver(Id id, Message message) {

    final GlacierMessage msg = (GlacierMessage) message;
    logger.debug("Received message {} with destination {} from {}", msg, id, msg.getSource().getId());

    if (msg instanceof GlacierDataMessage glacierDataMessage)
      trafficShaper.charge(glacierDataMessage);

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

    switch (msg) {
      case GlacierQueryMessage m -> requestHandler.handleQuery(m);
      case GlacierNeighborRequestMessage m -> neighborProtocol.handleNeighborRequest(m);
      case GlacierSyncMessage m -> syncProtocol.handleSyncRequest(m);
      case GlacierRefreshProbeMessage m -> bulkRefresh.handleProbe(m);
      case GlacierRefreshPatchMessage m -> bulkRefresh.handlePatch(m);
      case GlacierRangeQueryMessage m -> syncProtocol.handleRangeQuery(m);
      case GlacierRangeForwardMessage m -> syncProtocol.handleRangeForward(m);
      case GlacierFetchMessage m -> requestHandler.handleFetch(m);
      case GlacierDataMessage m when m.getTag() == GlacierTags.HANDOFF -> handoffProtocol.acceptFragments(m);
      case GlacierDataMessage m -> requestHandler.handleData(m);
      // TimeoutMessages are generated by the local node when a timeout expires.
      case GlacierTimeoutMessage m -> scheduler.timerExpired();
      // only ever sent as responses, handled above
      case GlacierNeighborResponseMessage _, GlacierRangeResponseMessage _, GlacierRefreshCompleteMessage _,
          GlacierRefreshResponseMessage _, GlacierResponseMessage _ ->
        panic("GLACIER ERROR - Received message " + msg + " of unknown type.");
    }
  }

  /**
   * This method is called when Glacier encounters a fatal error
   *
   * @param s Message describing the error, which is logged
   * @throws Error always
   */
  private void panic(String s) throws Error {
    logger.error("PANIC: {}", s);
    throw new Error("Panic");
  }
}
