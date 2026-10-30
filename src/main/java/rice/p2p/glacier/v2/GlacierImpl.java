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

import java.util.Arrays;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.glacier.Glacier;
import rice.p2p.glacier.GlacierException;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.VersioningPast;
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
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.GCPast;
import rice.p2p.past.rawserialization.JavaPastContentHandleDeserializer;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.rawserialization.PastContentHandleDeserializer;
import rice.p2p.util.DebugCommandHandler;
import rice.persistence.StorageManager;

@SuppressWarnings("unchecked")
public class GlacierImpl implements Glacier, GCPast, VersioningPast, Application, DebugCommandHandler {
  private final StorageManager fragmentStorage;
  private final StorageManager neighborStorage;
  private final GlacierConfig config;
  private final GlacierPolicy policy;
  private final Node node;
  private final int numFragments;
  private final String instance;
  private final int numSurvivors;
  private final Endpoint endpoint;
  private final IdFactory factory;
  private final GlacierDebugCommands debugCommands;
  private final GlacierRequestHandler requestHandler;
  private final GlacierMaintenance maintenance;
  private final GlacierNeighborProtocol neighborProtocol;
  private final GlacierSyncProtocol syncProtocol;
  private final GlacierHandoffProtocol handoffProtocol;
  private final GlacierBulkRefresh bulkRefresh;
  private final GlacierInserter inserter;
  private final GlacierRetriever retriever;
  private final GlacierStatisticsCollector statisticsCollector;
  private final GlacierRouter router;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierTrash trash;
  private final GlacierNeighborhood neighborhood;
  private final GlacierScheduler scheduler;
  private final FragmentLocator locator;





















  private Environment environment;
  private final Logger logger;

  private PastContentHandleDeserializer contentHandleDeserializer;

  public GlacierImpl(Node nodeArg, StorageManager fragmentStorageArg, StorageManager neighborStorageArg, int numFragmentsArg, int numSurvivorsArg, IdFactory factoryArg, String instanceArg, GlacierPolicy policyArg) {
    this.environment = nodeArg.getEnvironment();
    this.logger = environment.getLogManager().getLogger(GlacierImpl.class, instanceArg);

    this.config = GlacierConfig.from(environment.getParameters());



    this.fragmentStorage = fragmentStorageArg;
    this.neighborStorage = neighborStorageArg;
    this.trash = new GlacierTrash(fragmentStorageArg, logger);
    this.policy = policyArg;
    this.trafficShaper = new GlacierTrafficShaper(config, numFragmentsArg, environment, logger);
    this.node = nodeArg;
    this.instance = instanceArg;
    this.endpoint = node.buildEndpoint(this, instance);
    this.scheduler = new GlacierScheduler(endpoint, environment, logger);
    this.endpoint.setDeserializer((buf, type, priority, sender) -> {
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
    });
    this.contentHandleDeserializer = new JavaPastContentHandleDeserializer();
    this.numFragments = numFragmentsArg;
    this.numSurvivors = numSurvivorsArg;
    this.factory = factoryArg;
    this.locator = new FragmentLocator(factoryArg, numFragmentsArg);
    this.neighborhood = new GlacierNeighborhood(neighborStorage, factory, endpoint.getLocalNodeHandle().getId(), config.neighborTimeout(), environment, logger);
    this.statisticsCollector = new GlacierStatisticsCollector(config, scheduler, trafficShaper, neighborhood, trash, fragmentStorage, neighborStorage, instance, environment, logger);
    this.router = new GlacierRouter(endpoint, statisticsCollector, logger);
    this.retriever = new GlacierRetriever(config, numFragments, numSurvivors, instance, policy, endpoint, scheduler, router, locator, trafficShaper, trash, environment, logger);
    this.bulkRefresh = new GlacierBulkRefresh(config, numFragments, numSurvivors, policy, scheduler, router, locator, neighborhood, fragmentStorage, factory, environment, logger);
    this.inserter = new GlacierInserter(config, numFragments, numSurvivors, policy, endpoint, scheduler, router, locator, trafficShaper, retriever, bulkRefresh, environment, logger);
    this.syncProtocol = new GlacierSyncProtocol(config, numFragments, policy, endpoint, scheduler, router, locator, neighborhood, trafficShaper, fragmentStorage, factory, environment, logger);
    this.handoffProtocol = new GlacierHandoffProtocol(config, instance, policy, router, locator, neighborhood, trash, fragmentStorage, environment, logger);
    this.neighborProtocol = new GlacierNeighborProtocol(config, endpoint, router, neighborhood, neighborStorage, factory, environment, logger);
    this.maintenance = new GlacierMaintenance(config, numFragments, locator, neighborhood, trash, retriever, fragmentStorage, environment, logger);
    this.requestHandler = new GlacierRequestHandler(config, numFragments, numSurvivors, instance, policy, router, locator, neighborhood, retriever, fragmentStorage, environment, logger);
    this.debugCommands = new GlacierDebugCommands(config, numFragments, numSurvivors, instance, factory, scheduler, router, locator, neighborhood, trafficShaper, syncProtocol, retriever, inserter, fragmentStorage, neighborStorage, environment, logger);
    neighborhood.determineResponsibleRange();
    endpoint.register();
  }

  public void startup() {

    /* Neighbor requests */

    scheduler.addContinuation(neighborProtocol.newNeighborTask());

    /* Sync */

    scheduler.addContinuation(syncProtocol.newSyncTask());

    /* Handoff */

    scheduler.addContinuation(handoffProtocol.newHandoffTask());

    /* Garbage collection */

    scheduler.addContinuation(maintenance.newGarbageCollectionTask());

    /* Local scan */

    scheduler.addContinuation(maintenance.newLocalScanTask());

    /* Traffic shaper */

    scheduler.addContinuation(trafficShaper.newShaperTask());

    /* Statistics */

    scheduler.addContinuation(statisticsCollector.newReportingTask());
  }

  public void setTrashcan(StorageManager trashStorage) {
    trash.setTrashcan(trashStorage);
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

  public String handleDebugCommand(String command) {
    return debugCommands.handleDebugCommand(command);
  }

  public void insert(final PastContent obj, final long expiration, final Continuation<? super Boolean[], Exception> command) {
    inserter.insert(obj, expiration, command);
  }

  public void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    inserter.refresh(ids, versions, expirations, command);
  }

  public void insert(final PastContent obj, final Continuation<? super Boolean[], Exception> command) {
    insert(obj, GCPast.INFINITY_EXPIRATION, command);
  }

  public void refresh(Id[] ids, long[] expirations, Continuation<? super Object[], Exception> command) {
    long[] versions = new long[ids.length];
    Arrays.fill(versions, 0);
    refresh(ids, versions, expirations, command);
  }

  public void refresh(Id[] ids, long expiration, Continuation<? super Object[], Exception> command) {
    long[] expirations = new long[ids.length];
    Arrays.fill(expirations, expiration);
    refresh(ids, expirations, command);
  }

  public boolean forward(final RouteMessage message) {
    return true;
  }

  public void update(NodeHandle handle, boolean joined) {
    logger.atInfo().log(() -> "Leafset update: " + handle + " has " + (joined ? "joined" : "left"));

    if (!joined)
      return;

    neighborhood.neighborSeen(handle.getId(), environment.getTimeSource().currentTimeMillis());
  }

  public void lookupHandle(Id id, NodeHandle handle, Continuation<? super PastContentHandle, Exception> command) {
    command.receiveException(new UnsupportedOperationException("LookupHandle() is not supported on Glacier"));
  }

  public void lookupHandles(Id id, int num, Continuation<? super PastContentHandle[], Exception> command) {
    lookupHandles(id, 0, num, command);
  }

  public void lookupHandles(final Id id, final long version, int num, final Continuation<? super PastContentHandle[], Exception> command) {
    logger.info("lookupHandles({}v{}, n={})", id, version, num);

    retriever.retrieveManifest(new VersionKey(id, version), GlacierTags.LOOKUP_HANDLES, new Continuation<Object, Exception>() {
      boolean haveAnswered = false;
      public void receiveResult(Object o) {
        if (haveAnswered) {
          logger.debug("lookupHandles({}): received manifest {} but has already answered. Discarding...", id, o);
          return;
        }
        if (o instanceof Manifest) {
          logger.debug("lookupHandles({}): received manifest {}, returning handle...", id, o);
          haveAnswered = true;
          command.receiveResult(new PastContentHandle[] {
            new GlacierContentHandle(id, version, getLocalNodeHandle(), (Manifest) o)
          });
        } else {
          logger.warn("lookupHandles({}): Cannot retrieve manifest", id);
          haveAnswered = true;
          command.receiveResult(new PastContentHandle[] { null });
        }
      }
      public void receiveException(Exception e) {
        logger.warn("lookupHandles({}): Exception ", id, e);
        haveAnswered = true;
        command.receiveException(e);
      }
    });
  }

  public void lookup(Id id, long version, Continuation<? super PastContent, Exception> command) {
    VersionKey vkey = new VersionKey(id, version);
    logger.info("lookup({}v{})", id, version);
    retriever.retrieveObject(vkey, null, true, GlacierTags.LOOKUP, command);
  }

  public void lookup(Id id, boolean cache, Continuation<? super PastContent, Exception> command) {
    lookup(id, 0, command);
  }

  public void lookup(Id id, Continuation<? super PastContent, Exception> command) {
    lookup(id, 0, command);
  }

  public void fetch(PastContentHandle handle, Continuation<? super PastContent, Exception> command) {
    logger.info("fetch({})", handle.getId());

    if (!(handle instanceof GlacierContentHandle)) {
      command.receiveException(new GlacierException("Unknown handle type"));
      return;
    }

    GlacierContentHandle gch = (GlacierContentHandle) handle;
    logger.debug("exact: fetch({}v{})", gch.getId(), gch.getVersion());

    retriever.retrieveObject(new VersionKey(gch.getId(), gch.getVersion()), gch.getManifest(), true, GlacierTags.FETCH, command);
  }

  public void deliver(Id id, Message message) {

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
      return;
    } else {
      panic("GLACIER ERROR - Received message " + msg + " of unknown type.");
    }
  }

  public int getReplicationFactor() {
    return 1;
  }

  public NodeHandle getLocalNodeHandle() {
    return endpoint.getLocalNodeHandle();
  }

  public void setSyncInterval(int syncIntervalSec) {
    syncProtocol.setSyncInterval(syncIntervalSec * GlacierConfig.SECONDS);
  }

  public void setSyncMaxFragments(int syncMaxFragments) {
    syncProtocol.setSyncMaxFragments(syncMaxFragments);
  }

  public void setRateLimit(int rps) {
    trafficShaper.setRateLimit(rps);
  }

  public void setNeighborTimeout(long neighborTimeoutMin) {
    neighborhood.setNeighborTimeout(neighborTimeoutMin * GlacierConfig.MINUTES);
  }

  public void setBandwidthLimit(long bytesPerSecond, long maxBurst) {
    trafficShaper.setBandwidthLimit(bytesPerSecond, maxBurst);
  }

  public long getTrashSize() {
    return trash.getTrashSize();
  }

  public void emptyTrash(final Continuation<? super Boolean, Exception> c) {
    trash.emptyTrash(c);
  }

  public void addStatisticsListener(GlacierStatisticsListener gsl) {
    statisticsCollector.addListener(gsl);
  }

  public void removeStatisticsListener(GlacierStatisticsListener gsl) {
    statisticsCollector.removeListener(gsl);
  }

  public Environment getEnvironment() {
    return environment;
  }

  public String getInstance() {
    return instance;
  }

  public void setContentDeserializer(PastContentDeserializer deserializer) {
    retriever.setContentDeserializer(deserializer);
  }

  public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer) {
    contentHandleDeserializer = deserializer;
  }


}
