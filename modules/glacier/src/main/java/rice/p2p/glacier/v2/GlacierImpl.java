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
import java.util.Objects;

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
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.ExpiringPast;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.rawserialization.PastContentHandleDeserializer;
import rice.p2p.util.DebugCommandHandler;
import rice.persistence.CachedStorage;

/**
 * The Glacier application: implements the PAST interfaces on top of erasure-coded fragments, and wires
 * together the collaborators that do the work.
 *
 * <p>The ids that applications insert, look up and refresh are mapped to storage ids by
 * {@link GlacierPolicy#storageId(Id)}: under {@link GlacierDefaultPolicy}, each owner has its own namespace,
 * and a node finds and refreshes only the objects it inserted with the same key pair.
 */
public class GlacierImpl implements Glacier, ExpiringPast, VersioningPast, Application, DebugCommandHandler {
  private final Environment environment;
  private final Logger logger;
  private final String instance;
  private final Endpoint endpoint;
  private final GlacierScheduler scheduler;
  private final GlacierTrash trash;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierNeighborhood neighborhood;
  private final GlacierStatisticsCollector statisticsCollector;
  private final GlacierRetriever retriever;
  private final GlacierInserter inserter;
  private final GlacierSyncProtocol syncProtocol;
  private final GlacierHandoffProtocol handoffProtocol;
  private final GlacierNeighborProtocol neighborProtocol;
  private final GlacierMaintenance maintenance;
  private final GlacierMessageDispatcher dispatcher;
  private final GlacierDebugCommands debugCommands;
  private final GlacierPolicy policy;

  public GlacierImpl(Node node, CachedStorage fragmentStorage, CachedStorage neighborStorage, int numFragments, int numSurvivors, IdFactory factory, String instance, GlacierPolicy policy) {
    this.environment = node.getEnvironment();
    this.logger = environment.getLogManager().getLogger(GlacierImpl.class, instance);
    this.instance = instance;
    this.policy = Objects.requireNonNull(policy, "Glacier needs a policy; GlacierDefaultPolicy signs manifests");

    GlacierConfig config = GlacierConfig.fromParameters(environment.getParameters());

    this.trash = new GlacierTrash(fragmentStorage, logger);
    this.trafficShaper = new GlacierTrafficShaper(config, numFragments, environment, logger);
    this.endpoint = node.buildEndpoint(this, instance);
    this.scheduler = new GlacierScheduler(endpoint, environment, logger);
    this.endpoint.setDeserializer(GlacierMessageDispatcher.deserializer(endpoint));

    FragmentLocator locator = new FragmentLocator(factory, numFragments);
    this.neighborhood = new GlacierNeighborhood(neighborStorage, factory, endpoint.getLocalNodeHandle().getId(), config.neighborTimeout(), environment, logger);
    this.statisticsCollector = new GlacierStatisticsCollector(config, scheduler, trafficShaper, neighborhood, trash, fragmentStorage, neighborStorage, instance, environment, logger);
    GlacierRouter router = new GlacierRouter(endpoint, statisticsCollector, logger);
    this.retriever = new GlacierRetriever(config, numFragments, numSurvivors, instance, policy, endpoint, scheduler, router, locator, trafficShaper, trash, environment, logger);
    GlacierBulkRefresh bulkRefresh = new GlacierBulkRefresh(config, numFragments, numSurvivors, policy, scheduler, router, locator, neighborhood, fragmentStorage, factory, environment, logger);
    this.inserter = new GlacierInserter(config, numFragments, numSurvivors, policy, endpoint, scheduler, router, locator, trafficShaper, retriever, bulkRefresh, environment, logger);
    this.syncProtocol = new GlacierSyncProtocol(config, numFragments, policy, endpoint, scheduler, router, locator, neighborhood, trafficShaper, fragmentStorage, factory, environment, logger);
    this.handoffProtocol = new GlacierHandoffProtocol(config, instance, policy, router, locator, neighborhood, trash, fragmentStorage, environment, logger);
    this.neighborProtocol = new GlacierNeighborProtocol(config, endpoint, router, neighborhood, neighborStorage, factory, environment, logger);
    this.maintenance = new GlacierMaintenance(config, numFragments, locator, neighborhood, trash, retriever, fragmentStorage, environment, logger);
    GlacierRequestHandler requestHandler = new GlacierRequestHandler(numFragments, numSurvivors, instance, policy, router, locator, neighborhood, retriever, fragmentStorage, environment, logger);
    this.dispatcher = new GlacierMessageDispatcher(scheduler, trafficShaper, requestHandler, neighborProtocol, syncProtocol, bulkRefresh, handoffProtocol, logger);
    this.debugCommands = new GlacierDebugCommands(config, numFragments, numSurvivors, instance, factory, scheduler, router, locator, neighborhood, trafficShaper, syncProtocol, retriever, inserter, fragmentStorage, neighborStorage, environment, logger);

    neighborhood.determineResponsibleRange();
    endpoint.register();
  }

  /**
   * Starts the periodic tasks: neighbor exchange, sync, handoff, garbage collection, local scan, traffic
   * shaping and statistics.
   */
  public void startup() {
    scheduler.addContinuation(neighborProtocol.newNeighborTask());
    scheduler.addContinuation(syncProtocol.newSyncTask());
    scheduler.addContinuation(handoffProtocol.newHandoffTask());
    scheduler.addContinuation(maintenance.newGarbageCollectionTask());
    scheduler.addContinuation(maintenance.newLocalScanTask());
    scheduler.addContinuation(trafficShaper.newShaperTask());
    scheduler.addContinuation(statisticsCollector.newReportingTask());
  }

  @Override
  public String handleDebugCommand(String command) {
    return debugCommands.handleDebugCommand(command);
  }

  @Override
  public void insert(final PastContent obj, final Continuation<? super Boolean[], Exception> command) {
    insert(obj, ExpiringPast.INFINITY_EXPIRATION, command);
  }

  @Override
  public void insert(final PastContent obj, final long expiration, final Continuation<? super Boolean[], Exception> command) {
    inserter.insert(obj, expiration, command);
  }

  @Override
  public void refresh(Id[] ids, long[] expirations, Continuation<? super Object[], Exception> command) {
    long[] versions = new long[ids.length];
    Arrays.fill(versions, 0);
    refresh(ids, versions, expirations, command);
  }

  @Override
  public void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    Id[] storageIds = new Id[ids.length];
    for (int i = 0; i < ids.length; i++)
      storageIds[i] = policy.storageId(ids[i]);
    inserter.refresh(storageIds, versions, expirations, command);
  }

  @Override
  public boolean forward(final RouteMessage message) {
    return true;
  }

  @Override
  public void update(NodeHandle handle, boolean joined) {
    logger.atInfo().log(() -> "Leafset update: " + handle + " has " + (joined ? "joined" : "left"));

    if (!joined)
      return;

    neighborhood.neighborSeen(handle.getId(), environment.getTimeSource().currentTimeMillis());
  }

  @Override
  public void deliver(Id id, Message message) {
    dispatcher.deliver(id, message);
  }

  @Override
  public void lookupHandle(Id id, NodeHandle handle, Continuation<? super PastContentHandle, Exception> command) {
    command.receiveException(new UnsupportedOperationException("LookupHandle() is not supported on Glacier"));
  }

  @Override
  public void lookupHandles(Id id, int num, Continuation<? super PastContentHandle[], Exception> command) {
    lookupHandles(id, 0, num, command);
  }

  @Override
  public void lookupHandles(final Id id, final long version, int num, final Continuation<? super PastContentHandle[], Exception> command) {
    logger.info("lookupHandles({}v{}, n={})", id, version, num);

    retriever.retrieveManifest(new VersionKey(policy.storageId(id), version), GlacierTags.LOOKUP_HANDLES, new Continuation<Object, Exception>() {
      boolean haveAnswered = false;
      @Override
      public void receiveResult(Object o) {
        if (haveAnswered) {
          logger.debug("lookupHandles({}): received manifest {} but has already answered. Discarding...", id, o);
          return;
        }
        if (o instanceof Manifest manifest) {
          logger.debug("lookupHandles({}): received manifest {}, returning handle...", id, o);
          haveAnswered = true;
          command.receiveResult(new PastContentHandle[] {
            new GlacierContentHandle(id, version, getLocalNodeHandle(), manifest)
          });
        } else {
          logger.warn("lookupHandles({}): Cannot retrieve manifest", id);
          haveAnswered = true;
          command.receiveResult(new PastContentHandle[] { null });
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("lookupHandles({}): Exception ", id, e);
        haveAnswered = true;
        command.receiveException(e);
      }
    });
  }

  @Override
  public void lookup(Id id, long version, Continuation<? super PastContent, Exception> command) {
    VersionKey vkey = new VersionKey(policy.storageId(id), version);
    logger.info("lookup({}v{})", id, version);
    retriever.retrieveObject(vkey, null, true, GlacierTags.LOOKUP, command);
  }

  @Override
  public void lookup(Id id, boolean cache, Continuation<? super PastContent, Exception> command) {
    lookup(id, 0, command);
  }

  @Override
  public void lookup(Id id, Continuation<? super PastContent, Exception> command) {
    lookup(id, 0, command);
  }

  @Override
  public void fetch(PastContentHandle handle, Continuation<? super PastContent, Exception> command) {
    logger.info("fetch({})", handle.getId());

    if (!(handle instanceof GlacierContentHandle gch)) {
      command.receiveException(new GlacierException("Unknown handle type"));
      return;
    }

    logger.debug("exact: fetch({}v{})", gch.getId(), gch.getVersion());

    retriever.retrieveObject(new VersionKey(policy.storageId(gch.getId()), gch.getVersion()), gch.manifest(), true, GlacierTags.FETCH, command);
  }

  @Override
  public int getReplicationFactor() {
    return 1;
  }

  @Override
  public NodeHandle getLocalNodeHandle() {
    return endpoint.getLocalNodeHandle();
  }

  public void setSyncMaxFragments(int syncMaxFragments) {
    syncProtocol.setSyncMaxFragments(syncMaxFragments);
  }

  public void setRateLimit(int rps) {
    trafficShaper.setRateLimit(rps);
  }

  public long getTrashSize() {
    return trash.getTrashSize();
  }

  public void addStatisticsListener(GlacierStatisticsListener gsl) {
    statisticsCollector.addListener(gsl);
  }

  public void removeStatisticsListener(GlacierStatisticsListener gsl) {
    statisticsCollector.removeListener(gsl);
  }

  public String getInstance() {
    return instance;
  }

  @Override
  public void setContentDeserializer(PastContentDeserializer deserializer) {
    retriever.setContentDeserializer(deserializer);
  }

  @Override
  public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer) {
  }
}
