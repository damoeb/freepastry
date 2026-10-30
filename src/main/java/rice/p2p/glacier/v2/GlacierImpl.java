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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.FragmentKeySet;
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
    neighborhood.determineResponsibleRange();
    endpoint.register();
  }

  public void startup() {

    /* Neighbor requests */

    scheduler.addContinuation(new GlacierContinuation() {
      long nextTimeout;

      public String toString() {
        return "Neighbor continuation";
      }
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.expireNeighborsDelayAfterJoin();

        NodeHandleSet leafSet = endpoint.neighborSet(999);
        NodeHandle localHandle = getLocalNodeHandle();
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
          if (!leafSet.getHandle(k).getId().equals(getLocalNodeHandle().getId())) {
            neighborhood.neighborSeen(leafSet.getHandle(k).getId(), environment.getTimeSource().currentTimeMillis());
            if (logger.isInfoEnabled()) logger.info("Asking "+leafSet.getHandle(k).getId()+" about neighbors in "+leafRange);
            router.sendMessage(
              null,
              new GlacierNeighborRequestMessage(getMyUID(), leafRange, getLocalNodeHandle(), leafSet.getHandle(k).getId(), GlacierTags.NEIGHBOR),
              leafSet.getHandle(k)
            );
          }
        }
      }
      public void receiveResult(Object o) {
        if (o instanceof GlacierNeighborResponseMessage) {
          final GlacierNeighborResponseMessage gnrm = (GlacierNeighborResponseMessage) o;
          logger.atDebug().log(() -> "NeighborResponse from "+gnrm.getSource()+" with "+gnrm.numNeighbors()+" neighbors");
          for (int i=0; i<gnrm.numNeighbors(); i++)
            neighborhood.neighborSeen(gnrm.getNeighbor(i), gnrm.getLastSeen(i));
        } else {
          logger.warn("Unknown response in neighbor continuation: {} -- discarded", o);
        }
      }
      public void receiveException(Exception e) {
        logger.warn("Exception in neighbor continuation: ", e);
        terminate();
      }
      public void timeoutExpired() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.expireNeighborsInterval();

        neighborhood.expireNeighbors(endpoint.neighborSet(999));
      }
      public long getTimeout() {
        return nextTimeout;
      }
    });

    /* Sync */

    scheduler.addContinuation(syncProtocol.newSyncTask());

    /* Handoff */

    scheduler.addContinuation(handoffProtocol.newHandoffTask());

    /* Garbage collection */

    scheduler.addContinuation(new GlacierContinuation() {
      long nextTimeout;

      public String toString() {
        return "Garbage collector";
      }
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.garbageCollectionInterval();
      }
      public void receiveResult(Object o) {
        logger.warn("GC received object: {}", o);
      }
      public void receiveException(Exception e) {
        logger.warn("GC received exception: ", e);
      }
      public long getTimeout() {
        return nextTimeout;
      }
      public void timeoutExpired() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.garbageCollectionInterval();

        final long now = environment.getTimeSource().currentTimeMillis();
        IdSet fragments = fragmentStorage.scan();
        int doneSoFar = 0, candidates = 0;

        logger.atInfo().log(() -> "Garbage collection started at "+now+", scanning "+fragments.numElements()+" fragment(s)...");
        Iterator<Id> iter = fragments.getIterator();
        while (iter.hasNext()) {
          final Id thisKey = (Id) iter.next();
          final FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(thisKey);
          if (metadata != null) {
            if (metadata.getCurrentExpiration() < now) {
              candidates ++;
              if (doneSoFar < config.garbageCollectionMaxFragmentsPerRun()) {
                doneSoFar ++;
                trash.deleteFragment(thisKey, new Continuation<Object, Exception>() {
                  public void receiveResult(Object o) {
                    logger.atInfo().log(() -> "GC collected "+thisKey.toStringFull()+", expired "+(now-metadata.getCurrentExpiration())+" msec ago");
                  }
                  public void receiveException(Exception e) {
                    logger.atDebug().log(() -> "GC cannot collect "+thisKey.toStringFull());
                  }
                });
              }
            }
          } else {
            logger.atWarn().log(() -> "GC cannot read metadata in object "+thisKey.toStringFull()+", storage returned null");
          }
        }

        logger.atInfo().log(() -> "Garbage collection completed at "+environment.getTimeSource().currentTimeMillis());
        logger.info("Found {} candidate(s), collected {}", candidates, doneSoFar);
      }
    });

    /* Local scan */

    scheduler.addContinuation(new GlacierContinuation() {
      long nextTimeout;

      public String toString() {
        return "Local scan";
      }
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.localScanInterval();
      }
      public void receiveResult(Object o) {
        logger.warn("Local scan received object: {}", o);
      }
      public void receiveException(Exception e) {
        logger.warn("Local scan received exception: ", e);
      }
      public long getTimeout() {
        return nextTimeout;
      }
      public void timeoutExpired() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.jitterTerm(config.localScanInterval(), environment.getRandomSource());

        final IdSet fragments = fragmentStorage.scan();
        final long now = environment.getTimeSource().currentTimeMillis();
        java.util.TreeSet<VersionKey> queries = new java.util.TreeSet<VersionKey>();

        logger.atInfo().log(() -> "Performing local scan over "+fragments.numElements()+" fragment(s)...");
        Iterator<? extends Id> iter = fragments.getIterator();
        while (iter.hasNext()) {
          final FragmentKey thisKey = (FragmentKey) iter.next();
          FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(thisKey);
          if ((metadata != null) && (metadata.getCurrentExpiration() >= now)) {
            final Id thisObjectKey = thisKey.getVersionKey().getId();
            final long thisVersion = thisKey.getVersionKey().getVersion();
            final int thisFragmentID = thisKey.getFragmentID();
            final int fidLeft = (thisFragmentID + numFragments - 1) % numFragments;
            final int fidRight = (thisFragmentID + 1) % numFragments;

            if (neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisObjectKey, fidLeft, thisVersion))) {
              if (!fragments.isMemberId(thisKey.getPeerKey(fidLeft))) {
                logger.trace("Missing: {} L={}", thisKey, fidLeft);
                queries.add(thisKey.getVersionKey());
              }
            }

            if (neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisObjectKey, fidRight, thisVersion))) {
              if (!fragments.isMemberId(thisKey.getPeerKey(fidRight))) {
                logger.trace("Missing: {} R={}", thisKey, fidRight);
                queries.add(thisKey.getVersionKey());
              }
            }
          } else {
            logger.trace("Expired, ignoring in local scan: {}", thisKey);
          }
        }

        if (!queries.isEmpty()) {
          logger.info("Local scan completed; {} objects incomplete in local store", queries.size());
          iter = queries.iterator();
          int queriesSent = 0;

          while (iter.hasNext() && (queriesSent < config.localScanMaxFragmentsPerRun())) {
            final VersionKey thisVKey = (VersionKey) iter.next();

            int localFragmentID = 0;
            int queriesHere = 0;
            for (int i=0; i<numFragments; i++) {
              FragmentKey keyHere = new FragmentKey(thisVKey, i);
              if (fragments.isMemberId(keyHere)) {
                localFragmentID = i;
                break;
              } else if (neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(keyHere))) {
                queriesHere ++;
              }
            }

            logger.debug("Local scan: Fetching manifest for {} ({} pending queries)", thisVKey, queriesHere);
            queriesSent += queriesHere;

            fragmentStorage.getObject(new FragmentKey(thisVKey, localFragmentID), new Continuation<Object, Exception>() {
              public void receiveResult(Object o) {
                if (o instanceof FragmentAndManifest) {
                  final Manifest thisManifest = ((FragmentAndManifest)o).getManifest();

                  for (int i=0; i<numFragments; i++) {
                    final FragmentKey thisKey = new FragmentKey(thisVKey, i);
                    if (neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey))) {
                      if (!fragments.isMemberId(thisKey)) {
                        logger.debug("Local scan: Sending query for {}", thisKey);
                        final long tStart = environment.getTimeSource().currentTimeMillis();
                        retriever.rateLimitedRetrieveFragment(thisKey, thisManifest, GlacierTags.LOCAL_SCAN, new GlacierContinuation() {
                          public String toString() {
                            return "Local scan: Fetch fragment: "+thisKey;
                          }
                          public void receiveResult(Object o) {
                            if (o instanceof Fragment) {
                              logger.info("Local scan: Received fragment {} (from primary) matches existing manifest, storing...", thisKey);

                              FragmentAndManifest fam = new FragmentAndManifest((Fragment) o, thisManifest);

                              fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), 0, environment.getTimeSource().currentTimeMillis()), fam,
                                new Continuation<Object, Exception>() {
                                  public void receiveResult(Object o) {
                                    logger.debug("Local scan: Recovered fragment stored OK");
                                  }
                                  public void receiveException(Exception e) {
                                    logger.warn("Local scan: receiveException({}) while storing a fragment with existing manifest (key={})", e, thisKey);
                                  }
                                }
                              );
                            } else {
                              logger.warn("Local scan: FS received something other than a fragment: {}", o);
                            }
                          }
                          public void receiveException(Exception e) {
                            logger.warn("Local scan: Exception while recovering synced fragment {}: ", thisKey, e);
                            terminate();
                          }
                          public void timeoutExpired() {
                            logger.warn("Local scan: Timeout while fetching synced fragment {} -- aborted", thisKey);
                            terminate();
                          }
                          public long getTimeout() {
                            return tStart + config.overallRestoreTimeout();
                          }
                        });
                      }
                    }
                  }
                } else {
                  logger.warn("Local scan: Cannot retrieve {} from local store, received o={}", thisVKey, o);
                }
              }
              public void receiveException(Exception e) {
                logger.warn("Local scan: Cannot retrieve {} from local store, exception e=", thisVKey, e);
              }
            });
          }

          logger.info("{} queries sent after local scan", queriesSent);
        } else {
          logger.info("Local scan completed; no missing fragments");
        }
      }
    });

    /* Traffic shaper */

    scheduler.addContinuation(trafficShaper.newShaperTask());

    /* Statistics */

    scheduler.addContinuation(statisticsCollector.newReportingTask());
  }

  public void setTrashcan(StorageManager trashStorage) {
    trash.setTrashcan(trashStorage);
  }

  private static String dump(byte[] data, boolean linebreak) {
    final String hex = "0123456789ABCDEF";
    String result = "";

    for (int i=0; i<data.length; i++) {
      int d = data[i];
      if (d<0)
        d+= 256;
      int hi = (d>>4);
      int lo = (d&15);

      result = result + hex.charAt(hi) + hex.charAt(lo);
      if (linebreak && (((i%16)==15) || (i==(data.length-1))))
        result = result + "\n";
      else if (i!=(data.length-1))
        result = result + " ";
    }

    return result;
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

  /**
   * How long a debug command waits for its asynchronous result.
   */
  private static final long DEBUG_COMMAND_TIMEOUT = 2 * 60 * 1000;

  private String awaitDebugResult(CompletableFuture<String> result, long timeoutMillis) {
    return DebugCommandHandler.await(result, environment.getSelectorManager(), timeoutMillis);
  }

  public String handleDebugCommand(String command)
  {
    if (command.indexOf(" ") < 0)
      return null;

    String myInstance = "glacier."+instance.substring(instance.lastIndexOf("-") + 1);
    String requestedInstance = command.substring(0, command.indexOf(" "));
    String cmd = command.substring(requestedInstance.length() + 1);

    if (!requestedInstance.equals(myInstance) && !requestedInstance.equals("g"))
      return null;

    logger.info("Debug command: {}", cmd);

    if (cmd.startsWith("ls")) {
      FragmentKeySet keyset = (FragmentKeySet) fragmentStorage.scan();
      Iterator<Id> iter = keyset.getIterator();
      StringBuilder result = new StringBuilder();

      long now = environment.getTimeSource().currentTimeMillis();
      if (cmd.indexOf("-r") < 0)
        now = 0;

      result.append(keyset.numElements()+ " fragment(s)\n");

      while (iter.hasNext()) {
        FragmentKey thisKey = (FragmentKey) iter.next();
        boolean isMine = neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey));
        FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(thisKey);
        if (metadata != null) {
          result.append(((Id)thisKey).toStringFull()+" "+(isMine ? "OK" : "MI")+" "+
              (metadata.getCurrentExpiration()-now)+" "+(metadata.getPreviousExpiration()-now)+"\n");
        }
      }

      return result.toString();
    }

    if (cmd.startsWith("show config")) {
      return
        "numFragments = " + numFragments + "\n" +
        "numSurvivors = " + numSurvivors + "\n" +
        "config.insertTimeout() = " + (int)(config.insertTimeout() / GlacierConfig.SECONDS) + " sec\n" +
        "config.minFragmentsAfterInsert() = "+ config.minFragmentsAfterInsert() + "x" + numSurvivors + "\n" +
        "config.refreshTimeout() = " + (int)(config.refreshTimeout() / GlacierConfig.SECONDS) + " sec\n" +
        "config.expireNeighborsDelayAfterJoin() = " + (int)(config.expireNeighborsDelayAfterJoin() / GlacierConfig.SECONDS) + " sec\n" +
        "config.expireNeighborsInterval() = " + (int)(config.expireNeighborsInterval() / GlacierConfig.MINUTES) + " min\n" +
        "neighborTimeout = " + (int)(neighborhood.getNeighborTimeout() / GlacierConfig.HOURS) + " hrs\n" +
        "config.syncDelayAfterJoin() = " + (int)(config.syncDelayAfterJoin() / GlacierConfig.SECONDS) + " sec\n" +
        "config.syncMinRemainingLifetime() = " + (int)(config.syncMinRemainingLifetime() / GlacierConfig.SECONDS) + " sec\n" +
        "config.syncMinQuietTime() = " + (int)(config.syncMinQuietTime() / GlacierConfig.SECONDS) + " sec\n" +
        "syncBloomFilter = " + config.syncBloomFilterNumHashes() + " hashes, " + config.syncBloomFilterBitsPerKey() + " bpk\n" +
        "config.syncPartnersPerTrial() = " + config.syncPartnersPerTrial() + "\n" +
        "syncInterval = " + (int)(syncProtocol.getSyncInterval() / GlacierConfig.MINUTES) + " min\n" +
        "syncMaxFragments = " + syncProtocol.getSyncMaxFragments() + "\n" +
        "config.fragmentRequestMaxAttempts() = " + config.fragmentRequestMaxAttempts() + "\n" +
        "config.fragmentRequestTimeoutDefault() = " + (int)(config.fragmentRequestTimeoutDefault() / GlacierConfig.SECONDS) + " sec\n" +
        "config.manifestRequestTimeout() = " + (int)(config.manifestRequestTimeout() / GlacierConfig.SECONDS) + " sec\n" +
        "manifestBurst = " + config.manifestRequestInitialBurst() + " -> " + config.manifestRequestRetryBurst() + "\n" +
        "config.manifestAggregationFactor() = " + config.manifestAggregationFactor() + "\n" +
        "config.overallRestoreTimeout() = " + (int)(config.overallRestoreTimeout() / GlacierConfig.SECONDS) + " sec\n" +
        "config.handoffDelayAfterJoin() = " + (int)(config.handoffDelayAfterJoin() / GlacierConfig.SECONDS) + " sec\n" +
        "config.handoffInterval() = " + (int)(config.handoffInterval() / GlacierConfig.SECONDS) + " sec\n" +
        "config.handoffMaxFragments() = " + config.handoffMaxFragments() + "\n" +
        "config.garbageCollectionInterval() = " + (int)(config.garbageCollectionInterval() / GlacierConfig.MINUTES) + " min\n" +
        "config.garbageCollectionMaxFragmentsPerRun() = " + config.garbageCollectionMaxFragmentsPerRun() + "\n" +
        "config.localScanInterval() = " + (int)(config.localScanInterval() / GlacierConfig.MINUTES) + " min\n" +
        "config.localScanMaxFragmentsPerRun() = " + config.localScanMaxFragmentsPerRun() + "\n" +
        "config.restoreMaxRequestFactor() = " + config.restoreMaxRequestFactor() + "\n" +
        "config.restoreMaxBoosts() = " + config.restoreMaxBoosts() + "\n" +
        "config.rateLimitedCheckInterval() = " + (int)(config.rateLimitedCheckInterval() / GlacierConfig.SECONDS) + " sec\n" +
        "rateLimitedRequestsPerSecond = " + trafficShaper.getRateLimit() + "\n";
    }

    if (cmd.startsWith("flush") && config.faultInjectionEnabled()) {
      FragmentKeySet keyset = (FragmentKeySet) fragmentStorage.scan();
      Iterator<Id> iter = keyset.getIterator();

      while (iter.hasNext()) {
        FragmentKey thisKey = (FragmentKey) iter.next();
        fragmentStorage.unstore(thisKey, new Continuation<Object, Exception>() {
          public void receiveResult(Object o) {}
          public void receiveException(Exception e) {}
        });
      }

      return keyset.numElements()+ " objects deleted\n";
    }

    if (cmd.startsWith("refresh")) {
      String args = cmd.substring(8);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String keyArg = args.substring(0, args.lastIndexOf(' '));

      Id id = factory.buildIdFromToString(keyArg);
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(expirationArg);

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      refresh(new Id[] { id }, expiration, new Continuation<Object, Exception>() {
        public void receiveResult(Object o) {
          ret.complete("result("+o+")");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return "refresh("+id+", "+expiration+")="+awaitDebugResult(ret, DEBUG_COMMAND_TIMEOUT);
    }

    if (cmd.startsWith("neighbors")) {
      final Iterator<Id> iter = neighborStorage.scan().getIterator();
      final StringBuffer result = new StringBuffer();
      final long now = (cmd.indexOf("-r") < 0) ? 0 : environment.getTimeSource().currentTimeMillis();
      final CompletableFuture<String> ret = new CompletableFuture<String>();

      result.append(neighborStorage.scan().numElements()+ " neighbor(s)\n");

      Continuation<Object, Exception> c = new Continuation<Object, Exception>() {
        Id currentLookup;
        public void receiveResult(Object o) {
          if (o != null)
            result.append(currentLookup.toStringFull() + " " + (((Long)o).longValue() - now) + "\n");

          if (iter.hasNext()) {
            currentLookup = (Id) iter.next();
            neighborStorage.getObject(currentLookup, this);
          } else {
            ret.complete("OK");
          }
        }
        public void receiveException(Exception e) {
          ret.complete("Exception: "+e);
        }
      };

      c.receiveResult(null);
      String outcome = awaitDebugResult(ret, DEBUG_COMMAND_TIMEOUT);
      // the StringBuffer is synchronized, and complete() happens-after the last append
      result.append(outcome+"\n");
      return result.toString();
    }

    if (cmd.startsWith("status")) {
      String result = "";
      result = result + "Responsible for: "+neighborhood.getResponsibleRange() + "\n";
      result = result + "Local time: "+(new Date()) + "\n\n";
      result = result + fragmentStorage.scan().numElements() + " fragments\n";
      result = result + neighborStorage.scan().numElements() + " neighbors\n";
      result = result + scheduler.size() + " active continuations\n";
      result = result + trafficShaper.numPending() + " pending requests\n";

      return result;
    }

    if (cmd.startsWith("insert") && config.faultInjectionEnabled()) {
      String args = cmd.substring(7);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String numObjectsArg = args.substring(0, args.lastIndexOf(' '));

      final int numObjects = Integer.parseInt(numObjectsArg);
      final int lifetime = Integer.parseInt(expirationArg);
      String result = "";

      for (int i=0; i<numObjects; i++) {
        final Id randomID = factory.buildRandomId(environment.getRandomSource());
        result = result + randomID.toStringFull() + "\n";
        trafficShaper.enqueue(new VersionKey(randomID, 0), new Continuation.SimpleContinuation() {
          public void receiveResult(Object o) {
            insert(
              new DebugContent(randomID, false, 0, new byte[] {}),
              environment.getTimeSource().currentTimeMillis() + lifetime,
              new Continuation<Object, Exception>() {
                public void receiveResult(Object o) {
                }
                public void receiveException(Exception e) {
                }
              });
          }
        });
      }

      return result + numObjects + " object(s) with lifetime "+lifetime+"ms created\n";
    }

    if (cmd.startsWith("delete") && config.faultInjectionEnabled()) {
      String[] vkeyS = cmd.substring(7).split("[v#]");
      Id key = factory.buildIdFromToString(vkeyS[0]);
      long version = Long.parseLong(vkeyS[1]);
      VersionKey vkey = new VersionKey(key, version);
      FragmentKey id = new FragmentKey(vkey, Integer.parseInt(vkeyS[2]));

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      fragmentStorage.unstore(id, new Continuation<Object, Exception>() {
        public void receiveResult(Object o) {
          ret.complete("result("+o+")");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return "delete("+id+")="+awaitDebugResult(ret, DEBUG_COMMAND_TIMEOUT);
    }

    if (cmd.startsWith("burst") && config.faultInjectionEnabled()) {
      String[] vkeyS = cmd.substring(6).split("[v#]");
      Id key = factory.buildIdFromToString(vkeyS[0]);
      long version = Long.parseLong(vkeyS[1]);
      VersionKey vkey = new VersionKey(key, version);
      final FragmentKey id = new FragmentKey(vkey, Integer.parseInt(vkeyS[2]));
      final Id fragmentLoc = locator.getFragmentLocation(id);

      final String[] ret = new String[] { "" }; // only touched by the selector thread, until done completes
      final CompletableFuture<String> done = new CompletableFuture<String>();
      final long now = environment.getTimeSource().currentTimeMillis();
      scheduler.addContinuation(new GlacierContinuation() {
        int receivedSoFar = 0;
        final int total = 100;
        public String toString() {
          return "Burst continuation";
        }
        public void init() {
          for (int i=0; i<total; i++) {
            router.sendMessage(
              fragmentLoc,
              new GlacierQueryMessage(getMyUID(), new FragmentKey[] { id }, getLocalNodeHandle(), fragmentLoc, GlacierTags.DEBUG),
              null
            );
          }
        }
        public void receiveResult(Object o) {
          if (o instanceof GlacierResponseMessage) {
            ret[0] = ret[0] + (environment.getTimeSource().currentTimeMillis() - now) + " msec ("+((GlacierResponseMessage)o).getSource().getId()+")\n";
            if ((++receivedSoFar) == total)
              timeoutExpired();
          }
        }
        public void receiveException(Exception e) {
        }
        public void timeoutExpired() {
          done.complete(ret[0]);
          terminate();
        }
        public long getTimeout() {
          return now + 120 * GlacierConfig.SECONDS;
        }
      });

      return "burst("+id+")="+awaitDebugResult(done, 130 * GlacierConfig.SECONDS);
    }

    if (cmd.startsWith("manifest")) {
      String[] vkeyS = cmd.substring(9).split("v");
      Id key = factory.buildIdFromToString(vkeyS[0]);
      long version = Long.parseLong(vkeyS[1]);
      VersionKey vkey = new VersionKey(key, version);

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      retriever.retrieveManifest(vkey, GlacierTags.DEBUG, new Continuation<Object, Exception>() {
        public void receiveResult(Object o) {
          if (o instanceof Manifest)
            ret.complete(((Manifest)o).toStringFull());
          else
            ret.complete("result("+o+")");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return "manifest("+vkey+")="+awaitDebugResult(ret, DEBUG_COMMAND_TIMEOUT);
    }

    if (cmd.startsWith("retrieve")) {
      String[] vkeyS = cmd.substring(9).split("[v#]");
      Id key = factory.buildIdFromToString(vkeyS[0]);
      long version = Long.parseLong(vkeyS[1]);
      VersionKey vkey = new VersionKey(key, version);
      final FragmentKey id = new FragmentKey(vkey, Integer.parseInt(vkeyS[2]));
      final FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(id);

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      fragmentStorage.getObject(id, new Continuation<Object, Exception>() {
        public void receiveResult(Object o) {
          FragmentAndManifest fam = (FragmentAndManifest) o;
          MessageDigest md = null;
          try {
            md = MessageDigest.getInstance("SHA");
          } catch (NoSuchAlgorithmException e) {
            logger.error("SHA digest unavailable", e);
            ret.complete("exception(" + e + ")");
            return;
          }

          md.reset();
          md.update(fam.getFragment().getPayload());

          ret.complete("OK\n\nFragment: "+fam.getFragment().getPayload().length+" bytes, Hash=["+dump(md.digest(), false)+"], ID="+id.getFragmentID()+"\n\nValidation: " +
                   (fam.getManifest().validatesFragment(fam.getFragment(), id.getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance)) ? "OK" : "FAIL") + "\n\n" +
                   fam.getManifest().toStringFull()+"\n\nMetadata:\n - Stored since: "+metadata.getStoredSince()+
                   "\n - Current expiration: "+metadata.getCurrentExpiration()+"\n - Previous expiration: "+metadata.getPreviousExpiration()+"\n");
        }
        public void receiveException(Exception e) {
          ret.complete("exception("+e+")");
        }
      });

      return "retrieve("+id+")="+awaitDebugResult(ret, DEBUG_COMMAND_TIMEOUT);
    }

    if (cmd.startsWith("validate")) {
      FragmentKeySet keyset = (FragmentKeySet) fragmentStorage.scan();
      final Iterator<Id> iter = keyset.getIterator();
      final StringBuffer result = new StringBuffer();

      result.append(keyset.numElements()+ " fragment(s)\n");

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      if (iter.hasNext()) {
        final FragmentKey thisKey = (FragmentKey) iter.next();
        fragmentStorage.getObject(thisKey, new Continuation<Object, Exception>() {
          FragmentKey currentKey = thisKey;
          int totalChecks = 1, totalFailures = 0;
          public void receiveResult(Object o) {
            FragmentAndManifest fam = (FragmentAndManifest) o;
            boolean success = fam.getManifest().validatesFragment(fam.getFragment(), currentKey.getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance));
            if (!success)
              totalFailures ++;
            result.append(currentKey.toStringFull()+" "+ (success ? "OK" : "FAIL") + "\n");
            advance();
          }
          public void receiveException(Exception e) {
            totalFailures ++;
            result.append(currentKey.toStringFull()+" EXC: "+e+"\n");
            advance();
          }
          public void advance() {
            if (iter.hasNext()) {
              currentKey = (FragmentKey) iter.next();
              totalChecks ++;
              fragmentStorage.getObject(currentKey, this);
            } else {
              if (totalFailures == 0)
                ret.complete("OK ("+totalChecks+" fragments checked)");
              else
                ret.complete("FAIL, "+totalFailures+"/"+totalChecks+" fragments damaged");
            }
          }
        });

        return "validate="+awaitDebugResult(ret, DEBUG_COMMAND_TIMEOUT)+"\n\n"+result.toString();
      }

      return "validate: no objects\n\n" + result.toString();
    }

    if (cmd.startsWith("fetch")) {
      String[] vkeyS = cmd.substring(6).split("[v#]");
      Id key = factory.buildIdFromToString(vkeyS[0]);
      long version = Long.parseLong(vkeyS[1]);
      VersionKey vkey = new VersionKey(key, version);
      final FragmentKey id = new FragmentKey(vkey, Integer.parseInt(vkeyS[2]));
      final long now = environment.getTimeSource().currentTimeMillis();
      final Id fragmentLoc = locator.getFragmentLocation(id);

      final CompletableFuture<String> ret = new CompletableFuture<String>();
      scheduler.addContinuation(new GlacierContinuation() {
        public String toString() {
          return "DebugFetch continuation";
        }
        public void init() {
          router.sendMessage(
            fragmentLoc,
            new GlacierFetchMessage(getMyUID(), id, GlacierFetchMessage.FETCH_FRAGMENT_AND_MANIFEST, getLocalNodeHandle(), fragmentLoc, GlacierTags.DEBUG),
            null
          );
        }
        public void receiveResult(Object o) {
          if (o instanceof GlacierDataMessage) {
            GlacierDataMessage gdm = (GlacierDataMessage) o;
            MessageDigest md = null;
            try {
              md = MessageDigest.getInstance("SHA");
            } catch (NoSuchAlgorithmException e) {
              logger.error("SHA digest unavailable", e);
              ret.complete("Exception=" + e);
              terminate();
              return;
            }

            md.reset();
            md.update(gdm.getFragment(0).getPayload());

            ret.complete("\n\nResponse: "+gdm.getKey(0).toStringFull()+" ("+gdm.numKeys()+" keys)\n" +  "Holder: "+gdm.getSource()+"\n" +
                     "Fragment: "+gdm.getFragment(0).getPayload().length+" bytes, Hash=["+dump(md.digest(), false)+"]\n\nValidation: " +
                     (gdm.getManifest(0).validatesFragment(gdm.getFragment(0), gdm.getKey(0).getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance)) ? "OK" : "FAIL") + "\n\n" +
                     gdm.getManifest(0).toStringFull());

            terminate();
          } else {
            ret.complete("Received "+o);
            terminate();
          }
        }
        public void receiveException(Exception e) {
          ret.complete("Exception="+e);
          terminate();
        }
        public void timeoutExpired() {
          ret.complete("Timeout");
          terminate();
        }
        public long getTimeout() {
          return now + 5 * GlacierConfig.SECONDS;
        }
      });

      String fetched = awaitDebugResult(ret, 5 * GlacierConfig.SECONDS);
      if (fetched.startsWith("timeout"))
        fetched = "Timeout";

      return "fetch("+id+"@"+fragmentLoc+")="+fetched;
    }

    return null;
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

      /* When a QueryMessage arrives, we check whether we have the fragment
         with the corresponding key and then send back a ResponseMessage. */

      GlacierQueryMessage gqm = (GlacierQueryMessage) msg;
      FragmentKey[] keyA = new FragmentKey[gqm.numKeys()];
      boolean[] haveItA = new boolean[gqm.numKeys()];
      long[] expirationA = new long[gqm.numKeys()];
      boolean[] authoritativeA = new boolean[gqm.numKeys()];

      for (int i=0; i<gqm.numKeys(); i++) {
        Id fragmentLocation = locator.getFragmentLocation(gqm.getKey(i));
        if (logger.isInfoEnabled()) logger.info("Queried for " + gqm.getKey(i) + " (at "+fragmentLocation+")");

        keyA[i] = gqm.getKey(i);
        haveItA[i] = fragmentStorage.exists(gqm.getKey(i));
        if (haveItA[i]) {
          FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(gqm.getKey(i));
          if (metadata != null) {
            expirationA[i] = metadata.getCurrentExpiration();
          } else {
            if (logger.isWarnEnabled()) logger.warn("QUERY cannot read metadata in object "+gqm.getKey(i).toStringFull()+", storage returned null");
            expirationA[i] = 0;
            haveItA[i] = false;
          }
        } else {
          expirationA[i] = 0;
        }
        logger.debug("My range is {}", neighborhood.getResponsibleRange());
        logger.debug("Location is {}", fragmentLocation);
        authoritativeA[i] = neighborhood.getResponsibleRange().containsId(fragmentLocation);
        logger.debug("Result: haveIt={} amAuthority={} expiration={}", haveItA[i], authoritativeA[i], expirationA[i]);
      }

      router.sendMessage(
        null,
        new GlacierResponseMessage(gqm.getUID(), keyA, haveItA, expirationA, authoritativeA, getLocalNodeHandle(), gqm.getSource().getId(), true, gqm.getTag()),
        gqm.getSource()
      );

    } else if (msg instanceof GlacierNeighborRequestMessage) {
      final GlacierNeighborRequestMessage gnrm = (GlacierNeighborRequestMessage) msg;
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
        neighbors[i] = (Id)(iter.next());

      neighborStorage.getObject(neighbors[0], new Continuation<Object, Exception>() {
        int currentLookup = 0;

        public void receiveResult(Object o) {
          logger.debug("Continue: NeighborRequest from {} for range {}", gnrm.getSource().getId(), gnrm.getRequestedRange());

          if (o == null) {
            logger.warn("Problem while retrieving neighbors -- canceled");
            return;
          }

          if (o instanceof Long) {
            logger.debug("Retr: Neighbor {} was last seen at {}", neighbors[currentLookup], o);
            lastSeen[currentLookup] = ((Long)o).longValue();
            currentLookup ++;
            if (currentLookup < numRequested) {
              neighborStorage.getObject(neighbors[currentLookup], this);
            } else {
              logger.debug("Sending neighbor response...");
              router.sendMessage(
                null,
                new GlacierNeighborResponseMessage(gnrm.getUID(), neighbors, lastSeen, getLocalNodeHandle(), gnrm.getSource().getId(), gnrm.getTag()),
                gnrm.getSource()
              );
            }
          }
        }

        public void receiveException(Exception e) {
          logger.warn("Problem while retrieving neighbors in range {} for {} -- canceled", gnrm.getRequestedRange(), gnrm.getSource(), e);
        }
      });

      return;

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
      final GlacierFetchMessage gfm = (GlacierFetchMessage) msg;
      logger.atInfo().log(() -> "Fetch request for " + gfm.getKey(0) + ((gfm.getNumKeys()>1) ? (" and " + (gfm.getNumKeys() - 1) + " other keys") : "") + ", request="+gfm.getRequest());

      /* FetchMessages are sent during recovery to retrieve a fragment from
         another node. They are answered from the local fragment storage;
         keys that are not stored here get null entries in the response. */

      fragmentStorage.getObject(gfm.getKey(0), new Continuation<Object, Exception>() {
        int currentLookup = 0;
        Fragment fragment[] = new Fragment[gfm.getNumKeys()];
        Manifest manifest[] = new Manifest[gfm.getNumKeys()];
        int numFragments = 0, numManifests = 0;

        public void returnResponse() {
          logger.debug("Returning response with {} fragments, {} manifests ({} queries originally)", numFragments, numManifests, gfm.getNumKeys());
          router.sendMessage(
            null,
            new GlacierDataMessage(gfm.getUID(), gfm.getAllKeys(), fragment, manifest, getLocalNodeHandle(), gfm.getSource().getId(), true, gfm.getTag()),
            gfm.getSource()
          );
        }
        public void receiveResult(Object o) {
          if (o != null) {
            logger.atInfo().log(() -> "Fragment "+gfm.getKey(currentLookup)+" found ("+o+")");
            FragmentAndManifest fam = (FragmentAndManifest) o;
            fragment[currentLookup] = ((gfm.getRequest() & GlacierFetchMessage.FETCH_FRAGMENT)!=0) ? fam.getFragment() : null;
            if (fragment[currentLookup] != null)
              numFragments ++;
            manifest[currentLookup] = ((gfm.getRequest() & GlacierFetchMessage.FETCH_MANIFEST)!=0) ? fam.getManifest() : null;
            if (manifest[currentLookup] != null)
              numManifests ++;
          } else {
            logger.atInfo().log(() -> "Fragment "+gfm.getKey(currentLookup)+" not found");
            fragment[currentLookup] = null;
            manifest[currentLookup] = null;
          }

          nextLookup();
        }
        public void nextLookup() {
          currentLookup ++;
          if (currentLookup >= gfm.getNumKeys())
            returnResponse();
          else
            fragmentStorage.getObject(gfm.getKey(currentLookup), this);
        }
        public void receiveException(Exception e) {
          logger.atWarn().setCause(e).log(() -> "Exception while retrieving fragment "+gfm.getKey(currentLookup)+" (lookup #"+currentLookup+"), e=");
          fragment[currentLookup] = null;
          manifest[currentLookup] = null;
          nextLookup();
        }
      });
    } else if (msg instanceof GlacierDataMessage) {
      final GlacierDataMessage gdm = (GlacierDataMessage) msg;
      for (int i=0; i<gdm.numKeys(); i++) {
        final FragmentKey thisKey = gdm.getKey(i);
        final Fragment thisFragment = gdm.getFragment(i);
        final Manifest thisManifest = gdm.getManifest(i);

        if ((thisFragment != null) && (thisManifest != null)) {
          logger.info("Data: Fragment+Manifest for {}", thisKey);

          if (!neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey))) {
            logger.atWarn().log(() -> "Not responsible for "+thisKey+" (at "+locator.getFragmentLocation(thisKey)+") -- discarding");
            continue;
          }

          if (!policy.checkSignature(thisManifest, thisKey.getVersionKey())) {
            logger.warn("Manifest is not signed properly");
            continue;
          }

          if (!thisManifest.validatesFragment(thisFragment, thisKey.getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance))) {
            logger.warn("Manifest does not validate this fragment");
            continue;
          }

          if (!fragmentStorage.exists(thisKey)) {
            logger.debug("Verified ok. Storing locally.");

            FragmentAndManifest fam = new FragmentAndManifest(thisFragment, thisManifest);

            fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), 0, environment.getTimeSource().currentTimeMillis()), fam,
              new Continuation<Object, Exception>() {
                public void receiveResult(Object o) {
                  logger.info("Stored OK, sending receipt: {}", thisKey);

                  router.sendMessage(
                    null,
                    new GlacierResponseMessage(gdm.getUID(), thisKey, true, thisManifest.getExpiration(), neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey)), getLocalNodeHandle(), gdm.getSource().getId(), true, gdm.getTag()),
                    gdm.getSource()
                  );
                }

                public void receiveException(Exception e) {
                  logger.warn("receiveException({}) while storing a fragment -- unexpected, ignored (key={})", e, thisKey);
                }
              }
            );
          } else {
            logger.warn("We already have a fragment with this key! -- discarding");
            continue;
          }

          continue;
        }

        if ((thisFragment == null) && (thisManifest != null)) {

          if (!neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey))) {
            logger.atWarn().log(() -> "Not responsible for "+thisKey+" (at "+locator.getFragmentLocation(thisKey)+") -- discarding");
            continue;
          }

          /* We are being informed of a fragment that
               a) we should have, but currently don't, or
               b) we already have, but whose manifest will soon expire */

          if (fragmentStorage.exists(thisKey)) {
            final FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(thisKey);
            if ((metadata == null) || (metadata.getCurrentExpiration() < thisManifest.getExpiration())) {
              logger.atInfo().log(() -> "Replacing old manifest for "+thisKey+" (expires "+((metadata == null) ? "(broken)" : ""+metadata.getCurrentExpiration())+") by new one (expires "+thisManifest.getExpiration()+")");

              fragmentStorage.getObject(thisKey, new Continuation<Object, Exception>() {
                public void receiveResult(Object o) {
                  if (o instanceof FragmentAndManifest) {
                    FragmentAndManifest fam = (FragmentAndManifest) o;

                    logger.debug("Got FAM for {}, now replacing old manifest with new one...", thisKey);

                    String fault = null;

                    if (!thisManifest.validatesFragment(fam.getFragment(), thisKey.getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance)))
                      fault = "Update: Manifest does not validate this fragment";
                    if (!policy.checkSignature(thisManifest, thisKey.getVersionKey()))
                      fault = "Update: Manifest is not signed properly";
                    if (!Arrays.equals(thisManifest.getObjectHash(), fam.getManifest().getObjectHash()))
                      fault = "Update: Object hashes not equal";
                    for (int i=0; i<numFragments; i++)
                      if (!Arrays.equals(thisManifest.getFragmentHash(i), fam.getManifest().getFragmentHash(i)))
                        fault = "Update: Fragment hash #"+i+" does not match";

                    if (fault == null) {
                      fam.replaceManifest(thisManifest);
                      fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), ((metadata == null) ? 0 : metadata.getCurrentExpiration()), environment.getTimeSource().currentTimeMillis()), fam,
                        new Continuation<Object, Exception>() {
                          public void receiveResult(Object o) {
                            logger.debug("Old manifest for {} replaced OK, sending receipt", thisKey);
                            router.sendMessage(
                              null,
                              new GlacierResponseMessage(gdm.getUID(), thisKey, true, thisManifest.getExpiration(), true, getLocalNodeHandle(), gdm.getSource().getId(), true, gdm.getTag()),
                              gdm.getSource()
                            );
                          }
                          public void receiveException(Exception e) {
                            logger.warn("Cannot store refreshed manifest: ", e);
                          }
                        }
                      );
                    } else {
                      logger.warn(fault);
                    }
                  } else {
                    logger.warn("Fragment store returns something other than a FAM: {}", o);
                  }
                }
                public void receiveException(Exception e) {
                  logger.warn("Cannot retrieve FAM for {}: ", thisKey, e);
                }
              });
            } else {
              logger.atWarn().log(() -> "We already have exp="+((metadata == null) ? "(broken)" : ""+metadata.getCurrentExpiration())+", discarding manifest for "+thisKey+" with exp="+thisManifest.getExpiration());
            }

            continue;
          }

          logger.info("Data: Manifest for: {}, must fetch", thisKey);

          final long tStart = environment.getTimeSource().currentTimeMillis();
          retriever.rateLimitedRetrieveFragment(thisKey, thisManifest, GlacierTags.SYNC_FETCH, new GlacierContinuation() {
            public String toString() {
              return "Fetch synced fragment: "+thisKey;
            }
            public void receiveResult(Object o) {
              if (o instanceof Fragment) {
                if (!fragmentStorage.exists(thisKey)) {
                  logger.info("Received fragment {} (from primary) matches existing manifest, storing...", thisKey);

                  FragmentAndManifest fam = new FragmentAndManifest((Fragment) o, thisManifest);

                  fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), 0, environment.getTimeSource().currentTimeMillis()), fam,
                    new Continuation<Object, Exception>() {
                      public void receiveResult(Object o) {
                        logger.debug("Recovered fragment stored OK");
                      }
                      public void receiveException(Exception e) {
                        logger.warn("receiveException({}) while storing a fragment with existing manifest (key={})", e, thisKey);
                      }
                    }
                  );
                } else {
                  logger.warn("Received fragment {}, but it already exists in the fragment store", thisKey);
                }
              } else {
                logger.warn("FS received something other than a fragment: {}", o);
              }
            }
            public void receiveException(Exception e) {
              if (e instanceof GlacierNotEnoughFragmentsException) {
                GlacierNotEnoughFragmentsException gnf = (GlacierNotEnoughFragmentsException) e;
                logger.atInfo().log(() -> "Not enough fragments to reconstruct "+thisKey+": "+gnf.getChecked()+"/"+numFragments+" checked, "+gnf.getFound()+" found, "+numSurvivors+" needed");
              } else {
                logger.warn("Exception while recovering synced fragment {}: ", thisKey, e);
              }

              terminate();
            }
            public void timeoutExpired() {
              logger.warn("Timeout while fetching synced fragment {} -- aborted", thisKey);
              terminate();
            }
            public long getTimeout() {
              return tStart + config.overallRestoreTimeout();
            }
          });

          continue;
        }

        logger.warn("Case not implemented! -- GDM");
      }

      return;

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
