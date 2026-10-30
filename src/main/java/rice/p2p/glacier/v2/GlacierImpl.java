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
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
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
