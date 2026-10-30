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

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.FragmentKeySet;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.p2p.glacier.v2.messaging.GlacierQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.p2p.util.DebugCommandHandler;
import rice.p2p.util.HashUtils;
import rice.persistence.StorageManager;

/**
 * The debug commands of a Glacier instance, addressed as {@code glacier.<instance> <command>} or
 * {@code g <command>}: ls, show config, status, neighbors, refresh, manifest, retrieve, validate, fetch,
 * and, with fault injection enabled, flush, insert, delete and burst.
 */
final class GlacierDebugCommands {
  private final GlacierConfig config;
  private final int numFragments;
  private final int numSurvivors;
  private final String instance;
  private final IdFactory factory;
  private final GlacierScheduler scheduler;
  private final GlacierRouter router;
  private final FragmentLocator locator;
  private final GlacierNeighborhood neighborhood;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierSyncProtocol syncProtocol;
  private final GlacierRetriever retriever;
  private final GlacierInserter inserter;
  private final StorageManager fragmentStorage;
  private final StorageManager neighborStorage;
  private final Environment environment;
  private final Logger logger;

  GlacierDebugCommands(GlacierConfig config, int numFragments, int numSurvivors, String instance, IdFactory factory, GlacierScheduler scheduler, GlacierRouter router, FragmentLocator locator, GlacierNeighborhood neighborhood, GlacierTrafficShaper trafficShaper, GlacierSyncProtocol syncProtocol, GlacierRetriever retriever, GlacierInserter inserter, StorageManager fragmentStorage, StorageManager neighborStorage, Environment environment, Logger logger) {
    this.config = config;
    this.numFragments = numFragments;
    this.numSurvivors = numSurvivors;
    this.instance = instance;
    this.factory = factory;
    this.scheduler = scheduler;
    this.router = router;
    this.locator = locator;
    this.neighborhood = neighborhood;
    this.trafficShaper = trafficShaper;
    this.syncProtocol = syncProtocol;
    this.retriever = retriever;
    this.inserter = inserter;
    this.fragmentStorage = fragmentStorage;
    this.neighborStorage = neighborStorage;
    this.environment = environment;
    this.logger = logger;
  }

  /**
   * How long a debug command waits for its asynchronous result.
   */
  private static final long DEBUG_COMMAND_TIMEOUT = 2 * 60 * 1000;

  private String awaitDebugResult(CompletableFuture<String> result, long timeoutMillis) {
    return DebugCommandHandler.await(result, environment.getSelectorManager(), timeoutMillis);
  }

  String handleDebugCommand(String command) {
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
      inserter.refresh(new Id[] { id }, new long[] { 0 }, new long[] { expiration }, new Continuation<Object, Exception>() {
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
            inserter.insert(
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
              new GlacierQueryMessage(getMyUID(), new FragmentKey[] { id }, router.getLocalNodeHandle(), fragmentLoc, GlacierTags.DEBUG),
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
          ret.complete("OK\n\nFragment: "+fam.getFragment().getPayload().length+" bytes, Hash=["+dump(HashUtils.hash(fam.getFragment().getPayload()), false)+"], ID="+id.getFragmentID()+"\n\nValidation: " +
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
            new GlacierFetchMessage(getMyUID(), id, GlacierFetchMessage.FETCH_FRAGMENT_AND_MANIFEST, router.getLocalNodeHandle(), fragmentLoc, GlacierTags.DEBUG),
            null
          );
        }
        public void receiveResult(Object o) {
          if (o instanceof GlacierDataMessage) {
            GlacierDataMessage gdm = (GlacierDataMessage) o;
            ret.complete("\n\nResponse: "+gdm.getKey(0).toStringFull()+" ("+gdm.numKeys()+" keys)\n" +  "Holder: "+gdm.getSource()+"\n" +
                     "Fragment: "+gdm.getFragment(0).getPayload().length+" bytes, Hash=["+dump(HashUtils.hash(gdm.getFragment(0).getPayload()), false)+"]\n\nValidation: " +
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
}
