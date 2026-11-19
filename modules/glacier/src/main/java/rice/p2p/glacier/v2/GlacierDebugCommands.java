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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.GlacierException;
import rice.p2p.glacier.FragmentKeySet;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.p2p.glacier.v2.messaging.GlacierQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.p2p.util.DebugCommandHandler;
import rice.p2p.util.HashUtils;
import rice.persistence.CachedStorage;

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
  private final GlacierContinuationRegistry scheduler;
  private final GlacierMessageSender router;
  private final FragmentLocator locator;
  private final GlacierNeighborhood neighborhood;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierSyncProtocol syncProtocol;
  private final GlacierRetriever retriever;
  private final GlacierInserter inserter;
  private final CachedStorage fragmentStorage;
  private final CachedStorage neighborStorage;
  private final Environment environment;
  private final Logger logger;

  GlacierDebugCommands(GlacierConfig config, int numFragments, int numSurvivors, String instance, IdFactory factory, GlacierContinuationRegistry scheduler, GlacierMessageSender router, FragmentLocator locator, GlacierNeighborhood neighborhood, GlacierTrafficShaper trafficShaper, GlacierSyncProtocol syncProtocol, GlacierRetriever retriever, GlacierInserter inserter, CachedStorage fragmentStorage, CachedStorage neighborStorage, Environment environment, Logger logger) {
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

  @SuppressWarnings("StringSplitter") // splits debug command arguments; dropping trailing empty ones is intended
  String handleDebugCommand(String command) {
    if (command.indexOf(" ") < 0)
      return null;

    String myInstance = "glacier."+instance.substring(instance.lastIndexOf("-") + 1);
    String requestedInstance = command.substring(0, command.indexOf(" "));
    String instanceFreeCommand = command.substring(requestedInstance.length() + 1);

    if (!requestedInstance.equals(myInstance) && !requestedInstance.equals("g"))
      return null;

    logger.info("Debug command: {}", instanceFreeCommand);

    if (instanceFreeCommand.startsWith("ls")) {
      FragmentKeySet keyset = (FragmentKeySet) fragmentStorage.scan();
      Iterator<Id> iter = keyset.getIterator();
      StringBuilder result = new StringBuilder();

      long now = environment.getTimeSource().currentTimeMillis();
      if (instanceFreeCommand.indexOf("-r") < 0)
        now = 0;

      result.append(keyset.numElements()+ " fragment(s)\n");

      while (iter.hasNext()) {
        FragmentKey thisKey = (FragmentKey) iter.next();
        boolean isMine = neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey));
        FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(thisKey);
        if (metadata != null) {
          result.append(((Id)thisKey).toStringFull()+" "+(isMine ? "OK" : "MI")+" "+
              (metadata.currentExpiration()-now)+" "+(metadata.previousExpiration()-now)+"\n");
        }
      }

      return result.toString();
    }

    if (instanceFreeCommand.startsWith("show config")) {
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

    if (instanceFreeCommand.startsWith("flush") && config.faultInjectionEnabled()) {
      FragmentKeySet keyset = (FragmentKeySet) fragmentStorage.scan();
      Iterator<Id> iter = keyset.getIterator();

      while (iter.hasNext()) {
        FragmentKey thisKey = (FragmentKey) iter.next();
        fragmentStorage.unstore(thisKey, new Continuation<Object, Exception>() {
          @Override
          public void receiveResult(Object result) {}
          @Override
          public void receiveException(Exception e) {}
        });
      }

      return keyset.numElements()+ " objects deleted\n";
    }

    if (instanceFreeCommand.startsWith("refresh")) {
      String args = instanceFreeCommand.substring(8);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String keyArg = args.substring(0, args.lastIndexOf(' '));

      Id id = factory.buildIdFromString(keyArg);
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(expirationArg);

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      inserter.refresh(new Id[] { id }, new long[] { 0 }, new long[] { expiration }, new Continuation<Object, Exception>() {
        @Override
        public void receiveResult(Object result) {
          reply.complete("result("+result+")");
        }
        @Override
        public void receiveException(Exception e) {
          reply.complete("exception("+e+")");
        }
      });

      return "refresh("+id+", "+expiration+")="+awaitDebugResult(reply, DEBUG_COMMAND_TIMEOUT);
    }

    if (instanceFreeCommand.startsWith("neighbors")) {
      final Iterator<Id> iter = neighborStorage.scan().getIterator();
      @SuppressWarnings("JdkObsolete") // appended from lookup continuations that may run on different threads
      final StringBuffer result = new StringBuffer();
      final long now = (instanceFreeCommand.indexOf("-r") < 0) ? 0 : environment.getTimeSource().currentTimeMillis();
      final CompletableFuture<String> reply = new CompletableFuture<String>();

      result.append(neighborStorage.scan().numElements()+ " neighbor(s)\n");

      Continuation<Object, Exception> continuation = new Continuation<Object, Exception>() {
        Id currentLookup;
        @Override
        public void receiveResult(Object received) {
          if (received != null)
            result.append(currentLookup.toStringFull() + " " + (((Long)received).longValue() - now) + "\n");

          if (iter.hasNext()) {
            currentLookup = (Id) iter.next();
            neighborStorage.getObject(currentLookup, this);
          } else {
            reply.complete("OK");
          }
        }
        @Override
        public void receiveException(Exception e) {
          reply.complete("Exception: "+e);
        }
      };

      continuation.receiveResult(null);
      String outcome = awaitDebugResult(reply, DEBUG_COMMAND_TIMEOUT);
      // the StringBuffer is synchronized, and complete() happens-after the last append
      result.append(outcome+"\n");
      return result.toString();
    }

    if (instanceFreeCommand.startsWith("status")) {
      String result = "";
      result = result + "Responsible for: "+neighborhood.getResponsibleRange() + "\n";
      result = result + "Local time: "+DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.systemDefault())) + "\n\n";
      result = result + fragmentStorage.scan().numElements() + " fragments\n";
      result = result + neighborStorage.scan().numElements() + " neighbors\n";
      result = result + scheduler.size() + " active continuations\n";
      result = result + trafficShaper.numPending() + " pending requests\n";

      return result;
    }

    if (instanceFreeCommand.startsWith("insert") && config.faultInjectionEnabled()) {
      String args = instanceFreeCommand.substring(7);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String numObjectsArg = args.substring(0, args.lastIndexOf(' '));

      final int numObjects = Integer.parseInt(numObjectsArg);
      final int lifetime = Integer.parseInt(expirationArg);
      String result = "";
      final java.util.concurrent.atomic.AtomicInteger displaced = new java.util.concurrent.atomic.AtomicInteger();

      for (int i=0; i<numObjects; i++) {
        final Id randomID = factory.buildRandomId(environment.getRandomSource());
        result = result + randomID.toStringFull() + "\n";
        Continuation<Object, Exception> prev = trafficShaper.enqueue(new VersionKey(randomID, 0), new Continuation.SimpleContinuation() {
          @Override
          public void receiveResult(Object received) {
            inserter.insert(
              new DebugContent(randomID, false, 0, new byte[] {}),
              environment.getTimeSource().currentTimeMillis() + lifetime,
              new Continuation<Object, Exception>() {
                @Override
                public void receiveResult(Object result) {
                }
                @Override
                public void receiveException(Exception e) {
                }
              });
          }
          @Override
          public void receiveException(Exception e) {
            /* Displaced by a newer job for the same key: drop this insert instead of running it unshaped */
            displaced.incrementAndGet();
            logger.warn("Debug insert of {} dropped", randomID.toStringFull(), e);
          }
        });

        if (prev != null)
          prev.receiveException(new GlacierException("Key collision in traffic shaper (insert)"));
      }

      int dropped = displaced.get();
      return result + (numObjects - dropped) + " of " + numObjects + " object(s) with lifetime "+lifetime+"ms queued, " + dropped + " displaced in the traffic shaper\n";
    }

    if (instanceFreeCommand.startsWith("delete") && config.faultInjectionEnabled()) {
      String[] versionKeyParts = instanceFreeCommand.substring(7).split("[v#]");
      Id key = factory.buildIdFromString(versionKeyParts[0]);
      long version = Long.parseLong(versionKeyParts[1]);
      VersionKey versionKey = new VersionKey(key, version);
      FragmentKey id = new FragmentKey(versionKey, Integer.parseInt(versionKeyParts[2]));

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      fragmentStorage.unstore(id, new Continuation<Object, Exception>() {
        @Override
        public void receiveResult(Object result) {
          reply.complete("result("+result+")");
        }
        @Override
        public void receiveException(Exception e) {
          reply.complete("exception("+e+")");
        }
      });

      return "delete("+id+")="+awaitDebugResult(reply, DEBUG_COMMAND_TIMEOUT);
    }

    if (instanceFreeCommand.startsWith("burst") && config.faultInjectionEnabled()) {
      String[] versionKeyParts = instanceFreeCommand.substring(6).split("[v#]");
      Id key = factory.buildIdFromString(versionKeyParts[0]);
      long version = Long.parseLong(versionKeyParts[1]);
      VersionKey versionKey = new VersionKey(key, version);
      final FragmentKey id = new FragmentKey(versionKey, Integer.parseInt(versionKeyParts[2]));
      final Id fragmentLoc = locator.getFragmentLocation(id);

      final String[] reply = new String[] { "" }; // only touched by the selector thread, until done completes
      final CompletableFuture<String> done = new CompletableFuture<String>();
      final long now = environment.getTimeSource().currentTimeMillis();
      scheduler.addContinuation(new GlacierContinuation() {
        int receivedSoFar = 0;
        final int total = 100;
        @Override
        public String toString() {
          return "Burst continuation";
        }
        @Override
        public void init() {
          for (int i=0; i<total; i++) {
            router.sendMessage(
              fragmentLoc,
              new GlacierQueryMessage(getMyUID(), new FragmentKey[] { id }, router.getLocalNodeHandle(), fragmentLoc, GlacierTags.DEBUG),
              null
            );
          }
        }
        @Override
        public void receiveResult(Object result) {
          if (result instanceof GlacierResponseMessage glacierResponseMessage) {
            reply[0] = reply[0] + (environment.getTimeSource().currentTimeMillis() - now) + " msec ("+glacierResponseMessage.getSource().getId()+")\n";
            if (++receivedSoFar == total)
              timeoutExpired();
          }
        }
        @Override
        public void receiveException(Exception e) {
        }
        @Override
        public void timeoutExpired() {
          done.complete(reply[0]);
          terminate();
        }
        @Override
        public long getTimeout() {
          return now + 120 * GlacierConfig.SECONDS;
        }
      });

      return "burst("+id+")="+awaitDebugResult(done, 130 * GlacierConfig.SECONDS);
    }

    if (instanceFreeCommand.startsWith("manifest")) {
      String[] versionKeyParts = instanceFreeCommand.substring(9).split("v");
      Id key = factory.buildIdFromString(versionKeyParts[0]);
      long version = Long.parseLong(versionKeyParts[1]);
      VersionKey versionKey = new VersionKey(key, version);

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      retriever.retrieveManifest(versionKey, GlacierTags.DEBUG, new Continuation<Object, Exception>() {
        @Override
        public void receiveResult(Object result) {
          if (result instanceof Manifest manifest)
            reply.complete(manifest.toStringFull());
          else
            reply.complete("result("+result+")");
        }
        @Override
        public void receiveException(Exception e) {
          reply.complete("exception("+e+")");
        }
      });

      return "manifest("+versionKey+")="+awaitDebugResult(reply, DEBUG_COMMAND_TIMEOUT);
    }

    if (instanceFreeCommand.startsWith("retrieve")) {
      String[] versionKeyParts = instanceFreeCommand.substring(9).split("[v#]");
      Id key = factory.buildIdFromString(versionKeyParts[0]);
      long version = Long.parseLong(versionKeyParts[1]);
      VersionKey versionKey = new VersionKey(key, version);
      final FragmentKey id = new FragmentKey(versionKey, Integer.parseInt(versionKeyParts[2]));
      final FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(id);

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      fragmentStorage.getObject(id, new Continuation<Object, Exception>() {
        @Override
        public void receiveResult(Object result) {
          FragmentAndManifest fragmentAndManifest = (FragmentAndManifest) result;
          reply.complete("OK\n\nFragment: "+fragmentAndManifest.getFragment().getPayload().length+" bytes, Hash=["+dump(HashUtils.hash(fragmentAndManifest.getFragment().getPayload()), false)+"], ID="+id.fragmentId()+"\n\nValidation: " +
                   (fragmentAndManifest.getManifest().validatesFragment(fragmentAndManifest.getFragment(), id.fragmentId(), environment.getLogManager().getLogger(Manifest.class, instance)) ? "OK" : "FAIL") + "\n\n" +
                   fragmentAndManifest.getManifest().toStringFull()+"\n\nMetadata:\n - Stored since: "+metadata.storedSince()+
                   "\n - Current expiration: "+metadata.currentExpiration()+"\n - Previous expiration: "+metadata.previousExpiration()+"\n");
        }
        @Override
        public void receiveException(Exception e) {
          reply.complete("exception("+e+")");
        }
      });

      return "retrieve("+id+")="+awaitDebugResult(reply, DEBUG_COMMAND_TIMEOUT);
    }

    if (instanceFreeCommand.startsWith("validate")) {
      FragmentKeySet keyset = (FragmentKeySet) fragmentStorage.scan();
      final Iterator<Id> iter = keyset.getIterator();
      @SuppressWarnings("JdkObsolete") // appended from lookup continuations that may run on different threads
      final StringBuffer result = new StringBuffer();

      result.append(keyset.numElements()+ " fragment(s)\n");

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      if (iter.hasNext()) {
        final FragmentKey thisKey = (FragmentKey) iter.next();
        fragmentStorage.getObject(thisKey, new Continuation<Object, Exception>() {
          FragmentKey currentKey = thisKey;
          int totalChecks = 1, totalFailures = 0;
          @Override
          public void receiveResult(Object received) {
            if (received instanceof FragmentAndManifest fragmentAndManifest) {
              boolean success = fragmentAndManifest.getManifest().validatesFragment(fragmentAndManifest.getFragment(), currentKey.fragmentId(), environment.getLogManager().getLogger(Manifest.class, instance));
              if (!success)
                totalFailures ++;
              result.append(currentKey.toStringFull()+" "+ (success ? "OK" : "FAIL") + "\n");
            } else {
              /* gone since the scan, or not a fragment at all: count it as damaged instead of stalling the walk */
              totalFailures ++;
              result.append(currentKey.toStringFull()+" FAIL (" + ((received == null) ? "missing" : "not a fragment") + ")\n");
            }
            advance();
          }
          @Override
          public void receiveException(Exception e) {
            totalFailures ++;
            result.append(currentKey.toStringFull()+" EXC: "+e+"\n");
            advance();
          }
          void advance() {
            if (iter.hasNext()) {
              currentKey = (FragmentKey) iter.next();
              totalChecks ++;
              fragmentStorage.getObject(currentKey, this);
            } else {
              if (totalFailures == 0)
                reply.complete("OK ("+totalChecks+" fragments checked)");
              else
                reply.complete("FAIL, "+totalFailures+"/"+totalChecks+" fragments damaged");
            }
          }
        });

        return "validate="+awaitDebugResult(reply, DEBUG_COMMAND_TIMEOUT)+"\n\n"+result.toString();
      }

      return "validate: no objects\n\n" + result.toString();
    }

    if (instanceFreeCommand.startsWith("fetch")) {
      String[] versionKeyParts = instanceFreeCommand.substring(6).split("[v#]");
      Id key = factory.buildIdFromString(versionKeyParts[0]);
      long version = Long.parseLong(versionKeyParts[1]);
      VersionKey versionKey = new VersionKey(key, version);
      final FragmentKey id = new FragmentKey(versionKey, Integer.parseInt(versionKeyParts[2]));
      final long now = environment.getTimeSource().currentTimeMillis();
      final Id fragmentLoc = locator.getFragmentLocation(id);

      final CompletableFuture<String> reply = new CompletableFuture<String>();
      scheduler.addContinuation(new GlacierContinuation() {
        @Override
        public String toString() {
          return "DebugFetch continuation";
        }
        @Override
        public void init() {
          router.sendMessage(
            fragmentLoc,
            new GlacierFetchMessage(getMyUID(), id, GlacierFetchMessage.FETCH_FRAGMENT_AND_MANIFEST, router.getLocalNodeHandle(), fragmentLoc, GlacierTags.DEBUG),
            null
          );
        }
        @Override
        public void receiveResult(Object result) {
          if (result instanceof GlacierDataMessage dataMessage) {
            reply.complete("\n\nResponse: "+dataMessage.getKey(0).toStringFull()+" ("+dataMessage.numKeys()+" keys)\n" +  "Holder: "+dataMessage.getSource()+"\n" +
                     "Fragment: "+dataMessage.getFragment(0).getPayload().length+" bytes, Hash=["+dump(HashUtils.hash(dataMessage.getFragment(0).getPayload()), false)+"]\n\nValidation: " +
                     (dataMessage.getManifest(0).validatesFragment(dataMessage.getFragment(0), dataMessage.getKey(0).fragmentId(), environment.getLogManager().getLogger(Manifest.class, instance)) ? "OK" : "FAIL") + "\n\n" +
                     dataMessage.getManifest(0).toStringFull());

            terminate();
          } else {
            reply.complete("Received "+result);
            terminate();
          }
        }
        @Override
        public void receiveException(Exception e) {
          reply.complete("Exception="+e);
          terminate();
        }
        @Override
        public void timeoutExpired() {
          reply.complete("Timeout");
          terminate();
        }
        @Override
        public long getTimeout() {
          return now + 5 * GlacierConfig.SECONDS;
        }
      });

      String fetched = awaitDebugResult(reply, 5 * GlacierConfig.SECONDS);
      if (fetched.startsWith("timeout"))
        fetched = "Timeout";

      return "fetch("+id+"@"+fragmentLoc+")="+fetched;
    }

    return null;
  }

  private static String dump(byte[] data, boolean linebreak) {
    final String hexDigits = "0123456789ABCDEF";
    String result = "";

    for (int i=0; i<data.length; i++) {
      int d = data[i];
      if (d<0)
        d+= 256;
      int hi = (d>>4);
      int lo = (d&15);

      result = result + hexDigits.charAt(hi) + hexDigits.charAt(lo);
      if (linebreak && (((i%16)==15) || (i==(data.length-1))))
        result = result + "\n";
      else if (i!=(data.length-1))
        result = result + " ";
    }

    return result;
  }
}
