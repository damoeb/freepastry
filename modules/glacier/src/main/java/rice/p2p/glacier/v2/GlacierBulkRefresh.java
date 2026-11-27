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
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import rice.Continuation;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.GlacierException;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshCompleteMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshPatchMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshProbeMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshResponseMessage;
import rice.persistence.CachedStorage;

/**
 * The aggregated refresh protocol ({@code p2p_glacier_enableBulkRefresh}): the refreshing node probes,
 * fetches manifests and sends patches; the fragment holders answer probes and apply patches.
 */
final class GlacierBulkRefresh {
  /** How often a bulk refresh tries to fetch the manifests it still needs. */
  private static final int MANIFEST_FETCH_ATTEMPTS = 3;

  private final GlacierConfig config;
  private final int numFragments;
  private final int numSurvivors;
  private final GlacierPolicy policy;
  private final GlacierContinuationRegistry scheduler;
  private final GlacierMessageSender router;
  private final FragmentLocator locator;
  private final GlacierNeighborhood neighborhood;
  private final CachedStorage fragmentStorage;
  private final IdFactory factory;
  private final Environment environment;
  private final Logger logger;

  GlacierBulkRefresh(GlacierConfig config, int numFragments, int numSurvivors, GlacierPolicy policy, GlacierContinuationRegistry scheduler, GlacierMessageSender router, FragmentLocator locator, GlacierNeighborhood neighborhood, CachedStorage fragmentStorage, IdFactory factory, Environment environment, Logger logger) {
    this.config = config;
    this.numFragments = numFragments;
    this.numSurvivors = numSurvivors;
    this.policy = policy;
    this.scheduler = scheduler;
    this.router = router;
    this.locator = locator;
    this.neighborhood = neighborhood;
    this.fragmentStorage = fragmentStorage;
    this.factory = factory;
    this.environment = environment;
    this.logger = logger;
  }

  /**
   * Refreshes many objects at once: probes which nodes hold their fragments, fetches the manifests
   * that are missing, and sends each holder one patch message for all the objects it holds.
   */
  void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    scheduler.addContinuation(new GlacierContinuation() {
      int minAcceptable = (int)(numSurvivors * config.minFragmentsAfterInsert());
      FragmentKey[][] fragmentKey;
      VersionKey[] versionKey;
      Id[][] fragmentLocation;
      NodeHandle[][] fragmentHolder;
      boolean[][] fragmentChecked;
      List<NodeHandle> holders;
      Manifest manifests[];
      int successes[];
      boolean answered;
      long nextTimeout;
      int currentStage;
      int retriesRemaining;
      final int stageProbing = 1;
      final int stageFetchingManifests = 2;
      final int stagePatching = 3;

      @Override
      public String toString() {
        return "AggregateRefresh continuation ("+fragmentKey.length+" fragments)";
      }
      @Override
      public void init() {
        logger.info("Initializing AggregateRefresh continuation");

        fragmentKey = new FragmentKey[ids.length][numFragments];
        fragmentLocation = new Id[ids.length][numFragments];
        fragmentHolder = new NodeHandle[ids.length][numFragments];
        fragmentChecked = new boolean[ids.length][numFragments];
        manifests = new Manifest[ids.length];
        versionKey = new VersionKey[ids.length];
        successes = new int[ids.length];
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.bulkRefreshProbeInterval();
        currentStage = stageProbing;
        holders = new ArrayList<NodeHandle>();
        retriesRemaining = (int)(config.bulkRefreshMaxProbeFactor() * numFragments);
        answered = false;

        boolean haveFragmentMyself = false;
        for (int i=0; i<ids.length; i++) {
          manifests[i] = null;
          versionKey[i] = new VersionKey(ids[i], versions[i]);
          for (int j=0; j<numFragments; j++) {
            fragmentKey[i][j] = new FragmentKey(new VersionKey(ids[i], versions[i]), j);
            fragmentLocation[i][j] = locator.getFragmentLocation(fragmentKey[i][j]);
            fragmentChecked[i][j] = false;
            if (fragmentStorage.getMetadata(fragmentKey[i][j]) != null) {
              haveFragmentMyself = true;
              fragmentHolder[i][j] = router.getLocalNodeHandle();
            } else {
              fragmentHolder[i][j] = null;
            }
          }
        }

        if (haveFragmentMyself)
          holders.add(router.getLocalNodeHandle());

        Arrays.fill(successes, 0);

        logger.debug("AR Initialization completed, {} candidate objects. Triggering first probe...", fragmentKey.length);
        timeoutExpired();
      }
      @Override
      public void receiveResult(Object received) {
        if (received instanceof GlacierRefreshResponseMessage refreshResponseMessage) {
          IdRange thisRange = refreshResponseMessage.getRange();
          NodeHandle holder = refreshResponseMessage.isOnline() ? refreshResponseMessage.getSource() : null;

          logger.debug("AR got refresh response: range {}, online={}", thisRange, refreshResponseMessage.isOnline());
          if (thisRange != null) {
            for (int i=0; i<ids.length; i++) {
              for (int j=0; j<numFragments; j++) {
                if (thisRange.containsId(fragmentLocation[i][j])) {
                  fragmentChecked[i][j] = true;
                  fragmentHolder[i][j] = holder;
                }
              }
            }
          }

          if (!holders.contains(holder))
            holders.add(holder);
        } else if (received instanceof GlacierDataMessage dataMessage) {

          logger.atDebug().log(() -> "AR Received data message with "+dataMessage.numKeys()+" keys");
          for (int i=0; i<dataMessage.numKeys(); i++) {
            if ((dataMessage.getManifest(i) != null) && (dataMessage.getKey(i) != null)) {
              Manifest thisManifest = dataMessage.getManifest(i);
              FragmentKey thisKey = dataMessage.getKey(i);

              if (logger.isDebugEnabled()) logger.debug("AR Received manifest for "+dataMessage.getKey(i)+", checking signature...");
              if (policy.checkSignature(thisManifest, thisKey.versionKey())) {
                logger.debug("AR Signature OK");
                for (int j=0; j<ids.length; j++) {
                  if ((manifests[j] == null) && versionKey[j].equals(thisKey.versionKey())) {
                    manifests[j] = thisManifest;
                    logger.debug("AR Storing under #{}", j);
                  }
                }
              } else {
                logger.warn("AR Invalid signature");
              }
            }
          }
        } else if (received instanceof GlacierRefreshCompleteMessage refreshCompleteMessage) {
          logger.debug("AR Refresh completion reported by {}", refreshCompleteMessage.getSource());

          for (int i=0; i<refreshCompleteMessage.numKeys(); i++) {
            if (logger.isDebugEnabled()) logger.debug("AR Refresh completion: Key "+refreshCompleteMessage.getKey(i)+", "+refreshCompleteMessage.getUpdates(i)+" update(s)");

            int index = -1;
            for (int j=0; j<ids.length; j++) {
              if (refreshCompleteMessage.getKey(i).equals(versionKey[j]))
                index = j;
            }

            if (index >= 0) {
              int maxSuccesses = 0;
              for (int j=0; j<numFragments; j++) {
                if (!fragmentChecked[index][j] && (fragmentHolder[index][j] != null) && fragmentHolder[index][j].equals(refreshCompleteMessage.getSource())) {
                  maxSuccesses ++;
                  fragmentChecked[index][j] = true;
                }
              }

              if (refreshCompleteMessage.getUpdates(i) > maxSuccesses) {
                if (logger.isWarnEnabled()) logger.warn("Node "+refreshCompleteMessage.getSource()+" reports "+refreshCompleteMessage.getUpdates(i)+" for "+refreshCompleteMessage.getKey(i)+", but is responsible for only "+maxSuccesses+" fragments -- duplicate message, or under attack?");
                successes[index] += maxSuccesses;
              } else {
                successes[index] += (int) refreshCompleteMessage.getUpdates(i); // at most maxSuccesses, checked above
              }
            } else {
              if (logger.isWarnEnabled()) logger.warn("Node "+refreshCompleteMessage.getSource()+" reports completion for "+refreshCompleteMessage.getKey(i)+", but no refresh request matches?!?");
            }
          }

          if (!answered) {
            boolean allSuccessful = true;
            for (int i=0; i<successes.length; i++)
              if (successes[i] < minAcceptable)
                allSuccessful = false;

            if (allSuccessful) {
              logger.debug("AR Reporing success");

              Object[] result = new Object[ids.length];
              for (int i=0; i<ids.length; i++)
                result[i] = Boolean.valueOf(true);

              answered = true;
              command.receiveResult(result);
            }
          }
        } else {
          logger.warn("Unexpected result in AR continuation: {} -- discarded", received);
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Exception during AggregateRefresh: ", e);
        terminate();

        if (!answered) {
          Object[] result = new Object[ids.length];
          Exception exception = new GlacierException("Exception during refresh: "+e);

          for (int i=0; i<ids.length; i++)
            result[i] = exception;

          answered = true;
          command.receiveResult(result);
        }
      }
      @Override
      public void timeoutExpired() {
        if (currentStage == stageProbing) {
          nextTimeout = environment.getTimeSource().currentTimeMillis() + config.bulkRefreshProbeInterval();

          int nextProbe = environment.getRandomSource().nextInt(ids.length);
          int nextFID = environment.getRandomSource().nextInt(numFragments);
          int maxSteps = ids.length * numFragments;
          while ((maxSteps > 0) && fragmentChecked[nextProbe][nextFID]) {
            nextFID ++;
            if (nextFID >= numFragments) {
              nextFID = 0;
              nextProbe = (nextProbe + 1) % ids.length;
            }

            maxSteps --;
          }

          if (!fragmentChecked[nextProbe][nextFID] && (retriesRemaining > 0)) {
            logger.debug("AR Sending a probe to {} at {} ({} probes left)", fragmentKey[nextProbe][nextFID], fragmentLocation[nextProbe][nextFID], retriesRemaining);
            fragmentChecked[nextProbe][nextFID] = true;
            retriesRemaining --;
            router.sendMessage(
              fragmentLocation[nextProbe][nextFID],
              new GlacierRefreshProbeMessage(getMyUID(), fragmentLocation[nextProbe][nextFID], router.getLocalNodeHandle(), fragmentLocation[nextProbe][nextFID], GlacierTags.REFRESH),
              null
            );
          } else {
            currentStage = stageFetchingManifests;
            retriesRemaining = MANIFEST_FETCH_ATTEMPTS;
          }
        }

        if (currentStage == stageFetchingManifests) {
          nextTimeout = environment.getTimeSource().currentTimeMillis() + config.bulkRefreshManifestInterval();

          boolean[] objectCovered = new boolean[ids.length];
          boolean allObjectsCovered = true;
          for (int i=0; i<ids.length; i++) {
            objectCovered[i] = (manifests[i] != null);
            allObjectsCovered &= objectCovered[i];
          }

          if (!allObjectsCovered && (retriesRemaining-- > 0)) {
            logger.debug("AR Fetching manifests, {} attempts remaining", retriesRemaining);
            while (true) {
              int idx = environment.getRandomSource().nextInt(ids.length);
              int maxSteps = ids.length + 2;
              while (objectCovered[idx] && (--maxSteps>0))
                idx = (idx+1) % ids.length;
              if (maxSteps <= 0)
                break;

              int fid = environment.getRandomSource().nextInt(numFragments);
              maxSteps = numFragments + 2;
              while ((fragmentHolder[idx][fid] == null) && (--maxSteps>0))
                fid = (fid+1) % numFragments;

              if (fragmentHolder[idx][fid] != null) {
                NodeHandle thisHolder = fragmentHolder[idx][fid];
                List<Id> idsToQuery = new ArrayList<Id>();
                for (int i=0; i<ids.length; i++) {
                  if (!objectCovered[i]) {
                    for (int j=0; j<numFragments; j++) {
                      if ((fragmentHolder[i][j] != null) && fragmentHolder[i][j].equals(thisHolder)) {
                        idsToQuery.add(fragmentKey[i][j]);
                        objectCovered[i] = true;
                        break;
                      }
                    }
                  }
                }

                logger.debug("AR Asking {} for {} manifests", thisHolder, idsToQuery.size());
                for (int i=0; i<idsToQuery.size(); i+= config.bulkRefreshManifestAggregationFactor()) {
                  int idsHere = Math.min(idsToQuery.size() - i, config.bulkRefreshManifestAggregationFactor());
                  FragmentKey[] keys = new FragmentKey[idsHere];
                  for (int j=0; j<idsHere; j++)
                    keys[j] = (FragmentKey) idsToQuery.get(i+j);

                  logger.debug("AR Sending a manifest fetch with {} IDs, starting at {}", idsHere, keys[0]);
                  router.sendMessage(
                    null,
                    new GlacierFetchMessage(getMyUID(), keys, GlacierFetchMessage.FETCH_MANIFEST, router.getLocalNodeHandle(), thisHolder.getId(), GlacierTags.REFRESH),
                    thisHolder
                  );
                }
              } else {
                objectCovered[idx] = true;
              }
            }

            logger.debug("AR Manifest fetches sent; awaiting responses...");

          } else {
            currentStage = stagePatching;
            retriesRemaining = config.bulkRefreshPatchRetries();

            logger.debug("AR Patching manifests...");
            for (int i=0; i<ids.length; i++) {
              if (manifests[i] != null) {
                // only the owner can sign the extension; for other objects nothing is sent and the refresh fails
                manifests[i] = policy.updateManifest(versionKey[i], manifests[i], expirations[i]);
                if (manifests[i] == null)
                  logger.warn("AR Cannot extend {}: this node is not the owner", versionKey[i]);
              }
            }

            logger.debug("AR Done patching manifests");

            for (int i=0; i<ids.length; i++)
              for (int j=0; j<numFragments; j++)
                fragmentChecked[i][j] = ((fragmentHolder[i][j] == null) || (manifests[i] == null));
          }
        }

        if (currentStage == stagePatching) {
          nextTimeout = environment.getTimeSource().currentTimeMillis() + config.bulkRefreshPatchInterval();

          if (retriesRemaining-- > 0) {
            logger.debug("AR Sending patches... ({} retries left)", retriesRemaining);

            int totalPatchesSent = 0;
            for (int h=0; h<holders.size(); h++) {
              NodeHandle thisHolder = holders.get(h);

              /* Find out which patches this holder should get */

              boolean[] sendPatchForObject = new boolean[ids.length];
              int numPatches = 0;

              for (int i=0; i<ids.length; i++) {
                sendPatchForObject[i] = false;

                for (int j=0; j<numFragments; j++)
                  if (!fragmentChecked[i][j] && fragmentHolder[i][j].equals(thisHolder))
                    sendPatchForObject[i] = true;

                if (sendPatchForObject[i])
                  numPatches ++;
              }

              logger.debug("AR Holder #{} ({}) should get {} patches", h, thisHolder, numPatches);

              /* Send the patches */

              int nextPatch = 0;
              for (int i=0; i<numPatches; i+=config.bulkRefreshPatchAggregationFactor()) {
                int patchesHere = Math.min(numPatches-i, config.bulkRefreshPatchAggregationFactor());

                VersionKey[] keys = new VersionKey[patchesHere];
                long[] lifetimes = new long[patchesHere];
                byte[][] signatures = new byte[patchesHere][];

                for (int j=0; j<patchesHere; j++) {
                  while (!sendPatchForObject[nextPatch])
                    nextPatch ++;

                  keys[j] = versionKey[nextPatch];
                  lifetimes[j] = expirations[nextPatch];
                  signatures[j] = manifests[nextPatch].getSignature();
                  nextPatch ++;
                }

                logger.debug("AR Sending a patch with {} IDs, starting at {}, to {}", patchesHere, keys[0], thisHolder.getId());
                totalPatchesSent += patchesHere;

                router.sendMessage(
                  null,
                  new GlacierRefreshPatchMessage(getMyUID(), keys, lifetimes, signatures, router.getLocalNodeHandle(), thisHolder.getId(), GlacierTags.REFRESH),
                  thisHolder
                );
              }
            }

            if (totalPatchesSent == 0) {
              logger.debug("AR No patches sent; refresh seems to be complete...");
              retriesRemaining = 0;
              timeoutExpired();
            }
          } else {
            logger.debug("AR Giving up");
            terminate();

            Object[] result = new Object[ids.length];
            for (int i=0; i<ids.length; i++) {
              result[i] = (successes[i] >= minAcceptable) ? (Object) Boolean.valueOf(true) : (Object) new GlacierException("Only "+successes[i]+" fragments of "+versionKey[i]+" refreshed successfully; need "+minAcceptable);
              if (logger.isDebugEnabled()) logger.debug(" - AR Result for "+versionKey[i]+": " + ((result[i] instanceof Boolean) ? "OK" : "Failed") + " (with "+successes[i]+"/"+numFragments+" fragments, "+minAcceptable+" acceptable)");
            }

            if (!answered) {
              answered = true;
              command.receiveResult(result);
            }
          }
        }
      }
      @Override
      public long getTimeout() {
        return nextTimeout;
      }
    });
  }

  /**
   * Tells a refresh initiator which range around the requested id this node, or one of its neighbors,
   * is responsible for.
   */
  void handleProbe(final GlacierRefreshProbeMessage refreshPatchMessage) {
    Id requestedId = refreshPatchMessage.getRequestedId();

    logger.info("Refresh probe for {} (RR={})", requestedId, neighborhood.getResponsibleRange());

    Id[][] ranges = neighborhood.getNeighborRanges();
    IdRange returnedRange = null;
    boolean online = false;

    if (neighborhood.getResponsibleRange().containsId(requestedId)) {
      returnedRange = neighborhood.getResponsibleRange();
      online = true;
    } else {
      online = false;
      for (int i=0; i<ranges.length; i++) {
        IdRange thisRange = factory.buildIdRange(ranges[i][0], ranges[i][2]);
        logger.debug(" - {} ({})", thisRange, ranges[i][1]);
        if (thisRange.containsId(requestedId))
          returnedRange = thisRange;
      }
    }

    router.sendMessage(
      null,
      new GlacierRefreshResponseMessage(refreshPatchMessage.getUID(), returnedRange, online, router.getLocalNodeHandle(), refreshPatchMessage.getSource().getId(), refreshPatchMessage.getTag()),
      refreshPatchMessage.getSource()
    );
  }

  /**
   * Applies the new expirations and signatures of a refresh patch to the local fragments, and reports
   * how many fragments of each object were updated.
   */
  void handlePatch(final GlacierRefreshPatchMessage refreshPatchMessage) {
    logger.atInfo().log(() -> "AR Refresh patches received for "+refreshPatchMessage.numKeys()+" keys. Processing...");
    Continuation<Object, Exception> continuation = new Continuation<Object, Exception>() {
      static final int phaseFetch = 1;
      static final int phaseStore = 2;
      static final int phaseAdvance = 3;
      int[] successes = new int[refreshPatchMessage.numKeys()];
      int currentPhase = phaseAdvance;
      FragmentKey currentKey = null;
      int currentIndex = 0;
      int currentFID = -1;

      @Override
      public void receiveResult(Object result) {
        if (currentPhase == phaseFetch) {
          logger.debug("AR Patch: Got FAM for {}", currentKey);

          FragmentAndManifest fragmentAndManifest = (FragmentAndManifest) result;
          Manifest patched = null;
          if (fragmentAndManifest == null) {
            // the fragment vanished between exists() and getObject(): treat it as missing and go on
            logger.warn("AR Patch: Fragment {} vanished before it could be fetched, skipping", currentKey);
            currentPhase = phaseAdvance;
          } else {
            // a copy: the stored manifest must stay as it is unless the owner signed the new expiration
            patched = fragmentAndManifest.getManifest().withExpiration(refreshPatchMessage.getLifetime(currentIndex), refreshPatchMessage.getSignature(currentIndex));
          }

          if (patched != null && policy.checkSignature(patched, currentKey.versionKey())) {
            FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(currentKey);
            if (metadata != null) {
              if (metadata.currentExpiration() <= refreshPatchMessage.getLifetime(currentIndex)) {
                currentPhase = phaseStore;
                if (metadata.currentExpiration() == refreshPatchMessage.getLifetime(currentIndex)) {
                  logger.atDebug().log(() -> "AR Duplicate refresh request (prev="+metadata.previousExpiration()+" cur="+metadata.currentExpiration()+" updated="+refreshPatchMessage.getLifetime(currentIndex)+") -- ignoring");
                } else {
                  FragmentMetadata newMetadata = new FragmentMetadata(refreshPatchMessage.getLifetime(currentIndex), metadata.currentExpiration(), metadata.storedSince());
                  logger.debug("AR FAM {} updated ({} -> {}), writing to disk...", currentKey, newMetadata.previousExpiration(), newMetadata.currentExpiration());
                  fragmentAndManifest.replaceManifest(patched);
                  Futures.forward(fragmentStorage.store(currentKey, newMetadata, fragmentAndManifest), this);
                  return;
                }
              } else {
                logger.atWarn().log(() -> "RefreshPatch attempts to roll back lifetime from "+metadata.currentExpiration()+" to "+refreshPatchMessage.getLifetime(currentIndex));
                currentPhase = phaseStore;
              }
            } else {
              logger.warn("Cannot fetch metadata for key {}, got 'null'", currentKey);
              currentPhase = phaseAdvance;
            }
          } else if (fragmentAndManifest != null) {
            logger.warn("RefreshPatch with invalid signature: {}", currentKey);
            currentPhase = phaseAdvance;
          }
        }

        if (currentPhase == phaseStore) {
          logger.debug("AR Patch: Update completed for {}", currentKey);
          successes[currentIndex] ++;
          currentPhase = phaseAdvance;
        }

        if (currentPhase == phaseAdvance) {
          do {
            currentFID ++;
            if (currentFID >= numFragments) {
              currentFID = 0;
              currentIndex ++;
            }

            if (currentIndex >= refreshPatchMessage.numKeys()) {
              respond();
              return;
            }

            currentKey = new FragmentKey(refreshPatchMessage.getKey(currentIndex), currentFID);
          } while (!fragmentStorage.exists(currentKey));

          currentPhase = phaseFetch;
          logger.debug("AR Patch: Fetching FAM for {}", currentKey);
          Futures.forward(fragmentStorage.getObject(currentKey), this);
        }
      }
      void respond() {
        int totalSuccesses = 0;
        for (int success : successes)
          totalSuccesses += success;

        logger.debug("AR Patch: Sending response ({} updates total)", totalSuccesses);

        router.sendMessage(
          null,
          new GlacierRefreshCompleteMessage(refreshPatchMessage.getUID(), refreshPatchMessage.getAllKeys(), successes, router.getLocalNodeHandle(), refreshPatchMessage.getSource().getId(), refreshPatchMessage.getTag()),
          refreshPatchMessage.getSource()
        );
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Exception while processing AR patch (key {}, phase {}): ", currentKey, currentPhase, e);
        currentPhase = phaseAdvance;
        receiveResult(null);
      }
    };

    continuation.receiveResult(null);
  }
}
