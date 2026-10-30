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
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeForwardMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierSyncMessage;
import rice.persistence.StorageManager;

/**
 * The sync protocol, which lets a node recover the fragments it should hold: it periodically picks a
 * random fragment offset, finds the nodes responsible for the peer range (range query, range forward,
 * range response), and sends them a bloom filter of its fragments; they answer with the manifests of
 * the fragments it lacks, which it then fetches.
 */
final class GlacierSyncProtocol {
  private final GlacierConfig config;
  private final int numFragments;
  private final GlacierPolicy policy;
  private final Endpoint endpoint;
  private final GlacierScheduler scheduler;
  private final GlacierRouter router;
  private final FragmentLocator locator;
  private final GlacierNeighborhood neighborhood;
  private final GlacierTrafficShaper trafficShaper;
  private final StorageManager fragmentStorage;
  private final IdFactory factory;
  private final Environment environment;
  private final Logger logger;
  private long syncInterval;
  private int syncMaxFragments;

  GlacierSyncProtocol(GlacierConfig config, int numFragments, GlacierPolicy policy, Endpoint endpoint, GlacierScheduler scheduler, GlacierRouter router, FragmentLocator locator, GlacierNeighborhood neighborhood, GlacierTrafficShaper trafficShaper, StorageManager fragmentStorage, IdFactory factory, Environment environment, Logger logger) {
    this.config = config;
    this.numFragments = numFragments;
    this.policy = policy;
    this.endpoint = endpoint;
    this.scheduler = scheduler;
    this.router = router;
    this.locator = locator;
    this.neighborhood = neighborhood;
    this.trafficShaper = trafficShaper;
    this.fragmentStorage = fragmentStorage;
    this.factory = factory;
    this.environment = environment;
    this.logger = logger;
    this.syncInterval = config.syncInterval();
    this.syncMaxFragments = config.syncMaxFragments();
  }

  long getSyncInterval() {
    return syncInterval;
  }

  void setSyncInterval(long syncInterval) {
    this.syncInterval = syncInterval;
  }

  int getSyncMaxFragments() {
    return syncMaxFragments;
  }

  void setSyncMaxFragments(int syncMaxFragments) {
    this.syncMaxFragments = syncMaxFragments;
  }

  /**
   * @return the periodic task that asks the holders of a random peer fragment range which fragments
   *         this node is missing
   */
  GlacierContinuation newSyncTask() {
    return new GlacierContinuation() {
      long nextTimeout;
      int offset;

      public String toString() {
        return "Sync continuation";
      }
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.syncDelayAfterJoin();
      }
      public void receiveResult(Object o) {
        if (o instanceof GlacierRangeResponseMessage) {
          final GlacierRangeResponseMessage grrm = (GlacierRangeResponseMessage) o;

          Id ccwId = locator.getFragmentLocation(grrm.getCommonRange().getCCWId(), numFragments-offset, 0);
          Id cwId = locator.getFragmentLocation(grrm.getCommonRange().getCWId(), numFragments-offset, 0);
          final IdRange originalRange = factory.buildIdRange(ccwId, cwId);

          logger.info("Range response (offset: {}): {}, original={}", offset, grrm.getCommonRange(), originalRange);

          final IdSet keySet = fragmentStorage.scan();
          endpoint.process(() -> {
            BloomFilter bv = new BloomFilter((2*keySet.numElements()+5)*config.syncBloomFilterBitsPerKey(), config.syncBloomFilterNumHashes(), environment.getRandomSource());
            Iterator<Id> iter = keySet.getIterator();

            while (iter.hasNext()) {
              FragmentKey fkey = (FragmentKey)iter.next();
              Id thisPos = locator.getFragmentLocation(fkey);
              if (originalRange.containsId(thisPos)) {
                FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(fkey);
                if (metadata != null) {
                  long currentExp = metadata.getCurrentExpiration();
                  long prevExp = metadata.getPreviousExpiration();
                  logger.trace(" - Adding {} as {}, ecur={}, eprev={}", fkey, fkey.getVersionKey().getId(), currentExp, prevExp);
                  bv.add(hashInput(fkey.getVersionKey(), currentExp));
                  bv.add(hashInput(fkey.getVersionKey(), prevExp));
                } else {
                  logger.atWarn().log(() -> "SYNC Cannot read metadata of object "+fkey.toStringFull()+", storage returned null");
                }
              }
            }

            return bv;
          }, new Continuation<BloomFilter, Exception>() {
            public void receiveResult(BloomFilter o) {
              if (o instanceof BloomFilter) {
                BloomFilter bv = (BloomFilter) o;
                logger.debug("Got {}", bv);
                logger.atInfo().log(() -> keySet.numElements()+" keys added, sending sync request...");

                router.sendMessage(
                  null,
                  new GlacierSyncMessage(scheduler.getUID(), grrm.getCommonRange(), offset, bv, router.getLocalNodeHandle(), grrm.getSource().getId(), GlacierTags.SYNC),
                  grrm.getSource()
                );
              } else {
                logger.warn("While processing range response: Result is of unknown type: {} -- discarding request", o);
              }
            }
            public void receiveException(Exception e) {
              logger.warn("Exception while processing range response: {} -- discarding request", e, e);
            }
          });
        } else {
          logger.warn("Unknown result in sync continuation: {} -- discarded", o);
        }
      }
      public void receiveException(Exception e) {
        logger.warn("Exception in sync continuation: ", e);
        terminate();
      }
      public void timeoutExpired() {
        if (trafficShaper.getActiveRestores() > 0) {
          logger.info("Sync postponed; {} fetches pending", trafficShaper.getActiveRestores());
          nextTimeout = environment.getTimeSource().currentTimeMillis() + config.jitterTerm(config.syncRetryInterval(), environment.getRandomSource());
        } else {
          nextTimeout = environment.getTimeSource().currentTimeMillis() + config.jitterTerm(syncInterval, environment.getRandomSource());
          offset = 1+environment.getRandomSource().nextInt(numFragments-1);

          Id dest = locator.getFragmentLocation(router.getLocalNodeHandle().getId(), offset, 0);
          Id ccwId = locator.getFragmentLocation(neighborhood.getResponsibleRange().getCCWId(), offset, 0);
          Id cwId = locator.getFragmentLocation(neighborhood.getResponsibleRange().getCWId(), offset, 0);
          IdRange requestedRange = factory.buildIdRange(ccwId, cwId);

          logger.info("Sending range query for ({}) to {}", requestedRange, dest);
          router.sendMessage(
            dest,
            new GlacierRangeQueryMessage(getMyUID(), requestedRange, router.getLocalNodeHandle(), dest, GlacierTags.SYNC),
            null
          );
        }
      }
      public long getTimeout() {
        return nextTimeout;
      }
    };
  }

  /**
   * Compares the requester's bloom filter with the local fragments in the requested range, and sends the
   * manifests of the fragments the requester is missing.
   */
  void handleSyncRequest(final GlacierSyncMessage gsm) {
    logger.info("SyncRequest from {} for {} offset {}", gsm.getSource().getId(), gsm.getRange(), gsm.getOffsetFID());
    logger.atDebug().log(() -> "Contains "+gsm.getBloomFilter());

    Iterator<Id> iter = fragmentStorage.scan().getIterator();
    final IdRange range = gsm.getRange();
    final int offset = gsm.getOffsetFID();
    final BloomFilter bv = gsm.getBloomFilter();
    final long earliestAcceptableExpiration = environment.getTimeSource().currentTimeMillis() + config.syncMinRemainingLifetime();
    final long latestAcceptableStoredSince = environment.getTimeSource().currentTimeMillis() - config.syncMinQuietTime();

    final List<Id> missing = new ArrayList<Id>();

    while (iter.hasNext()) {
      FragmentKey fkey = (FragmentKey)iter.next();
      Id thisPos = locator.getFragmentLocation(fkey);
      if (range.containsId(thisPos)) {
        FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(fkey);
        if (metadata != null) {
          if (!bv.contains(hashInput(fkey.getVersionKey(), metadata.getCurrentExpiration()))) {
            if (metadata.getCurrentExpiration() >= earliestAcceptableExpiration) {
              if (metadata.getStoredSince() <= latestAcceptableStoredSince) {
                logger.trace("{} @{} - MISSING", fkey, thisPos);
                missing.add(fkey);
                if (missing.size() >= syncMaxFragments) {
                  logger.info("Limit of {} missing fragments reached", syncMaxFragments);
                  break;
                }
              } else {
                logger.atDebug().log(() -> fkey+" @"+thisPos+" - TOO FRESH (stored "+(environment.getTimeSource().currentTimeMillis()-metadata.getStoredSince())+"ms)");
              }
            } else {
              logger.atDebug().log(() -> fkey+" @"+thisPos+" - EXPIRES SOON (in "+(metadata.getCurrentExpiration()-environment.getTimeSource().currentTimeMillis())+"ms)");
            }
          } else {
            logger.trace("{} @{} - OK", fkey, thisPos);
          }
        } else {
          logger.atWarn().log(() -> "SYNC RESPONSE cannot read metadata in object "+fkey.toStringFull()+", storage returned null");
        }
      } else logger.trace("{} @{} - OUT OF RANGE", fkey, thisPos);
    }

    if (missing.isEmpty()) {
      logger.info("No fragments missing. OK. ");
      return;
    }

    logger.info("Sending {} fragments to {}", missing.size(), gsm.getSource().getId());

    fragmentStorage.getObject(missing.get(0), new Continuation<Object, Exception>() {
      int currentLookup = 0;
      int manifestIndex = 0;
      final int numLookups = missing.size();
      Manifest[] manifests = new Manifest[Math.min(numLookups, config.manifestAggregationFactor())];
      Fragment[] fragments = new Fragment[Math.min(numLookups, config.manifestAggregationFactor())];
      FragmentKey[] keys = new FragmentKey[Math.min(numLookups, config.manifestAggregationFactor())];

      public void receiveResult(Object o) {
        final FragmentKey thisKey = (FragmentKey) missing.get(currentLookup);

        if (o == null) {
          logger.warn("SYN2: Fragment {} not found -- canceled SYN", thisKey);
          return;
        }

        logger.debug("Retrieved manifest {} (dest={}, offset={})", thisKey, gsm.getSource().getId(), offset);

        FragmentAndManifest fam = (FragmentAndManifest) o;

        if (!policy.checkSignature(fam.getManifest(), thisKey.getVersionKey()))
          panic("Signature mismatch!!");

        fragments[manifestIndex] = null;
        manifests[manifestIndex] = fam.getManifest();
        int hisFID = thisKey.getFragmentID() - offset;
        if (hisFID < 0)
          hisFID += numFragments;
        if (hisFID >= numFragments)
          panic("Assertion failed: L938");
        keys[manifestIndex] = new FragmentKey(thisKey.getVersionKey(), hisFID);
        logger.atDebug().log(() -> "He should have key "+keys[manifestIndex]+" @"+locator.getFragmentLocation(keys[manifestIndex]));
        manifestIndex ++;
        currentLookup ++;
        if ((manifestIndex == config.manifestAggregationFactor()) || (currentLookup == numLookups)) {
          logger.debug("Sending a packet with {} manifests to {}", keys.length, gsm.getSource().getId());

          router.sendMessage(
            null,
            new GlacierDataMessage(scheduler.getUID(), keys, fragments, manifests, router.getLocalNodeHandle(), gsm.getSource().getId(), false, GlacierTags.SYNC_MANIFESTS),
            gsm.getSource()
          );

          manifestIndex = 0;
          manifests = new Manifest[Math.min(numLookups - currentLookup, config.manifestAggregationFactor())];
          keys = new FragmentKey[Math.min(numLookups - currentLookup, config.manifestAggregationFactor())];
          fragments = new Fragment[Math.min(numLookups - currentLookup, config.manifestAggregationFactor())];
        }

        if (currentLookup < numLookups)
          fragmentStorage.getObject(missing.get(currentLookup), this);
      }
      public void receiveException(Exception e) {
        logger.atWarn().log(() -> "SYN2: Exception while retrieving fragment "+missing.get(currentLookup)+", e="+e+" -- canceled SYN");
      }
    });
  }

  /**
   * Forwards a range query to each neighbor whose range intersects the requested one.
   */
  void handleRangeQuery(final GlacierRangeQueryMessage grqm) {
    IdRange requestedRange = grqm.getRequestedRange();

    logger.info("Range query for {}", requestedRange);

    Id[][] ranges = neighborhood.getNeighborRanges();

    for (int i=0; i<ranges.length; i++) {
      IdRange thisRange = factory.buildIdRange(ranges[i][0], ranges[i][2]);
      IdRange intersectRange = requestedRange.intersectRange(thisRange);
      if (!intersectRange.isEmpty()) {
        logger.debug("     - Intersects: {}, sending RangeForward", intersectRange);
        router.sendMessage(
          ranges[i][1],
          new GlacierRangeForwardMessage(grqm.getUID(), requestedRange, grqm.getSource(), router.getLocalNodeHandle(), ranges[i][1], grqm.getTag()),
          null
        );
      }
    }

    logger.debug("Finished processing range query");
  }

  /**
   * Answers a forwarded range query with the part of the requested range this node is responsible for.
   */
  void handleRangeForward(final GlacierRangeForwardMessage grfm) {
    if (!grfm.getDestination().equals(router.getLocalNodeHandle().getId())) {
      logger.warn("Not for us (dest={}, we={})", grfm.getDestination(), router.getLocalNodeHandle().getId());
      return;
    }

    IdRange commonRange = neighborhood.getResponsibleRange().intersectRange(grfm.getRequestedRange());
    if (!commonRange.isEmpty()) {
      logger.info("Range forward: Returning common range {} to requestor {}", commonRange, grfm.getRequestor());
      router.sendMessage(
        null,
        new GlacierRangeResponseMessage(grfm.getUID(), commonRange, router.getLocalNodeHandle(), grfm.getRequestor().getId(), grfm.getTag()),
        grfm.getRequestor()
      );
    } else {
      logger.warn("Received GRFM by {}, but no common range??!? -- ignored", grfm.getRequestor());
    }
  }

  /**
   * @return the bloom filter key of one version of an object with the given expiration
   */
  static byte[] hashInput(VersionKey vkey, long expiration) {
    byte[] a = vkey.toByteArray();
    byte[] b = new byte[a.length + 8];
    for (int i=0; i<a.length; i++)
      b[i] = a[i];

    b[a.length + 0] = (byte)(0xFF & (expiration>>56));
    b[a.length + 1] = (byte)(0xFF & (expiration>>48));
    b[a.length + 2] = (byte)(0xFF & (expiration>>40));
    b[a.length + 3] = (byte)(0xFF & (expiration>>32));
    b[a.length + 4] = (byte)(0xFF & (expiration>>24));
    b[a.length + 5] = (byte)(0xFF & (expiration>>16));
    b[a.length + 6] = (byte)(0xFF & (expiration>>8));
    b[a.length + 7] = (byte)(0xFF & (expiration));

    return b;
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
