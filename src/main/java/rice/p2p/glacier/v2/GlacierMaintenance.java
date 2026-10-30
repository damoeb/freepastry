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

import java.util.Iterator;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdSet;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.persistence.StorageManager;

/**
 * Local maintenance of the fragment storage: garbage collection of expired fragments, and the local scan
 * that recovers missing fragments of objects this node already holds part of.
 */
final class GlacierMaintenance {
  private final GlacierConfig config;
  private final int numFragments;
  private final FragmentLocator locator;
  private final GlacierNeighborhood neighborhood;
  private final GlacierTrash trash;
  private final GlacierRetriever retriever;
  private final StorageManager fragmentStorage;
  private final Environment environment;
  private final Logger logger;

  GlacierMaintenance(GlacierConfig config, int numFragments, FragmentLocator locator, GlacierNeighborhood neighborhood, GlacierTrash trash, GlacierRetriever retriever, StorageManager fragmentStorage, Environment environment, Logger logger) {
    this.config = config;
    this.numFragments = numFragments;
    this.locator = locator;
    this.neighborhood = neighborhood;
    this.trash = trash;
    this.retriever = retriever;
    this.fragmentStorage = fragmentStorage;
    this.environment = environment;
    this.logger = logger;
  }

  /**
   * @return the periodic task that deletes expired fragments
   */
  GlacierContinuation newGarbageCollectionTask() {
    return new GlacierContinuation() {
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
    };
  }

  /**
   * @return the periodic task that looks for objects of which this node holds some fragments but is
   *         missing others it is responsible for, and fetches those
   */
  GlacierContinuation newLocalScanTask() {
    return new GlacierContinuation() {
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

            fetchMissingFragments(thisVKey, localFragmentID, fragments);
          }

          logger.info("{} queries sent after local scan", queriesSent);
        } else {
          logger.info("Local scan completed; no missing fragments");
        }
      }
    };
  }

  /**
   * Reads the manifest of an incomplete object from a local fragment, and fetches the fragments of it
   * this node is responsible for but does not have.
   */
  private void fetchMissingFragments(final VersionKey thisVKey, int localFragmentID, final IdSet fragments) {
    fragmentStorage.getObject(new FragmentKey(thisVKey, localFragmentID), new Continuation<Object, Exception>() {
      public void receiveResult(Object o) {
        if (o instanceof FragmentAndManifest) {
          final Manifest thisManifest = ((FragmentAndManifest)o).getManifest();

          for (int i=0; i<numFragments; i++) {
            final FragmentKey thisKey = new FragmentKey(thisVKey, i);
            if (neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey))) {
              if (!fragments.isMemberId(thisKey)) {
                logger.debug("Local scan: Sending query for {}", thisKey);
                recoverFragment(thisKey, thisManifest);
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

  /**
   * Fetches one missing fragment through the traffic shaper, and stores it with the existing manifest.
   */
  private void recoverFragment(final FragmentKey thisKey, final Manifest thisManifest) {
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
