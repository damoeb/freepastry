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
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.p2p.glacier.v2.messaging.GlacierQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.persistence.CachedStorage;

/**
 * Answers the fragment requests of other nodes (queries and fetches), and accepts the fragments and
 * manifests they send for insertion, refresh and sync.
 */
final class GlacierRequestHandler {
  private final int numFragments;
  private final int numSurvivors;
  private final String instance;
  private final GlacierPolicy policy;
  private final GlacierMessageSender router;
  private final FragmentLocator locator;
  private final GlacierNeighborhood neighborhood;
  private final GlacierRetriever retriever;
  private final CachedStorage fragmentStorage;
  private final Environment environment;
  private final Logger logger;

  GlacierRequestHandler(int numFragments, int numSurvivors, String instance, GlacierPolicy policy, GlacierMessageSender router, FragmentLocator locator, GlacierNeighborhood neighborhood, GlacierRetriever retriever, CachedStorage fragmentStorage, Environment environment, Logger logger) {
    this.numFragments = numFragments;
    this.numSurvivors = numSurvivors;
    this.instance = instance;
    this.policy = policy;
    this.router = router;
    this.locator = locator;
    this.neighborhood = neighborhood;
    this.retriever = retriever;
    this.fragmentStorage = fragmentStorage;
    this.environment = environment;
    this.logger = logger;
  }

  /**
   * Answers a query: for each key, whether this node has the fragment, until when, and whether it is
   * responsible for it. Inserts, refreshes and handoffs start with a query.
   */
  void handleQuery(final GlacierQueryMessage queryMessage) {
    FragmentKey[] keys = new FragmentKey[queryMessage.numKeys()];
    boolean[] haveItA = new boolean[queryMessage.numKeys()];
    long[] expirationA = new long[queryMessage.numKeys()];
    boolean[] authoritativeA = new boolean[queryMessage.numKeys()];

    for (int i=0; i<queryMessage.numKeys(); i++) {
      Id fragmentLocation = locator.getFragmentLocation(queryMessage.getKey(i));
      if (logger.isInfoEnabled()) logger.info("Queried for " + queryMessage.getKey(i) + " (at "+fragmentLocation+")");

      keys[i] = queryMessage.getKey(i);
      haveItA[i] = fragmentStorage.exists(queryMessage.getKey(i));
      if (haveItA[i]) {
        FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(queryMessage.getKey(i));
        if (metadata != null) {
          expirationA[i] = metadata.currentExpiration();
        } else {
          if (logger.isWarnEnabled()) logger.warn("QUERY cannot read metadata in object "+queryMessage.getKey(i).toStringFull()+", storage returned null");
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
      new GlacierResponseMessage(queryMessage.getUID(), keys, haveItA, expirationA, authoritativeA, router.getLocalNodeHandle(), queryMessage.getSource().getId(), true, queryMessage.getTag()),
      queryMessage.getSource()
    );
  }

  /**
   * Answers a fetch with the requested fragments and/or manifests from the local fragment storage.
   */
  void handleFetch(final GlacierFetchMessage fetchMessage) {
    logger.atInfo().log(() -> "Fetch request for " + fetchMessage.getKey(0) + ((fetchMessage.getNumKeys()>1) ? (" and " + (fetchMessage.getNumKeys() - 1) + " other keys") : "") + ", request="+fetchMessage.getRequest());

    /* FetchMessages are sent during recovery to retrieve a fragment from
       another node. They are answered from the local fragment storage;
       keys that are not stored here get null entries in the response. */

    Futures.forward(fragmentStorage.getObject(fetchMessage.getKey(0)), new Continuation<Object, Exception>() {
      int currentLookup = 0;
      Fragment fragment[] = new Fragment[fetchMessage.getNumKeys()];
      Manifest manifest[] = new Manifest[fetchMessage.getNumKeys()];
      int numFragments = 0, numManifests = 0;

      void returnResponse() {
        logger.debug("Returning response with {} fragments, {} manifests ({} queries originally)", numFragments, numManifests, fetchMessage.getNumKeys());
        router.sendMessage(
          null,
          new GlacierDataMessage(fetchMessage.getUID(), fetchMessage.getAllKeys(), fragment, manifest, router.getLocalNodeHandle(), fetchMessage.getSource().getId(), true, fetchMessage.getTag()),
          fetchMessage.getSource()
        );
      }
      @Override
      public void receiveResult(Object result) {
        if (result != null) {
          logger.atInfo().log(() -> "Fragment "+fetchMessage.getKey(currentLookup)+" found ("+result+")");
          FragmentAndManifest fragmentAndManifest = (FragmentAndManifest) result;
          fragment[currentLookup] = ((fetchMessage.getRequest() & GlacierFetchMessage.FETCH_FRAGMENT)!=0) ? fragmentAndManifest.getFragment() : null;
          if (fragment[currentLookup] != null)
            numFragments ++;
          manifest[currentLookup] = ((fetchMessage.getRequest() & GlacierFetchMessage.FETCH_MANIFEST)!=0) ? fragmentAndManifest.getManifest() : null;
          if (manifest[currentLookup] != null)
            numManifests ++;
        } else {
          logger.atInfo().log(() -> "Fragment "+fetchMessage.getKey(currentLookup)+" not found");
          fragment[currentLookup] = null;
          manifest[currentLookup] = null;
        }

        nextLookup();
      }
      void nextLookup() {
        currentLookup ++;
        if (currentLookup >= fetchMessage.getNumKeys())
          returnResponse();
        else
          Futures.forward(fragmentStorage.getObject(fetchMessage.getKey(currentLookup)), this);
      }
      @Override
      public void receiveException(Exception e) {
        logger.atWarn().setCause(e).log(() -> "Exception while retrieving fragment "+fetchMessage.getKey(currentLookup)+" (lookup #"+currentLookup+"), e=");
        fragment[currentLookup] = null;
        manifest[currentLookup] = null;
        nextLookup();
      }
    });
  }

  /**
   * Accepts fragments and manifests sent to this node: stores new fragments, replaces manifests that
   * expire earlier, and fetches the fragments it should have when it only got their manifest.
   */
  void handleData(final GlacierDataMessage dataMessage) {
    for (int i=0; i<dataMessage.numKeys(); i++) {
      final FragmentKey thisKey = dataMessage.getKey(i);
      final Fragment thisFragment = dataMessage.getFragment(i);
      final Manifest thisManifest = dataMessage.getManifest(i);

      if ((thisFragment != null) && (thisManifest != null)) {
        logger.info("Data: Fragment+Manifest for {}", thisKey);

        if (!neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey))) {
          logger.atWarn().log(() -> "Not responsible for "+thisKey+" (at "+locator.getFragmentLocation(thisKey)+") -- discarding");
          continue;
        }

        if (!policy.checkSignature(thisManifest, thisKey.versionKey())) {
          logger.warn("Manifest is not signed properly");
          continue;
        }

        if (!thisManifest.validatesFragment(thisFragment, thisKey.fragmentId(), environment.getLogManager().getLogger(Manifest.class, instance))) {
          logger.warn("Manifest does not validate this fragment");
          continue;
        }

        if (!fragmentStorage.exists(thisKey)) {
          logger.debug("Verified ok. Storing locally.");

          FragmentAndManifest fragmentAndManifest = new FragmentAndManifest(thisFragment, thisManifest);

          Futures.forward(fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), 0, environment.getTimeSource().currentTimeMillis()), fragmentAndManifest), new Continuation<Object, Exception>() {
              @Override
              public void receiveResult(Object result) {
                logger.info("Stored OK, sending receipt: {}", thisKey);

                router.sendMessage(
                  null,
                  new GlacierResponseMessage(dataMessage.getUID(), thisKey, true, thisManifest.getExpiration(), neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey)), router.getLocalNodeHandle(), dataMessage.getSource().getId(), true, dataMessage.getTag()),
                  dataMessage.getSource()
                );
              }

              @Override
              public void receiveException(Exception e) {
                logger.warn("receiveException({}) while storing a fragment -- unexpected, ignored (key={})", e, thisKey);
              }
            });
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

        if (!policy.checkSignature(thisManifest, thisKey.versionKey())) {
          logger.warn("Manifest is not signed properly");
          continue;
        }

        /* We are being informed of a fragment that
             a) we should have, but currently don't, or
             b) we already have, but whose manifest will soon expire */

        if (fragmentStorage.exists(thisKey)) {
          final FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(thisKey);
          if ((metadata == null) || (metadata.currentExpiration() < thisManifest.getExpiration())) {
            logger.atInfo().log(() -> "Replacing old manifest for "+thisKey+" (expires "+((metadata == null) ? "(broken)" : ""+metadata.currentExpiration())+") by new one (expires "+thisManifest.getExpiration()+")");

            replaceManifest(dataMessage, thisKey, thisManifest, metadata);
          } else {
            logger.atWarn().log(() -> "We already have exp="+((metadata == null) ? "(broken)" : ""+metadata.currentExpiration())+", discarding manifest for "+thisKey+" with exp="+thisManifest.getExpiration());
          }

          continue;
        }

        logger.info("Data: Manifest for: {}, must fetch", thisKey);

        fetchSyncedFragment(thisKey, thisManifest);

        continue;
      }

      logger.warn("Case not implemented! -- GDM");
    }
  }

  /**
   * Replaces the manifest of a local fragment by a newer one, if the new one matches the fragment, and
   * confirms the update to the sender.
   */
  private void replaceManifest(final GlacierDataMessage dataMessage, final FragmentKey thisKey, final Manifest thisManifest, final FragmentMetadata metadata) {
    Futures.forward(fragmentStorage.getObject(thisKey), new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object received) {
        if (received instanceof FragmentAndManifest fragmentAndManifest) {

          logger.debug("Got FAM for {}, now replacing old manifest with new one...", thisKey);

          String fault = null;

          if (!thisManifest.validatesFragment(fragmentAndManifest.getFragment(), thisKey.fragmentId(), environment.getLogManager().getLogger(Manifest.class, instance)))
            fault = "Update: Manifest does not validate this fragment";
          if (!policy.checkSignature(thisManifest, thisKey.versionKey()))
            fault = "Update: Manifest is not signed properly";
          if (!Arrays.equals(thisManifest.getObjectHash(), fragmentAndManifest.getManifest().getObjectHash()))
            fault = "Update: Object hashes not equal";
          for (int i=0; i<numFragments; i++)
            if (!Arrays.equals(thisManifest.getFragmentHash(i), fragmentAndManifest.getManifest().getFragmentHash(i)))
              fault = "Update: Fragment hash #"+i+" does not match";

          if (fault == null) {
            fragmentAndManifest.replaceManifest(thisManifest);
            Futures.forward(fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), ((metadata == null) ? 0 : metadata.currentExpiration()), environment.getTimeSource().currentTimeMillis()), fragmentAndManifest), new Continuation<Object, Exception>() {
                @Override
                public void receiveResult(Object result) {
                  logger.debug("Old manifest for {} replaced OK, sending receipt", thisKey);
                  router.sendMessage(
                    null,
                    new GlacierResponseMessage(dataMessage.getUID(), thisKey, true, thisManifest.getExpiration(), true, router.getLocalNodeHandle(), dataMessage.getSource().getId(), true, dataMessage.getTag()),
                    dataMessage.getSource()
                  );
                }
                @Override
                public void receiveException(Exception e) {
                  logger.warn("Cannot store refreshed manifest: ", e);
                }
              });
          } else {
            logger.warn(fault);
          }
        } else {
          logger.warn("Fragment store returns something other than a FAM: {}", received);
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Cannot retrieve FAM for {}: ", thisKey, e);
      }
    });
  }

  /**
   * Fetches a fragment this node should have and only got the manifest for, through the traffic shaper,
   * and stores it.
   */
  private void fetchSyncedFragment(final FragmentKey thisKey, final Manifest thisManifest) {
    retriever.rateLimitedRetrieveFragment(thisKey, thisManifest, GlacierTags.SYNC_FETCH, new GlacierContinuation() {
      @Override
      public String toString() {
        return "Fetch synced fragment: "+thisKey;
      }
      @Override
      public void receiveResult(Object received) {
        if (received instanceof Fragment fragment) {
          if (!fragmentStorage.exists(thisKey)) {
            logger.info("Received fragment {} (from primary) matches existing manifest, storing...", thisKey);

            FragmentAndManifest fragmentAndManifest = new FragmentAndManifest(fragment, thisManifest);

            Futures.forward(fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), 0, environment.getTimeSource().currentTimeMillis()), fragmentAndManifest), new Continuation<Object, Exception>() {
                @Override
                public void receiveResult(Object result) {
                  logger.debug("Recovered fragment stored OK");
                }
                @Override
                public void receiveException(Exception e) {
                  logger.warn("receiveException({}) while storing a fragment with existing manifest (key={})", e, thisKey);
                }
              });
          } else {
            logger.warn("Received fragment {}, but it already exists in the fragment store", thisKey);
          }
        } else {
          logger.warn("FS received something other than a fragment: {}", received);
        }
      }
      @Override
      public void receiveException(Exception e) {
        if (e instanceof GlacierNotEnoughFragmentsException notEnoughFragments) {
          logger.atInfo().log(() -> "Not enough fragments to reconstruct "+thisKey+": "+notEnoughFragments.getChecked()+"/"+numFragments+" checked, "+notEnoughFragments.getFound()+" found, "+numSurvivors+" needed");
        } else {
          logger.warn("Exception while recovering synced fragment {}: ", thisKey, e);
        }

        terminate();
      }
      @Override
      public void timeoutExpired() {
        logger.warn("Timeout while fetching synced fragment {} -- aborted", thisKey);
        terminate();
      }
    });
  }
}
