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
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.persistence.CachedStorage;

/**
 * The handoff protocol: fragments that are stored here but now belong to another node's range are sent
 * to that node and deleted once it has them; fragments handed off to this node are verified and stored.
 */
final class GlacierHandoffProtocol {
  private final GlacierConfig config;
  private final String instance;
  private final GlacierPolicy policy;
  private final GlacierMessageSender router;
  private final FragmentLocator locator;
  private final GlacierNeighborhood neighborhood;
  private final GlacierTrash trash;
  private final CachedStorage fragmentStorage;
  private final Environment environment;
  private final Logger logger;

  GlacierHandoffProtocol(GlacierConfig config, String instance, GlacierPolicy policy, GlacierMessageSender router, FragmentLocator locator, GlacierNeighborhood neighborhood, GlacierTrash trash, CachedStorage fragmentStorage, Environment environment, Logger logger) {
    this.config = config;
    this.instance = instance;
    this.policy = policy;
    this.router = router;
    this.locator = locator;
    this.neighborhood = neighborhood;
    this.trash = trash;
    this.fragmentStorage = fragmentStorage;
    this.environment = environment;
    this.logger = logger;
  }

  /**
   * @return the periodic task that offers the fragments this node is no longer responsible for to
   *         their new holders, and deletes them once a holder has confirmed it has them
   */
  GlacierContinuation newHandoffTask() {
    return new GlacierContinuation() {
      long nextTimeout;

      @Override
      public String toString() {
        return "Handoff continuation";
      }
      @Override
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.handoffDelayAfterJoin();
      }
      @Override
      public void receiveResult(Object result) {
        if (result instanceof GlacierResponseMessage glacierResponseMessage) {
          handleResponse(glacierResponseMessage);
        } else {
          logger.warn("Unexpected response in handoff continuation: {} -- ignored", result);
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Exception in handoff continuation: ", e);
      }
      @Override
      public void timeoutExpired() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.jitterTerm(config.handoffInterval(), environment.getRandomSource());
        logger.info("Checking fragment storage for fragments to hand off...");
        logger.debug("Currently responsible for: {}", neighborhood.getResponsibleRange());
        Iterator<Id> iter = fragmentStorage.scan().getIterator();
        List<Id> handoffs = new ArrayList<Id>();
        Id destination = null;

        while (iter.hasNext()) {
          FragmentKey fragmentKey = (FragmentKey) iter.next();
          Id thisPos = locator.getFragmentLocation(fragmentKey);
          if (!neighborhood.getResponsibleRange().containsId(thisPos)) {
            logger.debug("Must hand off {} @{}", fragmentKey, thisPos);
            handoffs.add(fragmentKey);

            if (handoffs.size() >= config.handoffMaxFragments()) {
              logger.debug("Limit of {} reached for handoff", config.handoffMaxFragments());
              break;
            }

            if (destination == null)
              destination = thisPos;
          }
        }

        if (destination == null) {
          logger.debug("Nothing to hand off -- returning");
          return;
        }

        int numHandoffs = Math.min(handoffs.size(), config.handoffMaxFragments());
        logger.info("Handing off {} fragments (out of {})", numHandoffs, handoffs.size());
        FragmentKey[] keys = new FragmentKey[numHandoffs];
        for (int i=0; i<numHandoffs; i++)
          keys[i] = (FragmentKey) handoffs.get(i);

        router.sendMessage(
          destination,
          new GlacierQueryMessage(getMyUID(), keys, router.getLocalNodeHandle(), destination, GlacierTags.HANDOFF),
          null
        );
      }
      @Override
      public long getTimeout() {
        return nextTimeout;
      }
    };
  }

  /**
   * Hands off the fragments the new holder does not have yet, and deletes those it has.
   */
  private void handleResponse(final GlacierResponseMessage responseMessage) {
    logger.atDebug().log(() -> "Received handoff response from "+responseMessage.getSource().getId()+" with "+responseMessage.numKeys()+" keys");
    for (int i=0; i<responseMessage.numKeys(); i++) {
      final FragmentKey thisKey = responseMessage.getKey(i);
      if (responseMessage.getAuthoritative(i)) {
        if (responseMessage.hasFragment(i)) {
          Id thisPos = locator.getFragmentLocation(thisKey);
          if (!neighborhood.getResponsibleRange().containsId(thisPos)) {
            logger.debug("Deleting fragment {}", thisKey);
            deleteHandedOffFragment(thisKey);
          } else {
            logger.warn("Handoff response for {}, for which I am still responsible (attack?) -- ignored", thisKey);
          }
        } else {
          sendFragment(responseMessage, thisKey);
        }
      } else {
        if (logger.isDebugEnabled()) logger.debug("Ignoring fragment "+thisKey+" (haveIt="+responseMessage.hasFragment(i)+", authoritative="+responseMessage.getAuthoritative(i)+")");
      }
    }
  }

  private void deleteHandedOffFragment(final FragmentKey thisKey) {
    trash.deleteFragment(thisKey, new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        logger.debug("Handed off fragment deleted: {} (o={})", thisKey, result);
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Delete failed during handoff: {}, returned ", thisKey, e);
      }
    });
  }

  private void sendFragment(final GlacierResponseMessage responseMessage, final FragmentKey thisKey) {
    Futures.forward(fragmentStorage.getObject(thisKey), new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        if (result != null) {
          logger.info("Fragment {} found ({}), handing off...", thisKey, result);
          FragmentAndManifest fragmentAndManifest = (FragmentAndManifest) result;
          router.sendMessage(
            null,
            new GlacierDataMessage(responseMessage.getUID(), thisKey, fragmentAndManifest.getFragment(), fragmentAndManifest.getManifest(), router.getLocalNodeHandle(), responseMessage.getSource().getId(), false, GlacierTags.HANDOFF),
            responseMessage.getSource()
          );
        } else {
          logger.warn("Handoff failed; fragment {} not found in fragment store", thisKey);
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("Handoff failed; exception while fetching {}, e=", thisKey, e);
      }
    });
  }

  /**
   * Stores the fragments handed off to this node, and confirms each one to the sender. The data message is a
   * request, not a response: the sender's continuation UID means nothing in this node's scheduler.
   */
  void acceptFragments(final GlacierDataMessage dataMessage) {
    for (int i=0; i<dataMessage.numKeys(); i++) {
      final FragmentKey thisKey = dataMessage.getKey(i);
      final Fragment thisFragment = dataMessage.getFragment(i);
      final Manifest thisManifest = dataMessage.getManifest(i);

      if ((thisFragment != null) && (thisManifest != null)) {
        logger.info("Handoff: Received Fragment+Manifest for {}", thisKey);

        if (!neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey))) {
          logger.atWarn().log(() -> "Handoff: Not responsible for "+thisKey+" (at "+locator.getFragmentLocation(thisKey)+") -- discarding");
          continue;
        }

        if (!policy.checkSignature(thisManifest, thisKey.versionKey())) {
          logger.warn("Handoff: Manifest is not signed properly");
          continue;
        }

        if (!thisManifest.validatesFragment(thisFragment, thisKey.fragmentId(), environment.getLogManager().getLogger(Manifest.class, instance))) {
          logger.warn("Handoff: Manifest does not validate this fragment");
          continue;
        }

        if (!fragmentStorage.exists(thisKey)) {
          logger.debug("Handoff: Verified ok. Storing locally.");

          storeHandedOffFragment(dataMessage, thisKey, thisFragment, thisManifest);
        } else {
          logger.warn("Handoff: We already have a fragment with this key! -- sending response");
          router.sendMessage(
            null,
            new GlacierResponseMessage(dataMessage.getUID(), thisKey, true, thisManifest.getExpiration(), true, router.getLocalNodeHandle(), dataMessage.getSource().getId(), true, GlacierTags.HANDOFF),
            dataMessage.getSource()
          );

        }

      } else {
        logger.warn("Handoff: Either fragment or manifest are missing!");
      }
    }
  }

  private void storeHandedOffFragment(final GlacierDataMessage dataMessage, final FragmentKey thisKey, Fragment thisFragment, final Manifest thisManifest) {
    FragmentAndManifest fragmentAndManifest = new FragmentAndManifest(thisFragment, thisManifest);

    Futures.forward(fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), 0, environment.getTimeSource().currentTimeMillis()), fragmentAndManifest), new Continuation<Object, Exception>() {
        @Override
        public void receiveResult(Object result) {
          logger.info("Handoff: Stored OK, sending receipt: {}", thisKey);

          router.sendMessage(
            null,
            new GlacierResponseMessage(dataMessage.getUID(), thisKey, true, thisManifest.getExpiration(), neighborhood.getResponsibleRange().containsId(locator.getFragmentLocation(thisKey)), router.getLocalNodeHandle(), dataMessage.getSource().getId(), true, GlacierTags.HANDOFF),
            dataMessage.getSource()
          );
        }

        @Override
        public void receiveException(Exception e) {
          logger.warn("Handoff: receiveException({}) while storing a fragment -- unexpected, ignored (key={})", e, thisKey);
        }
      });
  }
}
