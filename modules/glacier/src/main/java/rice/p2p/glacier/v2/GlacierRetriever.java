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
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.GlacierException;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.p2p.past.PastContent;
import rice.p2p.past.rawserialization.PastContentDeserializer;

/**
 * Restores data from other nodes: the manifest of an object, a whole object from enough of its
 * fragments, or a single fragment (from the trash, its primary holder, or by restoring and re-encoding
 * the object). Adapts the fragment request timeout to how fast fragments arrive.
 */
final class GlacierRetriever {
  private final GlacierConfig config;
  private final int numFragments;
  private final int numSurvivors;
  private final String instance;
  private final GlacierPolicy policy;
  private final Endpoint endpoint;
  private final GlacierContinuationRegistry scheduler;
  private final GlacierMessageSender router;
  private final FragmentLocator locator;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierTrash trash;
  private final Environment environment;
  private final Logger logger;
  private long currentFragmentRequestTimeout;
  private PastContentDeserializer contentDeserializer;

  GlacierRetriever(GlacierConfig config, int numFragments, int numSurvivors, String instance, GlacierPolicy policy, Endpoint endpoint, GlacierContinuationRegistry scheduler, GlacierMessageSender router, FragmentLocator locator, GlacierTrafficShaper trafficShaper, GlacierTrash trash, Environment environment, Logger logger) {
    this.config = config;
    this.numFragments = numFragments;
    this.numSurvivors = numSurvivors;
    this.instance = instance;
    this.policy = policy;
    this.endpoint = endpoint;
    this.scheduler = scheduler;
    this.router = router;
    this.locator = locator;
    this.trafficShaper = trafficShaper;
    this.trash = trash;
    this.environment = environment;
    this.logger = logger;
    this.currentFragmentRequestTimeout = config.fragmentRequestTimeoutDefault();
    this.contentDeserializer = PastContentDeserializer.NONE;
  }

  void setContentDeserializer(PastContentDeserializer deserializer) {
    contentDeserializer = deserializer;
  }

  void retrieveManifest(final VersionKey key, final char tag, final Continuation<? super Manifest, Exception> command) {
    logger.debug("retrieveManifest(key={} tag={})", key, tag);
    scheduler.addContinuation(new GlacierContinuation() {
      private boolean checkedFragment[];
      private long timeout;

      @Override
      public String toString() {
        return "retrieveManifest("+key+")";
      }
      @Override
      public void init() {
        checkedFragment = new boolean[numFragments];
        Arrays.fill(checkedFragment, false);
        timeout = environment.getTimeSource().currentTimeMillis() + config.manifestRequestTimeout();
        for (int i=0; i<config.manifestRequestInitialBurst(); i++)
          sendRandomRequest();
      }
      int numCheckedFragments() {
        int result = 0;
        for (int i=0; i<checkedFragment.length; i++)
          if (checkedFragment[i])
            result ++;
        return result;
      }
      void sendRandomRequest() {
        if (numCheckedFragments() >= numFragments)
          return;

        int nextID;
        do {
          nextID = environment.getRandomSource().nextInt(numFragments);
        } while (checkedFragment[nextID]);

        checkedFragment[nextID] = true;
        FragmentKey nextKey = new FragmentKey(key, nextID);
        Id nextLocation = locator.getFragmentLocation(nextKey);
        logger.debug("retrieveManifest: Asking {} for {}", nextLocation, nextKey);
        router.sendMessage(
          nextLocation,
          new GlacierFetchMessage(getMyUID(), nextKey, GlacierFetchMessage.FETCH_MANIFEST, router.getLocalNodeHandle(), nextLocation, tag),
          null
        );
      }
      @Override
      public void receiveResult(Object result) {
        if (result instanceof GlacierDataMessage dataMessage) {

          if ((dataMessage.numKeys() > 0) && (dataMessage.getManifest(0) != null)) {
            logger.debug("retrieveManifest({}) received manifest", key);
            if (policy.checkSignature(dataMessage.getManifest(0), key)) {
              command.receiveResult(dataMessage.getManifest(0));
              terminate();
            } else {
              logger.atWarn().log(() -> "retrieveManifest("+key+"): invalid signature in "+dataMessage.getKey(0));
            }
          } else logger.warn("retrieveManifest({}) retrieved GDM without a manifest?!?", key);
        } else if (result instanceof GlacierResponseMessage glacierResponseMessage) {
          logger.atDebug().log(() -> "retrieveManifest("+key+"): Fragment not available:" + glacierResponseMessage.getKey(0));
          if (numCheckedFragments() < numFragments) {
            sendRandomRequest();
          } else {
            logger.warn("retrieveManifest({}): giving up", key);
            command.receiveResult(null);
            terminate();
          }
        } else {
          logger.warn("retrieveManifest({}) received unexpected object: {}", key, result);
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("retrieveManifest({}) received exception: ", key, e);
      }
      @Override
      public void timeoutExpired() {
        logger.atDebug().log(() -> "retrieveManifest("+key+"): Timeout ("+numCheckedFragments()+" fragments checked)");
        if (numCheckedFragments() < numFragments) {
          logger.debug("retrying...");
          for (int i=0; i<config.manifestRequestRetryBurst(); i++)
            sendRandomRequest();
          timeout += config.manifestRequestTimeout();
        } else {
          logger.warn("retrieveManifest({}): giving up", key);
          terminate();
          command.receiveResult(null);
        }
      }
      @Override
      public long getTimeout() {
        return timeout;
      }
    });
  }

  void retrieveObject(final VersionKey key, final Manifest manifest, final boolean beStrict, final char tag, final Continuation<? super PastContent, Exception> continuation) {
    scheduler.addContinuation(new GlacierContinuation() {
      private boolean checkedFragment[];
      private Fragment haveFragment[];
      private int attemptsLeft;
      private long timeout;

      int numHaveFragments() {
        int result = 0;
        for (int i=0; i<haveFragment.length; i++)
          if (haveFragment[i] != null)
            result ++;
        return result;
      }
      int numCheckedFragments() {
        int result = 0;
        for (int i=0; i<checkedFragment.length; i++)
          if (checkedFragment[i])
            result ++;
        return result;
      }
      @Override
      public String toString() {
        return "retrieveObject("+key+")";
      }
      @Override
      public void init() {
        trafficShaper.restoreStarted();

        checkedFragment = new boolean[numFragments];
        haveFragment = new Fragment[numFragments];
        for (int i = 0; i < numFragments; i++) {
          checkedFragment[i] = false;
          haveFragment[i] = null;
        }
        timeout = environment.getTimeSource().currentTimeMillis();
        attemptsLeft = config.restoreMaxBoosts();
        timeoutExpired();
      }
      private void localTerminate() {
        trafficShaper.restoreFinished();

        terminate();
      }
      @Override
      public void receiveResult(Object result) {
        if (result instanceof GlacierDataMessage dataMessage) {
          int fragmentID = dataMessage.getKey(0).fragmentId();

          if (!dataMessage.getKey(0).versionKey().equals(key) || (fragmentID<0) || (fragmentID>=numFragments)) {
            logger.atWarn().log(() -> "retrieveObject: Bad data message (contains "+dataMessage.getKey(0)+", expected "+key);
            return;
          }

          Fragment thisFragment = dataMessage.getFragment(0);
          if (thisFragment == null) {
            logger.atDebug().log(() -> "Fragment "+dataMessage.getKey(0)+" not available (GDM returned null), sending another request");
            if (numCheckedFragments() < numFragments)
              sendRandomRequest();
            return;
          }

          if (!checkedFragment[fragmentID]) {
            logger.warn("retrieveObject: Got fragment #{}, but we never requested it -- ignored", fragmentID);
            return;
          }

          if (haveFragment[fragmentID] != null) {
            logger.warn("retrieveObject: Got duplicate fragment #{} -- discarded", fragmentID);
            return;
          }

          if ((manifest!=null) && !manifest.validatesFragment(thisFragment, fragmentID, environment.getLogManager().getLogger(Manifest.class, instance))) {
            logger.warn("Got invalid fragment #{} -- discarded", fragmentID);
            return;
          }

          logger.atDebug().log(() -> "retrieveObject: Received fragment #"+fragmentID+" for "+dataMessage.getKey(0));
          haveFragment[fragmentID] = thisFragment;

          currentFragmentRequestTimeout -= config.fragmentRequestTimeoutDecrement();
          if (currentFragmentRequestTimeout < config.fragmentRequestTimeoutMin())
            currentFragmentRequestTimeout = config.fragmentRequestTimeoutMin();
          logger.debug("Timeout decreased to {}", currentFragmentRequestTimeout);

          if (numHaveFragments() >= numSurvivors) {

            /* Restore the object */

            Fragment[] material = new Fragment[numFragments];
            int numAdded = 0;

            for (int j = 0; j < numFragments; j++) {
              if ((haveFragment[j] != null) && (numAdded < numSurvivors)) {
                material[j] = haveFragment[j];
                numAdded ++;
              } else {
                material[j] = null;
              }
            }

            logger.debug("Decode object: {}", key);
            PastContent theObject = policy.decodeObject(material, endpoint, contentDeserializer);
            logger.debug("Decode complete: {}", key);

            if (theObject == null) {
              logger.warn("retrieveObject: Decoder delivered {}, unexpected -- failed", theObject);
              continuation.receiveException(new GlacierException("Decoder delivered "+theObject+", unexpected -- failed"));
            } else {
              continuation.receiveResult(theObject);
            }

            localTerminate();
          }
        } else if (result instanceof GlacierResponseMessage glacierResponseMessage) {
          logger.atDebug().log(() -> "Fragment "+glacierResponseMessage.getKey(0)+" not available");
          if (numCheckedFragments() < numFragments)
            sendRandomRequest();
        } else {
          logger.warn("retrieveObject: Unexpected result: {}", result);
        }

      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("retrieveObject: Exception ", e);
        continuation.receiveException(e);
        localTerminate();
      }
      void sendRandomRequest() {
        int nextID;

        do {
          nextID = environment.getRandomSource().nextInt(numFragments);
        } while (checkedFragment[nextID]);

        checkedFragment[nextID] = true;
        FragmentKey nextKey = new FragmentKey(key, nextID);
        Id nextLocation = locator.getFragmentLocation(nextKey);
        logger.debug("retrieveObject: Asking {} for {}", nextLocation, nextKey);
        router.sendMessage(
          nextLocation,
          new GlacierFetchMessage(getMyUID(), nextKey, GlacierFetchMessage.FETCH_FRAGMENT, router.getLocalNodeHandle(), nextLocation, tag),
          null
        );
      }
      @Override
      public void timeoutExpired() {
        if (attemptsLeft > 0) {
          logger.debug("retrieveObject: Retrying ({} attempts left)", attemptsLeft);
          if (attemptsLeft < config.restoreMaxBoosts()) {
            currentFragmentRequestTimeout *= 2;
            if (currentFragmentRequestTimeout > config.fragmentRequestTimeoutMax())
              currentFragmentRequestTimeout = config.fragmentRequestTimeoutMax();
            logger.debug("Timeout increased to {}", currentFragmentRequestTimeout);
          }

          timeout = timeout + currentFragmentRequestTimeout;
          attemptsLeft --;

          int numRequests = numSurvivors - numHaveFragments();
          if (attemptsLeft < (config.restoreMaxBoosts() - 1))
            numRequests = Math.min(2*numRequests, numFragments - numCheckedFragments());
          if ((attemptsLeft == 0) && beStrict)
            numRequests = numFragments - numCheckedFragments();

          for (int i=0; (i<numRequests) && (numCheckedFragments() < numFragments); i++) {
            sendRandomRequest();
          }
        } else {
          logger.atInfo().log(() -> "retrieveObject: Giving up on "+key+" ("+config.restoreMaxBoosts()+" attempts, "+numCheckedFragments()+" checked, "+numHaveFragments()+" gotten)");
          continuation.receiveException(new GlacierNotEnoughFragmentsException("Maximum number of attempts ("+config.restoreMaxBoosts()+") reached for key "+key, numCheckedFragments(), numHaveFragments()));
          localTerminate();
        }
      }
      @Override
      public long getTimeout() {
        return timeout;
      }
    });
  }

  /**
   * Retrieves one fragment: from the trash if it is there, else from its primary holder, else by
   * restoring the whole object and re-encoding the fragment.
   */
  void retrieveFragment(final FragmentKey key, final Manifest manifest, final char tag, final GlacierContinuation continuation) {
    trash.lookupFragment(key, new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        if (!deliverFromTrash(key, manifest, result, continuation))
          scheduler.addContinuation(new PrimaryFragmentFetch(key, manifest, tag, continuation));
      }
      @Override
      public void receiveException(Exception e) {
        logger.atWarn().log(() -> "Exception while checking for "+key.toStringFull()+" in trash storage -- fetching normally");
        scheduler.addContinuation(new PrimaryFragmentFetch(key, manifest, tag, continuation));
      }
    });
  }

  /**
   * @param result what the trash holds under the key, or null
   * @return true if o is a fragment that matches the manifest, and it was delivered to c
   */
  private boolean deliverFromTrash(final FragmentKey key, Manifest manifest, final Object result, GlacierContinuation continuation) {
    if (result != null) {
      if (result instanceof FragmentAndManifest fragmentAndManifest) {
        Fragment thisFragment = fragmentAndManifest.getFragment();
        if (manifest.validatesFragment(thisFragment, key.fragmentId(), environment.getLogManager().getLogger(Manifest.class, instance))) {
          logger.atDebug().log(() -> "retrieveFragment: Found in trash: "+key.toStringFull());
          continuation.receiveResult(thisFragment);
          return true;
        }

        logger.warn("Fragment found in trash, but does not match manifest?!? -- fetching normally");
      } else {
        logger.atWarn().log(() -> "Fragment "+key.toStringFull()+" found in trash, but object is not a FAM ("+result+")?!? -- ignoring");
      }
    }

    return false;
  }

  /**
   * Asks the primary holder of a fragment for it a few times; if that fails, restores the fragment from
   * the whole object.
   */
  private final class PrimaryFragmentFetch extends GlacierContinuation {
    private final FragmentKey key;
    private final Manifest manifest;
    private final char tag;
    private final GlacierContinuation continuation;
    private int attemptsLeft;
    private boolean inPhaseTwo;
    private long timeout;

    PrimaryFragmentFetch(FragmentKey key, Manifest manifest, char tag, GlacierContinuation continuation) {
      this.key = key;
      this.manifest = manifest;
      this.tag = tag;
      this.continuation = continuation;
    }

    @Override
    public String toString() {
      return "retrieveFragment("+key+")";
    }
    @Override
    public void init() {
      attemptsLeft = config.fragmentRequestMaxAttempts();
      timeout = environment.getTimeSource().currentTimeMillis();
      inPhaseTwo = false;
      timeoutExpired();
    }
    @Override
    public void receiveResult(Object result) {
      if (result instanceof GlacierResponseMessage responseMessage) {
        if (!responseMessage.getKey(0).equals(key)) {
          logger.warn("retrieveFragment: Response does not match key {} -- discarded", key);
          return;
        }

        if ((attemptsLeft > 0) && !responseMessage.hasFragment(0)) {
          attemptsLeft = 0;
          timeoutExpired();
        } else {
          logger.warn("retrieveFragment: Unexpected GlacierResponseMessage: {} (key={})", responseMessage, key);
        }

        return;
      }

      if (result instanceof GlacierDataMessage dataMessage) {
        if (!dataMessage.getKey(0).equals(key)) {
          logger.warn("retrieveFragment: Data does not match key {} -- discarded", key);
          return;
        }

        Fragment thisFragment = dataMessage.getFragment(0);
        if (thisFragment == null) {
          logger.warn("retrieveFragment: DataMessage does not contain any fragments -- discarded");
          return;
        }

        if (!manifest.validatesFragment(thisFragment, dataMessage.getKey(0).fragmentId(), environment.getLogManager().getLogger(Manifest.class, instance))) {
          logger.atWarn().log(() -> "Invalid fragment "+dataMessage.getKey(0)+" returned by primary -- ignored");
          return;
        }

        continuation.receiveResult(thisFragment);
        terminate();
        return;
      }

      logger.warn("retrieveFragment: Unknown result {} (key={})", result, key);
    }
    @Override
    public void receiveException(Exception e) {
      logger.warn("retrieveFragment: Exception ", e);
      continuation.receiveException(e);
      terminate();
    }
    @Override
    public void timeoutExpired() {
      if (attemptsLeft > 0) {
        logger.debug("retrieveFragment: Retrying ({} attempts left)", attemptsLeft);
        timeout = timeout + currentFragmentRequestTimeout;
        attemptsLeft --;
        router.sendMessage(
          key.versionKey().id(),
          new GlacierFetchMessage(getMyUID(), key, GlacierFetchMessage.FETCH_FRAGMENT, router.getLocalNodeHandle(), key.versionKey().id(), tag),
          null
        );
      } else {
        timeout = timeout + 3L * config.restoreMaxBoosts() * currentFragmentRequestTimeout;
        if (inPhaseTwo) {
          logger.warn("retrieveFragment: Already in phase two");
        }
        inPhaseTwo = true;

        restoreFragmentFromObject(key, manifest, tag, continuation);
        terminate();
      }
    }
    @Override
    public long getTimeout() {
      return timeout;
    }
  }

  /**
   * Restores the whole object, and re-encodes the one fragment from it.
   */
  private void restoreFragmentFromObject(final FragmentKey key, final Manifest manifest, final char tag, final GlacierContinuation continuation) {
    retrieveObject(key.versionKey(), manifest, false, tag, new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        if (result == null) {
          logger.warn("retrieveFragment: retrieveObject({}) failed, returns null", key.versionKey());
          continuation.receiveException(new GlacierException("Cannot restore either the object or the fragment -- try again later!"));
          return;
        }

        reencodeFragment(key, manifest, (PastContent) result, continuation);
      }
      @Override
      public void receiveException(Exception e) {
        continuation.receiveException(e);
      }
    });
  }

  /**
   * Encodes the one fragment of a restored object on the processing thread, and checks it against the
   * manifest.
   */
  private void reencodeFragment(final FragmentKey key, final Manifest manifest, final PastContent retrievedObject, final GlacierContinuation continuation) {
    Futures.forward(endpoint.process(() -> {
      logger.debug("Reencode object: {}", key.versionKey());
      boolean generateFragment[] = new boolean[numFragments];
      Arrays.fill(generateFragment, false);
      generateFragment[key.fragmentId()] = true;
      Fragment[] result = policy.encodeObject(retrievedObject, generateFragment);
      logger.debug("Reencode complete: {}", key.versionKey());
      return result;
    }), new Continuation<Fragment[], Exception>() {
      @Override
      public void receiveResult(Fragment[] frag) {
        if (!manifest.validatesFragment(frag[key.fragmentId()], key.fragmentId(), environment.getLogManager().getLogger(Manifest.class, instance))) {
          logger.warn("Reconstructed fragment #{} does not match manifest ??!?", key.fragmentId());
          continuation.receiveException(new GlacierException("Recovered object, but cannot re-encode it (strange!) -- try again later!"));
          return;
        }

        continuation.receiveResult(frag[key.fragmentId()]);
      }
      @Override
      public void receiveException(Exception e) {
        logger.error("Recovered object, but re-encode failed: ", e);
        continuation.receiveException(new GlacierException("Recovered object, but re-encode failed: "+e));
      }
    }, endpoint.getEnvironment());
  }

  void rateLimitedRetrieveFragment(final FragmentKey key, final Manifest manifest, final char tag, final GlacierContinuation continuation) {
    logger.debug("rateLimitedRetrieveFragment({})", key);
    if (trafficShaper.isPending(key)) {
      logger.debug("Fragment is already being retrieved -- discarding request");
      return;
    }

    logger.debug("Added pending job: retrieveFragment({})", key);
    Continuation<Object, Exception> prev = trafficShaper.enqueue(key, new Continuation.SimpleContinuation() {
      @Override
      public void receiveResult(Object result) {
        retrieveFragment(key, manifest, tag, continuation);
      }
      @Override
      public void receiveException(Exception e) {
        /* Displaced by a newer job for the same key: fail this request instead of running it unshaped */
        continuation.receiveException(e);
      }
    });

    if (prev != null)
      prev.receiveException(new GlacierException("Key collision in traffic shaper (rateLimitedRetrieveFragment)"));
  }
}
