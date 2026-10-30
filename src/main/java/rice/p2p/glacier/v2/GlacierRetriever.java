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
import rice.p2p.past.rawserialization.JavaPastContentDeserializer;
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
  private final GlacierScheduler scheduler;
  private final GlacierRouter router;
  private final FragmentLocator locator;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierTrash trash;
  private final Environment environment;
  private final Logger logger;
  private long currentFragmentRequestTimeout;
  private PastContentDeserializer contentDeserializer;

  GlacierRetriever(GlacierConfig config, int numFragments, int numSurvivors, String instance, GlacierPolicy policy, Endpoint endpoint, GlacierScheduler scheduler, GlacierRouter router, FragmentLocator locator, GlacierTrafficShaper trafficShaper, GlacierTrash trash, Environment environment, Logger logger) {
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
    this.contentDeserializer = new JavaPastContentDeserializer();
  }

  void setContentDeserializer(PastContentDeserializer deserializer) {
    contentDeserializer = deserializer;
  }

  void retrieveManifest(final VersionKey key, final char tag, final Continuation<? super Manifest, Exception> command) {
    logger.debug("retrieveManifest(key={} tag={})", key, tag);
    scheduler.addContinuation(new GlacierContinuation() {
      private boolean checkedFragment[];
      private long timeout;

      public String toString() {
        return "retrieveManifest("+key+")";
      }
      public void init() {
        checkedFragment = new boolean[numFragments];
        Arrays.fill(checkedFragment, false);
        timeout = environment.getTimeSource().currentTimeMillis() + config.manifestRequestTimeout();
        for (int i=0; i<config.manifestRequestInitialBurst(); i++)
          sendRandomRequest();
      }
      public int numCheckedFragments() {
        int result = 0;
        for (int i=0; i<checkedFragment.length; i++)
          if (checkedFragment[i])
            result ++;
        return result;
      }
      public void sendRandomRequest() {
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
      public void receiveResult(Object o) {
        if (o instanceof GlacierDataMessage) {
          GlacierDataMessage gdm = (GlacierDataMessage) o;

          if ((gdm.numKeys() > 0) && (gdm.getManifest(0) != null)) {
            logger.debug("retrieveManifest({}) received manifest", key);
            if (policy.checkSignature(gdm.getManifest(0), key)) {
              command.receiveResult(gdm.getManifest(0));
              terminate();
            } else {
              logger.atWarn().log(() -> "retrieveManifest("+key+"): invalid signature in "+gdm.getKey(0));
            }
          } else logger.warn("retrieveManifest({}) retrieved GDM without a manifest?!?", key);
        } else if (o instanceof GlacierResponseMessage) {
          logger.atDebug().log(() -> "retrieveManifest("+key+"): Fragment not available:" + ((GlacierResponseMessage)o).getKey(0));
          if (numCheckedFragments() < numFragments) {
            sendRandomRequest();
          } else {
            logger.warn("retrieveManifest({}): giving up", key);
            command.receiveResult(null);
            terminate();
          }
        } else {
          logger.warn("retrieveManifest({}) received unexpected object: {}", key, o);
        }
      }
      public void receiveException(Exception e) {
        logger.warn("retrieveManifest({}) received exception: ", key, e);
      }
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
      public long getTimeout() {
        return timeout;
      }
    });
  }

  void retrieveObject(final VersionKey key, final Manifest manifest, final boolean beStrict, final char tag, final Continuation<? super PastContent, Exception> c) {
    scheduler.addContinuation(new GlacierContinuation() {
      private boolean checkedFragment[];
      private Fragment haveFragment[];
      private int attemptsLeft;
      private long timeout;

      public int numHaveFragments() {
        int result = 0;
        for (int i=0; i<haveFragment.length; i++)
          if (haveFragment[i] != null)
            result ++;
        return result;
      }
      public int numCheckedFragments() {
        int result = 0;
        for (int i=0; i<checkedFragment.length; i++)
          if (checkedFragment[i])
            result ++;
        return result;
      }
      public String toString() {
        return "retrieveObject("+key+")";
      }
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
      public void receiveResult(Object o) {
        if (o instanceof GlacierDataMessage) {
          GlacierDataMessage gdm = (GlacierDataMessage) o;
          int fragmentID = gdm.getKey(0).getFragmentID();

          if (!gdm.getKey(0).getVersionKey().equals(key) || (fragmentID<0) || (fragmentID>=numFragments)) {
            logger.atWarn().log(() -> "retrieveObject: Bad data message (contains "+gdm.getKey(0)+", expected "+key);
            return;
          }

          Fragment thisFragment = gdm.getFragment(0);
          if (thisFragment == null) {
            logger.atDebug().log(() -> "Fragment "+((GlacierDataMessage)o).getKey(0)+" not available (GDM returned null), sending another request");
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

          logger.atDebug().log(() -> "retrieveObject: Received fragment #"+fragmentID+" for "+gdm.getKey(0));
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
              c.receiveException(new GlacierException("Decoder delivered "+theObject+", unexpected -- failed"));
            } else {
              c.receiveResult(theObject);
            }

            localTerminate();
          }
        } else if (o instanceof GlacierResponseMessage) {
          logger.atDebug().log(() -> "Fragment "+((GlacierResponseMessage)o).getKey(0)+" not available");
          if (numCheckedFragments() < numFragments)
            sendRandomRequest();
        } else {
          logger.warn("retrieveObject: Unexpected result: {}", o);
        }

        return;
      }
      public void receiveException(Exception e) {
        logger.warn("retrieveObject: Exception ", e);
        c.receiveException(e);
        localTerminate();
      }
      public void sendRandomRequest() {
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
          c.receiveException(new GlacierNotEnoughFragmentsException("Maximum number of attempts ("+config.restoreMaxBoosts()+") reached for key "+key, numCheckedFragments(), numHaveFragments()));
          localTerminate();
        }
      }
      public long getTimeout() {
        return timeout;
      }
    });
  }

  void retrieveFragment(final FragmentKey key, final Manifest manifest, final char tag, final GlacierContinuation c) {
    final Continuation<Object, Exception> c2 = new Continuation<Object, Exception>() {
      public void receiveResult(Object o) {
        if (o != null) {
          if (o instanceof FragmentAndManifest) {
            Fragment thisFragment = ((FragmentAndManifest)o).getFragment();
            if (manifest.validatesFragment(thisFragment, key.getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance))) {
              logger.atDebug().log(() -> "retrieveFragment: Found in trash: "+key.toStringFull());
              c.receiveResult(thisFragment);
              return;
            }

            logger.warn("Fragment found in trash, but does not match manifest?!? -- fetching normally");
          } else {
            logger.atWarn().log(() -> "Fragment "+key.toStringFull()+" found in trash, but object is not a FAM ("+o+")?!? -- ignoring");
          }
        }

        scheduler.addContinuation(new GlacierContinuation() {
          private int attemptsLeft;
          private boolean inPhaseTwo;
          private long timeout;

          public String toString() {
            return "retrieveFragment("+key+")";
          }
          public void init() {
            attemptsLeft = config.fragmentRequestMaxAttempts();
            timeout = environment.getTimeSource().currentTimeMillis();
            inPhaseTwo = false;
            timeoutExpired();
          }
          public void receiveResult(Object o) {
            if (o instanceof GlacierResponseMessage) {
              GlacierResponseMessage grm = (GlacierResponseMessage) o;
              if (!grm.getKey(0).equals(key)) {
                logger.warn("retrieveFragment: Response does not match key {} -- discarded", key);
                return;
              }

              if ((attemptsLeft > 0) && !grm.getHaveIt(0)) {
                attemptsLeft = 0;
                timeoutExpired();
              } else {
                logger.warn("retrieveFragment: Unexpected GlacierResponseMessage: {} (key={})", grm, key);
              }

              return;
            }

            if (o instanceof GlacierDataMessage) {
              GlacierDataMessage gdm = (GlacierDataMessage) o;
              if (!gdm.getKey(0).equals(key)) {
                logger.warn("retrieveFragment: Data does not match key {} -- discarded", key);
                return;
              }

              Fragment thisFragment = gdm.getFragment(0);
              if (thisFragment == null) {
                logger.warn("retrieveFragment: DataMessage does not contain any fragments -- discarded");
                return;
              }

              if (!manifest.validatesFragment(thisFragment, gdm.getKey(0).getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance))) {
                logger.atWarn().log(() -> "Invalid fragment "+gdm.getKey(0)+" returned by primary -- ignored");
                return;
              }

              c.receiveResult(thisFragment);
              terminate();
              return;
            }

            logger.warn("retrieveFragment: Unknown result {} (key={})", o, key);
          }
          public void receiveException(Exception e) {
            logger.warn("retrieveFragment: Exception ", e);
            c.receiveException(e);
            terminate();
          }
          public void timeoutExpired() {
            if (attemptsLeft > 0) {
              logger.debug("retrieveFragment: Retrying ({} attempts left)", attemptsLeft);
              timeout = timeout + currentFragmentRequestTimeout;
              attemptsLeft --;
              router.sendMessage(
                key.getVersionKey().getId(),
                new GlacierFetchMessage(getMyUID(), key, GlacierFetchMessage.FETCH_FRAGMENT, router.getLocalNodeHandle(), key.getVersionKey().getId(), tag),
                null
              );
            } else {
              timeout = timeout + 3 * config.restoreMaxBoosts() * currentFragmentRequestTimeout;
              if (inPhaseTwo) {
                logger.warn("retrieveFragment: Already in phase two");
              }
              inPhaseTwo = true;

              retrieveObject(key.getVersionKey(), manifest, false, tag, new Continuation<Object, Exception>() {
                public void receiveResult(Object o) {
                  if (o == null) {
                    logger.warn("retrieveFragment: retrieveObject({}) failed, returns null", key.getVersionKey());
                    c.receiveException(new GlacierException("Cannot restore either the object or the fragment -- try again later!"));
                    return;
                  }

                  final PastContent retrievedObject = (PastContent) o;
                  endpoint.process(() -> {
                    logger.debug("Reencode object: {}", key.getVersionKey());
                    boolean generateFragment[] = new boolean[numFragments];
                    Arrays.fill(generateFragment, false);
                    generateFragment[key.getFragmentID()] = true;
                    Fragment[] result = policy.encodeObject(retrievedObject, generateFragment);
                    logger.debug("Reencode complete: {}", key.getVersionKey());
                    return result;
                  }, new Continuation<Fragment[], Exception>() {
                    public void receiveResult(Fragment[] o) {
                      Fragment[] frag = (Fragment[]) o;

                      if (!manifest.validatesFragment(frag[key.getFragmentID()], key.getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance))) {
                        logger.warn("Reconstructed fragment #{} does not match manifest ??!?", key.getFragmentID());
                        c.receiveException(new GlacierException("Recovered object, but cannot re-encode it (strange!) -- try again later!"));
                        return;
                      }

                      c.receiveResult(frag[key.getFragmentID()]);
                    }
                    public void receiveException(Exception e) {
                      logger.error("Recovered object, but re-encode failed: ", e);
                      c.receiveException(new GlacierException("Recovered object, but re-encode failed: "+e));
                    }
                  });
                }
                public void receiveException(Exception e) {
                  c.receiveException(e);
                }
              });

              terminate();
            }
          }
          public long getTimeout() {
            return timeout;
          }
        });
      }
      public void receiveException(Exception e) {
        logger.atWarn().log(() -> "Exception while checking for "+key.toStringFull()+" in trash storage -- ignoring");
      }
    };

    trash.lookupFragment(key, c2);
  }

  void rateLimitedRetrieveFragment(final FragmentKey key, final Manifest manifest, final char tag, final GlacierContinuation c) {
    logger.debug("rateLimitedRetrieveFragment({})", key);
    if (trafficShaper.isPending(key)) {
      logger.debug("Fragment is already being retrieved -- discarding request");
      return;
    }

    logger.debug("Added pending job: retrieveFragment({})", key);
    Continuation<Object, Exception> prev = trafficShaper.enqueue(key, new Continuation.SimpleContinuation() {
      public void receiveResult(Object o) {
        retrieveFragment(key, manifest, tag, c);
      }
    });

    if (prev != null)
      prev.receiveException(new GlacierException("Key collision in traffic shaper (rateLimitedRetrieveFragment)"));
  }
}
