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
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.GlacierException;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.p2p.past.PastContent;
import rice.p2p.past.gc.GCPastContent;

/**
 * Inserts objects (erasure-codes them, creates their manifests and sends the fragments to their
 * holders), and refreshes the expiration of stored objects one by one or through {@link GlacierBulkRefresh}.
 */
final class GlacierInserter {
  private final GlacierConfig config;
  private final int numFragments;
  private final int numSurvivors;
  private final GlacierPolicy policy;
  private final Endpoint endpoint;
  private final GlacierScheduler scheduler;
  private final GlacierRouter router;
  private final FragmentLocator locator;
  private final GlacierTrafficShaper trafficShaper;
  private final GlacierRetriever retriever;
  private final GlacierBulkRefresh bulkRefresh;
  private final Environment environment;
  private final Logger logger;

  GlacierInserter(GlacierConfig config, int numFragments, int numSurvivors, GlacierPolicy policy, Endpoint endpoint, GlacierScheduler scheduler, GlacierRouter router, FragmentLocator locator, GlacierTrafficShaper trafficShaper, GlacierRetriever retriever, GlacierBulkRefresh bulkRefresh, Environment environment, Logger logger) {
    this.config = config;
    this.numFragments = numFragments;
    this.numSurvivors = numSurvivors;
    this.policy = policy;
    this.endpoint = endpoint;
    this.scheduler = scheduler;
    this.router = router;
    this.locator = locator;
    this.trafficShaper = trafficShaper;
    this.retriever = retriever;
    this.bulkRefresh = bulkRefresh;
    this.environment = environment;
    this.logger = logger;
  }

  void insert(final PastContent obj, final long expiration, final Continuation<? super Boolean[], Exception> command) {
    long theVersion = (obj instanceof GCPastContent) ? ((GCPastContent)obj).getVersion() : 0;
    final VersionKey vkey = new VersionKey(obj.getId(), theVersion);

    logger.atInfo().log(() -> "insert(" + obj + " (id=" + vkey.toStringFull() + ", mutable=" + obj.isMutable() + ")");

    endpoint.process(() -> {
      boolean[] generateFragment = new boolean[numFragments];
      Arrays.fill(generateFragment, true);
      return policy.encodeObject(obj, generateFragment);
    }, new Continuation<Fragment[], Exception>() {
      public void receiveResult(Fragment[] o) {
        final Fragment[] fragments = (Fragment[]) o;
        if (fragments == null) {
          command.receiveException(new GlacierException("Cannot encode object"));
          return;
        }

        logger.atDebug().log(() -> "insert(" + vkey.toStringFull() + ") encoded fragments OK, creating manifests...");

        endpoint.process(() -> policy.createManifests(vkey, obj, fragments, expiration), new Continuation<Manifest[], Exception>() {
          public void receiveResult(Manifest[] o) {
            if (o instanceof Manifest[]) {
              final Manifest[] manifests = (Manifest[]) o;
              if (manifests == null) {
                command.receiveException(new GlacierException("Cannot create manifests"));
                return;
              }

              /* distribute() reports a Boolean[] for insertions */
              distribute(vkey, fragments, manifests, expiration, GlacierTags.INSERT, new Continuation.StandardContinuation<Object, Boolean[], Exception>(command) {
                public void receiveResult(Object o) {
                  parent.receiveResult((Boolean[]) o);
                }
              });
            } else {
              logger.atWarn().log(() -> "insert(" + vkey.toStringFull() + ") cannot create manifests - returned o="+o);
              command.receiveException(new GlacierException("Cannot create manifests in insert()"));
            }
          }
          public void receiveException(Exception e) {
            logger.atWarn().log(() -> "insert(" + vkey.toStringFull() + ") cannot create manifests - exception e="+e);
            command.receiveException(e);
          }
        });
      }
      public void receiveException(Exception e) {
        logger.error("EncodeObject failed: e=", e);
        command.receiveException(new GlacierException("EncodeObject failed: e="+e));
      }
    });
  }

  void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    if (!config.enableBulkRefresh()) {
      /* Ordinary refresh method (safe in 'hostile' environments) */

      final Continuation.MultiContinuation<Object[]> mc = new Continuation.MultiContinuation<Object[]>(command, ids.length);
      for (int i=0; i<ids.length; i++) {
        final Continuation<Object, Exception> thisContinuation = mc.getSubContinuation(i);
        final Id thisId = ids[i];
        final long thisVersion = versions[i];
        final long thisExpiration = expirations[i];

        logger.atInfo().log(() -> "refresh("+thisId.toStringFull()+"v"+thisVersion+", exp="+thisExpiration+")");

        final VersionKey thisVersionKey = new VersionKey(thisId, thisVersion);
        Continuation<Object, Exception> prev = trafficShaper.enqueue(thisVersionKey, new Continuation.SimpleContinuation() {
          public void receiveResult(Object o) {
            retriever.retrieveManifest(thisVersionKey, GlacierTags.REFRESH, new Continuation<Object, Exception>() {
              public void receiveResult(Object o) {
                if (o instanceof Manifest) {
                  Manifest manifest = (Manifest) o;

                  logger.atDebug().log(() -> "refresh("+thisId.toStringFull()+"v"+thisVersion+"): Got manifest");
                  manifest = policy.updateManifest(new VersionKey(thisId, thisVersion), manifest, thisExpiration);
                  Manifest[] manifests = new Manifest[numFragments];
                  for (int i=0; i<numFragments; i++)
                    manifests[i] = manifest;
                  distribute(new VersionKey(thisId, thisVersion), null, manifests, thisExpiration, GlacierTags.REFRESH, thisContinuation);
                } else {
                  logger.warn("refresh({}v{}): Cannot retrieve manifest", thisId, thisVersion);
                  thisContinuation.receiveResult(new GlacierException("Cannot retrieve manifest -- retry later"));
                }
              }
              public void receiveException(Exception e) {
                logger.warn("refresh({}v{}): Exception while retrieving manifest: ", thisId, thisVersion, e);
                thisContinuation.receiveException(e);
              }
            });
          }
        });

        if (prev != null)
          prev.receiveException(new GlacierException("Key collision in traffic shaper (refresh)"));
      }
    } else {
      /* Aggregated refresh method */

      bulkRefresh.refresh(ids, versions, expirations, command);
    }
  }

  private void distribute(final VersionKey key, final Fragment[] fragments, final Manifest[] manifests, final long expiration, final char tag, final Continuation<Object, Exception> command) {
    final long tStart = environment.getTimeSource().currentTimeMillis();
    scheduler.addContinuation(new GlacierContinuation() {
      NodeHandle[] holder;
      boolean[] receiptReceived;
      boolean doInsert = (fragments != null);
      boolean doRefresh = !doInsert;
      boolean answered = false;
      boolean inhibitInsertions = true;
      int minAcceptable = (int)(numSurvivors * config.minFragmentsAfterInsert());

      public String toString() {
        return whoAmI() + " continuation for "+key;
      }
      private int numReceiptsReceived() {
        int result = 0;
        for (int i=0; i<receiptReceived.length; i++)
          if (receiptReceived[i])
            result ++;
        return result;
      }
      private int numHoldersKnown() {
        int result = 0;
        for (int i=0; i<holder.length; i++)
          if (holder[i] != null)
            result ++;
        return result;
      }
      private String whoAmI() {
        return (doRefresh) ? "Refresh" : "Insert";
      }
      public void init() {
        logger.atInfo().log(() -> "Initializing "+whoAmI()+" continuation for " + key);
        holder = new NodeHandle[numFragments];
        receiptReceived = new boolean[numFragments];

        /* Send queries */

        logger.debug("Sending queries for {}", key);
        for (int i = 0; i < numFragments; i++) {
          Id fragmentLoc = locator.getFragmentLocation(key.getId(), i, key.getVersion());
          FragmentKey keys[] = new FragmentKey[1];
          keys[0] = new FragmentKey(key, i);

          logger.debug("Query #{} to {}", i, fragmentLoc);
          router.sendMessage(
            fragmentLoc,
            new GlacierQueryMessage(getMyUID(), keys, router.getLocalNodeHandle(), fragmentLoc, tag),
            null
          );
        }
      }
      public void receiveResult(Object o) {
        if (o instanceof GlacierResponseMessage) {
          GlacierResponseMessage grm = (GlacierResponseMessage) o;
          if (!grm.getKey(0).getVersionKey().equals(key)) {
            logger.atWarn().log(() -> whoAmI()+" response got routed to the wrong key: "+key);
            return;
          }

          /* Sanity checks */

          int fragmentID = grm.getKey(0).getFragmentID();
          if (fragmentID < numFragments) {
            if (grm.getAuthoritative(0)) {

              /* OK, so the message makes sense. Let's see... */

              if (doInsert && !grm.getHaveIt(0)) {

                /* If this is an insertion, and the holder is telling us that he does not
                   have the fragment, we send it to him */

                if (holder[fragmentID] == null) {
                  holder[fragmentID] = grm.getSource();
                  if (!inhibitInsertions) {
                    logger.atDebug().log(() -> "Got insert response, sending fragment "+grm.getKey(0));
                    router.sendMessage(
                      null,
                      new GlacierDataMessage(getMyUID(), grm.getKey(0), fragments[fragmentID], manifests[fragmentID], router.getLocalNodeHandle(), grm.getSource().getId(), false, tag),
                      grm.getSource()
                    );
                  } else {
                    if (numHoldersKnown() >= minAcceptable) {
                      logger.atDebug().log(() -> "Got "+numHoldersKnown()+" insert responses, sending fragments...");
                      inhibitInsertions = false;
                      for (int i=0; i<holder.length; i++) {
                        if (holder[i] != null) {
                          logger.debug("Sending fragment #{}", i);
                          router.sendMessage(
                            null,
                            new GlacierDataMessage(getMyUID(), new FragmentKey(key, i), fragments[i], manifests[i], router.getLocalNodeHandle(), holder[i].getId(), false, tag),
                            holder[i]
                          );
                        }
                      }

                      logger.debug("Done sending fragments, now accepting further responses");
                    } else {
                      logger.atDebug().log(() -> "Got insert response #"+numHoldersKnown()+" ("+minAcceptable+" needed to start insertion)");
                    }
                  }
                } else {
                  logger.warn("Received two insert responses for the same fragment -- discarded");
                }

              } else if (grm.getHaveIt(0) && (grm.getExpiration(0) < expiration)) {

                /* If the holder has an old version of the fragment, we send the manifest only. */

                if (holder[fragmentID] == null) {
                  holder[fragmentID] = grm.getSource();
                  logger.atDebug().log(() -> "Got refresh response (exp="+grm.getExpiration(0)+"<"+expiration+"), sending manifest "+grm.getKey(0));
                  router.sendMessage(
                    null,
                    new GlacierDataMessage(getMyUID(), grm.getKey(0), null, manifests[fragmentID], router.getLocalNodeHandle(), grm.getSource().getId(), false, tag),
                    grm.getSource()
                  );

                  /* Refreshes are not acknowledged */

                  if (doRefresh) {
                    receiptReceived[fragmentID] = true;
                    if ((numReceiptsReceived() >= minAcceptable) && !answered) {
                      answered = true;
                      reportSuccess();
                    }
                  }
                } else {
                  logger.warn("Received two refresh responses for the same fragment -- discarded");
                }
              } else if (grm.getHaveIt(0) && (grm.getExpiration(0) >= expiration)) {

                /* If the holder has a current version of the fragment, we are happy */

                logger.atDebug().log(() -> "Receipt received after "+whoAmI()+": "+grm.getKey(0));
                receiptReceived[fragmentID] = true;
                if ((numReceiptsReceived() >= minAcceptable) && !answered) {
                  answered = true;
                  reportSuccess();
                }
              }
            } else {
              logger.atDebug().log(() -> whoAmI() + " response, but not authoritative -- ignoring");
            }
          } else {
            logger.atWarn().log(() -> "Fragment ID too large in " + whoAmI() + " response -- discarded");
          }

          return;
        } else {
          logger.atWarn().log(() -> "Unknown response to "+whoAmI()+" continuation: "+o+" -- discarded");
        }
      }
      public void receiveException(Exception e) {
        logger.atWarn().setCause(e).log(() -> "Exception during "+whoAmI()+"("+key+"): ");
        if (!answered) {
          answered = true;
          command.receiveException(new GlacierException("Exception while inserting/refreshing: "+e));
        }
        terminate();
      }
      private void reportSuccess() {
        logger.atDebug().log(() -> "Reporting success for "+key+", "+numReceiptsReceived()+"/"+numFragments+" receipts received so far");
        if (doInsert)
          command.receiveResult(new Boolean[] { Boolean.valueOf(true) });
        else
          command.receiveResult(Boolean.valueOf(true));
      }
      public void timeoutExpired() {
        if (numReceiptsReceived() >= minAcceptable) {
          logger.atInfo().log(() -> whoAmI()+" of "+key+" successful, "+numReceiptsReceived()+"/"+numFragments+" receipts received");
          if (!answered) {
            answered = true;
            reportSuccess();
          }
        } else {
          logger.atWarn().log(() -> whoAmI()+" "+key+" failed, only "+numReceiptsReceived()+"/"+numFragments+" receipts received");
          if (!answered) {
            answered = true;
            command.receiveException(new GlacierException(whoAmI()+" failed, did not receive enough receipts"));
          }
        }

        terminate();
      }
      public long getTimeout() {
        return tStart + ((doRefresh) ? config.refreshTimeout() : config.insertTimeout());
      }
    });
  }
}
