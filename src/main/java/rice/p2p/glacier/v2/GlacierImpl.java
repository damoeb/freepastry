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

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.FragmentKeySet;
import rice.p2p.glacier.Glacier;
import rice.p2p.glacier.GlacierException;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.VersioningPast;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierFetchMessage;
import rice.p2p.glacier.v2.messaging.GlacierMessage;
import rice.p2p.glacier.v2.messaging.GlacierNeighborRequestMessage;
import rice.p2p.glacier.v2.messaging.GlacierNeighborResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeForwardMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierRangeResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshCompleteMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshPatchMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshProbeMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierSyncMessage;
import rice.p2p.glacier.v2.messaging.GlacierTimeoutMessage;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.GCPast;
import rice.p2p.past.gc.GCPastContent;
import rice.p2p.past.rawserialization.JavaPastContentDeserializer;
import rice.p2p.past.rawserialization.JavaPastContentHandleDeserializer;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.rawserialization.PastContentHandleDeserializer;
import rice.p2p.util.DebugCommandHandler;
import rice.persistence.PersistentStorage;
import rice.persistence.Storage;
import rice.persistence.StorageManager;

@SuppressWarnings("unchecked")
public class GlacierImpl implements Glacier, GCPast, VersioningPast, Application, DebugCommandHandler {
  private final StorageManager fragmentStorage;
  private final StorageManager neighborStorage;
  private final GlacierConfig config;
  private final GlacierPolicy policy;
  private final Node node;
  private final int numFragments;
  private final String instance;
  private final int numSurvivors;
  private final Endpoint endpoint;
  private final IdFactory factory;
  private final GlacierScheduler scheduler;
  private final FragmentLocator locator;
  private final Hashtable<Id, Continuation<Object, Exception>> pendingTraffic;
  private StorageManager trashStorage;
  private IdRange responsibleRange;
  private GlacierStatistics statistics;
  private final Vector<GlacierStatisticsListener> listeners;
  private long currentFragmentRequestTimeout;
  private long tokenBucket;
  private long bucketLastUpdated;
  private long bucketMin;
  private long bucketMax;
  private long bucketConsumed;





  private long neighborTimeout;

  private long syncInterval;
  private int syncMaxFragments;








  private int rateLimitedRequestsPerSecond;


  private long bucketTokensPerSecond;
  private long bucketMaxBurstSize;



  private int[] numActiveRestores;


  private Environment environment;
  private final Logger logger;

  private PastContentDeserializer contentDeserializer;
  private PastContentHandleDeserializer contentHandleDeserializer;

  public GlacierImpl(Node nodeArg, StorageManager fragmentStorageArg, StorageManager neighborStorageArg, int numFragmentsArg, int numSurvivorsArg, IdFactory factoryArg, String instanceArg, GlacierPolicy policyArg) {
    this.environment = nodeArg.getEnvironment();
    this.logger = environment.getLogManager().getLogger(GlacierImpl.class, instanceArg);

    this.config = GlacierConfig.from(environment.getParameters());
    this.syncInterval = config.syncInterval();
    this.syncMaxFragments = config.syncMaxFragments();
    this.rateLimitedRequestsPerSecond = config.rateLimitedRequestsPerSecond();
    this.neighborTimeout = config.neighborTimeout();
    this.bucketTokensPerSecond = config.bucketTokensPerSecond();
    this.bucketMaxBurstSize = config.bucketMaxBurstSize();



    this.fragmentStorage = fragmentStorageArg;
    this.neighborStorage = neighborStorageArg;
    this.trashStorage = null;
    this.policy = policyArg;
    this.node = nodeArg;
    this.instance = instanceArg;
    this.endpoint = node.buildEndpoint(this, instance);
    this.scheduler = new GlacierScheduler(endpoint, environment, logger);
    this.endpoint.setDeserializer((buf, type, priority, sender) -> {
      switch(type) {
        case GlacierDataMessage.TYPE:
          return GlacierDataMessage.build(buf, endpoint);
        case GlacierFetchMessage.TYPE:
          return GlacierFetchMessage.build(buf, endpoint);
        case GlacierNeighborRequestMessage.TYPE:
          return GlacierNeighborRequestMessage.build(buf, endpoint);
        case GlacierNeighborResponseMessage.TYPE:
          return GlacierNeighborResponseMessage.build(buf, endpoint);
        case GlacierQueryMessage.TYPE:
          return GlacierQueryMessage.build(buf, endpoint);
        case GlacierRangeForwardMessage.TYPE:
          return GlacierRangeForwardMessage.build(buf, endpoint);
        case GlacierRangeQueryMessage.TYPE:
          return GlacierRangeQueryMessage.build(buf, endpoint);
        case GlacierRangeResponseMessage.TYPE:
          return GlacierRangeResponseMessage.build(buf, endpoint);
        case GlacierRefreshCompleteMessage.TYPE:
          return GlacierRefreshCompleteMessage.build(buf, endpoint);
        case GlacierRefreshPatchMessage.TYPE:
          return GlacierRefreshPatchMessage.build(buf, endpoint);
        case GlacierRefreshProbeMessage.TYPE:
          return GlacierRefreshProbeMessage.build(buf, endpoint);
        case GlacierRefreshResponseMessage.TYPE:
          return GlacierRefreshResponseMessage.build(buf, endpoint);
        case GlacierResponseMessage.TYPE:
          return GlacierResponseMessage.build(buf, endpoint);
        case GlacierSyncMessage.TYPE:
          return GlacierSyncMessage.build(buf, endpoint);
      }
      throw new IllegalArgumentException("Unknown type:"+type);
    });
    this.contentDeserializer = new JavaPastContentDeserializer();
    this.contentHandleDeserializer = new JavaPastContentHandleDeserializer();
    this.numFragments = numFragmentsArg;
    this.numSurvivors = numSurvivorsArg;
    this.factory = factoryArg;
    this.locator = new FragmentLocator(factoryArg, numFragmentsArg);
    this.responsibleRange = null;
    this.pendingTraffic = new Hashtable<Id, Continuation<Object, Exception>>();
    this.statistics = new GlacierStatistics(GlacierTags.MAX, environment);
    this.listeners = new Vector<GlacierStatisticsListener>();
    this.numActiveRestores = new int[1];
    this.numActiveRestores[0] = 0;
    this.currentFragmentRequestTimeout = config.fragmentRequestTimeoutDefault();
    this.tokenBucket = 0;
    this.bucketLastUpdated = environment.getTimeSource().currentTimeMillis();
    determineResponsibleRange();
    endpoint.register();
  }

  public void startup() {

    /* Neighbor requests */

    scheduler.addContinuation(new GlacierContinuation() {
      long nextTimeout;

      public String toString() {
        return "Neighbor continuation";
      }
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.expireNeighborsDelayAfterJoin();

        NodeHandleSet leafSet = endpoint.neighborSet(999);
        NodeHandle localHandle = getLocalNodeHandle();
        NodeHandle cwExtreme = localHandle;
        NodeHandle ccwExtreme = localHandle;

        for (int i=0; i<leafSet.size(); i++) {
          NodeHandle thisHandle = leafSet.getHandle(i);
          if (localHandle.getId().clockwise(thisHandle.getId())) {
            if (cwExtreme.getId().clockwise(thisHandle.getId()))
              cwExtreme = thisHandle;
          } else {
            if (ccwExtreme.getId().clockwise(thisHandle.getId()))
              ccwExtreme = thisHandle;
          }
        }

        IdRange leafRange = factory.buildIdRange(ccwExtreme.getId(), cwExtreme.getId());

        for (int k=0; k<leafSet.size(); k++) {
          if (!leafSet.getHandle(k).getId().equals(getLocalNodeHandle().getId())) {
            neighborSeen(leafSet.getHandle(k).getId(), environment.getTimeSource().currentTimeMillis());
            if (logger.isInfoEnabled()) logger.info("Asking "+leafSet.getHandle(k).getId()+" about neighbors in "+leafRange);
            sendMessage(
              null,
              new GlacierNeighborRequestMessage(getMyUID(), leafRange, getLocalNodeHandle(), leafSet.getHandle(k).getId(), GlacierTags.NEIGHBOR),
              leafSet.getHandle(k)
            );
          }
        }
      }
      public void receiveResult(Object o) {
        if (o instanceof GlacierNeighborResponseMessage) {
          final GlacierNeighborResponseMessage gnrm = (GlacierNeighborResponseMessage) o;
          logger.atDebug().log(() -> "NeighborResponse from "+gnrm.getSource()+" with "+gnrm.numNeighbors()+" neighbors");
          for (int i=0; i<gnrm.numNeighbors(); i++)
            neighborSeen(gnrm.getNeighbor(i), gnrm.getLastSeen(i));
        } else {
          logger.warn("Unknown response in neighbor continuation: {} -- discarded", o);
        }
      }
      public void receiveException(Exception e) {
        logger.warn("Exception in neighbor continuation: ", e);
        terminate();
      }
      public void timeoutExpired() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.expireNeighborsInterval();

        final long earliestAcceptableDate = environment.getTimeSource().currentTimeMillis() - neighborTimeout;
        IdSet allNeighbors = neighborStorage.scan();
        Iterator<Id> iter = allNeighbors.getIterator();
        NodeHandleSet leafSet = endpoint.neighborSet(999);

        logger.info("Checking neighborhood for expired certificates...");

        while (iter.hasNext()) {
          final Id thisNeighbor = (Id) iter.next();
          if (leafSet.memberHandle(thisNeighbor)) {
            logger.debug("CNE: Refreshing current neighbor: {}", thisNeighbor);
            neighborSeen(thisNeighbor, environment.getTimeSource().currentTimeMillis());
          } else {
            logger.debug("CNE: Retrieving {}", thisNeighbor);
            neighborStorage.getObject(thisNeighbor, new Continuation<Object, Exception>() {
              public void receiveResult(Object o) {
                if (o==null) {
                  logger.warn("CNE: Cannot retrieve neighbor {}", thisNeighbor);
                  return;
                }

                long lastSeen = ((Long)o).longValue();
                if (lastSeen < earliestAcceptableDate) {
                  logger.info("CNE: Removing expired neighbor {} ({}<{})", thisNeighbor, lastSeen, earliestAcceptableDate);
                  neighborStorage.unstore(thisNeighbor, new Continuation<Object, Exception>() {
                    public void receiveResult(Object o) {
                      logger.debug("CNE unstore successful: {}, returned {}", thisNeighbor, o);
                    }
                    public void receiveException(Exception e) {
                      logger.atWarn().log(() -> "CNE unstore failed: "+thisNeighbor+", returned "+e);
                    }
                  });
                } else {
                  logger.info("CNE: Neighbor {} still active, last seen {}", thisNeighbor, lastSeen);
                }
              }
              public void receiveException(Exception e) {
                logger.atWarn().log(() -> "CNE: Exception while retrieving neighbor "+thisNeighbor+", e="+e);
              }
            });
          }
        }

        determineResponsibleRange();
      }
      public long getTimeout() {
        return nextTimeout;
      }
    });

    /* Sync */

    scheduler.addContinuation(new GlacierContinuation() {
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
                  bv.add(getHashInput(fkey.getVersionKey(), currentExp));
                  bv.add(getHashInput(fkey.getVersionKey(), prevExp));
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

                sendMessage(
                  null,
                  new GlacierSyncMessage(scheduler.getUID(), grrm.getCommonRange(), offset, bv, getLocalNodeHandle(), grrm.getSource().getId(), GlacierTags.SYNC),
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
        if (numActiveRestores[0] > 0) {
          logger.info("Sync postponed; {} fetches pending", numActiveRestores[0]);
          nextTimeout = environment.getTimeSource().currentTimeMillis() + jitterTerm(config.syncRetryInterval());
        } else {
          nextTimeout = environment.getTimeSource().currentTimeMillis() + jitterTerm(syncInterval);
          offset = 1+environment.getRandomSource().nextInt(numFragments-1);

          Id dest = locator.getFragmentLocation(getLocalNodeHandle().getId(), offset, 0);
          Id ccwId = locator.getFragmentLocation(responsibleRange.getCCWId(), offset, 0);
          Id cwId = locator.getFragmentLocation(responsibleRange.getCWId(), offset, 0);
          IdRange requestedRange = factory.buildIdRange(ccwId, cwId);

          logger.info("Sending range query for ({}) to {}", requestedRange, dest);
          sendMessage(
            dest,
            new GlacierRangeQueryMessage(getMyUID(), requestedRange, getLocalNodeHandle(), dest, GlacierTags.SYNC),
            null
          );
        }
      }
      public long getTimeout() {
        return nextTimeout;
      }
    });

    /* Handoff */

    scheduler.addContinuation(new GlacierContinuation() {
      long nextTimeout;

      public String toString() {
        return "Handoff continuation";
      }
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.handoffDelayAfterJoin();
      }
      public void receiveResult(Object o) {
        if (o instanceof GlacierResponseMessage) {
          final GlacierResponseMessage grm = (GlacierResponseMessage) o;
          logger.atDebug().log(() -> "Received handoff response from "+grm.getSource().getId()+" with "+grm.numKeys()+" keys");
          for (int i=0; i<grm.numKeys(); i++) {
            final FragmentKey thisKey = grm.getKey(i);
            if (grm.getAuthoritative(i)) {
              if (grm.getHaveIt(i)) {
                Id thisPos = locator.getFragmentLocation(thisKey);
                if (!responsibleRange.containsId(thisPos)) {
                  logger.debug("Deleting fragment {}", thisKey);
                  deleteFragment(thisKey, new Continuation<Object, Exception>() {
                    public void receiveResult(Object o) {
                      logger.debug("Handed off fragment deleted: {} (o={})", thisKey, o);
                    }
                    public void receiveException(Exception e) {
                      logger.warn("Delete failed during handoff: {}, returned ", thisKey, e);
                    }
                  });
                } else {
                  logger.warn("Handoff response for {}, for which I am still responsible (attack?) -- ignored", thisKey);
                }
              } else {
                fragmentStorage.getObject(thisKey, new Continuation<Object, Exception>() {
                  public void receiveResult(Object o) {
                    if (o != null) {
                      logger.info("Fragment {} found ({}), handing off...", thisKey, o);
                      FragmentAndManifest fam = (FragmentAndManifest) o;
                      sendMessage(
                        null,
                        new GlacierDataMessage(grm.getUID(), thisKey, fam.getFragment(), fam.getManifest(), getLocalNodeHandle(), grm.getSource().getId(), true, GlacierTags.HANDOFF),
                        grm.getSource()
                      );
                    } else {
                      logger.warn("Handoff failed; fragment {} not found in fragment store", thisKey);
                    }
                  }
                  public void receiveException(Exception e) {
                    logger.warn("Handoff failed; exception while fetching {}, e=", thisKey, e);
                  }
                });
              }
            } else {
              if (logger.isDebugEnabled()) logger.debug("Ignoring fragment "+thisKey+" (haveIt="+grm.getHaveIt(i)+", authoritative="+grm.getAuthoritative(i)+")");
            }
          }
        } else if (o instanceof GlacierDataMessage) {
          final GlacierDataMessage gdm = (GlacierDataMessage) o;
          for (int i=0; i<gdm.numKeys(); i++) {
            final FragmentKey thisKey = gdm.getKey(i);
            final Fragment thisFragment = gdm.getFragment(i);
            final Manifest thisManifest = gdm.getManifest(i);

            if ((thisFragment != null) && (thisManifest != null)) {
              logger.info("Handoff: Received Fragment+Manifest for {}", thisKey);

              if (!responsibleRange.containsId(locator.getFragmentLocation(thisKey))) {
                logger.atWarn().log(() -> "Handoff: Not responsible for "+thisKey+" (at "+locator.getFragmentLocation(thisKey)+") -- discarding");
                continue;
              }

              if (!policy.checkSignature(thisManifest, thisKey.getVersionKey())) {
                logger.warn("Handoff: Manifest is not signed properly");
                continue;
              }

              if (!thisManifest.validatesFragment(thisFragment, thisKey.getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance))) {
                logger.warn("Handoff: Manifest does not validate this fragment");
                continue;
              }

              if (!fragmentStorage.exists(thisKey)) {
                logger.debug("Handoff: Verified ok. Storing locally.");

                FragmentAndManifest fam = new FragmentAndManifest(thisFragment, thisManifest);

                fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), 0, environment.getTimeSource().currentTimeMillis()), fam,
                  new Continuation<Object, Exception>() {
                    public void receiveResult(Object o) {
                      logger.info("Handoff: Stored OK, sending receipt: {}", thisKey);

                      sendMessage(
                        null,
                        new GlacierResponseMessage(gdm.getUID(), thisKey, true, thisManifest.getExpiration(), responsibleRange.containsId(locator.getFragmentLocation(thisKey)), getLocalNodeHandle(), gdm.getSource().getId(), true, GlacierTags.HANDOFF),
                        gdm.getSource()
                      );
                    }

                    public void receiveException(Exception e) {
                      logger.warn("Handoff: receiveException({}) while storing a fragment -- unexpected, ignored (key={})", e, thisKey);
                    }
                  }
                );
              } else {
                logger.warn("Handoff: We already have a fragment with this key! -- sending response");
                sendMessage(
                  null,
                  new GlacierResponseMessage(gdm.getUID(), thisKey, true, thisManifest.getExpiration(), true, getLocalNodeHandle(), gdm.getSource().getId(), true, GlacierTags.HANDOFF),
                  gdm.getSource()
                );

                continue;
              }

              continue;
            } else {
              logger.warn("Handoff: Either fragment or manifest are missing!");
              continue;
            }
          }
        } else {
          logger.warn("Unexpected response in handoff continuation: {} -- ignored", o);
        }
      }
      public void receiveException(Exception e) {
        logger.warn("Exception in handoff continuation: ", e);
      }
      public void timeoutExpired() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + jitterTerm(config.handoffInterval());
        logger.info("Checking fragment storage for fragments to hand off...");
        logger.debug("Currently responsible for: {}", responsibleRange);
        Iterator<Id> iter = fragmentStorage.scan().getIterator();
        List<Id> handoffs = new ArrayList<Id>();
        Id destination = null;

        while (iter.hasNext()) {
          FragmentKey fkey = (FragmentKey) iter.next();
          Id thisPos = locator.getFragmentLocation(fkey);
          if (!responsibleRange.containsId(thisPos)) {
            logger.debug("Must hand off {} @{}", fkey, thisPos);
            handoffs.add(fkey);

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

        sendMessage(
          destination,
          new GlacierQueryMessage(getMyUID(), keys, getLocalNodeHandle(), destination, GlacierTags.HANDOFF),
          null
        );
      }
      public long getTimeout() {
        return nextTimeout;
      }
    });

    /* Garbage collection */

    scheduler.addContinuation(new GlacierContinuation() {
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
                deleteFragment(thisKey, new Continuation<Object, Exception>() {
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
    });

    /* Local scan */

    scheduler.addContinuation(new GlacierContinuation() {
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
        nextTimeout = environment.getTimeSource().currentTimeMillis() + jitterTerm(config.localScanInterval());

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

            if (responsibleRange.containsId(locator.getFragmentLocation(thisObjectKey, fidLeft, thisVersion))) {
              if (!fragments.isMemberId(thisKey.getPeerKey(fidLeft))) {
                logger.trace("Missing: {} L={}", thisKey, fidLeft);
                queries.add(thisKey.getVersionKey());
              }
            }

            if (responsibleRange.containsId(locator.getFragmentLocation(thisObjectKey, fidRight, thisVersion))) {
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
              } else if (responsibleRange.containsId(locator.getFragmentLocation(keyHere))) {
                queriesHere ++;
              }
            }

            logger.debug("Local scan: Fetching manifest for {} ({} pending queries)", thisVKey, queriesHere);
            queriesSent += queriesHere;

            fragmentStorage.getObject(new FragmentKey(thisVKey, localFragmentID), new Continuation<Object, Exception>() {
              public void receiveResult(Object o) {
                if (o instanceof FragmentAndManifest) {
                  final Manifest thisManifest = ((FragmentAndManifest)o).getManifest();

                  for (int i=0; i<numFragments; i++) {
                    final FragmentKey thisKey = new FragmentKey(thisVKey, i);
                    if (responsibleRange.containsId(locator.getFragmentLocation(thisKey))) {
                      if (!fragments.isMemberId(thisKey)) {
                        logger.debug("Local scan: Sending query for {}", thisKey);
                        final long tStart = environment.getTimeSource().currentTimeMillis();
                        rateLimitedRetrieveFragment(thisKey, thisManifest, GlacierTags.LOCAL_SCAN, new GlacierContinuation() {
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

          logger.info("{} queries sent after local scan", queriesSent);
        } else {
          logger.info("Local scan completed; no missing fragments");
        }
      }
    });

    /* Traffic shaper */

    scheduler.addContinuation(new GlacierContinuation() {
      long nextTimeout;

      public String toString() {
        return "Traffic shaper";
      }
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.rateLimitedCheckInterval();
      }
      public void receiveResult(Object o) {
        logger.warn("TS received object: {}", o);
      }
      public void receiveException(Exception e) {
        logger.warn("TS received exception: ", e);
      }
      public long getTimeout() {
        return nextTimeout;
      }
      public void timeoutExpired() {
        /* Use relative timeout to avoid backlog! */
        nextTimeout = environment.getTimeSource().currentTimeMillis() + (1 * GlacierConfig.SECONDS);

        if (pendingTraffic.isEmpty()) {
          logger.debug("Traffic shaper: Idle");
          nextTimeout += config.rateLimitedCheckInterval();
          return;
        }

        int numCurrentRestores = 0;
        synchronized (numActiveRestores) {
          numCurrentRestores = numActiveRestores[0];
        }

        logger.info("Traffic shaper: {} jobs waiting ({} active jobs, {} tokens)", pendingTraffic.size(), numCurrentRestores, tokenBucket);

        updateTokenBucket();
        if ((numCurrentRestores < config.maxActiveRestores()) && (tokenBucket>0)) {
          for (int i=0; i<rateLimitedRequestsPerSecond; i++) {
            if (!pendingTraffic.isEmpty()) {
              Enumeration<Id> keys = pendingTraffic.keys();
              Object thisKey = (Object) keys.nextElement();
              logger.debug("Sending request {}", thisKey);
              Continuation<Object, Exception> c = pendingTraffic.remove(thisKey);
              c.receiveResult(Boolean.valueOf(true));
            }
          }
        }
      }
    });

    /* Statistics */

    scheduler.addContinuation(new GlacierContinuation() {
      long nextTimeout;

      public String toString() {
        return "Statistics";
      }
      public void init() {
        nextTimeout = environment.getTimeSource().currentTimeMillis() + config.statisticsReportInterval();
      }
      public void receiveResult(Object o) {
        logger.warn("STAT received object: {}", o);
      }
      public void receiveException(Exception e) {
        logger.warn("STAT received exception: ", e);
      }
      public long getTimeout() {
        return nextTimeout;
      }
      public void timeoutExpired() {
        nextTimeout += config.statisticsReportInterval();

        if (!listeners.isEmpty()) {
          statistics.setPendingRequests(pendingTraffic.size());
          statistics.setNumNeighbors(neighborStorage.scan().numElements());
          statistics.setNumFragments(fragmentStorage.scan().numElements());
          statistics.setNumContinuations(scheduler.size());
          statistics.setResponsibleRange(responsibleRange);
          statistics.setActiveFetches(numActiveRestores[0]);
          statistics.setBucketMin(bucketMin);
          statistics.setBucketMax(bucketMax);
          statistics.setBucketConsumed(bucketConsumed);
          statistics.setBucketTokensPerSecond(bucketTokensPerSecond);
          statistics.setBucketMaxBurstSize(bucketMaxBurstSize);
          bucketMin = tokenBucket;
          bucketMax = tokenBucket;
          bucketConsumed = 0;

          Storage storageF = fragmentStorage.getStorage();
          if (storageF instanceof PersistentStorage)
            statistics.setFragmentStorageSize(((PersistentStorage)storageF).getTotalSize());

          if (trashStorage != null)
            statistics.setNumObjectsInTrash(trashStorage.scan().numElements());

          Storage storageT = (trashStorage == null) ? null : trashStorage.getStorage();
          if (storageT instanceof PersistentStorage)
            statistics.setTrashStorageSize(((PersistentStorage)storageT).getTotalSize());

          if (config.logStatistics())
            statistics.dump(environment.getLogManager().getLogger(GlacierStatistics.class, instance));

          // listeners are added and removed by other threads; copying the Vector is atomic, enumerating it is not
          for (GlacierStatisticsListener gsl : new ArrayList<GlacierStatisticsListener>(listeners)) {
            gsl.receiveStatistics(statistics);
          }
        }

        statistics = new GlacierStatistics(GlacierTags.MAX, environment);
      }
    });
  }

  protected void updateTokenBucket() {
    final long now = environment.getTimeSource().currentTimeMillis();
    final long contentsBefore = tokenBucket;

    while (bucketLastUpdated < now) {
      bucketLastUpdated += GlacierConfig.SECONDS/10;
      tokenBucket += bucketTokensPerSecond/10;
      if (tokenBucket > bucketMaxBurstSize)
        tokenBucket = bucketMaxBurstSize;
    }

    if (bucketMax < tokenBucket)
      bucketMax = tokenBucket;

    logger.atDebug().log(() -> "Token bucket contains "+tokenBucket+" tokens (added "+(tokenBucket-contentsBefore)+")");
  }

  private long jitterTerm(long basis) {
    return (long)((1-config.jitterRange())*basis) + environment.getRandomSource().nextInt((int)(2*config.jitterRange()*basis));
  }

  private void deleteFragment(final Id fkey, final Continuation<? super Boolean, Exception> command) {
    if (trashStorage != null) {
      logger.atInfo().log(() -> "Moving fragment "+fkey.toStringFull()+" to trash");
      fragmentStorage.getObject(fkey, new Continuation<Object, Exception>() {
        public void receiveResult(Object o) {
          logger.atDebug().log(() -> "Fragment "+fkey.toStringFull()+" retrieved, storing in trash");
          if (o != null) {
            trashStorage.store(fkey, null, (Serializable) o, new Continuation<Object, Exception>() {
              public void receiveResult(Object o) {
                logger.atDebug().log(() -> "Deleting fragment "+fkey.toStringFull());
                fragmentStorage.unstore(fkey, command);
              }
              public void receiveException(Exception e) {
                logger.atWarn().setCause(e).log(() -> "Cannot store in trash: "+fkey.toStringFull()+", e=");
                command.receiveException(e);
              }
            });
          } else {
            receiveException(new GlacierException("Move to trash: Fragment "+fkey+" does not exist?!?"));
          }
        }
        public void receiveException(Exception e) {
          logger.warn("Cannot retrieve fragment {} for deletion: e=", fkey, e);
          command.receiveException(new GlacierException("Cannot retrieve fragment "+fkey+" for deletion"));
        }
      });
    } else {
      logger.atInfo().log(() -> "Deleting fragment "+fkey.toStringFull());
      fragmentStorage.unstore(fkey, command);
    }
  }

  public void sendMessage(Id id, GlacierMessage message, NodeHandle hint) {
    String className = message.getClass().getName();
    logger.atInfo().log(() -> "Send " + ((hint == null) ? "OVR" : "DIR") + " T" + ((int)message.getTag()) + " " + className.substring(className.lastIndexOf('.') + 8));
    statistics.countMessageSent(message.getTag());
    endpoint.route(id, message, hint);
  }

  public void setTrashcan(StorageManager trashStorage) {
    this.trashStorage = trashStorage;
  }

  private byte[] getHashInput(VersionKey vkey, long expiration) {
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

  private void determineResponsibleRange() {
    Id cwPeer = null, ccwPeer = null, xcwPeer = null, xccwPeer = null, myNodeId = getLocalNodeHandle().getId();

    logger.debug("Determining responsible range");

    Iterator<Id> iter = neighborStorage.scan().getIterator();
    while (iter.hasNext()) {
      Id thisNeighbor = (Id) iter.next();
      logger.trace("Considering neighbor: {}", thisNeighbor);
      if (myNodeId.clockwise(thisNeighbor)) {
        if ((cwPeer == null) || thisNeighbor.isBetween(myNodeId, cwPeer))
          cwPeer = thisNeighbor;
        if ((xcwPeer == null) || xcwPeer.clockwise(thisNeighbor))
          xcwPeer = thisNeighbor;
      } else {
        if ((ccwPeer == null) || thisNeighbor.isBetween(ccwPeer, myNodeId))
          ccwPeer = thisNeighbor;
        if ((xccwPeer == null) || !xccwPeer.clockwise(thisNeighbor))
          xccwPeer = thisNeighbor;
      }
    }

    if (ccwPeer == null)
      ccwPeer = xcwPeer;
    if (cwPeer == null)
      cwPeer = xccwPeer;

    if (logger.isDebugEnabled()) logger.debug("XCCW: "+xccwPeer+" CCW: "+ccwPeer+" ME: "+myNodeId+" CW: "+cwPeer+" XCW: "+xcwPeer);

    if ((ccwPeer == null) || (cwPeer == null)) {
      responsibleRange = factory.buildIdRange(myNodeId, myNodeId);
      return;
    }

    Id.Distance ccwHalfDistance;
    if (!myNodeId.clockwise(ccwPeer))
      ccwHalfDistance = ccwPeer.distanceFromId(myNodeId).shiftDistance(1,0);
    else
      ccwHalfDistance = ccwPeer.longDistanceFromId(myNodeId).shiftDistance(1,0);

    Id.Distance cwHalfDistance;
    if (myNodeId.clockwise(cwPeer))
      cwHalfDistance = cwPeer.distanceFromId(myNodeId).shiftDistance(1,0);
    else
      cwHalfDistance = cwPeer.longDistanceFromId(myNodeId).shiftDistance(1,0);

    responsibleRange = factory.buildIdRange(
      ccwPeer.addToId(ccwHalfDistance),
      myNodeId.addToId(cwHalfDistance)
    );

    logger.info("New range: {}", responsibleRange);
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

  /**
   * How long a debug command waits for its asynchronous result.
   */
  private static final long DEBUG_COMMAND_TIMEOUT = 2 * 60 * 1000;

  private String awaitDebugResult(CompletableFuture<String> result, long timeoutMillis) {
    return DebugCommandHandler.await(result, environment.getSelectorManager(), timeoutMillis);
  }

  public String handleDebugCommand(String command)
  {
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
        boolean isMine = responsibleRange.containsId(locator.getFragmentLocation(thisKey));
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
        "neighborTimeout = " + (int)(neighborTimeout / GlacierConfig.HOURS) + " hrs\n" +
        "config.syncDelayAfterJoin() = " + (int)(config.syncDelayAfterJoin() / GlacierConfig.SECONDS) + " sec\n" +
        "config.syncMinRemainingLifetime() = " + (int)(config.syncMinRemainingLifetime() / GlacierConfig.SECONDS) + " sec\n" +
        "config.syncMinQuietTime() = " + (int)(config.syncMinQuietTime() / GlacierConfig.SECONDS) + " sec\n" +
        "syncBloomFilter = " + config.syncBloomFilterNumHashes() + " hashes, " + config.syncBloomFilterBitsPerKey() + " bpk\n" +
        "config.syncPartnersPerTrial() = " + config.syncPartnersPerTrial() + "\n" +
        "syncInterval = " + (int)(syncInterval / GlacierConfig.MINUTES) + " min\n" +
        "syncMaxFragments = " + syncMaxFragments + "\n" +
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
        "rateLimitedRequestsPerSecond = " + rateLimitedRequestsPerSecond + "\n";
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
      refresh(new Id[] { id }, expiration, new Continuation<Object, Exception>() {
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
      result = result + "Responsible for: "+responsibleRange + "\n";
      result = result + "Local time: "+(new Date()) + "\n\n";
      result = result + fragmentStorage.scan().numElements() + " fragments\n";
      result = result + neighborStorage.scan().numElements() + " neighbors\n";
      result = result + scheduler.size() + " active continuations\n";
      result = result + pendingTraffic.size() + " pending requests\n";

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
        pendingTraffic.put(new VersionKey(randomID, 0), new Continuation.SimpleContinuation() {
          public void receiveResult(Object o) {
            insert(
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
            sendMessage(
              fragmentLoc,
              new GlacierQueryMessage(getMyUID(), new FragmentKey[] { id }, getLocalNodeHandle(), fragmentLoc, GlacierTags.DEBUG),
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
      retrieveManifest(vkey, GlacierTags.DEBUG, new Continuation<Object, Exception>() {
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
          MessageDigest md = null;
          try {
            md = MessageDigest.getInstance("SHA");
          } catch (NoSuchAlgorithmException e) {
            logger.error("SHA digest unavailable", e);
            ret.complete("exception(" + e + ")");
            return;
          }

          md.reset();
          md.update(fam.getFragment().getPayload());

          ret.complete("OK\n\nFragment: "+fam.getFragment().getPayload().length+" bytes, Hash=["+dump(md.digest(), false)+"], ID="+id.getFragmentID()+"\n\nValidation: " +
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
          sendMessage(
            fragmentLoc,
            new GlacierFetchMessage(getMyUID(), id, GlacierFetchMessage.FETCH_FRAGMENT_AND_MANIFEST, getLocalNodeHandle(), fragmentLoc, GlacierTags.DEBUG),
            null
          );
        }
        public void receiveResult(Object o) {
          if (o instanceof GlacierDataMessage) {
            GlacierDataMessage gdm = (GlacierDataMessage) o;
            MessageDigest md = null;
            try {
              md = MessageDigest.getInstance("SHA");
            } catch (NoSuchAlgorithmException e) {
              logger.error("SHA digest unavailable", e);
              ret.complete("Exception=" + e);
              terminate();
              return;
            }

            md.reset();
            md.update(gdm.getFragment(0).getPayload());

            ret.complete("\n\nResponse: "+gdm.getKey(0).toStringFull()+" ("+gdm.numKeys()+" keys)\n" +  "Holder: "+gdm.getSource()+"\n" +
                     "Fragment: "+gdm.getFragment(0).getPayload().length+" bytes, Hash=["+dump(md.digest(), false)+"]\n\nValidation: " +
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

  public void insert(final PastContent obj, final Continuation<? super Boolean[], Exception> command) {
    insert(obj, GCPast.INFINITY_EXPIRATION, command);
  }

  public void refresh(Id[] ids, long[] expirations, Continuation<? super Object[], Exception> command) {
    long[] versions = new long[ids.length];
    Arrays.fill(versions, 0);
    refresh(ids, versions, expirations, command);
  }

  public void refresh(Id[] ids, long expiration, Continuation<? super Object[], Exception> command) {
    long[] expirations = new long[ids.length];
    Arrays.fill(expirations, expiration);
    refresh(ids, expirations, command);
  }

  public void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation<? super Object[], Exception> command) {
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
        Continuation<Object, Exception> prev = pendingTraffic.put(thisVersionKey, new Continuation.SimpleContinuation() {
          public void receiveResult(Object o) {
            retrieveManifest(thisVersionKey, GlacierTags.REFRESH, new Continuation<Object, Exception>() {
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

        public String toString() {
          return "AggregateRefresh continuation ("+fragmentKey.length+" fragments)";
        }
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
                fragmentHolder[i][j] = getLocalNodeHandle();
              } else {
                fragmentHolder[i][j] = null;
              }
            }
          }

          if (haveFragmentMyself)
            holders.add(getLocalNodeHandle());

          Arrays.fill(successes, 0);

          logger.debug("AR Initialization completed, {} candidate objects. Triggering first probe...", fragmentKey.length);
          timeoutExpired();
        }
        public void receiveResult(Object o) {
          if (o instanceof GlacierRefreshResponseMessage) {
            GlacierRefreshResponseMessage grrm = (GlacierRefreshResponseMessage) o;
            IdRange thisRange = grrm.getRange();
            NodeHandle holder = grrm.isOnline() ? grrm.getSource() : null;

            logger.debug("AR got refresh response: range {}, online={}", thisRange, grrm.isOnline());
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
          } else if (o instanceof GlacierDataMessage) {
            GlacierDataMessage gdm = (GlacierDataMessage) o;

            logger.atDebug().log(() -> "AR Received data message with "+gdm.numKeys()+" keys");
            for (int i=0; i<gdm.numKeys(); i++) {
              if ((gdm.getManifest(i) != null) && (gdm.getKey(i) != null)) {
                Manifest thisManifest = gdm.getManifest(i);
                FragmentKey thisKey = gdm.getKey(i);

                if (logger.isDebugEnabled()) logger.debug("AR Received manifest for "+gdm.getKey(i)+", checking signature...");
                if (policy.checkSignature(thisManifest, thisKey.getVersionKey())) {
                  logger.debug("AR Signature OK");
                  for (int j=0; j<ids.length; j++) {
                    if ((manifests[j] == null) && (versionKey[j].equals(thisKey.getVersionKey()))) {
                      manifests[j] = thisManifest;
                      logger.debug("AR Storing under #{}", j);
                    }
                  }
                } else {
                  logger.warn("AR Invalid signature");
                }
              }
            }
          } else if (o instanceof GlacierRefreshCompleteMessage) {
            GlacierRefreshCompleteMessage grcm = (GlacierRefreshCompleteMessage) o;
            logger.debug("AR Refresh completion reported by {}", grcm.getSource());

            for (int i=0; i<grcm.numKeys(); i++) {
              if (logger.isDebugEnabled()) logger.debug("AR Refresh completion: Key "+grcm.getKey(i)+", "+grcm.getUpdates(i)+" update(s)");

              int index = -1;
              for (int j=0; j<ids.length; j++) {
                if (grcm.getKey(i).equals(versionKey[j]))
                  index = j;
              }

              if (index >= 0) {
                int maxSuccesses = 0;
                for (int j=0; j<numFragments; j++) {
                  if (!fragmentChecked[index][j] && (fragmentHolder[index][j] != null) && (fragmentHolder[index][j].equals(grcm.getSource()))) {
                    maxSuccesses ++;
                    fragmentChecked[index][j] = true;
                  }
                }

                if (grcm.getUpdates(i) > maxSuccesses) {
                  if (logger.isWarnEnabled()) logger.warn("Node "+grcm.getSource()+" reports "+grcm.getUpdates(i)+" for "+grcm.getKey(i)+", but is responsible for only "+maxSuccesses+" fragments -- duplicate message, or under attack?");
                  successes[index] += maxSuccesses;
                } else {
                  successes[index] += grcm.getUpdates(i);
                }
              } else {
                if (logger.isWarnEnabled()) logger.warn("Node "+grcm.getSource()+" reports completion for "+grcm.getKey(i)+", but no refresh request matches?!?");
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
            logger.warn("Unexpected result in AR continuation: {} -- discarded", o);
          }
        }
        public void receiveException(Exception e) {
          logger.warn("Exception during AggregateRefresh: ", e);
          terminate();

          if (!answered) {
            Object[] result = new Object[ids.length];
            Exception ee = new GlacierException("Exception during refresh: "+e);

            for (int i=0; i<ids.length; i++)
              result[i] = ee;

            answered = true;
            command.receiveResult(result);
          }
        }
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
              sendMessage(
                fragmentLocation[nextProbe][nextFID],
                new GlacierRefreshProbeMessage(getMyUID(), fragmentLocation[nextProbe][nextFID], getLocalNodeHandle(), fragmentLocation[nextProbe][nextFID], GlacierTags.REFRESH),
                null
              );
            } else {
              currentStage = stageFetchingManifests;
              retriesRemaining = 3;
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

            if (!allObjectsCovered && ((retriesRemaining--) > 0)) {
              logger.debug("AR Fetching manifests, {} attempts remaining", retriesRemaining);
              while (true) {
                int idx = environment.getRandomSource().nextInt(ids.length);
                int maxSteps = ids.length + 2;
                while (objectCovered[idx] && ((--maxSteps)>0))
                  idx = (idx+1) % ids.length;
                if (maxSteps <= 0)
                  break;

                int fid = environment.getRandomSource().nextInt(numFragments);
                maxSteps = numFragments + 2;
                while ((fragmentHolder[idx][fid] == null) && ((--maxSteps)>0))
                  fid = (fid+1) % numFragments;

                if (fragmentHolder[idx][fid] != null) {
                  NodeHandle thisHolder = fragmentHolder[idx][fid];
                  List<Id> idsToQuery = new ArrayList<Id>();
                  for (int i=0; i<ids.length; i++) {
                    if (!objectCovered[i]) {
                      for (int j=0; j<numFragments; j++) {
                        if ((fragmentHolder[i][j] != null) && (fragmentHolder[i][j].equals(thisHolder))) {
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
                    sendMessage(
                      null,
                      new GlacierFetchMessage(getMyUID(), keys, GlacierFetchMessage.FETCH_MANIFEST, getLocalNodeHandle(), thisHolder.getId(), GlacierTags.REFRESH),
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
              for (int i=0; i<ids.length; i++)
                if (manifests[i] != null)
                  manifests[i] = policy.updateManifest(versionKey[i], manifests[i], expirations[i]);

              logger.debug("AR Done patching manifests");

              for (int i=0; i<ids.length; i++)
                for (int j=0; j<numFragments; j++)
                  fragmentChecked[i][j] = ((fragmentHolder[i][j] == null) || (manifests[i] == null));
            }
          }

          if (currentStage == stagePatching) {
            nextTimeout = environment.getTimeSource().currentTimeMillis() + config.bulkRefreshPatchInterval();

            if ((retriesRemaining--) > 0) {
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

                  sendMessage(
                    null,
                    new GlacierRefreshPatchMessage(getMyUID(), keys, lifetimes, signatures, getLocalNodeHandle(), thisHolder.getId(), GlacierTags.REFRESH),
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
                result[i] = (successes[i] >= minAcceptable) ? (Object)(Boolean.valueOf(true)) : (Object)(new GlacierException("Only "+successes[i]+" fragments of "+versionKey[i]+" refreshed successfully; need "+minAcceptable));
                if (logger.isDebugEnabled()) logger.debug(" - AR Result for "+versionKey[i]+": " + ((result[i] instanceof Boolean) ? "OK" : "Failed") + " (with "+successes[i]+"/"+numFragments+" fragments, "+minAcceptable+" acceptable)");
              }

              answered = true;
              command.receiveResult(result);
            }
          }
        }
        public long getTimeout() {
          return nextTimeout;
        }
      });
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
          sendMessage(
            fragmentLoc,
            new GlacierQueryMessage(getMyUID(), keys, getLocalNodeHandle(), fragmentLoc, tag),
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
                    sendMessage(
                      null,
                      new GlacierDataMessage(getMyUID(), grm.getKey(0), fragments[fragmentID], manifests[fragmentID], getLocalNodeHandle(), grm.getSource().getId(), false, tag),
                      grm.getSource()
                    );
                  } else {
                    if (numHoldersKnown() >= minAcceptable) {
                      logger.atDebug().log(() -> "Got "+numHoldersKnown()+" insert responses, sending fragments...");
                      inhibitInsertions = false;
                      for (int i=0; i<holder.length; i++) {
                        if (holder[i] != null) {
                          logger.debug("Sending fragment #{}", i);
                          sendMessage(
                            null,
                            new GlacierDataMessage(getMyUID(), new FragmentKey(key, i), fragments[i], manifests[i], getLocalNodeHandle(), holder[i].getId(), false, tag),
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
                  sendMessage(
                    null,
                    new GlacierDataMessage(getMyUID(), grm.getKey(0), null, manifests[fragmentID], getLocalNodeHandle(), grm.getSource().getId(), false, tag),
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

  public void insert(final PastContent obj, final long expiration, final Continuation<? super Boolean[], Exception> command) {
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

  public void neighborSeen(Id nodeId, long when) {

    if (nodeId.equals(getLocalNodeHandle().getId()))
      return;

    logger.debug("Neighbor {} was seen at {}", nodeId, when);

    if (when > environment.getTimeSource().currentTimeMillis()) {
      if (logger.isWarnEnabled()) logger.warn("Neighbor: "+when+" is in the future (now="+environment.getTimeSource().currentTimeMillis()+")");
      when = environment.getTimeSource().currentTimeMillis();
    }

    final Id fNodeId = nodeId;
    final long fWhen = when;

    neighborStorage.getObject(nodeId,
      new Continuation<Object, Exception>() {
        public void receiveResult(Object o) {
          logger.debug("Continue: neighborSeen ({}, {}) after getObject", fNodeId, fWhen);

          final long previousWhen = (o!=null) ? ((Long)o).longValue() : 0;
          logger.debug("Neighbor: {} previously seen at {}", fNodeId, previousWhen);
          if (previousWhen >= fWhen) {
            logger.debug("Neighbor: No update needed (new TS={})", fWhen);
            return;
          }

          neighborStorage.store(fNodeId, null, Long.valueOf(fWhen),
            new Continuation<Object, Exception>() {
              public void receiveResult(Object o) {
                logger.debug("Continue: neighborSeen ({}, {}) after store", fNodeId, fWhen);
                logger.debug("Neighbor: Updated {} from {} to {}", fNodeId, previousWhen, fWhen);
                determineResponsibleRange();
              }
              public void receiveException(Exception e) {
                logger.warn("receiveException({}) while storing a neighbor ({})", e, fNodeId);
              }
            }
          );
        }
        public void receiveException(Exception e) {
          logger.warn("receiveException({}) while retrieving a neighbor ({})", e, fNodeId);
        }
      }
    );
  }

  public boolean forward(final RouteMessage message) {
    return true;
  }

  public void update(NodeHandle handle, boolean joined) {
    logger.atInfo().log(() -> "Leafset update: " + handle + " has " + (joined ? "joined" : "left"));

    if (!joined)
      return;

    neighborSeen(handle.getId(), environment.getTimeSource().currentTimeMillis());
  }

  public void lookupHandle(Id id, NodeHandle handle, Continuation<? super PastContentHandle, Exception> command) {
    command.receiveException(new UnsupportedOperationException("LookupHandle() is not supported on Glacier"));
  }

  public void lookupHandles(Id id, int num, Continuation<? super PastContentHandle[], Exception> command) {
    lookupHandles(id, 0, num, command);
  }

  public void lookupHandles(final Id id, final long version, int num, final Continuation<? super PastContentHandle[], Exception> command) {
    logger.info("lookupHandles({}v{}, n={})", id, version, num);

    retrieveManifest(new VersionKey(id, version), GlacierTags.LOOKUP_HANDLES, new Continuation<Object, Exception>() {
      boolean haveAnswered = false;
      public void receiveResult(Object o) {
        if (haveAnswered) {
          logger.debug("lookupHandles({}): received manifest {} but has already answered. Discarding...", id, o);
          return;
        }
        if (o instanceof Manifest) {
          logger.debug("lookupHandles({}): received manifest {}, returning handle...", id, o);
          haveAnswered = true;
          command.receiveResult(new PastContentHandle[] {
            new GlacierContentHandle(id, version, getLocalNodeHandle(), (Manifest) o)
          });
        } else {
          logger.warn("lookupHandles({}): Cannot retrieve manifest", id);
          haveAnswered = true;
          command.receiveResult(new PastContentHandle[] { null });
        }
      }
      public void receiveException(Exception e) {
        logger.warn("lookupHandles({}): Exception ", id, e);
        haveAnswered = true;
        command.receiveException(e);
      }
    });
  }

  public void lookup(Id id, long version, Continuation<? super PastContent, Exception> command) {
    VersionKey vkey = new VersionKey(id, version);
    logger.info("lookup({}v{})", id, version);
    retrieveObject(vkey, null, true, GlacierTags.LOOKUP, command);
  }

  public void lookup(Id id, boolean cache, Continuation<? super PastContent, Exception> command) {
    lookup(id, 0, command);
  }

  public void lookup(Id id, Continuation<? super PastContent, Exception> command) {
    lookup(id, 0, command);
  }

  public void fetch(PastContentHandle handle, Continuation<? super PastContent, Exception> command) {
    logger.info("fetch({})", handle.getId());

    if (!(handle instanceof GlacierContentHandle)) {
      command.receiveException(new GlacierException("Unknown handle type"));
      return;
    }

    GlacierContentHandle gch = (GlacierContentHandle) handle;
    logger.debug("exact: fetch({}v{})", gch.getId(), gch.getVersion());

    retrieveObject(new VersionKey(gch.getId(), gch.getVersion()), gch.getManifest(), true, GlacierTags.FETCH, command);
  }

  public void retrieveManifest(final VersionKey key, final char tag, final Continuation<? super Manifest, Exception> command) {
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
        sendMessage(
          nextLocation,
          new GlacierFetchMessage(getMyUID(), nextKey, GlacierFetchMessage.FETCH_MANIFEST, getLocalNodeHandle(), nextLocation, tag),
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

  public void retrieveObject(final VersionKey key, final Manifest manifest, final boolean beStrict, final char tag, final Continuation<? super PastContent, Exception> c) {
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
        synchronized (numActiveRestores) {
          numActiveRestores[0] ++;
        }

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
        synchronized (numActiveRestores) {
          numActiveRestores[0] --;
        }

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
        sendMessage(
          nextLocation,
          new GlacierFetchMessage(getMyUID(), nextKey, GlacierFetchMessage.FETCH_FRAGMENT, getLocalNodeHandle(), nextLocation, tag),
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

  public void retrieveFragment(final FragmentKey key, final Manifest manifest, final char tag, final GlacierContinuation c) {
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
              sendMessage(
                key.getVersionKey().getId(),
                new GlacierFetchMessage(getMyUID(), key, GlacierFetchMessage.FETCH_FRAGMENT, getLocalNodeHandle(), key.getVersionKey().getId(), tag),
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

    if (trashStorage!=null) {
      trashStorage.getObject(key, new Continuation<Object, Exception>() {
        public void receiveResult(Object o) {
          if (o != null) {
            logger.atDebug().log(() -> "retrieveFragment: Key "+key.toStringFull()+" found in trash, retrieving...");
          } else {
            logger.atDebug().log(() -> "retrieveFragment: Key "+key.toStringFull()+" not found in trash");
          }

          c2.receiveResult(o);
        }

        public void receiveException(Exception e) {
          logger.atWarn().log(() -> "Exception while getting object " + key + " from trash " + e);
          c2.receiveResult(null);
        }
      });
    } else {
      logger.atDebug().log(() -> "retrieveFragment: Key "+key.toStringFull()+" not found in trash");
      c2.receiveResult(null);
    }
  }

  public void rateLimitedRetrieveFragment(final FragmentKey key, final Manifest manifest, final char tag, final GlacierContinuation c) {
    logger.debug("rateLimitedRetrieveFragment({})", key);
    if (pendingTraffic.containsKey(key)) {
      logger.debug("Fragment is already being retrieved -- discarding request");
      return;
    }

    logger.debug("Added pending job: retrieveFragment({})", key);
    Continuation<Object, Exception> prev = pendingTraffic.put(key, new Continuation.SimpleContinuation() {
      public void receiveResult(Object o) {
        retrieveFragment(key, manifest, tag, c);
      }
    });

    if (prev != null)
      prev.receiveException(new GlacierException("Key collision in traffic shaper (rateLimitedRetrieveFragment)"));
  }

  public Id[][] getNeighborRanges() {
    Iterator<Id> iter = neighborStorage.scan().getIterator();
    List<Id> ccwIDs = new ArrayList<Id>();
    List<Id> cwIDs = new ArrayList<Id>();
    Id myID = getLocalNodeHandle().getId();

    while (iter.hasNext()) {
      Id thisNeighbor = (Id)iter.next();
      if (myID.clockwise(thisNeighbor))
        cwIDs.add(thisNeighbor);
      else
        ccwIDs.add(thisNeighbor);
    }

    for (int j=0; j<2; j++) {
      List<Id> v = (j==0) ? cwIDs : ccwIDs;
      boolean madeProgress = true;
      while (madeProgress) {
        madeProgress = false;
        for (int i=0; i<(v.size()-1); i++) {
          if (v.get(i+1).clockwise(v.get(i))) {
            Id h = v.get(i);
            v.set(i, v.get(i+1));
            v.set(i+1, h);
            madeProgress = true;
          }
        }
      }
    }

    List<Id> allIDs = new ArrayList<Id>();
    allIDs.addAll(ccwIDs);
    allIDs.add(myID);
    allIDs.addAll(cwIDs);

    Id[][] result = new Id[allIDs.size()][3];
    for (int i=0; i<allIDs.size(); i++) {
      Id currentElement = allIDs.get(i);
      Id cwId, ccwId;

      if (i>0) {
        Id previousElement = allIDs.get(i-1);
        ccwId = previousElement.addToId(previousElement.distanceFromId(currentElement).shiftDistance(1,0));
      } else {
        ccwId = currentElement;
      }

      if (i<(allIDs.size()-1)) {
        Id nextElement = allIDs.get(i+1);
        cwId = currentElement.addToId(currentElement.distanceFromId(nextElement).shiftDistance(1,0));
      } else {
        cwId = currentElement;
      }

      result[i][0] = ccwId;
      result[i][1] = currentElement;
      result[i][2] = cwId;
    }

    return result;
  }

  public void deliver(Id id, Message message) {

    final GlacierMessage msg = (GlacierMessage) message;
    logger.debug("Received message {} with destination {} from {}", msg, id, msg.getSource().getId());

    if (msg instanceof GlacierDataMessage) {
      GlacierDataMessage gdm = (GlacierDataMessage) msg;
      long thisSize = 1000;
      updateTokenBucket();
      for (int i=0; i<gdm.numKeys(); i++) {
        if (gdm.getFragment(i) != null)
          thisSize += gdm.getFragment(i).getPayload().length;
        if (gdm.getManifest(i) != null)
          thisSize += numFragments * 21;
      }

      tokenBucket -= thisSize;
      bucketConsumed += thisSize;
      if (bucketMin > tokenBucket)
        bucketMin = tokenBucket;

      logger.debug("Token bucket contains {} tokens (consumed {})", tokenBucket, thisSize);
    }

    if (msg.isResponse()) {
      GlacierContinuation gc = scheduler.get(msg.getUID());

      if (gc != null) {
        if (!gc.hasTerminated()) {
          logger.debug("Resuming [{}]", gc);
          gc.syncReceiveResult(msg);
          logger.debug("---");
        } else {
          logger.debug("Message UID#{} is response, but continuation has already terminated", msg.getUID());
        }
      } else {
        logger.debug("Unusual: Message UID#{} is response, but continuation not found", msg.getUID());
      }

      return;
    }

    if (msg instanceof GlacierQueryMessage) {

      /* When a QueryMessage arrives, we check whether we have the fragment
         with the corresponding key and then send back a ResponseMessage. */

      GlacierQueryMessage gqm = (GlacierQueryMessage) msg;
      FragmentKey[] keyA = new FragmentKey[gqm.numKeys()];
      boolean[] haveItA = new boolean[gqm.numKeys()];
      long[] expirationA = new long[gqm.numKeys()];
      boolean[] authoritativeA = new boolean[gqm.numKeys()];

      for (int i=0; i<gqm.numKeys(); i++) {
        Id fragmentLocation = locator.getFragmentLocation(gqm.getKey(i));
        if (logger.isInfoEnabled()) logger.info("Queried for " + gqm.getKey(i) + " (at "+fragmentLocation+")");

        keyA[i] = gqm.getKey(i);
        haveItA[i] = fragmentStorage.exists(gqm.getKey(i));
        if (haveItA[i]) {
          FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(gqm.getKey(i));
          if (metadata != null) {
            expirationA[i] = metadata.getCurrentExpiration();
          } else {
            if (logger.isWarnEnabled()) logger.warn("QUERY cannot read metadata in object "+gqm.getKey(i).toStringFull()+", storage returned null");
            expirationA[i] = 0;
            haveItA[i] = false;
          }
        } else {
          expirationA[i] = 0;
        }
        logger.debug("My range is {}", responsibleRange);
        logger.debug("Location is {}", fragmentLocation);
        authoritativeA[i] = responsibleRange.containsId(fragmentLocation);
        logger.debug("Result: haveIt={} amAuthority={} expiration={}", haveItA[i], authoritativeA[i], expirationA[i]);
      }

      sendMessage(
        null,
        new GlacierResponseMessage(gqm.getUID(), keyA, haveItA, expirationA, authoritativeA, getLocalNodeHandle(), gqm.getSource().getId(), true, gqm.getTag()),
        gqm.getSource()
      );

    } else if (msg instanceof GlacierNeighborRequestMessage) {
      final GlacierNeighborRequestMessage gnrm = (GlacierNeighborRequestMessage) msg;
      final IdSet requestedNeighbors = neighborStorage.scan(gnrm.getRequestedRange());
      final int numRequested = requestedNeighbors.numElements();

      if (numRequested < 1) {
        logger.debug("No neighbors in that range -- canceled");
        return;
      }

      logger.info("Neighbor request for {}, found {} neighbors", gnrm.getRequestedRange(), numRequested);

      final Id[] neighbors = new Id[numRequested];
      final long[] lastSeen = new long[numRequested];

      Iterator<Id> iter = requestedNeighbors.getIterator();
      for (int i=0; i<numRequested; i++)
        neighbors[i] = (Id)(iter.next());

      neighborStorage.getObject(neighbors[0], new Continuation<Object, Exception>() {
        int currentLookup = 0;

        public void receiveResult(Object o) {
          logger.debug("Continue: NeighborRequest from {} for range {}", gnrm.getSource().getId(), gnrm.getRequestedRange());

          if (o == null) {
            logger.warn("Problem while retrieving neighbors -- canceled");
            return;
          }

          if (o instanceof Long) {
            logger.debug("Retr: Neighbor {} was last seen at {}", neighbors[currentLookup], o);
            lastSeen[currentLookup] = ((Long)o).longValue();
            currentLookup ++;
            if (currentLookup < numRequested) {
              neighborStorage.getObject(neighbors[currentLookup], this);
            } else {
              logger.debug("Sending neighbor response...");
              sendMessage(
                null,
                new GlacierNeighborResponseMessage(gnrm.getUID(), neighbors, lastSeen, getLocalNodeHandle(), gnrm.getSource().getId(), gnrm.getTag()),
                gnrm.getSource()
              );
            }
          }
        }

        public void receiveException(Exception e) {
          logger.warn("Problem while retrieving neighbors in range {} for {} -- canceled", gnrm.getRequestedRange(), gnrm.getSource(), e);
        }
      });

      return;

    } else if (msg instanceof GlacierSyncMessage) {

      final GlacierSyncMessage gsm = (GlacierSyncMessage) msg;

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
            if (!bv.contains(getHashInput(fkey.getVersionKey(), metadata.getCurrentExpiration()))) {
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

            sendMessage(
              null,
              new GlacierDataMessage(scheduler.getUID(), keys, fragments, manifests, getLocalNodeHandle(), gsm.getSource().getId(), false, GlacierTags.SYNC_MANIFESTS),
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

      return;

    } else if (msg instanceof GlacierRefreshProbeMessage) {
      final GlacierRefreshProbeMessage grpm = (GlacierRefreshProbeMessage) msg;
      Id requestedId = grpm.getRequestedId();

      logger.info("Refresh probe for {} (RR={})", requestedId, responsibleRange);

      Id[][] ranges = getNeighborRanges();
      IdRange returnedRange = null;
      boolean online = false;

      if (responsibleRange.containsId(requestedId)) {
        returnedRange = responsibleRange;
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

      sendMessage(
        null,
        new GlacierRefreshResponseMessage(grpm.getUID(), returnedRange, online, getLocalNodeHandle(), grpm.getSource().getId(), grpm.getTag()),
        grpm.getSource()
      );

    } else if (msg instanceof GlacierRefreshPatchMessage) {
      final GlacierRefreshPatchMessage grpm = (GlacierRefreshPatchMessage) msg;

      logger.atInfo().log(() -> "AR Refresh patches received for "+grpm.numKeys()+" keys. Processing...");
      Continuation<Object, Exception> c = new Continuation<Object, Exception>() {
        static final int phaseFetch = 1;
        static final int phaseStore = 2;
        static final int phaseAdvance = 3;
        int[] successes = new int[grpm.numKeys()];
        int currentPhase = phaseAdvance;
        FragmentKey currentKey = null;
        int currentIndex = 0;
        int currentFID = -1;

        public void receiveResult(Object o) {
          if (currentPhase == phaseFetch) {
            logger.debug("AR Patch: Got FAM for {}", currentKey);

            FragmentAndManifest fam = (FragmentAndManifest) o;
            fam.getManifest().update(grpm.getLifetime(currentIndex), grpm.getSignature(currentIndex));

            if (policy.checkSignature(fam.getManifest(), currentKey.getVersionKey())) {
              FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(currentKey);
              if (metadata != null) {
                if (metadata.getCurrentExpiration() <= grpm.getLifetime(currentIndex)) {
                  currentPhase = phaseStore;
                  if (metadata.getCurrentExpiration() == grpm.getLifetime(currentIndex)) {
                    logger.atDebug().log(() -> "AR Duplicate refresh request (prev="+metadata.getPreviousExpiration()+" cur="+metadata.getCurrentExpiration()+" updated="+grpm.getLifetime(currentIndex)+") -- ignoring");
                  } else {
                    FragmentMetadata newMetadata = new FragmentMetadata(grpm.getLifetime(currentIndex), metadata.getCurrentExpiration(), metadata.getStoredSince());
                    logger.debug("AR FAM {} updated ({} -> {}), writing to disk...", currentKey, newMetadata.getPreviousExpiration(), newMetadata.getCurrentExpiration());
                    fragmentStorage.store(currentKey, newMetadata, fam, this);
                    return;
                  }
                } else {
                  logger.atWarn().log(() -> "RefreshPatch attempts to roll back lifetime from "+metadata.getCurrentExpiration()+" to "+grpm.getLifetime(currentIndex));
                  currentPhase = phaseStore;
                }
              } else {
                logger.warn("Cannot fetch metadata for key {}, got 'null'", currentKey);
                currentPhase = phaseAdvance;
              }
            } else {
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

              if (currentIndex >= grpm.numKeys()) {
                respond();
                return;
              }

              currentKey = new FragmentKey(grpm.getKey(currentIndex), currentFID);
            } while (!fragmentStorage.exists(currentKey));

            currentPhase = phaseFetch;
            logger.debug("AR Patch: Fetching FAM for {}", currentKey);
            fragmentStorage.getObject(currentKey, this);
          }
        }
        public void respond() {
          int totalSuccesses = 0;
          for (int success : successes)
            totalSuccesses += success;

          logger.debug("AR Patch: Sending response ({} updates total)", totalSuccesses);

          sendMessage(
            null,
            new GlacierRefreshCompleteMessage(grpm.getUID(), grpm.getAllKeys(), successes, getLocalNodeHandle(), grpm.getSource().getId(), grpm.getTag()),
            grpm.getSource()
          );
        }
        public void receiveException(Exception e) {
          logger.warn("Exception while processing AR patch (key {}, phase {}): ", currentKey, currentPhase, e);
          currentPhase = phaseAdvance;
          receiveResult(null);
        }
      };

      c.receiveResult(null);

    } else if (msg instanceof GlacierRangeQueryMessage) {
      final GlacierRangeQueryMessage grqm = (GlacierRangeQueryMessage) msg;
      IdRange requestedRange = grqm.getRequestedRange();

      logger.info("Range query for {}", requestedRange);

      Id[][] ranges = getNeighborRanges();

      for (int i=0; i<ranges.length; i++) {
        IdRange thisRange = factory.buildIdRange(ranges[i][0], ranges[i][2]);
        IdRange intersectRange = requestedRange.intersectRange(thisRange);
        if (!intersectRange.isEmpty()) {
          logger.debug("     - Intersects: {}, sending RangeForward", intersectRange);
          sendMessage(
            ranges[i][1],
            new GlacierRangeForwardMessage(grqm.getUID(), requestedRange, grqm.getSource(), getLocalNodeHandle(), ranges[i][1], grqm.getTag()),
            null
          );
        }
      }

      logger.debug("Finished processing range query");

      return;

    } else if (msg instanceof GlacierRangeForwardMessage) {
      GlacierRangeForwardMessage grfm = (GlacierRangeForwardMessage) msg;

      if (!grfm.getDestination().equals(getLocalNodeHandle().getId())) {
        logger.warn("Not for us (dest={}, we={})", grfm.getDestination(), getLocalNodeHandle().getId());
        return;
      }

      IdRange commonRange = responsibleRange.intersectRange(grfm.getRequestedRange());
      if (!commonRange.isEmpty()) {
        logger.info("Range forward: Returning common range {} to requestor {}", commonRange, grfm.getRequestor());
        sendMessage(
          null,
          new GlacierRangeResponseMessage(grfm.getUID(), commonRange, getLocalNodeHandle(), grfm.getRequestor().getId(), grfm.getTag()),
          grfm.getRequestor()
        );
      } else {
        logger.warn("Received GRFM by {}, but no common range??!? -- ignored", grfm.getRequestor());
      }

      return;

    } else if (msg instanceof GlacierFetchMessage) {
      final GlacierFetchMessage gfm = (GlacierFetchMessage) msg;
      logger.atInfo().log(() -> "Fetch request for " + gfm.getKey(0) + ((gfm.getNumKeys()>1) ? (" and " + (gfm.getNumKeys() - 1) + " other keys") : "") + ", request="+gfm.getRequest());

      /* FetchMessages are sent during recovery to retrieve a fragment from
         another node. They are answered from the local fragment storage;
         keys that are not stored here get null entries in the response. */

      fragmentStorage.getObject(gfm.getKey(0), new Continuation<Object, Exception>() {
        int currentLookup = 0;
        Fragment fragment[] = new Fragment[gfm.getNumKeys()];
        Manifest manifest[] = new Manifest[gfm.getNumKeys()];
        int numFragments = 0, numManifests = 0;

        public void returnResponse() {
          logger.debug("Returning response with {} fragments, {} manifests ({} queries originally)", numFragments, numManifests, gfm.getNumKeys());
          sendMessage(
            null,
            new GlacierDataMessage(gfm.getUID(), gfm.getAllKeys(), fragment, manifest, getLocalNodeHandle(), gfm.getSource().getId(), true, gfm.getTag()),
            gfm.getSource()
          );
        }
        public void receiveResult(Object o) {
          if (o != null) {
            logger.atInfo().log(() -> "Fragment "+gfm.getKey(currentLookup)+" found ("+o+")");
            FragmentAndManifest fam = (FragmentAndManifest) o;
            fragment[currentLookup] = ((gfm.getRequest() & GlacierFetchMessage.FETCH_FRAGMENT)!=0) ? fam.getFragment() : null;
            if (fragment[currentLookup] != null)
              numFragments ++;
            manifest[currentLookup] = ((gfm.getRequest() & GlacierFetchMessage.FETCH_MANIFEST)!=0) ? fam.getManifest() : null;
            if (manifest[currentLookup] != null)
              numManifests ++;
          } else {
            logger.atInfo().log(() -> "Fragment "+gfm.getKey(currentLookup)+" not found");
            fragment[currentLookup] = null;
            manifest[currentLookup] = null;
          }

          nextLookup();
        }
        public void nextLookup() {
          currentLookup ++;
          if (currentLookup >= gfm.getNumKeys())
            returnResponse();
          else
            fragmentStorage.getObject(gfm.getKey(currentLookup), this);
        }
        public void receiveException(Exception e) {
          logger.atWarn().setCause(e).log(() -> "Exception while retrieving fragment "+gfm.getKey(currentLookup)+" (lookup #"+currentLookup+"), e=");
          fragment[currentLookup] = null;
          manifest[currentLookup] = null;
          nextLookup();
        }
      });
    } else if (msg instanceof GlacierDataMessage) {
      final GlacierDataMessage gdm = (GlacierDataMessage) msg;
      for (int i=0; i<gdm.numKeys(); i++) {
        final FragmentKey thisKey = gdm.getKey(i);
        final Fragment thisFragment = gdm.getFragment(i);
        final Manifest thisManifest = gdm.getManifest(i);

        if ((thisFragment != null) && (thisManifest != null)) {
          logger.info("Data: Fragment+Manifest for {}", thisKey);

          if (!responsibleRange.containsId(locator.getFragmentLocation(thisKey))) {
            logger.atWarn().log(() -> "Not responsible for "+thisKey+" (at "+locator.getFragmentLocation(thisKey)+") -- discarding");
            continue;
          }

          if (!policy.checkSignature(thisManifest, thisKey.getVersionKey())) {
            logger.warn("Manifest is not signed properly");
            continue;
          }

          if (!thisManifest.validatesFragment(thisFragment, thisKey.getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance))) {
            logger.warn("Manifest does not validate this fragment");
            continue;
          }

          if (!fragmentStorage.exists(thisKey)) {
            logger.debug("Verified ok. Storing locally.");

            FragmentAndManifest fam = new FragmentAndManifest(thisFragment, thisManifest);

            fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), 0, environment.getTimeSource().currentTimeMillis()), fam,
              new Continuation<Object, Exception>() {
                public void receiveResult(Object o) {
                  logger.info("Stored OK, sending receipt: {}", thisKey);

                  sendMessage(
                    null,
                    new GlacierResponseMessage(gdm.getUID(), thisKey, true, thisManifest.getExpiration(), responsibleRange.containsId(locator.getFragmentLocation(thisKey)), getLocalNodeHandle(), gdm.getSource().getId(), true, gdm.getTag()),
                    gdm.getSource()
                  );
                }

                public void receiveException(Exception e) {
                  logger.warn("receiveException({}) while storing a fragment -- unexpected, ignored (key={})", e, thisKey);
                }
              }
            );
          } else {
            logger.warn("We already have a fragment with this key! -- discarding");
            continue;
          }

          continue;
        }

        if ((thisFragment == null) && (thisManifest != null)) {

          if (!responsibleRange.containsId(locator.getFragmentLocation(thisKey))) {
            logger.atWarn().log(() -> "Not responsible for "+thisKey+" (at "+locator.getFragmentLocation(thisKey)+") -- discarding");
            continue;
          }

          /* We are being informed of a fragment that
               a) we should have, but currently don't, or
               b) we already have, but whose manifest will soon expire */

          if (fragmentStorage.exists(thisKey)) {
            final FragmentMetadata metadata = (FragmentMetadata) fragmentStorage.getMetadata(thisKey);
            if ((metadata == null) || (metadata.getCurrentExpiration() < thisManifest.getExpiration())) {
              logger.atInfo().log(() -> "Replacing old manifest for "+thisKey+" (expires "+((metadata == null) ? "(broken)" : ""+metadata.getCurrentExpiration())+") by new one (expires "+thisManifest.getExpiration()+")");

              fragmentStorage.getObject(thisKey, new Continuation<Object, Exception>() {
                public void receiveResult(Object o) {
                  if (o instanceof FragmentAndManifest) {
                    FragmentAndManifest fam = (FragmentAndManifest) o;

                    logger.debug("Got FAM for {}, now replacing old manifest with new one...", thisKey);

                    String fault = null;

                    if (!thisManifest.validatesFragment(fam.getFragment(), thisKey.getFragmentID(), environment.getLogManager().getLogger(Manifest.class, instance)))
                      fault = "Update: Manifest does not validate this fragment";
                    if (!policy.checkSignature(thisManifest, thisKey.getVersionKey()))
                      fault = "Update: Manifest is not signed properly";
                    if (!Arrays.equals(thisManifest.getObjectHash(), fam.getManifest().getObjectHash()))
                      fault = "Update: Object hashes not equal";
                    for (int i=0; i<numFragments; i++)
                      if (!Arrays.equals(thisManifest.getFragmentHash(i), fam.getManifest().getFragmentHash(i)))
                        fault = "Update: Fragment hash #"+i+" does not match";

                    if (fault == null) {
                      fam.replaceManifest(thisManifest);
                      fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), ((metadata == null) ? 0 : metadata.getCurrentExpiration()), environment.getTimeSource().currentTimeMillis()), fam,
                        new Continuation<Object, Exception>() {
                          public void receiveResult(Object o) {
                            logger.debug("Old manifest for {} replaced OK, sending receipt", thisKey);
                            sendMessage(
                              null,
                              new GlacierResponseMessage(gdm.getUID(), thisKey, true, thisManifest.getExpiration(), true, getLocalNodeHandle(), gdm.getSource().getId(), true, gdm.getTag()),
                              gdm.getSource()
                            );
                          }
                          public void receiveException(Exception e) {
                            logger.warn("Cannot store refreshed manifest: ", e);
                          }
                        }
                      );
                    } else {
                      logger.warn(fault);
                    }
                  } else {
                    logger.warn("Fragment store returns something other than a FAM: {}", o);
                  }
                }
                public void receiveException(Exception e) {
                  logger.warn("Cannot retrieve FAM for {}: ", thisKey, e);
                }
              });
            } else {
              logger.atWarn().log(() -> "We already have exp="+((metadata == null) ? "(broken)" : ""+metadata.getCurrentExpiration())+", discarding manifest for "+thisKey+" with exp="+thisManifest.getExpiration());
            }

            continue;
          }

          logger.info("Data: Manifest for: {}, must fetch", thisKey);

          final long tStart = environment.getTimeSource().currentTimeMillis();
          rateLimitedRetrieveFragment(thisKey, thisManifest, GlacierTags.SYNC_FETCH, new GlacierContinuation() {
            public String toString() {
              return "Fetch synced fragment: "+thisKey;
            }
            public void receiveResult(Object o) {
              if (o instanceof Fragment) {
                if (!fragmentStorage.exists(thisKey)) {
                  logger.info("Received fragment {} (from primary) matches existing manifest, storing...", thisKey);

                  FragmentAndManifest fam = new FragmentAndManifest((Fragment) o, thisManifest);

                  fragmentStorage.store(thisKey, new FragmentMetadata(thisManifest.getExpiration(), 0, environment.getTimeSource().currentTimeMillis()), fam,
                    new Continuation<Object, Exception>() {
                      public void receiveResult(Object o) {
                        logger.debug("Recovered fragment stored OK");
                      }
                      public void receiveException(Exception e) {
                        logger.warn("receiveException({}) while storing a fragment with existing manifest (key={})", e, thisKey);
                      }
                    }
                  );
                } else {
                  logger.warn("Received fragment {}, but it already exists in the fragment store", thisKey);
                }
              } else {
                logger.warn("FS received something other than a fragment: {}", o);
              }
            }
            public void receiveException(Exception e) {
              if (e instanceof GlacierNotEnoughFragmentsException) {
                GlacierNotEnoughFragmentsException gnf = (GlacierNotEnoughFragmentsException) e;
                logger.atInfo().log(() -> "Not enough fragments to reconstruct "+thisKey+": "+gnf.getChecked()+"/"+numFragments+" checked, "+gnf.getFound()+" found, "+numSurvivors+" needed");
              } else {
                logger.warn("Exception while recovering synced fragment {}: ", thisKey, e);
              }

              terminate();
            }
            public void timeoutExpired() {
              logger.warn("Timeout while fetching synced fragment {} -- aborted", thisKey);
              terminate();
            }
            public long getTimeout() {
              return tStart + config.overallRestoreTimeout();
            }
          });

          continue;
        }

        logger.warn("Case not implemented! -- GDM");
      }

      return;

    } else if (msg instanceof GlacierTimeoutMessage) {

      /* TimeoutMessages are generated by the local node when a
         timeout expires. */

      scheduler.timerExpired();
      return;
    } else {
      panic("GLACIER ERROR - Received message " + msg + " of unknown type.");
    }
  }

  public int getReplicationFactor() {
    return 1;
  }

  public NodeHandle getLocalNodeHandle() {
    return endpoint.getLocalNodeHandle();
  }

  public void setSyncInterval(int syncIntervalSec) {
    this.syncInterval = syncIntervalSec * GlacierConfig.SECONDS;
  }

  public void setSyncMaxFragments(int syncMaxFragments) {
    this.syncMaxFragments = syncMaxFragments;
  }

  public void setRateLimit(int rps) {
    this.rateLimitedRequestsPerSecond = rps;
  }

  public void setNeighborTimeout(long neighborTimeoutMin) {
    this.neighborTimeout = neighborTimeoutMin * GlacierConfig.MINUTES;
  }

  public void setBandwidthLimit(long bytesPerSecond, long maxBurst) {
    this.bucketTokensPerSecond = bytesPerSecond;
    this.bucketMaxBurstSize = maxBurst;
  }

  public long getTrashSize() {
    if (trashStorage == null)
      return 0;

    return trashStorage.getStorage().getTotalSize();
  }

  public void emptyTrash(final Continuation<? super Boolean, Exception> c) {
    if (trashStorage != null) {
      logger.info("Emptying trash (removing all objects)");

      trashStorage.flush(c);
    } else {
      c.receiveResult(null);
    }
  }

  public void addStatisticsListener(GlacierStatisticsListener gsl) {
    listeners.add(gsl);
  }

  public void removeStatisticsListener(GlacierStatisticsListener gsl) {
    listeners.removeElement(gsl);
  }

  public Environment getEnvironment() {
    return environment;
  }

  public String getInstance() {
    return instance;
  }

  public void setContentDeserializer(PastContentDeserializer deserializer) {
    contentDeserializer = deserializer;
  }

  public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer) {
    contentHandleDeserializer = deserializer;
  }


}
