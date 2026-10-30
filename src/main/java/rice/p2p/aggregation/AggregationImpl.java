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
package rice.p2p.aggregation;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.aggregation.messaging.AggregationMessage;
import rice.p2p.aggregation.messaging.AggregationTimeoutMessage;
import rice.p2p.aggregation.messaging.NonAggregate;
import rice.p2p.aggregation.raw.RawAggregate;
import rice.p2p.aggregation.raw.RawAggregateFactory;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.VersioningPast;
import rice.p2p.glacier.v2.GlacierContentHandle;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.GCPast;
import rice.p2p.past.gc.GCPastContent;
import rice.p2p.past.rawserialization.JavaPastContentDeserializer;
import rice.p2p.past.rawserialization.JavaPastContentHandleDeserializer;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.rawserialization.PastContentHandleDeserializer;
import rice.p2p.util.DebugCommandHandler;
import rice.persistence.StorageManager;

@SuppressWarnings("unchecked")
public class AggregationImpl implements GCPast, VersioningPast, Aggregation, Application, DebugCommandHandler {
  private final Past aggregateStore;
  private final StorageManager waitingList;
  private final AggregationPolicy policy;
  private final AggregateList aggregateList;
  private final Endpoint endpoint;
  private final Past objectStore;
  private final String instance;
  private final IdFactory factory;
  private final Node node;

  private final char tiFlush = 1;
  private final char tiMonitor = 2;
  private final char tiConsolidate = 3;
  private final char tiStatistics = 4;
  private final char tiExpire = 5;
  private final Hashtable<Integer, CancellableTask> timers;
  private AggregationStatistics stats;

  private final AggregationConfig config;
  private final AggregateStorer storer;
  private final AggregateFlusher flusher;
  private final AggregateTree tree;
  private final AggregateExpiration aggregateExpiration;
  private final ObjectRefresher refresher;
  private final ObjectRetriever retriever;
  private final AggregationMonitor monitor;
  private final AggregationDebugCommands debugCommands;
  private final AggregateConsolidator consolidator;

  private Environment environment;
  private final Logger logger;

  private PastContentDeserializer contentDeserializer;
  private PastContentHandleDeserializer contentHandleDeserializer;

  private AggregateFactory aggregateFactory;

  public AggregationImpl(Node node, Past aggregateStore, Past objectStore, StorageManager waitingList, String configFileName, IdFactory factory, String instance) throws IOException {
    this(node, aggregateStore, objectStore, waitingList, configFileName, factory, instance, null, null);
  }

  public AggregationImpl(Node node, Past aggregateStore, Past objectStore, StorageManager waitingList, String configFileName, IdFactory factory, String instance, AggregationPolicy policy, AggregateFactory aggregateFactory) throws IOException {
    this.environment = node.getEnvironment();
    logger = environment.getLogManager().getLogger(AggregationImpl.class, instance);

    config = new AggregationConfig(environment.getParameters());

    this.aggregateFactory = aggregateFactory;
    if (this.aggregateFactory == null) {
      this.aggregateFactory = getDefaultAggregateFactory();
    }

    this.endpoint = node.buildEndpoint(this, instance);
    this.endpoint.setDeserializer((buf, type, priority, sender) -> {
      // the only message type, AggregationTimeoutMessage, is scheduled locally and never deserialized
      throw new IOException("Unknown type: " + type + " in " + AggregationImpl.this);
    });

    this.waitingList = waitingList;
    this.instance = instance;

    this.contentDeserializer = new JavaPastContentDeserializer();
    this.contentHandleDeserializer = new JavaPastContentHandleDeserializer();

    this.aggregateStore = aggregateStore;
    this.aggregateStore.setContentDeserializer((buf, endpoint, contentType) -> {
      switch(contentType) {
        case RawAggregate.TYPE:
          return new RawAggregate(buf, endpoint, contentDeserializer);
        case NonAggregate.TYPE:
          short subType = buf.readShort();
          return contentDeserializer.deserializePastContent(buf, endpoint, subType);
      }
      throw new IllegalArgumentException("Unknown Type:"+contentType);
    });

    this.aggregateStore.setContentHandleDeserializer((buf, endpoint, contentType) -> {
      switch(contentType) {
        case AggregateHandle.TYPE:
          return new AggregateHandle(buf, endpoint);
      }
      throw new IllegalArgumentException("Unknown Type:"+contentType);
    });

    this.objectStore = objectStore;
    this.node = node;
    this.timers = new Hashtable<Integer, CancellableTask>();
    this.aggregateList = new AggregateList(configFileName, getLocalNodeHandle().getId().toString(), factory, config.isAggregateLogEnabled(), instance, environment);
    this.storer = new AggregateStorer(endpoint, factory, aggregateStore, aggregateList, environment.getTimeSource(), logger);
    this.stats = aggregateList.getStatistics(config.getStatsGranularity(), config.getStatsRange(), config.getNominalReferenceCount());
    if (policy == null) {
      this.policy = getDefaultPolicy();
    } else {
      this.policy = policy;
    }

    this.factory = factory;
    this.consolidator = new AggregateConsolidator(aggregateStore, aggregateList, this.aggregateFactory, storer, config, environment.getTimeSource(), environment.getRandomSource(), logger);
    this.retriever = new ObjectRetriever(objectStore, aggregateStore, aggregateList, endpoint, factory, environment.getTimeSource(), logger);
    this.refresher = new ObjectRefresher(objectStore, waitingList, aggregateList, this.policy, config, logger);
    this.aggregateExpiration = new AggregateExpiration(aggregateStore, aggregateList, config, environment.getTimeSource(), logger);
    this.tree = new AggregateTree(aggregateStore, objectStore, aggregateList, storer, this.aggregateFactory, config, environment.getTimeSource(), logger);
    this.flusher = new AggregateFlusher(waitingList, aggregateList, this.aggregateFactory, storer, config, logger);
    this.monitor = new AggregationMonitor(this, objectStore, aggregateStore, aggregateList, factory, environment, config, logger);
    this.debugCommands = new AggregationDebugCommands(this, aggregateStore, objectStore, aggregateList, waitingList, monitor, factory, environment, config, instance, logger);

    if (!aggregateList.readOK()) {
      logger.warn("Failed to read configuration file; aggregate list must be rebuilt!");
    } else {
      logger.atInfo().log(() -> "Aggregate list read OK -- current root: " + ((aggregateList.getRoot() == null) ? "null" : aggregateList.getRoot().toStringFull()));
    }

    aggregateExpiration.removeDeadAggregates();

    addTimer(jitterTerm(config.getFlushDelayAfterJoin()), tiFlush);
    addTimer(jitterTerm(config.getAggrRefreshDelayAfterJoin()), tiExpire);
    addTimer(jitterTerm(config.getConsolidationDelayAfterJoin()), tiConsolidate);
    addTimer(config.getStatsInterval(), tiStatistics);
    if (config.isMonitorEnabled())
      addTimer(config.getMonitorRefreshInterval(), tiMonitor);
    endpoint.register();
  }

  private static AggregationPolicy getDefaultPolicy() {
    return new AggregationDefaultPolicy();
  }

  private static AggregateFactory getDefaultAggregateFactory() {
    return new RawAggregateFactory();
  }

  private long jitterTerm(long basis) {
    return (long)((1-config.getJitterRange())*basis) + environment.getRandomSource().nextInt((int)(2*config.getJitterRange()*basis));
  }

  /**
   * Schedule a timer event
   *
   * @param timeoutMsec Length of the delay (in milliseconds)
   * @param timeoutID Identifier (to distinguish between multiple timers)
   */
  private void addTimer(long timeoutMsec, char timeoutID) {
    /*
     *  We schedule an AggregationTimeoutMessage with the ID of the
     *  requested timer. This message will be delivered if the timer
     *  expires and it has not been removed in the meantime.
     */
    CancellableTask timer = endpoint.scheduleMessage(new AggregationTimeoutMessage(timeoutID, getLocalNodeHandle()), timeoutMsec);
    timers.put(Integer.valueOf(timeoutID), timer);
  }

  /**
   * Cancel a timer event that has not yet occurred
   *
   * @param timeoutID Identifier of the timer event to be cancelled
   */
  private void removeTimer(int timeoutID) {
    CancellableTask timer = (CancellableTask) timers.remove(Integer.valueOf(timeoutID));

    if (timer != null) {
      timer.cancel();
    }
  }

  private void panic(String s) throws Error {
    Error err = new Error("Panic "+s);
    logger.error("PANIC: {}", s, err);
    throw err;
  }

  public String handleDebugCommand(String command) {
    return debugCommands.handleDebugCommand(command);
  }

  private void timerExpired(char timerID) {
    logger.atDebug().log(() -> "TIMER EXPIRED: #" + (int) timerID);

    switch (timerID) {
      case tiFlush :
      {
        logger.info("Scheduled flush, waiting list: {}", waitingList.getSize());

        flusher.formAggregates(new Continuation<Boolean, Exception>() {
          public void receiveResult(Boolean o) {
            logger.debug("Scheduled flush: Success (o={})", o);
          }
          public void receiveException(Exception e) {
            logger.warn("Scheduled flush: Failure (e={})", e, e);
          }
        });

        logger.atInfo().log(() -> "Waiting list: "+waitingList.getSize()+" Scan: "+getNumObjectsWaiting()+" Max: "+(config.getMaxObjectsInAggregate() * config.getMaxAggregatesPerRun()));

        if (getNumObjectsWaiting() >= (config.getMaxObjectsInAggregate() * config.getMaxAggregatesPerRun())) {
          logger.info("Retrying later");
          addTimer(jitterTerm(config.getFlushStressInterval()), tiFlush);
        } else {
          logger.info("OK, waiting for next deadline");
          addTimer(jitterTerm(config.getFlushInterval()), tiFlush);
        }

        break;
      }
      case tiExpire :
      {
        aggregateExpiration.refreshAggregates();
        tree.reconnectTree();
        addTimer(jitterTerm(config.getAggrRefreshInterval()), tiExpire);
        break;
      }
      case tiConsolidate :
      {
        consolidator.consolidateAggregates();
        addTimer(jitterTerm(config.getConsolidationInterval()), tiConsolidate);
        break;
      }
      case tiMonitor :
      {
        monitor.refreshMonitored();

        addTimer(config.getMonitorRefreshInterval(), tiMonitor);
        break;
      }
      case tiStatistics :
      {
        stats = aggregateList.getStatistics(config.getStatsGranularity(), config.getStatsRange(), config.getNominalReferenceCount());
        stats.dump(environment.getLogManager().getLogger(AggregationStatistics.class, instance));
        addTimer(config.getStatsInterval(), tiStatistics);
        break;
      }
      default:
      {
        panic("Unknown timer expired: " + (int) timerID);
      }
    }
  }

  public void refresh(Id[] ids, long expiration, Continuation<? super Object[], Exception> command) {
    long[] expirations = new long[ids.length];
    Arrays.fill(expirations, expiration);
    refresh(ids, expirations, command);
  }

  public void refresh(final Id[] ids, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    refresher.refresh(ids, expirations, command);
  }

  public void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation<? super Object[], Exception> command) {
    refresher.refresh(ids, versions, expirations, command);
  }

  public Serializable getHandle() {
    return aggregateList.getRoot();
  }

  public void setHandle(Serializable handle, Continuation<? super Boolean, Exception> command) {
    tree.setHandle(handle, command);
  }

  public void insert(final PastContent obj, final Continuation<? super Boolean[], Exception> command) {
    insert(obj, INFINITY_EXPIRATION, command);
  }

  public void insert(final PastContent obj, final long lifetime, final Continuation<? super Boolean[], Exception> command) {

    long theVersion;
    if (obj instanceof GCPastContent) {
      theVersion = ((GCPastContent)obj).getVersion();
    } else {
      theVersion = 0;
    }

    final VersionKey vkey = new VersionKey(obj.getId(), theVersion);
    final long theVersionF = theVersion;
    final int theSize = AggregateStorer.getSize(obj, logger);

    if (policy.shouldBeAggregated(obj, theSize)) {
      logger.info("AGGREGATE INSERT: {} version={} size={} class={}", obj.getId(), theVersion, theSize, obj.getClass().getName());

      if (objectStore instanceof GCPast)
        ((GCPast)objectStore).insert(obj, lifetime, command);
      else
        objectStore.insert(obj, command);

      waitingList.store(vkey, new ObjectDescriptor(obj.getId(), theVersionF, lifetime, lifetime, theSize), obj, new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {
        }
        public void receiveException(Exception e) {
          logger.warn("Exception while storing aggregate: {} (e={})", obj.getId(), e, e);
        }
      });
    } else {
      logger.info("INSERT WITHOUT AGGREGATION: {} version={} size={} class={}", obj.getId(), theVersionF, theSize, obj.getClass().getName());

      Continuation<Boolean[], Exception> c = new Continuation<Boolean[], Exception>() {
        boolean otherSucceeded = false;
        boolean otherFailed = false;

        public void receiveResult(Boolean[] o) {
          logger.debug("INSERT {} receiveResult({}), otherSucc={} otherFail={}", obj.getId(), o, otherSucceeded, otherFailed);
          if (otherSucceeded) {
            if (!otherFailed) {
              logger.debug("--reporting Success");
              command.receiveResult(new Boolean[] { Boolean.valueOf(true) });
            }
          } else {
            otherSucceeded = true;
          }
        }
        public void receiveException(Exception e) {
          logger.debug("INSERT {} receiveException({}), otherSucc={} otherFail={}", obj.getId(), e, otherSucceeded, otherFailed);
          logger.debug("--reporting Failure");
          command.receiveException(e);
          otherFailed = true;
        }
      };

      if (objectStore instanceof GCPast)
        ((GCPast)objectStore).insert(obj, lifetime, c);
      else
        objectStore.insert(obj, c);

      if (aggregateStore instanceof GCPast)
        ((GCPast)aggregateStore).insert(new NonAggregate(obj), lifetime, c);
      else
        aggregateStore.insert(new NonAggregate(obj), c);
    }
  }

  public void lookup(final Id id, boolean cache, final Continuation<? super PastContent, Exception> command) {
    retriever.lookup(id, cache, command);
  }

  public void lookup(final Id id, final long version, final Continuation<? super PastContent, Exception> command) {
    retriever.lookup(id, version, command);
  }

  public void lookup(Id id, Continuation<? super PastContent, Exception> command) {
    lookup(id, true, command);
  }

  public void lookupHandles(final Id id, final long version, final int max, final Continuation<? super PastContentHandle[], Exception> command) {
    ((VersioningPast)aggregateStore).lookupHandles(id, version, max, command);
  }

  public void lookupHandle(final Id id, final NodeHandle handle, final Continuation<? super PastContentHandle, Exception> command) {
    command.receiveException(new UnsupportedOperationException("LookupHandle() is not supported on Aggregation"));
  }

  public void lookupHandles(final Id id, final int max, final Continuation<? super PastContentHandle[], Exception> command) {
    retriever.lookupHandles(id, max, command);
  }

  public void fetch(PastContentHandle handle, Continuation<? super PastContent, Exception> command) {

    /* Note that we never give out any handles from the aggregate store, so this must
       be an object store handle */
    if (handle instanceof GlacierContentHandle)
      aggregateStore.fetch(handle, command);
    else
      objectStore.fetch(handle, command);
  }

  public void flush(Id id, Continuation<? super Boolean, Exception> command) {
    flusher.flush(id, command);
  }

  public void flush(final Continuation<? super Boolean, Exception> command) {
    flusher.formAggregates(command);
  }

  public void rollback(Id id, Continuation<? super PastContent, Exception> command) {
    retriever.rollback(id, command);
  }

  public void reset(Continuation<? super Boolean, Exception> command) {
    aggregateList.clear();

    Iterator<Id> iter = waitingList.scan().getIterator();
    while (iter.hasNext()) {
      VersionKey thisKey = (VersionKey) iter.next();
      waitingList.unstore(thisKey, new Continuation<Boolean, Exception>() {
        public void receiveResult(Boolean o) {}
        public void receiveException(Exception e) {}
      });
    }

    command.receiveResult(Boolean.valueOf(true));
  }

  public NodeHandle getLocalNodeHandle() {
    return objectStore.getLocalNodeHandle();
  }

  public int getReplicationFactor() {
    return objectStore.getReplicationFactor();
  }

  public boolean forward(RouteMessage message) {
    return true;
  }

  public void update(NodeHandle handle, boolean joined) {
  }

  public void deliver(Id id, Message message) {

    final AggregationMessage msg = (AggregationMessage) message;
    logger.debug("Received message {} with destination {} from {}", msg, id, msg.getSource().getId());

    if (msg instanceof AggregationTimeoutMessage) {

      /* TimeoutMessages are generated by the local node when a
         timeout expires. */

      AggregationTimeoutMessage gtm = (AggregationTimeoutMessage) msg;
      timerExpired((char) gtm.getUID());
      return;
    } else {
      panic("AGGREGATION ERROR - Received message " + msg + " of unknown type.");
    }
  }

  public void setFlushInterval(int flushIntervalSec) {
    config.setFlushInterval(flushIntervalSec * AggregationConfig.SECONDS);
  }

  public void setMaxAggregateSize(int maxAggregateSize) {
    config.setMaxAggregateSize(maxAggregateSize);
  }

  public void setMaxObjectsInAggregate(int maxObjectsInAggregate) {
    config.setMaxObjectsInAggregate(maxObjectsInAggregate);
  }

  public void setRenewThreshold(int expirationRenewThresholdHrs) {
    config.setExpirationRenewThreshold(expirationRenewThresholdHrs * AggregationConfig.HOURS);
  }

  public void setConsolidationInterval(long consolidationIntervalSec) {
    config.setConsolidationInterval(consolidationIntervalSec * AggregationConfig.SECONDS);
  }

  public void setConsolidationThreshold(long consolidationThresholdSec) {
    config.setConsolidationThreshold(consolidationThresholdSec * AggregationConfig.SECONDS);
  }

  public void setConsolidationMinObjectsPerAggregate(int minObjectsInAggregateArg) {
    config.setConsolidationMinObjectsInAggregate(minObjectsInAggregateArg);
  }

  public void setConsolidationMinUtilization(double minUtilization) {
    config.setConsolidationMinComponentsAlive(minUtilization);
  }

  public Past getAggregateStore() {
    return aggregateStore;
  }

  public Past getObjectStore() {
    return objectStore;
  }

  public int getNumObjectsWaiting() {
    return waitingList.scan().numElements();
  }

  public AggregationStatistics getStatistics() {
    return stats;
  }

  public String getInstance() {
    return instance;
  }

  public Environment getEnvironment() {
    return environment;
  }

  public void setContentDeserializer(PastContentDeserializer deserializer) {
    contentDeserializer = deserializer;
    objectStore.setContentDeserializer(contentDeserializer);
  }

  public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer) {
    contentHandleDeserializer = deserializer;
    objectStore.setContentHandleDeserializer(deserializer);
  }
}
