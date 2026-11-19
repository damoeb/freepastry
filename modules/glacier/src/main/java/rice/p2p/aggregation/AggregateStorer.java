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

import java.util.Arrays;
import java.io.IOException;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.gc.ExpiringPast;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Stores a new aggregate: computes its content hash (on the processing thread), inserts it into the
 * aggregate store and, once that succeeded, records it in the aggregate list as the new root.
 */
final class AggregateStorer {
  private final Endpoint endpoint;
  private final IdFactory factory;
  private final Past aggregateStore;
  private final AggregateList aggregateList;
  private final TimeSource timeSource;
  private final Logger logger;

  AggregateStorer(Endpoint endpoint, IdFactory factory, Past aggregateStore, AggregateList aggregateList, TimeSource timeSource, Logger logger) {
    this.endpoint = endpoint;
    this.factory = factory;
    this.aggregateStore = aggregateStore;
    this.aggregateList = aggregateList;
    this.timeSource = timeSource;
    this.logger = logger;
  }

  /**
   * The lifetime of an aggregate is the longest refreshed lifetime of its components.
   */
  static long chooseAggregateLifetime(ObjectDescriptor[] components) {
    long maxLifetime = 0;

    for (int i=0; i<components.length; i++)
      if (components[i].getRefreshedLifetime() > maxLifetime)
        maxLifetime = components[i].getRefreshedLifetime();

    return maxLifetime;
  }

  /**
   * @return the serialized size of the object, or 0 if it cannot be serialized
   */
  static int getSize(PastContent obj, Logger logger) {
    try {
      SimpleOutputBuffer buf = new SimpleOutputBuffer();
      buf.writeShort(obj.getType());
      obj.serialize(buf);
      return buf.getWritten();
    } catch (IOException ioe) {
      logger.atWarn().log(() -> "Cannot serialize object, size unknown: "+ioe);
    }

    return 0;
  }

  void storeAggregate(final Aggregate aggr, final long expiration, final ObjectDescriptor[] desc, final Id[] pointers, final Continuation<? super Boolean, Exception> command) {
    logger.debug("storeAggregate() schedules content hash computation...");
    endpoint.process(() -> {
      logger.debug("storeAggregate() starts working on content hash...");
      return factory.buildId(aggr.getContentHash());
    }, new Continuation<Id, Exception>() {
      @Override
      public void receiveResult(Id o) {
        if (o != null) {
          aggr.setId((Id) o);
          insertAggregate(aggr, expiration, desc, pointers, command);
        } else {
          logger.warn("storeAggregate() cannot determine content hash, received {}", o);
          command.receiveException(new AggregationException("storeAggregate() cannot determine content hash"));
        }
      }
      @Override
      public void receiveException(Exception e) {
        logger.warn("storeAggregate() cannot determine content hash, exception ", e);
        command.receiveException(e);
      }
    });
  }

  private void insertAggregate(final Aggregate aggr, final long expiration, final ObjectDescriptor[] desc, final Id[] pointers, final Continuation<? super Boolean, Exception> command) {
    logger.atInfo().log(() -> "Storing aggregate, CH="+aggr.getId()+", expiration="+expiration+" (rel "+(expiration-timeSource.currentTimeMillis())+") with "+desc.length+" objects:");
    for (int j=0; j<desc.length; j++)
      logger.info("#{}: {}", j, desc[j]);

    Continuation<Boolean[], Exception> c = new Continuation<Boolean[], Exception>() {
      @Override
      public void receiveResult(Boolean[] o) {
        commitAggregate(aggr, expiration, desc, pointers, o, command);
      }
      @Override
      public void receiveException(Exception e) {
        command.receiveException(e);
      }
    };

    if (aggregateStore instanceof ExpiringPast gcPast)
      gcPast.insert(aggr, expiration, c);
    else
      aggregateStore.insert(aggr, c);
  }

  private void commitAggregate(Aggregate aggr, long expiration, ObjectDescriptor[] desc, Id[] pointers, Boolean[] o, Continuation<? super Boolean, Exception> command) {
    AggregateDescriptor adc = new AggregateDescriptor(
      aggr.getId(),
      expiration,
      desc,
      pointers
    );

    if (o != null) {
      aggregateList.addAggregateDescriptor(adc);
      aggregateList.setRoot(aggr.getId());
      aggregateList.writeToDisk();
      logger.debug("Aggregate inserted successfully");
      command.receiveResult(Boolean.valueOf(true));
    } else {
      logger.warn("Unexpected result in aggregate insert (commit): {}", Arrays.toString(o));
      command.receiveException(new AggregationException("Unexpected result (commit): "+Arrays.toString(o)));
    }
  }
}
