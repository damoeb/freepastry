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


package rice.p2p.past;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;

/**
 * The replica a {@link PastImpl} fetches when the replication asks it to: the policy fetches the object, and the
 * object is stored locally only if it is the one that was asked for and it accepts the insert, as an insert does.
 */
final class ReplicaFetcher {
  private final PastImpl past;
  private final Logger logger;

  ReplicaFetcher(PastImpl past, Logger logger) {
    this.past = past;
    this.logger = logger;
  }

  CompletableFuture<Boolean> fetch(final Id id, NodeHandle hint) {
    logger.trace("Sending out replication fetch request for the id {}", id);

    return past.policy.fetch(id, hint, past.backup, past).thenCompose(result -> {
      if (result == null) {
        logger.warn("Could not fetch id {} - policy returned null in namespace {}", id, past.getInstance());
        return CompletableFuture.completedFuture(Boolean.FALSE);
      }
      logger.trace("inserting replica of id {}", id);

      // the peer that answered is not trusted: store only what was asked for, never another id's object
      if (!(result instanceof PastContent content)) {
        logger.warn("Dropping fetched replica of id {}: not PastContent but {}", id, result.getClass().getName());
        return CompletableFuture.completedFuture(Boolean.FALSE);
      }
      if (!id.equals(content.getId())) {
        logger.warn("Dropping fetched replica of id {}: it carries the id {}", id, content.getId());
        return CompletableFuture.completedFuture(Boolean.FALSE);
      }
      return storeReplica(id, content);
    });
  }

  /**
   * Stores a replica fetched for the id, letting the content check it against what is stored locally, as an
   * insert does. A refusal drops the replica.
   */
  private CompletableFuture<Boolean> storeReplica(final Id id, final PastContent content) {
    return past.storage.getObject(id).thenCompose(existing -> {
      PastContent checked;
      try {
        checked = content.checkInsert(id, (PastContent) existing);
      } catch (PastException e) {
        logger.warn("Dropping fetched replica of id {}: {}", id, e.getMessage());
        return CompletableFuture.completedFuture(Boolean.FALSE);
      }
      return past.storage.getStorage().store(id, null, checked);
    });
  }
}
