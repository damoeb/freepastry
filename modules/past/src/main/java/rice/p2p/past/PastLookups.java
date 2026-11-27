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
import rice.Futures;
import rice.p2p.commonapi.Id;
import rice.p2p.past.messaging.LookupMessage;

/**
 * The lookup of an object by its id on behalf of a {@link PastImpl}: first the local storage, then the node the id
 * routes to, then the replicas. It calls back into the Past for storage, requests and caching, so a subclass that
 * overrides those still takes part.
 */
final class PastLookups {
  private final PastImpl past;
  private final Logger logger;

  PastLookups(PastImpl past, Logger logger) {
    this.past = past;
    this.logger = logger;
  }

  CompletableFuture<PastContent> lookup(final Id id, final boolean cache) {
    logger.atTrace().log(() -> " Performing lookup on " + id.toStringFull());

    return past.storage.getObject(id).thenCompose(result -> {
      if (result != null && past.isAuthentic(id, (PastContent) result)) {
        return CompletableFuture.completedFuture((PastContent) result);
      }
      return lookupViaMessage(id, cache);
    });
  }

  /**
   * Second step of a lookup: asks the node the key routes to. If that node has no authentic object, or the message
   * failed, all replicas are tried, just in case; this may fail too, but at least we tried.
   */
  private CompletableFuture<PastContent> lookupViaMessage(final Id id, final boolean cache) {
    return past.<PastContent>sendRequest(id, new LookupMessage(past.nextRequestId(), id, past.getLocalNodeHandle(), id))
        .handle((content, failure) -> failure == null ? content : null)
        .thenCompose(content -> {
          if (content != null && past.isAuthentic(id, content)) {
            return cacheAndReply(content, cache);
          }
          return lookupViaReplicas(id, cache);
        });
  }

  /**
   * Last step of a lookup: the first replica that has a handle is fetched, and the lookup answers null if there is
   * none, which means the object doesn't exist anywhere.
   */
  private CompletableFuture<PastContent> lookupViaReplicas(final Id id, final boolean cache) {
    return past.lookupHandles(id, past.getReplicationFactor() + 1).thenCompose(handles -> {
      for (PastContentHandle handle : handles) {
        if (handle != null) {
          return past.fetch(handle).thenCompose(content -> cacheAndReply(content, cache));
        }
      }

      // there were no replicas of the object
      return CompletableFuture.completedFuture(null);
    });
  }

  /** Tries to cache a looked-up object locally for future use, and answers with it whether or not caching worked. */
  private CompletableFuture<PastContent> cacheAndReply(final PastContent content, final boolean cache) {
    if (!cache) {
      return CompletableFuture.completedFuture(content);
    }

    return past.cache(content).handle((_, failure) -> {
      if (failure != null) {
        logger.warn("Caching after lookup failed, continuing", Futures.unwrap(failure));
      }
      return content;
    });
  }
}
