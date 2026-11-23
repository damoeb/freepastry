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

import org.slf4j.Logger;
import rice.Continuation;
import rice.Continuation.NamedContinuation;
import rice.Continuation.SimpleContinuation;
import rice.Continuation.StandardContinuation;
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

  void lookup(final Id id, final boolean cache, final Continuation<? super PastContent, Exception> command) {
    logger.atTrace().log(() -> " Performing lookup on " + id.toStringFull());

    Futures.forward(past.storage.getObject(id), new StandardContinuation<Object, PastContent, Exception>(command) {
      @Override
      public void receiveResult(Object result) {
        if (result != null && past.isAuthentic(id, (PastContent) result)) {
          command.receiveResult((PastContent) result);
        } else {
          lookupViaMessage(id, cache, command);
        }
      }
    });
  }

  /**
   * Second step of a lookup: asks the node the key routes to. If that node has no authentic object, or the message
   * failed, all replicas are tried, just in case; this may fail too, but at least we tried.
   */
  private void lookupViaMessage(final Id id, final boolean cache, final Continuation<? super PastContent, Exception> command) {
    Futures.forward(past.<PastContent>sendRequest(id, new LookupMessage(past.nextRequestId(), id, past.getLocalNodeHandle(), id)), new NamedContinuation<PastContent, Exception>("LookupMessage for " + id, command) {
      @Override
      public void receiveResult(final PastContent content) {
        if (content != null && past.isAuthentic(id, content)) {
          cacheAndReply(content, cache, command);
        } else {
          lookupViaReplicas(id, cache, command);
        }
      }

      @Override
      public void receiveException(Exception e) {
        receiveResult(null);
      }
    });
  }

  /**
   * Last step of a lookup: the first replica that has a handle is fetched, and the lookup answers null if there is
   * none, which means the object doesn't exist anywhere.
   */
  private void lookupViaReplicas(final Id id, final boolean cache, final Continuation<? super PastContent, Exception> command) {
    past.lookupHandles(id, past.getReplicationFactor() + 1, new StandardContinuation<PastContentHandle[], PastContent, Exception>(command) {
      @Override
      public void receiveResult(PastContentHandle[] handles) {
        for (PastContentHandle handle : handles) {
          if (handle != null) {
            past.fetch(handle, new StandardContinuation<PastContent, PastContent, Exception>(command) {
              @Override
              public void receiveResult(final PastContent content) {
                cacheAndReply(content, cache, command);
              }
            });

            return;
          }
        }

        // there were no replicas of the object
        command.receiveResult(null);
      }
    });
  }

  /** Tries to cache a looked-up object locally for future use, and answers with it whether or not caching worked. */
  private void cacheAndReply(final PastContent content, final boolean cache, final Continuation<? super PastContent, Exception> command) {
    if (!cache) {
      command.receiveResult(content);
      return;
    }

    past.cache(content, new SimpleContinuation() {
      @Override
      public void receiveResult(Object object) {
        command.receiveResult(content);
      }

      @Override
      public void receiveException(Exception e) {
        logger.warn("Caching after lookup failed, continuing", e);
        command.receiveResult(content);
      }
    });
  }
}
