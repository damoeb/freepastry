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
package rice.environment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

/**
 * The pending data-path futures of one {@link Environment}, so that destroying the environment fails them
 * instead of leaving their callers waiting forever. A future leaves the set when it completes, so the set holds
 * only what is still pending.
 */
final class PendingFutures {

  // by identity: a future has no equals(); guarded by this
  private final Set<CompletableFuture<?>> pending = Collections.newSetFromMap(new IdentityHashMap<CompletableFuture<?>, Boolean>());

  // set once by failAll(); guarded by this
  private boolean closed;

  /**
   * Adds a pending future; fails it at once if the environment is already destroyed.
   *
   * @param future the future
   * @return {@code future}
   */
  <T extends @Nullable Object> CompletableFuture<T> track(CompletableFuture<T> future) {
    if (future.isDone()) return future;
    boolean isClosed;
    synchronized (this) {
      isClosed = closed;
      if (!isClosed) pending.add(future);
    }
    if (isClosed) {
      future.completeExceptionally(new EnvironmentDestroyedException());
      return future;
    }
    // the stage that forgets it can't fail, so nobody needs to observe it
    var _ = future.whenComplete((_, _) -> forget(future));
    return future;
  }

  @SuppressWarnings("CollectionUndefinedEquality") // an identity set
  private synchronized void forget(CompletableFuture<?> future) {
    pending.remove(future);
  }

  /**
   * Fails every pending future with an {@link EnvironmentDestroyedException}, and from now on every future
   * that is tracked. Their dependent stages run on the calling thread.
   */
  void failAll() {
    List<CompletableFuture<?>> toFail;
    synchronized (this) {
      closed = true;
      toFail = new ArrayList<CompletableFuture<?>>(pending);
      pending.clear();
    }
    for (CompletableFuture<?> future : toFail) {
      future.completeExceptionally(new EnvironmentDestroyedException());
    }
  }

  /**
   * @return how many tracked futures are still pending
   */
  synchronized int size() {
    return pending.size();
  }
}
