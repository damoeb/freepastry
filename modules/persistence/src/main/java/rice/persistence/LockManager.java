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
package rice.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;

/**
 * Serializes work on an id: {@link #lock(Id)} grants the lock at once if it is free, and otherwise when the
 * holders queued before release it with {@link #unlock(Id)}, in FIFO order.
 */
public class LockManager {

  // the queued grants per locked id; an id is locked while it is a key
  private final HashMap<Id, List<CompletableFuture<Void>>> locks;

  // guards locks; private, so that code holding a reference to this object can't contend for it
  private final Object locksLock = new Object();

  private final Environment environment;

  private final Logger logger;

  public LockManager(Environment env) {
    this.locks = new HashMap<Id, List<CompletableFuture<Void>>>();
    this.environment = env;
    this.logger = env.getLogManager().getLogger(CachedStorageImpl.class, null);
  }

  /**
   * Waits for the lock on {@code id}; release it with {@link #unlock(Id)}.
   *
   * @param id the id to lock
   * @return completes when the lock is granted: before this method returns if the lock is free, otherwise when
   *     the holders queued before release it (on the thread that unlocks). A queued grant is tracked by the
   *     Environment, so destroying it fails the grant; a cancelled grant is skipped.
   */
  public CompletableFuture<Void> lock(Id id) {
    CompletableFuture<Void> grant = new CompletableFuture<Void>();
    boolean isFree;
    logger.debug("locking on id {}", id);
    synchronized (locksLock) {
      List<CompletableFuture<Void>> queue = locks.get(id);
      isFree = (queue == null);
      if (isFree) {
        locks.put(id, new ArrayList<CompletableFuture<Void>>());
        logger.trace("locking on id {}; no contention", id);
      } else {
        logger.trace("locking on id {}; blocked on {} earlier requests", id, queue.size());
        if (queue.size() > 10) logger.info("locking on id {}; {} requests in queue.  That seems large", id, queue.size());
        queue.add(grant);
      }
    }
    if (isFree) {
      grant.complete(null);
      return grant;
    }
    return environment.track(grant);
  }

  /**
   * Releases the lock on {@code id}, and grants it to the next one queued, if any.
   *
   * @param id the locked id
   */
  public void unlock(Id id) {
    logger.debug("unlocking on id {}", id);
    while (true) {
      CompletableFuture<Void> next;
      synchronized (locksLock) {
        List<CompletableFuture<Void>> queue = locks.get(id);
        if (queue == null) {
          logger.warn("unlocking on id {}; no lock currently held!!", id);
          return;
        }
        if (queue.isEmpty()) {
          logger.trace("unlocking on id {}; last out the door -- removing lock ", id);
          locks.remove(id);
          return;
        }
        next = queue.remove(0);
      }
      logger.trace("unlocking on id {}; granting the next request", id);
      // a grant that was cancelled (or failed by a destroyed environment) is skipped
      if (next.complete(null)) return;
    }
  }
}
