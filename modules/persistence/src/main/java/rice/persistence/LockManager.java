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
import rice.Continuation;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;

@SuppressWarnings("unchecked")
public class LockManager {

  // map: Id -> List<Continuation>
  private HashMap<Id, List<Continuation<?, ?>>> locks;

  // guards locks; private, so that code holding a reference to this object can't contend for it
  private final Object locksLock = new Object();

  private final Logger logger;

  public LockManager(Environment env) {
    this.locks = new HashMap<Id, List<Continuation<?, ?>>>();
    this.logger =  env.getLogManager().getLogger(CachedStorageImpl.class,null);
  }


  public void lock(Id id, Continuation<?, ?> continuation) {
    Continuation<?, ?> pendingContinuation = null;
    logger.debug("locking on id {} for continuation {}", id, continuation);
    synchronized (locksLock) {
      if (locks.containsKey(id)) {
        List<Continuation<?, ?>> locklist = locks.get(id);
        logger.trace("locking on id {}; blocked on {} earlier continuations", id, locklist.size());
        if (locklist.size() > 10) logger.info("locking on id {}; {} continuations in queue.  That seems large", id, locklist.size());
        locklist.add(continuation);
      } else {
        locks.put(id, new ArrayList<Continuation<?, ?>>());
        logger.trace("locking on id {}; no contention so running {}", id, continuation);
        pendingContinuation = continuation;
      }
    }
    if (pendingContinuation != null)
      pendingContinuation.receiveResult(null);
  }

  /**
   * Waits for the lock on {@code id}; release it with {@link #unlock(Id)}.
   *
   * @param id the id to lock
   * @return completes when the lock is granted: before this method returns if the lock is free, otherwise when
   *     the holders queued before release it
   */
  public CompletableFuture<Void> lock(Id id) {
    return Futures.<Void>call(continuation -> lock(id, continuation));
  }

  public void unlock(Id id) {
    Continuation<?, ?> pendingContinuation = null;
    logger.debug("unlocking on id {}", id);
    synchronized (locksLock) {
      if (locks.containsKey(id)) {
        if (locks.get(id).isEmpty()) {
          logger.trace("unlocking on id {}; last out the door -- removing lock ", id);
          locks.remove(id);
        } else {
          Continuation<?, ?> next = locks.get(id).remove(0);
          logger.trace("unlocking on id {}; starting next continuation {}", id, next);
          pendingContinuation = next;
        }
      } else {
        logger.warn("unlocking on id {}; no lock currently held!!", id);
      }
    }
    if (pendingContinuation != null)
      pendingContinuation.receiveResult(null);
  }

}
