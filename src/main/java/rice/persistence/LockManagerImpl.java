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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;

@SuppressWarnings("unchecked")
public class LockManagerImpl implements LockManager {

  // map: Id -> List<Continuation>
  private HashMap<Id, List<Continuation<?, ?>>> locks;

  // guards locks; private, so that code holding a reference to this object can't contend for it
  private final Object locksLock = new Object();

  private final Logger logger;

  public LockManagerImpl(Environment env) {
    this.locks = new HashMap<Id, List<Continuation<?, ?>>>();
    this.logger =  env.getLogManager().getLogger(StorageManagerImpl.class,null);
  }


  public void lock(Id id, Continuation<?, ?> c) {
    Continuation<?, ?> torun = null;
    logger.debug("locking on id {} for continuation {}", id, c);
    synchronized (locksLock) {
      if (locks.containsKey(id)) {
        List<Continuation<?, ?>> locklist = locks.get(id);
        logger.trace("locking on id {}; blocked on {} earlier continuations", id, locklist.size());
        if (locklist.size() > 10) logger.info("locking on id {}; {} continuations in queue.  That seems large", id, locklist.size());
        locklist.add(c);
      } else {
        locks.put(id, new LinkedList<Continuation<?, ?>>());
        logger.trace("locking on id {}; no contention so running {}", id, c);
        torun = c;
      }
    }
    if (torun != null)
      torun.receiveResult(null);
  }

  public void unlock(Id id) {
    Continuation<?, ?> torun = null;
    logger.debug("unlocking on id {}", id);
    synchronized (locksLock) {
      if (locks.containsKey(id)) {
        if (locks.get(id).isEmpty()) {
          logger.trace("unlocking on id {}; last out the door -- removing lock ", id);
          locks.remove(id);
        } else {
          Continuation<?, ?> next = locks.get(id).remove(0);
          logger.trace("unlocking on id {}; starting next continuation {}", id, next);
          torun = next;
        }
      } else {
        logger.warn("unlocking on id {}; no lock currently held!!", id);
      }
    }
    if (torun != null)
      torun.receiveResult(null);
  }

}
