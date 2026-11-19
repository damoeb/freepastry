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


import org.slf4j.Logger;
import rice.Continuation;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.GlacierException;
import rice.persistence.CachedStorage;
import rice.persistence.PersistentStorage;
import rice.persistence.Storage;

/**
 * Deletes fragments from the fragment storage. When a trash storage is set, deleted fragments are
 * moved there first, and fragments that must be recovered are looked up there before asking other nodes.
 */
class GlacierTrash {
  private final CachedStorage fragmentStorage;
  private final Logger logger;
  private CachedStorage trashStorage;

  GlacierTrash(CachedStorage fragmentStorage, Logger logger) {
    this.fragmentStorage = fragmentStorage;
    this.logger = logger;
    this.trashStorage = null;
  }

  void deleteFragment(final Id fkey, final Continuation<? super Boolean, Exception> command) {
    if (trashStorage != null) {
      logger.atInfo().log(() -> "Moving fragment "+fkey.toStringFull()+" to trash");
      fragmentStorage.getObject(fkey, new Continuation<Object, Exception>() {
        @Override
        public void receiveResult(Object result) {
          logger.atDebug().log(() -> "Fragment "+fkey.toStringFull()+" retrieved, storing in trash");
          if (result != null) {
            moveToTrash(fkey, result, command);
          } else {
            receiveException(new GlacierException("Move to trash: Fragment "+fkey+" does not exist?!?"));
          }
        }
        @Override
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

  /**
   * Stores a fragment in the trash, then removes it from the fragment storage.
   */
  private void moveToTrash(final Id fkey, Object fam, final Continuation<? super Boolean, Exception> command) {
    trashStorage.store(fkey, null, fam, new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        logger.atDebug().log(() -> "Deleting fragment "+fkey.toStringFull());
        fragmentStorage.unstore(fkey, command);
      }
      @Override
      public void receiveException(Exception e) {
        logger.atWarn().setCause(e).log(() -> "Cannot store in trash: "+fkey.toStringFull()+", e=");
        command.receiveException(e);
      }
    });
  }

  /**
   * Looks for a deleted fragment in the trash. Delivers the stored object, or null if there is no trash,
   * the fragment is not in it, or the trash cannot be read.
   */
  void lookupFragment(final FragmentKey key, final Continuation<Object, Exception> continuation) {
    if (trashStorage!=null) {
      trashStorage.getObject(key, new Continuation<Object, Exception>() {
        @Override
        public void receiveResult(Object result) {
          if (result != null) {
            logger.atDebug().log(() -> "retrieveFragment: Key "+key.toStringFull()+" found in trash, retrieving...");
          } else {
            logger.atDebug().log(() -> "retrieveFragment: Key "+key.toStringFull()+" not found in trash");
          }

          continuation.receiveResult(result);
        }

        @Override
        public void receiveException(Exception e) {
          logger.atWarn().log(() -> "Exception while getting object " + key + " from trash " + e);
          continuation.receiveResult(null);
        }
      });
    } else {
      logger.atDebug().log(() -> "retrieveFragment: Key "+key.toStringFull()+" not found in trash");
      continuation.receiveResult(null);
    }
  }

  long getTrashSize() {
    if (trashStorage == null)
      return 0;

    return trashStorage.getStorage().getTotalSize();
  }

  void addStatistics(GlacierStatistics statistics) {
    if (trashStorage != null)
      statistics.setNumObjectsInTrash(trashStorage.scan().numElements());

    Storage storageT = (trashStorage == null) ? null : trashStorage.getStorage();
    if (storageT instanceof PersistentStorage persistentStorage)
      statistics.setTrashStorageSize(persistentStorage.getTotalSize());
  }
}
