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

/**
 * The size accounting of {@link PersistentStorage}: the quota and the space currently used on disk.
 */
final class StorageQuota {

  private long storageSize;         // The amount of storage allowed to be used
  private long usedSize;            // The amount of storage currently in use

  /**
   * @param size the size of the storage in bytes, or -1 for unlimited
   */
  StorageQuota(long size) {
    this.storageSize = size;
  }

  /**
   * gets the amount of storage that the persistence Manager uses
   *
   * @return int the amount of storage in bytes allocated for use
   */
  long getStorageSize() {
    if (storageSize > 0)
      return storageSize;
    else
      return Long.MAX_VALUE;
  }

  /**
   * Sets the amount of storage that the persistence Manager uses
   *
   * @param size the amount of storage available to use in bytes
   * @return boolean, true if the operation succeeds false if it doesn't
   */
  boolean setStorageSize(long size) {
    if(storageSize <= size){
        storageSize = size;
        return true;
    }
    else if( size > usedSize){
        storageSize = size;
        return true;
    }
    else {
       return false;
    }
  }

  /**
   *
   * Increases the amount of storage recorded as used
   *
   * @param i the amount to increase usage by
   */
  void increaseUsedSpace(long i){
     usedSize = usedSize + i;
  }

  /**
   *
   * decreases the amount of storage recorded as used
   *
   * @param i the amount to decrease usage by
   */
  void decreaseUsedSpace(long i){
     usedSize = usedSize - i;
  }

  /**
   *
   * Gets the amount of space currently being used on disk
   *
   * @return long the amount of space being used
   *
   */
  long getUsedSpace(){
    return usedSize;
  }
}
