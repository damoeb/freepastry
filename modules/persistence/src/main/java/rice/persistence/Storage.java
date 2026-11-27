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

/*
 * @(#) Storage.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 *
 * @version $Id: Storage.java 4654 2009-01-08 16:33:07Z jeffh $
 */

import java.util.concurrent.CompletableFuture;

import rice.p2p.commonapi.Id;

/**
 * This interface is the abstraction of something which provides a
 * local storage service, such as a persistence storage service or
 * an in-memory storage service.  Two implementations are provided,
 * the PersistentStorage and MemoryStorage, respsectively.
 */
@SuppressWarnings("unchecked")
public interface Storage extends Catalog {

  /**
   * Stores an object under the given id, replacing what was stored there.
   *
   * @param id The object's id.
   * @param metadata The object's metadata
   * @param obj The object to store.
   * @return completes with whether the object was stored
   */
  CompletableFuture<Boolean> store(Id id, Object metadata, Object obj);

  /**
   * Removes the object stored under the given id.
   *
   * @param id The object's id.
   * @return completes with whether an object was removed
   */
  CompletableFuture<Boolean> unstore(Id id);
}
