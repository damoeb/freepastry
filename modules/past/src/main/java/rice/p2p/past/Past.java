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

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.rawserialization.PastContentHandleDeserializer;

/**
 * This interface is exported by all instances of Past.  An instance
 * of Past provides a distributed hash table (DHT) service.  Each
 * instance stores tuples consisting of a key and an object of a
 * particular type, which must implement the interface PastContent.
 *
 * Past is event-driven, so all methods are asynchronous: they return a future, completed on the selector
 * thread. A failed future may wrap its exception in a CompletionException; unwrap it with Futures.unwrap.
 *
 * @version $Id: Past.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
public interface Past {


  /**
   * Inserts an object with the given ID into this instance of Past.
   *
   * @param obj the object to be inserted
   * @return completed on the selector thread with one Boolean per replica the insert was sent to (true where the
   *     replica stored the object or has not answered yet when enough others did), or failed with a
   *     PastException if too few replicas stored it
   */
  public CompletableFuture<Boolean[]> insert(PastContent obj);


  /**
   * Retrieves the object stored under the given ID, and caches it locally; see
   * {@link #lookup(Id, boolean)}.
   *
   * @param id the key to be queried
   * @return completed on the selector thread with the object, or null if no node has it
   */
  public CompletableFuture<PastContent> lookup(Id id);


  /**
   * Retrieves the object stored in this instance of Past with the given ID; the effect is that of a
   * lookupHandles() and a fetch() of the nearest handle. The client must authenticate the object.
   *
   * @param id the key to be queried
   * @param cache Whether or not the result should be cached
   * @return completed on the selector thread with the object, or null if no node has it
   */
  public CompletableFuture<PastContent> lookup(Id id, boolean cache);


  /**
   * Retrieves the handles of up to max replicas of the object stored under the given ID, each from a different
   * primary storage root.
   *
   * @param id the key to be queried
   * @param max the maximal number of replicas requested
   * @return completed on the selector thread with one entry per replica asked, null where it has no handle or
   *     did not answer
   */
  public CompletableFuture<PastContentHandle[]> lookupHandles(Id id, int max);


  /**
   * Retrieves the handle for the given object stored on the requested node.
   *
   * @param id the key to be queried
   * @param handle The node on which the handle is requested
   * @return completed on the selector thread with the handle, or null if the node has no such object
   */
  public CompletableFuture<PastContentHandle> lookupHandle(Id id, NodeHandle handle);


  /**
   * Retrieves the object associated with a given content handle. The client must authenticate the object.
   *
   * @param handle the key to be queried
   * @return completed on the selector thread with the object, or failed with a PastException
   */
  public CompletableFuture<PastContent> fetch(PastContentHandle handle);

  /**
   * get the nodeHandle of the local Past node
   *
   * @return the nodehandle
   */
  public NodeHandle getLocalNodeHandle();

  /**
   * Returns the number of replicas used in this Past
   *
   * @return the number of replicas for each object
   */
  public int getReplicationFactor();

  public void setContentDeserializer(PastContentDeserializer deserializer);
  public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer);

}
