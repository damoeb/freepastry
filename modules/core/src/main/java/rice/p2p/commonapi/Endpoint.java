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

package rice.p2p.commonapi;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import rice.Continuation;
import rice.Executable;
import rice.environment.Environment;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.commonapi.rawserialization.NodeHandleReader;
import rice.p2p.commonapi.rawserialization.RawMessage;

/**
 *
 *
 * Interface which represents a node in a peer-to-peer system, regardless of
 * the underlying protocol.  This represents the *local* node, upon which applications
 * can call methods.
 *
 * @version $Id: Endpoint.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public interface Endpoint extends NodeHandleReader {

  /**
   * Returns this node's id, which is its identifier in the namespace.
   *
   * @return The local node's id
   */
  public Id getId();

  /**
   * This method makes an attempt to route the message to the root of the given id.
   * The hint handle will be the first hop in the route. If the id field is null, then
   * the message is routed directly to the given node, and delivers the message
   * there.  If the hint field is null, then this method makes an attempt to route
   * the message to the root of the given id.  Note that one of the id and hint fields can
   * be null, but not both.
   *
   * @param id The destination Id of the message.
   * @param message The message to deliver
   * @param hint The first node to send this message to, optional
   * @param deliverAckToMe notified when the message is sent
   * @param options transport options for this message, may be null
   * @return can cancel the request
   */
  MessageReceipt route(Id id, RawMessage message, NodeHandle hint,
      DeliveryNotification deliverAckToMe, Map<String, Object> options);
  MessageReceipt route(Id id, RawMessage message, NodeHandle hint,
      DeliveryNotification deliverAckToMe);
  MessageReceipt route(Id id, RawMessage message, NodeHandle hint);



  /**
   * This call produces a list of nodes that can be used as next hops on a route towards
   * the given id, such that the resulting route satisfies the overlay protocol's bounds
   * on the number of hops taken.  If the safe flag is specified, then the fraction of
   * faulty nodes returned is no higher than the fraction of faulty nodes in the overlay.
   *
   * @param id The destination id.
   * @param num The number of nodes to return.
   * @param safe Whether or not to return safe nodes.
   */
  NodeHandleSet localLookup(Id id, int num, boolean safe);

  /**
   * This methods returns an unordered set of nodehandles on which are neighbors of the local
   * node in the id space.  Up to num handles are returned.
   *
   * NOTE: This is not the same thing as the neighborhood set as described in some early papers.
   * The if you are looking for the neighborhood set, see localLookup()
   *
   * @param num The number of desired handle to return.
   */
  NodeHandleSet neighborSet(int num);

  /**
   * This method returns an ordered set of nodehandles on which replicas of an object with
   * a given id can be stored.  The call returns nodes up to and including a node with maxRank.
   *
   * This method is equivalent to calling
   *
   * replicaSet(id, maxRank, getLocalNodeHandle(), neighborSet(Integer.MAX_VALUE));
   *
   * @param id The object's id.
   * @param maxRank The number of desired replicas.
   */
  NodeHandleSet replicaSet(Id id, int maxRank);

  /**
   * This methods returns an ordered set of nodehandles on which replicas of an object with
   * a given id can be stored.  The call returns nodes up to and including a node with maxRank.
   * This call also allows the application to provide a remote "center" node, as well as
   * other nodes in the vicinity.
   *
   * @param id The object's id.
   * @param maxRank The number of desired replicas.
   * @param root The root handle of the remote set
   * @param set The set of other nodes around the root handle
   * @return the replica set
   */
  NodeHandleSet replicaSet(Id id, int maxRank, NodeHandle root, NodeHandleSet set);

  /**
   * This operation provides information about ranges of keys for which the node is currently
   * a rank-root. The operations throws an exception if the range could not be determined.
   * It is an error to query the range of a node not present in the neighbor set as
   * returned bythe update upcall or the neighborSet call. Certain implementations may return
   * an error if rank is greater than zero. Some protocols may have multiple, disjoint ranges
   * of keys for which a given node is responsible. The parameter lkey allows the caller to
   * specify which region should be returned. If the node referenced by is responsible for key
   * lkey, then the resulting range includes lkey. Otherwise, the result is the nearest range
   * clockwise from lkey for which is responsible.
   *
   * @param handle The handle whose range to check.
   * @param rank The root rank.
   * @param lkey An "index" in case of multiple ranges.
   */
  IdRange range(NodeHandle handle, int rank, Id lkey) throws RangeCannotBeDeterminedException;

  /**
   * This operation provides information about ranges of keys for which the node is currently
   * a rank-root. The operations returns null if the range could not be determined, the range
   * otherwise. It is an error to query the range of a node not present in the neighbor set as
   * returned bythe update upcall or the neighborSet call. Certain implementations may return
   * an error if rank is greater than zero. Some protocols may have multiple, disjoint ranges
   * of keys for which a given node is responsible. The parameter lkey allows the caller to
   * specify which region should be returned. If the node referenced by is responsible for key
   * lkey, then the resulting range includes lkey. Otherwise, the result is the nearest range
   * clockwise from lkey for which is responsible.
   *
   * @param handle The handle whose range to check.
   * @param rank The root rank.
   * @param lkey An "index" in case of multiple ranges.
   * @param cumulative Whether to return the cumulative or single range
   */
  IdRange range(NodeHandle handle, int rank, Id lkey, boolean cumulative) throws RangeCannotBeDeterminedException;

  /**
   * Returns a handle to the local node below this endpoint.  This node handle is serializable,
   * and can therefore be sent to other nodes in the network and still be valid.
   *
   * @return A NodeHandle referring to the local node.
   */
  NodeHandle getLocalNodeHandle();

  /**
   * Schedules a message to be delivered to this application after the provided number of
   * milliseconds.
   *
   * @param message The message to be delivered
   * @param delay The number of milliseconds to wait before delivering the message
   */
  CancellableTask scheduleMessage(Message message, long delay);

  /**
   * Schedules a message to be delivered to this application every period number of
   * milliseconds, after delay number of milliseconds have passed.
   *
   * @param message The message to be delivered
   * @param delay The number of milliseconds to wait before delivering the first message
   * @param period The number of milliseconds to wait before delivering subsequent messages
   */
  CancellableTask scheduleMessage(Message message, long delay, long period);

  /**
   * Schedules a message to be delivered to this application every period number of
   * milliseconds, after delay number of milliseconds have passed.
   *
   * @param message The message to be delivered
   * @param delay The number of milliseconds to wait before delivering the first message
   * @param period The number of milliseconds to wait before delivering subsequent messages
   */
  CancellableTask scheduleMessageAtFixedRate(Message message, long delay, long period);

  /**
   * Schedules a job for processing on the dedicated processing thread.  CPU intensive jobs, such
   * as encryption, erasure encoding, or bloom filter creation should never be done in the context
   * of the underlying node's thread, and should only be done via this method.
   *
   * @param task The task to run on the processing thread
   * @param command The command to return the result to once it's done
   */
  <R, E extends Exception> void process(Executable<R, E> task, Continuation<R, E> command);

  /**
   * Returns a unique instance name of this endpoint, sort of a mailbox name for this
   * application.
   *
   * @return The unique instance name of this application
   */
  public String getApplicationInstanceName();

  /**
   * Returns the environment.  This allows the nodes to be virtualized within the JVM
   * @return the environment for this node/app.
   */
  public Environment getEnvironment();

  /**
   * Set's the acceptor for this application.  If no acceptor is set, then when a remote
   * node's application opens a socket here, they will get an *Exception*
   *
   * @param receiver calls receiveSocket() when a new AppSocket is opened to this application
   * from a remote node.
   * Note that you must call accept() again after each socket is received to properly handle
   * socket backlogging
   */
  public void accept(AppSocketReceiver receiver);

  /**
   * Opens a connection to this application on a remote node.
   * If no acceptor is set, then receiver will get an *Exception*
   *
   * @param receiver calls receiveSocket() when a new AppSocket is opened to this application
   * on a remote node.
   */
  public void connect(NodeHandle handle, AppSocketReceiver receiver, int timeout);

  /**
   * Sets the deserializer that reads this endpoint's raw messages back; every message an application
   * routes is a {@link RawMessage}, so it has to set one before it can receive any.
   *
   * @param md the deserializer for this endpoint's raw messages
   */
  public void setDeserializer(MessageDeserializer md);

  /**
   * Returns the deserializer, {@link MessageDeserializer#NONE} until one is set.
   * @return the deserializer
   */
  public MessageDeserializer getDeserializer();

  /**
   * default value is true
   *
   * Consistent routing causes RouteMessages to be dropped if we are not sure that we are
   * responsible for our current id range.  If you set this to false, you will be delivered
   * messages to forward() and deliver() even if there is possible inconsistent routing.
   *
   * (The overlay still does it's best to deliver the RouteMessage to the proper key,
   * but some protocols can guarantee consistency, for example by using leases between
   * nodes)
   *
   * Note that when true, the overlay never purposely drops messages routed with a null
   * key:
   * endpoint.deliver(null, msg, targetNodeHandle) // will be delivered regardless of consistency
   */
  public void setRoutingConsistencyRequired(boolean val);

  /**
   * Can we guarantee that this id is currently ours, and routing will be consistent?
   * Note that this has some real timing implications, so don't cache the returned value.
   * Note that it always returns false if you are not the root of the id.
   *
   * @param id the id to verify
   * @return true if routing consistency can currently be guaranteed for this id
   */
  public boolean isRoutingConsistentFor(Id id);

  /**
   * Reads an Id written with Id.serialize()
   *
   * @param buf the buffer to read the id from
   * @param type the type of the id, as returned by Id.getType()
   * @return the id
   * @throws IOException if reading from buf fails
   */
  public Id readId(InputBuffer buf, short type) throws IOException;

  /**
   * Returns an ordered list of the nearest known neighbors.
   * Note that proximity is usually an estimate, and can quickly become stale.
   *
   * @return List of NodeHandle
   */
  public List<NodeHandle> networkNeighbors(int num);

  /**
   * To use Raw Serialization
   */
  public IdRange readIdRange(InputBuffer buf) throws IOException;

  public NodeHandleSet readNodeHandleSet(InputBuffer buf, short type) throws IOException;

  @Override
  public NodeHandle readNodeHandle(InputBuffer buf) throws IOException;

  /**
   * Call this after you have set up your Endpoint:
   *   called setDeserializer(), called accept().
   */
  public void register();

  /**
   * Returns the last known proximity of the node; the handle does not have to be "coalesced".
   * Smaller values imply greater proximity.
   *
   * @param nh the node
   * @return the proximity metric value
   */
  public int proximity(NodeHandle nh);

  /**
   * Returns the last known liveness of the node.
   *
   * @param nh the node
   * @return whether the node is believed to be alive
   */
  public boolean isAlive(NodeHandle nh);

  /**
   * Uses these options as defaults.
   */
  public void setSendOptions(Map<String, Object> options);
}
