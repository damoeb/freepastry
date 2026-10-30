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

import rice.environment.Environment;

/**
 * @(#) Node.java
 *
 * Interface which represents a node in a peer-to-peer system, regardless of
 * the underlying protocol.  This represents a factory, in a sense, that will
 * give a application an Endpoint which it can use to send and receive
 * messages.
 *
 * @version $Id: Node.java 3992 2007-12-02 20:09:18Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public interface Node {

  /**
   * Returns the Id of this node
   *
   * @return This node's Id
   */
  public Id getId();

  /**
   * Returns a factory for Ids specific to this node's protocol.
   *
   * @return A factory for creating Ids.
   */
  public IdFactory getIdFactory();

  /**
   * Returns a handle to the local node. This node handle is serializable, and
   * can therefore be sent to other nodes in the network and still be valid.
   *
   * @return A NodeHandle referring to the local node.
   */
  public NodeHandle getLocalNodeHandle();

  /**
   * Returns the environment.  This allows the nodes to be virtualized within the JVM
   * @return the environment for this node/app.
   */
  public Environment getEnvironment();

  /**
   * Same as register application, but returns an unregistered Endpoint.  This allows
   * the application to finish initialization that may require the endpoint
   * before it receives messages from the network and notification of changes.
   *
   * When then application is ready, it must call endpoint.register() to receive messages.
   *
   * @param application
   * @param instance
   * @return
   */
  public Endpoint buildEndpoint(Application application, String instance);

  /**
   * For debugging: print the internal routing state of the Node.
   * @return
   */
  public String printRouteState();
}
