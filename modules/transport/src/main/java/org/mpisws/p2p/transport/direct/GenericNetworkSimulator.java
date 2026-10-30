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
package org.mpisws.p2p.transport.direct;

import org.mpisws.p2p.transport.liveness.LivenessProvider;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;

public interface GenericNetworkSimulator<Identifier, MessageType> extends LivenessProvider<Identifier> {

  /**
   * Get the environment related to a specific node.
   */
  public Environment getEnvironment(Identifier i);

  /**
   * Determines delivery time from a to b.
   *
   * @param a a node id.
   * @param b another node id.
   *
   * @return delay of b from a.
   */
  public float networkDelay(Identifier a, Identifier b);


  /**
   * Deliver message.
   *
   * @param msg message to deliver.
   * @param to the node to deliver it to.
   * @param from the node that sent it.
   * @param delay how long to delay to deliver the message
   */
  public Cancellable deliverMessage(MessageType msg, Identifier to, Identifier from, int delay);

  /**
   * Schedule a delivery.
   *
   * @param del the delivery to execute.
   * @param delay how long to delay to deliver the message
   */
  public CancellableTask enqueueDelivery(Delivery del, int delay);

  public DirectTransportLayer<Identifier, MessageType> getTL(Identifier i);

  public boolean isAlive(Identifier i);

  /**
   * Kill identifier.
   */
  public void remove(Identifier i);


}
