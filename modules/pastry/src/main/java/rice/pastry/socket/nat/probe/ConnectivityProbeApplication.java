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
package rice.pastry.socket.nat.probe;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.mpisws.p2p.transport.multiaddress.AddressStrategy;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.networkinfo.ProbeStrategy;
import org.mpisws.p2p.transport.networkinfo.Prober;
import org.mpisws.p2p.transport.sourceroute.AddressPolicyForwardStrategy;
import rice.Continuation;
import rice.p2p.commonapi.Cancellable;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.client.PastryAppl;
import rice.pastry.messaging.Message;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.nat.rendezvous.RendezvousSocketNodeHandle;
import rice.pastry.transport.PastryMessageReceipt;
import rice.pastry.transport.PastryMessageSendListener;

public class ConnectivityProbeApplication extends PastryAppl implements ProbeStrategy {
  Prober prober;
  AddressStrategy addressStrategy;

  public ConnectivityProbeApplication(PastryNode pn, Prober prober, AddressStrategy addressStrategy) {
    super(pn, null, 0, null);
    this.prober = prober;
    this.addressStrategy = addressStrategy;

    setDeserializer((buf, type, priority, sender) -> {
      return switch (ProbeMessageType.find(type)) {
        case PROBE_REQUEST -> ProbeRequestMessage.build(buf, getAddress());
        case null -> throw new IOException("Unknown type: "+type);
      };
    });
  }

  @Override
  public void messageForApplication(Message msg) {
    ProbeRequestMessage probeRequest = (ProbeRequestMessage)msg;
    handleProbeRequestMessage(probeRequest);
  }

  /**
   * Probes the address a peer asks for, unless it is one this node must not contact for someone else (see
   * {@link AddressPolicyForwardStrategy#isAcceptable}).
   */
  public void handleProbeRequestMessage(ProbeRequestMessage probeRequest) {
    logger.debug("handleProbeRequestMessage({})", probeRequest);
    MultiInetSocketAddress local = ((SocketNodeHandle)thePastryNode.getLocalHandle()).getAddress();
    InetSocketAddress target = addressStrategy.getAddress(local, probeRequest.getProbeRequester());
    if (!AddressPolicyForwardStrategy.isAcceptable(AddressPolicyForwardStrategy.addresses(local), target)) {
      logger.info("Refusing to probe {} for {}", target, probeRequest);
      return;
    }
    prober.probe(target, probeRequest.getUID(), null, null);
  }

  /**
   * Send a ProbeRequestMessage to a node in the leafset.
   *
   * The node must not have the same external address as addr.
   * If no such candidate can be found, use someone who does.
   * If there are no candidates at all, send the message to self (or call handleProbeRequest()
   */
  @Override
  public Cancellable requestProbe(MultiInetSocketAddress addr, long uniqueId, final Continuation<Boolean, Exception> deliverResultToMe) {
    logger.debug("requestProbe({},{},{})", addr, uniqueId, deliverResultToMe);
    // Step 1: find valid helpers

    // make a list of valid candidates
    ArrayList<NodeHandle> valid = new ArrayList<NodeHandle>();

    Iterator<NodeHandle> candidates = thePastryNode.getLeafSet().iterator();
    while(candidates.hasNext()) {
      SocketNodeHandle nodeHandle = (SocketNodeHandle)candidates.next();
      // don't pick self
      if (!nodeHandle.equals(thePastryNode.getLocalHandle())) {
        // if nh will send to addr's outermost address
        if (addressStrategy.getAddress(nodeHandle.getAddress(), addr).equals(addr.getOutermostAddress())) {
          valid.add(nodeHandle);
        }
      }
    }

    // if there are no valid nodes, use the other nodes
    if (valid.isEmpty()) {
      deliverResultToMe.receiveResult(false);
      return null;
    }

    // Step 2: choose one randomly
    NodeHandle handle = valid.get(thePastryNode.getEnvironment().getRandomSource().nextInt(valid.size()));

    // Step 3: send the probeRequest
    ProbeRequestMessage probeRequest = new ProbeRequestMessage(addr, uniqueId, getAddress());
    return thePastryNode.send(handle, probeRequest, new PastryMessageSendListener() {
      @Override
      public void sent(PastryMessageReceipt msg) {
        deliverResultToMe.receiveResult(true);
      }
      @Override
      public void sendFailed(PastryMessageReceipt msg, Exception reason) {
        deliverResultToMe.receiveResult(false);
      }
    }, null);
  }

  /**
   * The external addresses of the leaf set members that a new node can contact directly, which it may ask to probe
   * its own address. A rendezvous handle says whether its node is firewalled; a plain socket handle comes from a ring
   * without NAT traversal, where every node is contacted directly.
   */
  @Override
  public Collection<InetSocketAddress> getExternalAddresses() {
    ArrayList<InetSocketAddress> result = new ArrayList<InetSocketAddress>();
    for (NodeHandle nodeHandle : thePastryNode.getLeafSet()) {
      if (nodeHandle instanceof RendezvousSocketNodeHandle rsnh && !rsnh.canContactDirect()) {
        continue;
      }
      if (nodeHandle instanceof SocketNodeHandle socketNodeHandle) {
        result.add(socketNodeHandle.getInetSocketAddress());
      }
    }

    return result;
  }
}
