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
package rice.pastry.socket;

import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_SUSPECTED;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.slf4j.Logger;
import rice.Continuation;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.Id;
import rice.pastry.JoinFailedException;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.standard.ProximityNeighborSelector;
import rice.selector.SelectorLoop;
import rice.selector.TimerTask;

/**
 * Boots a node of a {@link SocketPastryNodeFactory}: finds the nodes at the boot addresses, runs proximity neighbor
 * selection on them, then hands the result to {@link PastryNode#doneNode}.
 */
public class SocketNodeBootstrapper implements Bootstrapper<InetSocketAddress> {
  /** How long boot() waits for the remaining bootstrap handles before starting PNS. */
  private static final int BOOT_HANDLE_WAIT_MILLIS = 20000;

  protected PastryNode pn;
  private final TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, RawMessage> tl;
  private final SocketNodeHandleFactory handleFactory;
  private final ProximityNeighborSelector pns;
  /** {@code socket_seed}: the node may start a new ring even when it was given boot addresses */
  private final boolean seed;
  /** the selector of the factory's environment, which runs the boot timeout */
  private final SelectorLoop factorySelector;
  protected Logger logger;

  // we need to store this in a mutable final object
  final List<LivenessListener<NodeHandle>> listener = new ArrayList<LivenessListener<NodeHandle>>();

  /**
   * @param seed {@code socket_seed}: the node may start a new ring even when it was given boot addresses
   * @param factorySelector the selector of the factory's environment, which runs the boot timeout
   */
  public SocketNodeBootstrapper(PastryNode pn,
      TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, RawMessage> tl,
      SocketNodeHandleFactory handleFactory,
      ProximityNeighborSelector pns, boolean seed, SelectorLoop factorySelector) {
    this.logger = pn.getEnvironment().getLogManager().getLogger(SocketNodeBootstrapper.class, null);
    this.pn = pn;
    this.tl = tl;
    this.handleFactory = handleFactory;
    this.pns = pns;
    this.seed = seed;
    this.factorySelector = factorySelector;
  }

  /**
   * True if the boot address designates this node: the same port as the node's own, and either its exact address or
   * one that reaches this host (loopback, wildcard, or an address of a local network interface).
   */
  static boolean isSelf(InetSocketAddress candidate, InetSocketAddress own) {
    if (candidate.equals(own)) return true;
    if (candidate.getPort() != own.getPort()) return false;
    InetAddress address = candidate.getAddress();
    if (address == null) return false;
    if (address.isLoopbackAddress() || address.isAnyLocalAddress()) return true;
    try {
      return NetworkInterface.getByInetAddress(address) != null;
    } catch (SocketException e) {
      return false;
    }
  }

  /**
   * A loopback boot address cannot be reached when this node listens on a non-loopback address, so it is mapped to
   * this node's own bind address with the boot address's port. Other boot addresses are returned unchanged.
   */
  InetSocketAddress mapLoopbackBoot(InetSocketAddress boot, InetSocketAddress own) {
    InetAddress bootAddress = boot.getAddress();
    InetAddress ownAddress = own.getAddress();
    if (bootAddress == null || ownAddress == null) return boot;
    if (!bootAddress.isLoopbackAddress() || ownAddress.isLoopbackAddress() || ownAddress.isAnyLocalAddress()) return boot;
    InetSocketAddress mapped = new InetSocketAddress(ownAddress, boot.getPort());
    logger.info("boot address {} is loopback but this node is bound to {}, booting to {} instead", boot, own, mapped);
    return mapped;
  }

  protected void bootAsBootstrap() {
    pn.doneNode(Collections.EMPTY_LIST);
  }

  /**
   * Non-blocking, and may be called on any thread: the boot itself runs on the selector thread, where the join and
   * the readiness change belong.
   */
  @Override
  public void boot(Collection<InetSocketAddress> bootaddresses) {
    SelectorLoop selector = pn.getEnvironment().getSelectorManager();
    if (selector.isSelectorThread()) {
      bootOnSelector(bootaddresses);
    } else {
      selector.invoke(() -> bootOnSelector(bootaddresses));
    }
  }

  /**
   * The problem is that we don't have any NodeHandles yet, only Ip addresses.  So, we're going to check liveness on a
   * bogus node handle, and when the WrongEpochAddress message comes back, the bogus node will be faulty, but a new
   * node (the real NodeHandle at that address) will come alive.  We can discover this by installing a LivenessListener
   * on the transport layer.
   *
   * Here is what happens:
   * Create the "bogus" NodeHandles and store them in tempBootHandles
   * Register LivenessListener on transport layer.
   * Ping all of the "bogus" NodeHandles
   * When liveness changes to Non-Faulty it means we have found a new node.
   * When we collect all of the bootaddresses, or there is a timeout, we call continuation.receiveResult()
   * Continuation.receiveResult() unregisters the LivenessListener and then calls ProximityNeighborSelection.nearNodes()
   * PNS.nearNodes() calls into the continuation which calls PastryNode.doneNode()
   */
  protected void bootOnSelector(Collection<InetSocketAddress> bootaddresses_temp) {
    logger.debug("boot({})", bootaddresses_temp);
    final InetSocketAddress ownAddress = ((SocketNodeHandle)pn.getLocalHandle()).getAddress().getInnermostAddress();
    final Collection<InetSocketAddress> bootaddresses;
    if (bootaddresses_temp == null) {
      bootaddresses = Collections.EMPTY_LIST;
    } else {
      bootaddresses = bootaddresses_temp.stream().map(a -> mapLoopbackBoot(a, ownAddress)).toList();
    }

    final boolean seed = this.seed || bootaddresses.isEmpty() || bootaddresses.stream().anyMatch(a -> isSelf(a, ownAddress));

    if (bootaddresses.isEmpty() ||
        (bootaddresses.size() == 1 && seed)) {
      logger.info("boot() calling pn.doneNode(empty)");
      bootAsBootstrap();
      return;
    }

    // bogus handles
    final Collection<SocketNodeHandle> tempBootHandles = new ArrayList<SocketNodeHandle>(bootaddresses.size());

    // real handles
    final Collection<rice.pastry.NodeHandle> bootHandles =
      new HashSet<rice.pastry.NodeHandle>();

    TransportLayerNodeHandle<MultiInetSocketAddress> local = tl.getLocalIdentifier();
    InetSocketAddress localAddr = local.getAddress().getInnermostAddress();

    // fill in tempBootHandles
    for (InetSocketAddress addr : bootaddresses) {
      logger.trace("addr:{} local:{}", addr, localAddr);
      if (!isSelf(addr, localAddr)) {
        tempBootHandles.add(getTempNodeHandle(addr));
      }
    }

    // every boot address was this node itself: nobody else to join, so start a new ring
    if (tempBootHandles.isEmpty()) {
      logger.info("boot() calling pn.doneNode(empty), all boot addresses are self");
      bootAsBootstrap();
      return;
    }

    // this is the end of the task, but we have to declare it here
    final Continuation<Collection<NodeHandle>, Exception> beginPns = new Continuation<Collection<NodeHandle>, Exception>(){
      boolean done = false; // to make sure this is only called once
      /**
       * This is usually going to get called twice.  The first time when bootHandles is complete,
       * the second time on a timeout.
       *
       */
      @Override
      public void receiveResult(Collection<NodeHandle> initialSet) {
        // make sure this only gets called once
        if (done) return;
        done = true;
        logger.debug("boot() beginning pns with {}", initialSet);

        // remove the listener
        pn.getLivenessProvider().removeLivenessListener(listener.get(0));

        // do proximity neighbor selection
        pns.getNearHandles(initialSet, new Continuation<Collection<NodeHandle>, Exception>(){

          @Override
          public void receiveResult(Collection<NodeHandle> result) {
            // done!!!
            if (!seed && result.isEmpty()) {
              pn.joinFailed(new JoinFailedException("Cannot join ring.  All bootstraps are faulty."+bootaddresses));
              return;
            }
            logger.info("boot() calling pn.doneNode({})", result);
            pn.doneNode(result);
          }

          @Override
          public void receiveException(Exception exception) {
          }

        });
      }

      @Override
      public void receiveException(Exception exception) {
      }
    };


    // Create the listener for the "real" nodes coming online based on WrongAddress messages from the "Bogus" ones
    listener.add(
      new LivenessListener<NodeHandle>() {
        Logger logger = pn.getEnvironment().getLogManager().getLogger(SocketPastryNodeFactory.class, null);
        @Override
        public void livenessChanged(NodeHandle i2, int val, Map<String, Object> options) {
          SocketNodeHandle i = (SocketNodeHandle)i2;
          logger.debug("livenessChanged({},{})", i, val);
          if (val <= LIVENESS_SUSPECTED && i.getEpoch() != -1L) {
            boolean complete = false;

            // add the new handle
            synchronized(bootHandles) {
              bootHandles.add((SocketNodeHandle)i);
              if (bootHandles.size() == tempBootHandles.size()) {
                complete = true;
              }
            }
            if (complete) {
              beginPns.receiveResult(bootHandles);
            }
          }
        }
      });

    logger.debug("boot() adding liveness listener");
    // register the listener
    pn.getLivenessProvider().addLivenessListener(listener.get(0));

    logger.debug("boot() checking liveness");
    // check liveness on the bogus nodes
    for (SocketNodeHandle h : tempBootHandles) {
      checkLiveness(h, null);
    }

    // need to synchronize, because this can be called on any thread
    synchronized(bootHandles) {
      if (bootHandles.size() < tempBootHandles.size()) {
        // only wait BOOT_HANDLE_WAIT_MILLIS for the nodes
        factorySelector.schedule(new TimerTask(){

          @Override
          public void run() {
            logger.debug("boot() timer expiring, attempting to start pns (it may have already started)");

            beginPns.receiveResult(bootHandles);
          }
        }, BOOT_HANDLE_WAIT_MILLIS);
      }
    }

    // the root node (no boot addresses)
    if (tempBootHandles.isEmpty()) {
      logger.debug("invoking receiveResult (this is probably the first node in the ring)");
      factorySelector.invoke(() -> beginPns.receiveResult(bootHandles));
    }

    logger.debug("boot() returning");

  }
  /**
   * Used as a helper to SocketNodeBootstrapper.boot() to generate a temporary node handle
   *
   */
  protected SocketNodeHandle getTempNodeHandle(InetSocketAddress addr) {
    return handleFactory.getNodeHandle(new MultiInetSocketAddress(addr), -1, Id.build());
  }



  protected void checkLiveness(SocketNodeHandle h, Map<String, Object> options) {
    pn.getLivenessProvider().checkLiveness(h, null);
  }
}
