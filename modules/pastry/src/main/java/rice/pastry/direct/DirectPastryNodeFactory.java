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

package rice.pastry.direct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.direct.DirectTransportLayer;
import org.mpisws.p2p.transport.proximity.ProximityListener;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Slf4jLogManager;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.NodeHandleFactoryListener;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.routing.RoutingTable;
import rice.pastry.standard.ConsistentJoinConfig;
import rice.pastry.standard.ProximityNeighborSelector;
import rice.pastry.standard.StandardLeafSetProtocol;
import rice.pastry.transport.NodeHandleAdapter;
import rice.pastry.transport.TLDeserializer;
import rice.pastry.transport.TransportPastryNodeFactory;
import rice.selector.SelectorManager;

/**
 * Pastry node factory for direct connections between nodes (local instances).
 *
 * @version $Id: DirectPastryNodeFactory.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Andrew Ladd
 * @author Sitaram Iyer
 * @author Rongmei Zhang/Y. Charlie Hu
 */
@SuppressWarnings("unchecked")
public class DirectPastryNodeFactory extends TransportPastryNodeFactory {
  private final NodeIdFactory nidFactory;

  private final NetworkSimulator<DirectNodeHandle, RawMessage> simulator;

  private final Collection<NodeHandleFactoryListener<? super NodeHandle>> listeners =
    new ArrayList<NodeHandleFactoryListener<? super NodeHandle>>();

  private static final int SIMULATED_MAX_TIME_TO_BE_SCHEDULED = 120000;

  private final NetworkSimulatorConfig simulatorConfig;

  /**
   * Main constructor.
   *
   * @param nf the NodeIdFactory
   * @param sim the NetworkSimulator
   * @param env the Environment
   */
  public DirectPastryNodeFactory(NodeIdFactory nf, NetworkSimulator<DirectNodeHandle,RawMessage> sim, Environment env) {
    super(env);
    nidFactory = nf;
    simulator = sim;
    simulatorConfig = NetworkSimulatorConfig.fromParameters(env.getParameters());
  }

  /**
   * A simulated node is scheduled far less often than a real one, so it is given longer before it assumes it was
   * not scheduled; the environment's parameters are left alone, as they are shared.
   */
  @Override
  protected ConsistentJoinConfig getConsistentJoinConfig(PastryNode pn) {
    ConsistentJoinConfig config = super.getConsistentJoinConfig(pn);
    return new ConsistentJoinConfig(SIMULATED_MAX_TIME_TO_BE_SCHEDULED, config.retryInterval(),
        config.failedRetentionTime(), config.maxFailedToSend(), config.cleanupInterval());
  }

  @Override
  protected LeafSetProtocol getLeafSetProtocol(PastryNode pn, LeafSet leafSet,
      RoutingTable routeTable) {
    if (simulatorConfig.guaranteeConsistency()) {
      return super.getLeafSetProtocol(pn, leafSet, routeTable);
    } else {
      StandardLeafSetProtocol ret = new StandardLeafSetProtocol(pn,pn.getLocalHandle(),leafSet, routeTable);
      ret.register();
      return ret;
    }
  }

  @Override
  public PastryNode newNode() throws IOException {
    return newNode(nidFactory.generateNodeId());
  }

  HashMap<Id, NodeRecord> recordTable = new HashMap<Id, NodeRecord>();

  /**
   * Manufacture a new Pastry node.
   *
   * @return a new PastryNode
   */
  @Override
  public PastryNode newNode(Id nodeId) throws IOException {

      // this code builds a different environment for each PastryNode
      Environment environment = this.environment;
      if (factoryConfig.multipleNodes()) {
        if (this.environment.getLogManager() instanceof Slf4jLogManager slm) {
          environment = new Environment(
              this.environment.getSelectorManager(),
              this.environment.getProcessor(),
            this.environment.getRandomSource(),
            this.environment.getTimeSource(),
            slm.clone("0x"+nodeId.toStringBare()),
            this.environment.getParameters(),
            this.environment.getExceptionStrategy());
        }
      }
      PastryNode pn = new PastryNode(nodeId, environment);
      nodeHandleHelper(pn);

      return pn;
  }


  @Override
  protected NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory<?> handleFactory) throws IOException {
    DirectNodeHandle localhandle = new DirectNodeHandle(pn, simulator);
    return localhandle;
  }

  @Override
  protected NodeHandleFactory<?> getNodeHandleFactory(PastryNode pn) throws IOException {
    // simulator handles are shared objects: they are never serialized, and each one is already canonical
    return new NodeHandleFactory<NodeHandle>(){

      @Override
      public NodeHandle readNodeHandle(InputBuffer buf) throws IOException {
        throw new IOException("The direct simulator does not deserialize node handles");
      }

      @Override
      public NodeHandle coalesce(NodeHandle handle) {
        notifyListeners(handle);
        return handle;
      }

      /**
       * Notify the listeners that this new handle has come along.
       */
      void notifyListeners(NodeHandle nh) {
        Collection<NodeHandleFactoryListener<? super NodeHandle>> temp = listeners;
        synchronized (listeners) {
          temp = new ArrayList<NodeHandleFactoryListener<? super NodeHandle>>(listeners);
        }
        for (NodeHandleFactoryListener<? super NodeHandle> foo:temp) {
          foo.nodeHandleFound(nh);
        }
      }

      @Override
      public void addNodeHandleFactoryListener(
          NodeHandleFactoryListener<? super NodeHandle> listener) {
        synchronized(listeners) {
          listeners.add(listener);
        }
      }


      @Override
      public void removeNodeHandleFactoryListener(
          NodeHandleFactoryListener<? super NodeHandle> listener) {
        synchronized(listeners) {
          listeners.remove(listener);
        }
      }
    };
  }

  @Override
  protected NodeHandleAdapter getNodeHandleAdapter(final PastryNode pn, NodeHandleFactory<?> handleFactory, TLDeserializer deserializer) throws IOException {
    NodeRecord nr = (NodeRecord)recordTable.get(pn.getId());
    if (nr == null) {
      nr = simulator.generateNodeRecord();
      recordTable.put(pn.getNodeId(),nr);
    }
    TransportLayer<? extends NodeHandle, RawMessage> tl = getDirectTransportLayer(pn, nr);

    NodeHandleAdapter nha = new NodeHandleAdapter(tl,simulator.getLivenessProvider(),new ProximityProvider<NodeHandle>(){
      @Override
      public int proximity(NodeHandle i, Map<String, Object> options) {
        return (int)simulator.proximity((DirectNodeHandle)pn.getLocalHandle(), (DirectNodeHandle)i);
      }

      // proximity won't change, so don't worry about it
      List<ProximityListener<NodeHandle>> proxListeners = new ArrayList<ProximityListener<NodeHandle>>();
      @Override
      public void addProximityListener(ProximityListener<NodeHandle> name) {
        proxListeners.add(name);
      }

      @Override
      public boolean removeProximityListener(ProximityListener<NodeHandle> name) {
        return proxListeners.remove(name);
      }

      @Override
      public void clearState(NodeHandle i) {

      }
    });

    return nha;
  }

  /**
   * Override me
   */
  protected TransportLayer<? extends NodeHandle, RawMessage> getDirectTransportLayer(PastryNode pn, NodeRecord nr) {
    DirectNodeHandle local = (DirectNodeHandle)pn.getLocalHandle();
    DirectTransportLayer<DirectNodeHandle, RawMessage> tl = new DirectTransportLayer<DirectNodeHandle, RawMessage>(
        local, simulator.getGenericSimulator(), simulator.getLivenessProvider(), pn.getEnvironment());
    simulator.registerNode(local, tl, nr);

    return tl;
  }

  @Override
  protected Bootstrapper<NodeHandle> getBootstrapper(final PastryNode pn, NodeHandleAdapter tl, NodeHandleFactory<?> handleFactory, final ProximityNeighborSelector pns) {
    // boot() may be called on any thread, but PNS, the join and the readiness change belong on the selector
    return bootaddresses -> {
      Runnable boot = () -> pns.getNearHandles(bootaddresses, new Continuation<Collection<NodeHandle>, Exception>() {
        @Override
        public void receiveResult(Collection<NodeHandle> result) {
          logger.info("boot() calling pn.doneNode({})", result);
          pn.doneNode(result);
        }
        @Override
        public void receiveException(Exception exception) {
        }
      });
      SelectorManager selector = pn.getEnvironment().getSelectorManager();
      if (selector.isSelectorThread()) {
        boot.run();
      } else {
        selector.invoke(boot);
      }
    };
  }
}
