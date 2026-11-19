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
package rice.pastry.transport;

import java.io.IOException;

import rice.environment.Environment;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.ReadyStrategy;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.join.JoinProtocol;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.messaging.MessageDispatch;
import rice.pastry.pns.ProximityNeighborSelectionApplication;
import rice.pastry.routing.RouteSetProtocol;
import rice.pastry.routing.RouterStrategy;
import rice.pastry.routing.RoutingTable;
import rice.pastry.standard.ConsistentJoinConfig;
import rice.pastry.standard.ConsistentJoinProtocol;
import rice.pastry.standard.PeriodicLeafSetProtocol;
import rice.pastry.standard.ProximityNeighborSelector;
import rice.pastry.standard.RapidRerouter;
import rice.pastry.standard.StandardRouteSetProtocol;
import rice.pastry.standard.StandardRouter;

@SuppressWarnings("unchecked")
public abstract class TransportPastryNodeFactory extends PastryNodeFactory {

  /**
   * Large period (in seconds) means infrequent, 0 means never.
   */
  private final int leafSetMaintFreq;

  private final int routeSetMaintFreq;

  /**
   * The factory parameters, read at construction.
   */
  protected final PastryFactoryConfig factoryConfig;


  /**
   * Constructor.
   *
   * @param env The environment.
   */
  public TransportPastryNodeFactory(Environment env) {
    super(env);
    factoryConfig = PastryFactoryConfig.fromParameters(env.getParameters());
    leafSetMaintFreq = factoryConfig.leafSetMaintFreq();
    routeSetMaintFreq = factoryConfig.routeSetMaintFreq();
  }

  public PastryNode nodeHandleHelper(PastryNode pn) throws IOException {

    NodeHandleFactory<?> handleFactory = getNodeHandleFactory(pn);
    NodeHandle localhandle = getLocalHandle(pn, handleFactory);

    TransportLayerDeserializer deserializer = getTransportLayerDeserializer(handleFactory,pn);

    MessageDispatch msgDisp = new MessageDispatch(pn, deserializer);
    RoutingTable routeTable = new RoutingTable(localhandle, rtMax, rtBase,
        pn);
    LeafSet leafSet = new LeafSet(localhandle, lSetSize, routeTable);
    StandardRouter router = new RapidRerouter(pn, msgDisp, getRouterStrategy(pn));
    pn.setElements(localhandle, msgDisp, leafSet, routeTable, router);


    NodeHandleAdapter nodeHandleAdapter = getNodeHandleAdapter(pn, handleFactory, deserializer);

    pn.setSocketElements(leafSetMaintFreq, routeSetMaintFreq,
        nodeHandleAdapter, nodeHandleAdapter, nodeHandleAdapter, handleFactory);

    router.register();

    registerApps(pn, leafSet, routeTable, nodeHandleAdapter, handleFactory);

    return pn;
  }

  protected RouterStrategy getRouterStrategy(PastryNode pn) {
    return null; // use the default one
  }

  protected void registerApps(PastryNode pn, LeafSet leafSet, RoutingTable routeTable, NodeHandleAdapter nodeHandleAdapter, NodeHandleFactory<?> handleFactory) {
    ProximityNeighborSelector neighborSelector = getProximityNeighborSelector(pn);

    Bootstrapper<?> bootstrapper = getBootstrapper(pn, nodeHandleAdapter, handleFactory, neighborSelector);

    RouteSetProtocol rsProtocol = getRouteSetProtocol(pn, leafSet, routeTable);

    LeafSetProtocol lsProtocol = getLeafSetProtocol(pn, leafSet, routeTable);

    ReadyStrategy readyStrategy;
    if (lsProtocol instanceof ReadyStrategy rs) {
      readyStrategy = rs;
    } else {
      readyStrategy = pn.getDefaultReadyStrategy();
    }

    JoinProtocol jProtocol = getJoinProtocol(pn, leafSet, routeTable, readyStrategy);

    pn.setJoinProtocols(bootstrapper, jProtocol, lsProtocol, rsProtocol);
  }

  protected RouteSetProtocol getRouteSetProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable) {
    StandardRouteSetProtocol rsProtocol = new StandardRouteSetProtocol(pn, routeTable);
    rsProtocol.register();
    return rsProtocol;
  }

  protected LeafSetProtocol getLeafSetProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable) {
    PeriodicLeafSetProtocol lsProtocol = new PeriodicLeafSetProtocol(pn,
        pn.getLocalHandle(), leafSet, routeTable);
    lsProtocol.register();
    return lsProtocol;

  }

  protected JoinProtocol getJoinProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable, ReadyStrategy lsProtocol) {
    ConsistentJoinProtocol jProtocol = new ConsistentJoinProtocol(pn,
        pn.getLocalHandle(), routeTable, leafSet, lsProtocol, null, getConsistentJoinConfig(pn));
    jProtocol.register();
    return jProtocol;
  }

  /**
   * The parameters of the join protocol; can be overridden to differ from the environment's without changing them.
   */
  protected ConsistentJoinConfig getConsistentJoinConfig(PastryNode pn) {
    return ConsistentJoinConfig.fromParameters(pn.getEnvironment().getParameters());
  }

  protected TransportLayerDeserializer getTransportLayerDeserializer(NodeHandleFactory<?> handleFactory, PastryNode pn) {
    TransportLayerDeserializer deserializer = new TransportLayerDeserializer(handleFactory, pn.getEnvironment());
    return deserializer;
  }

  /**
   * Can be overridden.
   */
  protected ProximityNeighborSelector getProximityNeighborSelector(PastryNode pn) {
    if (factoryConfig.usePns()) {
      ProximityNeighborSelectionApplication pns = new ProximityNeighborSelectionApplication(pn);
      pns.register();
      return pns;
    }

    // do nothing
    return (bootHandles, deliverResultToMe) -> {
      deliverResultToMe.receiveResult(bootHandles);
      return null;
    };
  }

  protected abstract NodeHandle getLocalHandle(PastryNode pn,
      NodeHandleFactory<?> handleFactory) throws IOException;
  protected abstract NodeHandleAdapter getNodeHandleAdapter(PastryNode pn,
      NodeHandleFactory<?> handleFactory, TransportLayerDeserializer deserializer) throws IOException;
  protected abstract NodeHandleFactory<?> getNodeHandleFactory(PastryNode pn) throws IOException;
  protected abstract Bootstrapper<?> getBootstrapper(PastryNode pn,
      NodeHandleAdapter tl,
      NodeHandleFactory<?> handleFactory,
      ProximityNeighborSelector neighborSelector);
}
