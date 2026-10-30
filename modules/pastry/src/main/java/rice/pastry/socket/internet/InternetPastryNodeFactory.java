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
package rice.pastry.socket.internet;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.networkinfo.CantVerifyConnectivityException;
import org.mpisws.p2p.transport.networkinfo.ConnectivityResult;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.pastry.Id;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.socket.nat.CantFindFirewallException;
import rice.pastry.socket.nat.NATHandler;
import rice.pastry.socket.nat.StubNATHandler;
import rice.pastry.socket.nat.connectivityverifiier.ConnectivityVerifier;
import rice.pastry.socket.nat.connectivityverifiier.ConnectivityVerifierImpl;
import rice.pastry.socket.nat.rendezvous.RendezvousSocketPastryNodeFactory;
import rice.selector.TimerTask;

/**
 * This Factory is designed for real deployments (with NATs).
 *
 * Optimized for the following use cases (in order)
 * 1) Internet Routable, No firewall
 * 2) Internet Routable, Firewall (not NAT)
 * 3) NAT: User Configured Port Forwarding
 * 4) NAT: UPnP (only with a NATHandler supplied via nat_handler_class; the default is StubNATHandler)
 * 5) NAT: No port forwarding
 *
 * @author Jeff Hoye
 *
 */
public class InternetPastryNodeFactory extends
    RendezvousSocketPastryNodeFactory {
  /** How long the connectivity check may take before the node starts without it. */
  private static final int CONNECTIVITY_TIMEOUT_MILLIS = 10000;
  /** Delay after a timed-out connectivity check, so cancelling it can release the bind address. */
  private static final int RETRY_AFTER_TIMEOUT_DELAY_MILLIS = 1000;

  /**
   * NAT policy variables
   */
  public static final int ALWAYS = 1;
  public static final int PREFIX_MATCH = 2;
  public static final int NEVER = 3;
  /**
   * Don't check bootstrap nodes
   */
  public static final int BOOT = 4;
  public static final int OVERWRITE = 1;
  public static final int USE_DIFFERENT_PORT = 2;
  public static final int FAIL = 3;
  public static final int RENDEZVOUS = 4;

  NATHandler natHandler;

  ConnectivityVerifier connectivityVerifier;
  Collection<InetSocketAddress> probeAddresses;

  /**
   * The NAT and firewall policies, read at construction.
   */
  protected NatConfig natConfig;

  /**
   * The ordered addresses of the nat propagation from most external to most internal
   *
   * Null if localAddress is Internet routable
   * 1 value per NAT
   */
  InetAddress[] externalAddresses;

  /**
   * May block for more than a second to determine network information.
   *
   * @param nf can be null, but must call newNode() with a NodeId of the new PastryNode
   * @param bindAddress the NIC to use (null will choose one that can access the Internet)
   * @param startPort the port of the first created node, will be incremented for additional nodes,
   *   can be specified on a per-Node basis by calling newNode() with a MultiInetSocketAddress
   * @param env can't be null
   * @param handler if null, uses the class named by the param "nat_handler_class", or a
   *   StubNATHandler if that param is not set
   * @param probeAddresses a list of bootstrap nodes' Internet routable addresses, used to establish
   *   firewall information
   * @param externalAddresses ordered addresses of the nat propagation from most external to most internal,
   *   null will use natHandler and probeAddresses to determine this
   */
  public InternetPastryNodeFactory(NodeIdFactory nf, InetAddress bindAddress,
      int startPort, Environment env, NATHandler handler, Collection<InetSocketAddress> probeAddresses, InetAddress[] externalAddresses)
      throws IOException {
    super(nf, bindAddress, startPort, env, false);

    natConfig = NatConfig.fromParameters(env.getParameters());
    // get a natHandler
    this.natHandler = handler;
    if (natHandler == null) {
      this.natHandler = getDefaultNatHandler(env, this.getBindAddress());
    }

    this.probeAddresses = probeAddresses;

    this.externalAddresses = externalAddresses;

    if (socketConfig.hasExternalAddress()) {
      externalAddresses = new InetAddress[1];
      externalAddresses[0] = socketConfig.externalAddress().getAddress();
    }

    this.connectivityVerifier = new ConnectivityVerifierImpl(this);

    // sets/verifies externalAddress
    findExternalAddressIfNecessary(this.getBindAddress()); // blocking call
  }

  /**
   * Return a NATHandler
   *
   * @param localAddress the address of the interface we should search for a NAT on
   */
  protected NATHandler getDefaultNatHandler(Environment env, InetAddress localAddress) {
    if (natConfig.natHandlerClass() != null) {
      String natHandlerClassName = natConfig.natHandlerClass();
      try {
        Class<?> loaded = Class.forName(natHandlerClassName);
        if (!NATHandler.class.isAssignableFrom(loaded)) {
          throw new IllegalArgumentException("nat_handler_class " + natHandlerClassName + " does not implement "
              + NATHandler.class.getName());
        }
        Class<? extends NATHandler> natHandlerClass = loaded.asSubclass(NATHandler.class);
        Constructor<? extends NATHandler> constructor = natHandlerClass.getConstructor(Environment.class, InetAddress.class);
        return constructor.newInstance(env, localAddress);
      } catch (ClassNotFoundException e) {
        logger.info("Didn't find UPnP libs, skipping UPnP");
        return new StubNATHandler(env, localAddress);
      } catch (NoClassDefFoundError e) {
        logger.info("Didn't find UPnP libs, skipping UPnP");
        return new StubNATHandler(env, localAddress);
      } catch (InvocationTargetException e) {
        logger.info("Didn't find UPnP libs, skipping UPnP");
        return new StubNATHandler(env, localAddress);
      } catch (ReflectiveOperationException e) {
        logger.warn("Error constructing NATHandler.", e);
        throw new RuntimeException(e);
      }
    } else {
      return new StubNATHandler(env, localAddress);
    }
  }

  protected boolean shouldFindExternalAddress(InetAddress address) {
    switch (getFireWallPolicyVariable(natConfig.natSearchPolicy())) {
    case NEVER:
      return false;
    case PREFIX_MATCH:
      return !isInternetRoutablePrefix(address);
    case ALWAYS:
      return true;
    }
    return true;
  }

  /**
   * Sets/Verifies externalAddresses
   *
   * Return true if all is well.
   * Return false if a firewall should be found but couldn't.
   *
   * Throws an exception if firewall was found and disagrees with the existing externalAddress (if it's not null).
   *
   */
  protected boolean findExternalAddressIfNecessary(InetAddress address /*, Collection<InetSocketAddress> probeAddresses*/) throws IOException {
    if (!shouldFindExternalAddress(address)) return true;

    try {
      natHandler.findFireWall(address); // warning, this is blocking...
    } catch (CantFindFirewallException cffe) {
      logger.info("Can't find firewall, continuing. For better performance, enable UPnP.  Will try to verify if user configured a port forward rule..." + cffe);
      // ignore
      return false;
    } catch (IOException ioe) {
      logger.atWarn().log(() -> ioe.toString());
      // ignore
      return false;
    }
    if (this.externalAddresses == null) {
      this.externalAddresses = new InetAddress[1];
      this.externalAddresses[0] = natHandler.getFireWallExternalAddress();
      if (this.externalAddresses[0] == null) {
        // couldn't find firewall
        return false;
      } else {
        // all is well
        return true;
      }
    } else {
      if (externalAddresses[0].equals(natHandler.getFireWallExternalAddress())) {
        // the firewall says the same thing as our external address list
        return true;
      } else {
        // the firewall disagrees with the existing external address list
        throw new IOException("Firewall disagrees with the externalAddresses. externalAddresses:"+externalAddresses[0]+" firewall:"+natHandler.getFireWallExternalAddress());
      }
    }
  }

  /**
   * This is where the action takes place.
   *
   * 1) Make sure the proxyAddress is valid and good
   *   a) add the external address/port if needed
   *
   * 2) Try to configure the firewall
   *
   * 3) call newNodeSelectorHelper
   *
   */
  @Override
  protected void newNodeSelector(final Id nodeId,
      final MultiInetSocketAddress proxyAddress,
      final Continuation<PastryNode, IOException> deliverResultToMe,
      Map<String, Object> initialVars) {

    // make sure the innermost address is valid
    if (!proxyAddress.getInnermostAddress().getAddress().equals(this.getBindAddress())) {
      throw new RuntimeException("proxyAddress.innermostAddress() must be the local bind address. proxyAddress:"+proxyAddress+" bindAddress:"+this.getBindAddress());
    }

    // may be running on the LAN, don't bother with anything
    if (!shouldFindExternalAddress(proxyAddress.getInnermostAddress().getAddress())) {
      verifyConnectivityThenMakeNewNode(nodeId, proxyAddress, deliverResultToMe);
      return;
    }

    // we know we are in a NAT type situation now
    if (proxyAddress.getNumAddresses() > 2) {
      throw new RuntimeException("this factory only supports 1 layer deep NAT configurations try setting nat_search_policy = never if you are sure that your NAT configuration is "+proxyAddress);
    }

    // we know that there are 1 or 2 addresses, and that the first is the localAddress
    if (proxyAddress.getNumAddresses() == 1) {
      // determine the pAddress
      findExternalAddress(nodeId, proxyAddress.getInnermostAddress(), deliverResultToMe);
    } else {
      openFirewallPort(nodeId, proxyAddress.getInnermostAddress(), deliverResultToMe, proxyAddress.getOutermostAddress().getAddress(), proxyAddress.getOutermostAddress().getPort());
    }
  }
  // this needs to ask natted nodes for known non-natted nodes, then call findExternalAddress again,

  /**
   * Finds the external address, calls openFirewallPort()
   */
  protected void findExternalAddress(final Id nodeId,
      final InetSocketAddress bindAddress,
      final Continuation<PastryNode, IOException> deliverResultToMe) {

    // see if it's specified in the configuration
    if (socketConfig.hasExternalAddress()) {
      // get it from the param
      InetSocketAddress pAddress = socketConfig.externalAddress();
      openFirewallPort(nodeId, bindAddress, deliverResultToMe, pAddress.getAddress(), pAddress.getPort());
    } else {
      // pull self from probeAddresses
      Collection<InetSocketAddress> myProbeAddresses = null;
      Collection<InetSocketAddress> nonInternetRoutable = null;
      if (this.probeAddresses != null) {
        myProbeAddresses = new ArrayList<InetSocketAddress>(probeAddresses);
        nonInternetRoutable = new ArrayList<InetSocketAddress>();
        while(myProbeAddresses.remove(bindAddress));

        // pull non-internet routable addresses
        Iterator<InetSocketAddress> i = myProbeAddresses.iterator();
        while (i.hasNext()) {
          InetSocketAddress foo = i.next();
          if (!isInternetRoutablePrefix(foo.getAddress())) {
            nonInternetRoutable.add(foo);
            i.remove();
          }
        }
      }

      if ((myProbeAddresses == null || myProbeAddresses.isEmpty()) && (nonInternetRoutable != null && !nonInternetRoutable.isEmpty())) {
        findExternalNodes(nodeId, bindAddress, nonInternetRoutable, deliverResultToMe);
      } else {
        findExternalAddressHelper(nodeId,bindAddress,deliverResultToMe, myProbeAddresses);
      }
    }
  }

  /**
   * Probe the internalAddresses to get more externalAddresses, then call findExternalAddressHelper
   */
  protected void findExternalNodes(final Id nodeId,
      final InetSocketAddress bindAddress,
      final Collection<InetSocketAddress> nonInternetRoutable,
      final Continuation<PastryNode, IOException> deliverResultToMe) {
    if (nonInternetRoutable == null || nonInternetRoutable.isEmpty()) findExternalAddressHelper(nodeId, bindAddress, deliverResultToMe, null);

    connectivityVerifier.findExternalNodes(bindAddress, nonInternetRoutable, new Continuation<Collection<InetSocketAddress>, IOException>() {

      @Override
      public void receiveResult(Collection<InetSocketAddress> result) {
        findExternalAddressHelper(nodeId, bindAddress, deliverResultToMe, result);
      }

      @Override
      public void receiveException(IOException exception) {
        if (nonInternetRoutable == null || nonInternetRoutable.isEmpty()) findExternalAddressHelper(nodeId, bindAddress, deliverResultToMe, null);
      }
    });
  }

  protected void findExternalAddressHelper(final Id nodeId,
      final InetSocketAddress bindAddress,
      final Continuation<PastryNode, IOException> deliverResultToMe,
      Collection<InetSocketAddress> myProbeAddresses) {
    // try the probeAddresses
    if (myProbeAddresses != null && !myProbeAddresses.isEmpty()) {
      connectivityVerifier.findExternalAddress(bindAddress, myProbeAddresses, new Continuation<InetAddress, IOException>() {

        @Override
        public void receiveResult(InetAddress result) {
          if (externalAddresses != null) {
            if (!externalAddresses[0].equals(result)) {
              deliverResultToMe.receiveException(new IOException("Probe address ("+result+") does not match specified externalAddress ("+externalAddresses[0]+")."));
              return;
            }
          }
          openFirewallPort(nodeId, bindAddress, deliverResultToMe, result, -1);
        }

        @Override
        public void receiveException(IOException exception) {
          deliverResultToMe.receiveException(exception);
        }
      });
    } else {
      // try the firewall
      openFirewallPort(nodeId, bindAddress, deliverResultToMe, natHandler.getFireWallExternalAddress(), -1);
    }
  }


  /**
   * Attempt to open the firewall on the specified port
   * if it doesn't work, uses Rendezvous
   *
   * @param requestedPort the external firewall port to (attempt to) use, -1 to use anything
   */
  protected void openFirewallPort(final Id nodeId,
      final InetSocketAddress bindAddress,
      final Continuation<PastryNode, IOException> deliverResultToMe,
      InetAddress externalAddress, int requestedPort) {
    int firewallSearchTries = socketConfig.findPortMaxTries();
    String firewallAppName = socketConfig.natAppName();

    int port;
    if (requestedPort == -1) {
      port = bindAddress.getPort();
    } else {
      port = requestedPort;
    }

    /**
     * Set this to true to just give up and use Rendezvous
     */
    boolean rendezvous = false;
    try {
      // if we can talk to the firewall at all
      if (natHandler.getFireWallExternalAddress() == null) {
        rendezvous = true;
      } else {
        int availableFireWallPort = natHandler.findAvailableFireWallPort(bindAddress.getPort(),
            port, firewallSearchTries, firewallAppName);

        if (requestedPort == -1 || availableFireWallPort == port) {
          // success
          port = availableFireWallPort;
        } else {
          // decide how to handle this
          switch (getFireWallPolicyVariable(natConfig.natStatePolicy())) {
            case OVERWRITE:
              break;
            case FAIL:
              // todo: would be useful to pass the app that is bound to that
              // port
              deliverResultToMe.receiveException(new BindException(
                  "Firewall is already bound to the requested port:"
                      + externalAddress+":"+port));
              return;
            case RENDEZVOUS:
              rendezvous = true;
              break;
            case USE_DIFFERENT_PORT:
              port = availableFireWallPort;
              break;
          }
        }
      }
      if (rendezvous) {
        // this could go either way... since the connectivity check will fail, it'll show up natted, but it used to be zero
        port = bindAddress.getPort();
      } else {
        natHandler.openFireWallPort(bindAddress.getPort(), port, firewallAppName);
      }
    } catch (IOException ioe) {
      // doesn't matter, can just rendezvous
      port = 0;
    }

    // if we found an externalAddress under any mechanism, use it, otherwise dont.
    MultiInetSocketAddress fullAddress;
    if (externalAddress == null) {
      fullAddress = new MultiInetSocketAddress(bindAddress);
    } else {
      fullAddress = new MultiInetSocketAddress(new InetSocketAddress(externalAddress, port), bindAddress);
    }
    verifyConnectivityThenMakeNewNode(nodeId, fullAddress, deliverResultToMe);
  }


  /**
   * Verifies the connectivity (if necessary), then calls super.newNodeSelector()
   *
   * if connectivity fails, then uses Rendezvous
   *
   */
  protected void verifyConnectivityThenMakeNewNode(final Id nodeId,
      final MultiInetSocketAddress proxyAddress,
      final Continuation<PastryNode, IOException> deliverResultToMe) {

    if (proxyAddress.getOutermostAddress().getPort()<1) {
      newNodeSelector(nodeId, proxyAddress, deliverResultToMe, null, true);
      return;
    }

    if (!shouldCheckConnectivity(proxyAddress, probeAddresses)) {
      newNodeSelector(nodeId, proxyAddress, deliverResultToMe, null, false);
      return;
    }

    final boolean[] timeout = new boolean[1];
    timeout[0] = false;
    final Cancellable[] cancelme = new Cancellable[1];

    final TimerTask timer = new TimerTask() {
      @Override
      public void run() {
        timeout[0] = true;

        // clear up the bind address
        cancelme[0].cancel();

        // invoke to let the cancel succeed, seems to need to take a second sometimes
        environment.getSelectorManager().schedule(new TimerTask() {
          @Override
          public void run() {
            newNodeSelector(nodeId, proxyAddress, deliverResultToMe, null, true);
          }
        }, RETRY_AFTER_TIMEOUT_DELAY_MILLIS);
      }
    };
    environment.getSelectorManager().getTimer().schedule(timer, CONNECTIVITY_TIMEOUT_MILLIS);

    cancelme[0] = connectivityVerifier.verifyConnectivity(proxyAddress, probeAddresses, new ConnectivityResult() {
      boolean udpSuccess = false;
      boolean tcpSuccess = false;

      @Override
      public void udpSuccess(InetSocketAddress from, Map<String, Object> options) {
        udpSuccess = true;
        complete();
      }

      @Override
      public void tcpSuccess(InetSocketAddress from, Map<String, Object> options) {
        tcpSuccess = true;
        complete();
      }

      void complete() {
        if (tcpSuccess && udpSuccess && !timeout[0]) {
          timer.cancel();
          newNodeSelector(nodeId, proxyAddress, deliverResultToMe, null, false);
        }
      }

      @Override
      public void receiveException(Exception e) {
        timer.cancel();
        if (e instanceof CantVerifyConnectivityException) {
          // mark node firewalled if internal address matches the prefix, otherwise not firewalled
          if (shouldFindExternalAddress(proxyAddress.getInnermostAddress().getAddress())) {
            newNodeSelector(nodeId, proxyAddress, deliverResultToMe, null, true);
          } else {
            newNodeSelector(nodeId, proxyAddress, deliverResultToMe, null, false);
          }
        } else {
          newNodeSelector(nodeId, proxyAddress, deliverResultToMe, null, true);
        }
      }
    });
  }

  protected boolean isInternetRoutable(MultiInetSocketAddress proxyAddress) {
    if (proxyAddress.getNumAddresses() == 1) {
      InetSocketAddress address = proxyAddress.getInnermostAddress();
      if (isInternetRoutablePrefix(address.getAddress())) {
        return true;
      }
    }
    return false;
  }


  /**
   * @param val a policy parameter value, see {@link NatConfig}
   */
  protected int getFireWallPolicyVariable(String val) {
    if (val.equalsIgnoreCase("prefix"))
      return PREFIX_MATCH;
    if (val.equalsIgnoreCase("change"))
      return USE_DIFFERENT_PORT;
    if (val.equalsIgnoreCase("never"))
      return NEVER;
    if (val.equalsIgnoreCase("overwrite"))
      return OVERWRITE;
    if (val.equalsIgnoreCase("always"))
      return ALWAYS;
    if (val.equalsIgnoreCase("boot"))
      return BOOT;
    if (val.equalsIgnoreCase("fail"))
      return FAIL;
    if (val.equalsIgnoreCase("rendezvous"))
      return RENDEZVOUS;
    throw new RuntimeException("Unknown firewall policy value " + val);
  }

  protected boolean shouldCheckConnectivity(MultiInetSocketAddress proxyAddress, Collection<InetSocketAddress> bootstraps) {
    if (bootstraps == null) return false;
    switch (getFireWallPolicyVariable(natConfig.firewallTestPolicy())) {
    case NEVER:
      return false;
    case BOOT:
      // don't do it if we're the bootstrap node
      if (!bootstraps.contains(proxyAddress.getOutermostAddress())) return true;
      // continue to PREFIX_MATCH
    case PREFIX_MATCH:
      return !isInternetRoutable(proxyAddress);
    case ALWAYS:
      return true;
    } // switch
    return true; // will probably never happen
  }

}
