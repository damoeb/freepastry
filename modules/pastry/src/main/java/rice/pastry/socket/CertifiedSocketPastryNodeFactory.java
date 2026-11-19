package rice.pastry.socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.commonapi.MessageOptionsDecorator;
import org.mpisws.p2p.transport.identity.BindStrategy;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import rice.environment.Environment;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.standard.CertifiedNodeIdFactory;
import rice.pastry.standard.NodeIdCertificate;

/**
 * A {@link SocketPastryNodeFactory} for rings that bind node ids to certificates, strict by default. The node
 * id is the one the CA put in the node's certificate ({@link CertifiedNodeIdFactory}); the TCP connections run
 * over mutual TLS with client authentication required, and a handshake with a peer whose certificate common name
 * is not a node id fails ({@link NodeIdCertificate#isNodeId}); a {@link CertifiedIdBindStrategy} refuses a handle
 * whose id differs from the certificate.
 * <p>
 * The node also trusts only authenticated handles. The routing table and the leaf set admit a handle (an id at an
 * address and epoch) only after a TLS connection to or from that very handle presented a certificate for its id
 * ({@link CertifiedIdBindStrategy#isAuthenticated(TransportLayerNodeHandle)}): that the id was authenticated at
 * another address is not enough, since any node can advertise another node's id at an address of its choice. A
 * handle learned over the wire is first verified by opening a TLS connection to its address (the handle is admitted
 * when that succeeds). Routed application messages from that handle wait meanwhile in a small bounded queue
 * ({@link rice.pastry.DeferredDeliveries}): they are delivered in order once the handle is verified and dropped if
 * the check fails or times out. These verification connections go to addresses that others name, so they are
 * limited: one at a time per address, none to an address whose check failed less than
 * {@link #PROBE_RETRY_DELAY_MILLIS} ago, and at most {@link #MAX_PROBES_IN_FLIGHT} at once. A handle that is not
 * checked for that reason is not admitted; the protocols offer it again later.
 * Datagrams carry no certificate: a received one never reaches Pastry or an application, and a message that asks
 * to be sent as a datagram goes over TLS instead. Only the layers below the identity layer (liveness pings and
 * pongs, network-info probes) still use UDP.
 * Routing towards an id, and the originator named in a routed message, are not covered: see {@code README.md}.
 */
public class CertifiedSocketPastryNodeFactory extends SocketPastryNodeFactory {

  private final KeyStore store;
  private final char[] keyPassword;
  /** verification connections under way at most, to all addresses together */
  public static final int MAX_PROBES_IN_FLIGHT = 32;

  /** how long no verification connection is opened to an address after one to it failed */
  public static final long PROBE_RETRY_DELAY_MILLIS = 5_000;

  /** addresses whose failed check is remembered */
  private static final int MAX_FAILED_ADDRESSES = 1024;

  private final CertifiedIdBindStrategy bindStrategy = new CertifiedIdBindStrategy();

  /** addresses with a verification connection under way, so one is opened at a time per address (selector thread only) */
  private final Set<Object> probedAddresses = new HashSet<>();

  /** when the last verification connection to an address failed (selector thread only) */
  private final Map<Object, Long> failedProbeMillis = new LinkedHashMap<>(16, 0.75f, true) {
    @Override
    protected boolean removeEldestEntry(Map.Entry<Object, Long> eldest) {
      return size() > MAX_FAILED_ADDRESSES;
    }
  };

  /**
   * @param store a PKCS12 key store as written by {@code CertificateAuthorityTool -store}: the node's key and certificate, and the
   *     CA certificate as the trust anchor; it is both the key store and the trust store of the TLS layer
   * @param keyPassword the password of the node's key (also handed to the TLS layer)
   * @throws GeneralSecurityException if the certificate is not valid for the CA now, or does not belong to the node's key
   */
  public CertifiedSocketPastryNodeFactory(KeyStore store, char[] keyPassword, int startPort, Environment env) throws IOException, GeneralSecurityException {
    super(CertifiedNodeIdFactory.fromKeyStore(store, keyPassword, env), startPort, env);
    this.store = store;
    this.keyPassword = keyPassword.clone();
  }

  /**
   * @return the trust store of the TLS layer for a node store: it holds the CA certificate only, so that no node
   *     certificate (not even this node's own) is a trust anchor
   */
  public static KeyStore trustStoreOf(KeyStore store) {
    return store;
  }

  /** @return the bind strategy, which tells which ids are authenticated */
  public CertifiedIdBindStrategy getCertifiedIdBindStrategy() {
    return bindStrategy;
  }

  @Override
  protected TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> getSourceRouteTransportLayer(
      TransportLayer<MultiInetSocketAddress, ByteBuffer> etl, PastryNode pn, MultiAddressSourceRouteFactory esrFactory) {
    try {
      return new SSLTransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer>(
          super.getSourceRouteTransportLayer(etl, pn, esrFactory), store, keyPassword, store,
          SSLTransportLayer.CLIENT_AUTH_REQUIRED, getSubjectValidator(), pn.getEnvironment());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** @return the test a peer certificate's common name must pass: it must be a node id */
  public Predicate<String> getSubjectValidator() {
    return NodeIdCertificate::isNodeId;
  }

  @Override
  protected BindStrategy<TransportLayerNodeHandle<MultiInetSocketAddress>, SourceRoute<MultiInetSocketAddress>> getBindStrategy() {
    return bindStrategy;
  }

  /**
   * Sends every message from Pastry and its applications over TLS, also one that asks for a datagram
   * ({@link WireTransportLayer#OPTION_TRANSPORT_TYPE}): a datagram carries no certificate, so the receiving node
   * would drop it ({@link CertifiedIdBindStrategy#acceptsMessage}).
   */
  @Override
  protected MessageOptionsDecorator getOptionsAdder(PastryNode pn) {
    MessageOptionsDecorator adder = super.getOptionsAdder(pn);
    return (options, message) -> OptionsFactory.removeOption(adder.addOptions(options, message), WireTransportLayer.OPTION_TRANSPORT_TYPE);
  }

  @Override
  public PastryNode nodeHandleHelper(PastryNode pn) throws IOException {
    PastryNode result = super.nodeHandleHelper(pn);
    result.setIdentityPolicy(handle -> isAuthenticatedOrVerify(pn, handle));
    return result;
  }

  /**
   * @return true if the handle presented its certificate over TLS; otherwise starts verifying it, unless the limits
   *     on verification connections say no, and returns false
   */
  private boolean isAuthenticatedOrVerify(PastryNode pn, NodeHandle handle) {
    if (handle.getNodeId().equals(pn.getNodeId())) {
      // only this node holds the certificate for its id
      return handle.equals(pn.getLocalHandle());
    }
    if (!(handle instanceof TransportLayerNodeHandle<?> transportHandle)) {
      return false;
    }
    if (bindStrategy.isAuthenticated(transportHandle)) {
      return true;
    }
    Object address = transportHandle.getAddress();
    long nowMillis = pn.getEnvironment().getTimeSource().currentTimeMillis();
    Long failedMillis = failedProbeMillis.get(address);
    if (probedAddresses.contains(address) || probedAddresses.size() >= MAX_PROBES_IN_FLIGHT
        || (failedMillis != null && nowMillis - failedMillis < PROBE_RETRY_DELAY_MILLIS)) {
      return false;
    }
    probedAddresses.add(address);
    pn.probe(handle, verified -> {
      probedAddresses.remove(address);
      boolean authenticated = verified.booleanValue() && bindStrategy.isAuthenticated(transportHandle);
      if (authenticated) {
        failedProbeMillis.remove(address);
        // the admission that was deferred; each gate asks again and now says yes
        pn.getLeafSet().put(handle);
        pn.getRoutingTable().put(handle);
      } else {
        failedProbeMillis.put(address, pn.getEnvironment().getTimeSource().currentTimeMillis());
      }
      pn.identityResolved(handle, authenticated);
    });
    return false;
  }
}
