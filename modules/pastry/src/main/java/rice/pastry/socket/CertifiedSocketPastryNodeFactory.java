package rice.pastry.socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.identity.BindStrategy;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.mpisws.p2p.transport.ssl.SSLTransportLayerImpl;
import rice.environment.Environment;
import rice.pastry.Id;
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
 * The node also trusts only authenticated ids. The routing table and the leaf set admit a handle only after a TLS
 * certificate for its id was seen, and application messages sent directly to this node are delivered only from
 * an authenticated sender. An id learned over the wire is first verified by opening a TLS connection to it (the
 * handle is admitted when that succeeds). Direct application messages from that id wait meanwhile in a small bounded
 * queue ({@link rice.pastry.DeferredDeliveries}): they are delivered in order once the id is verified and dropped if
 * the check fails or times out.
 * Routing towards an id, and the originator named in a routed message, are not covered: see {@code README.md}.
 */
public class CertifiedSocketPastryNodeFactory extends SocketPastryNodeFactory {

  private final KeyStore store;
  private final CertifiedIdBindStrategy bindStrategy = new CertifiedIdBindStrategy();

  /** ids with a verification connection under way, so one is opened at a time per id (selector thread only) */
  private final Set<Id> verifying = new HashSet<>();

  /**
   * @param store a PKCS12 key store as written by {@code CAToolImpl -store}: the node's key and certificate, and the
   *     CA certificate as the trust anchor; it is both the key store and the trust store of the TLS layer
   * @param keyPassword the password of the node's key (blank for {@code CAToolImpl -store})
   * @throws GeneralSecurityException if the certificate is not valid for the CA now, or does not belong to the node's key
   */
  public CertifiedSocketPastryNodeFactory(KeyStore store, char[] keyPassword, int startPort, Environment env) throws IOException, GeneralSecurityException {
    super(CertifiedNodeIdFactory.fromKeyStore(store, keyPassword, env), startPort, env);
    this.store = store;
  }

  /** @return the bind strategy, which tells which ids are authenticated */
  public CertifiedIdBindStrategy getCertifiedIdBindStrategy() {
    return bindStrategy;
  }

  @Override
  protected TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> getSourceRouteTransportLayer(
      TransportLayer<MultiInetSocketAddress, ByteBuffer> etl, PastryNode pn, MultiAddressSourceRouteFactory esrFactory) {
    try {
      return new SSLTransportLayerImpl<SourceRoute<MultiInetSocketAddress>, ByteBuffer>(
          super.getSourceRouteTransportLayer(etl, pn, esrFactory), store, store,
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

  @Override
  public PastryNode nodeHandleHelper(PastryNode pn) throws IOException {
    PastryNode ret = super.nodeHandleHelper(pn);
    ret.setIdentityPolicy(handle -> isAuthenticatedOrVerify(pn, handle));
    return ret;
  }

  /** @return true if the handle's id is authenticated; otherwise starts verifying it and returns false */
  private boolean isAuthenticatedOrVerify(PastryNode pn, NodeHandle handle) {
    Id id = handle.getNodeId();
    if (id.equals(pn.getNodeId()) || bindStrategy.isAuthenticated(id)) {
      return true;
    }
    if (verifying.add(id)) {
      pn.probe(handle, verified -> {
        verifying.remove(id);
        boolean authenticated = verified.booleanValue() && bindStrategy.isAuthenticated(id);
        if (authenticated) {
          // the admission that was deferred; each gate asks again and now says yes
          pn.getLeafSet().put(handle);
          pn.getRoutingTable().put(handle);
        }
        pn.identityResolved(id, authenticated);
      });
    }
    return false;
  }
}
