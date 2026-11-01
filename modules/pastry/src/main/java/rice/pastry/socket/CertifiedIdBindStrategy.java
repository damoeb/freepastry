package rice.pastry.socket;

import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.identity.BindStrategy;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.pastry.standard.NodeIdCertificate;

/**
 * Accepts a node handle only if its id is the one the peer's TLS certificate is bound to: the id must
 * equal the certificate's common name, which {@code SSLTransportLayerImpl} reports in the socket options
 * as {@link SSLTransportLayer#OPTION_CERT_SUBJECT}. A peer that claims another id than its certificate
 * grants is refused (the binding is not made) and a warning is logged. Return it from
 * {@code SocketPastryNodeFactory.getBindStrategy()} when the factory inserts the SSL layer; see
 * {@link NodeIdCertificate} for the whole design.
 * <p>
 * Options without a certificate subject (UDP datagrams, which TLS does not cover) are accepted: this
 * strategy authenticates TCP connections only. Use it with {@code SSLTransportLayer.CLIENT_AUTH_REQUIRED},
 * otherwise connecting peers carry no certificate at all.
 */
public class CertifiedIdBindStrategy implements BindStrategy<TransportLayerNodeHandle<MultiInetSocketAddress>, SourceRoute<MultiInetSocketAddress>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CertifiedIdBindStrategy.class);

  @Override
  public boolean accept(TransportLayerNodeHandle<MultiInetSocketAddress> u, @Nullable SourceRoute<MultiInetSocketAddress> l, @Nullable Map<String, Object> options) {
    Object subject = options == null ? null : options.get(SSLTransportLayer.OPTION_CERT_SUBJECT);
    if (subject == null) {
      return true;
    }
    if (NodeIdCertificate.matches(u.getNodeId(), subject)) {
      return true;
    }
    LOGGER.warn("Rejecting node {} which claims an id that does not match its certificate subject {}", u, subject);
    return false;
  }
}
