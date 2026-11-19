package rice.pastry.socket;

import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.identity.BindStrategy;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.pastry.Id;
import rice.pastry.standard.NodeIdCertificate;

/**
 * Binds a node handle to its node id only as far as a TLS certificate vouches for it. The certificate's
 * common name is reported by {@code SSLTransportLayer} in the socket options as
 * {@link SSLTransportLayer#OPTION_CERT_SUBJECT}. Every id is in one of three states:
 * <ul>
 * <li><b>unverified</b> (pending): the id was only learned over the wire (a handle from routing state, or a
 * datagram, neither carries a certificate). The handle is accepted, so a node can route towards it and connect
 * to it, but it is not an authenticated identity: {@link #isAuthenticated} is false.</li>
 * <li><b>authenticated</b>: a TLS connection presented a certificate whose common name equals the id. This
 * promotes a pending id.</li>
 * <li><b>refused</b>: a connection presented a certificate whose common name differs from the id the peer
 * claims. The binding is not made, a warning is logged and the id loses its pending state. An authenticated id
 * stays authenticated, as the certificate proved it.</li>
 * </ul>
 * Received messages are passed up only from the channel that authenticates their sender ({@link #acceptsMessage}):
 * a TLS connection whose certificate is the sender's. Datagrams never are. And only a certificate changes the
 * binding of an authenticated id ({@link #replace}).
 * Return it from {@code SocketPastryNodeFactory.getBindStrategy()} when the factory inserts the SSL layer, with
 * {@code SSLTransportLayer.CLIENT_AUTH_REQUIRED} (otherwise connecting peers carry no certificate) and a
 * subject validator that only lets node ids pass ({@link NodeIdCertificate#isNodeId}); see
 * {@link NodeIdCertificate} for the whole design. Both states are bounded, the least recently used id is
 * forgotten first.
 */
public class CertifiedIdBindStrategy implements BindStrategy<TransportLayerNodeHandle<MultiInetSocketAddress>, SourceRoute<MultiInetSocketAddress>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CertifiedIdBindStrategy.class);

  /** ids remembered per state */
  private static final int MAX_TRACKED_IDS = 4096;

  private final Map<Id, Boolean> authenticated = lru();
  private final Map<Id, Boolean> pending = lru();

  private static Map<Id, Boolean> lru() {
    return new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<Id, Boolean> eldest) {
        return size() > MAX_TRACKED_IDS;
      }
    };
  }

  @Override
  public synchronized boolean accept(TransportLayerNodeHandle<MultiInetSocketAddress> u, @Nullable SourceRoute<MultiInetSocketAddress> l, @Nullable Map<String, Object> options) {
    Id id = u.getNodeId();
    Object subject = options == null ? null : options.get(SSLTransportLayer.OPTION_CERT_SUBJECT);
    if (subject == null) {
      if (!authenticated.containsKey(id)) {
        pending.put(id, Boolean.TRUE);
      }
      return true;
    }
    if (NodeIdCertificate.matches(id, subject)) {
      pending.remove(id);
      authenticated.put(id, Boolean.TRUE);
      return true;
    }
    pending.remove(id);
    LOGGER.warn("Rejecting node {} which claims an id that does not match its certificate subject {}", u, subject);
    return false;
  }

  /**
   * Passes a received message up only if it came over a TLS connection whose certificate is the sender's: the
   * options must carry a certificate subject that matches the sender's id. A datagram has no certificate (TLS
   * covers the sockets only), so no datagram goes up to Pastry or its applications, whatever sender it names; the
   * liveness layer, which is below the identity layer's upper half, still gets its pings and pongs.
   */
  @Override
  public boolean acceptsMessage(TransportLayerNodeHandle<MultiInetSocketAddress> from, @Nullable Map<String, Object> options) {
    return certifies(from, options);
  }

  /**
   * An authenticated node's binding is changed only by a certificate. The identity layer binds each address to
   * one handle, and a handle with the same address and a newer epoch used to replace it and mark the old one dead
   * for good. Without a certificate (a datagram's header, a handle deserialized from a message) that is anyone's
   * claim, so:
   * <ul>
   * <li>offered without a certificate, a handle never replaces one whose id is authenticated
   * ({@link Replacement#KEEP});</li>
   * <li>offered with its certificate, it replaces one whose id is not authenticated, whatever their epochs (an
   * unverified handle cannot block the certified one);</li>
   * <li>otherwise the epochs decide, as without certificates.</li>
   * </ul>
   */
  @Override
  public Replacement replace(TransportLayerNodeHandle<MultiInetSocketAddress> old, TransportLayerNodeHandle<MultiInetSocketAddress> u, @Nullable Map<String, Object> options) {
    boolean oldAuthenticated = isAuthenticated(old.getNodeId());
    if (!certifies(u, options)) {
      return oldAuthenticated ? Replacement.KEEP : Replacement.ASK_NODE_CHANGE_STRATEGY;
    }
    return oldAuthenticated ? Replacement.ASK_NODE_CHANGE_STRATEGY : Replacement.REPLACE;
  }

  /**
   * A binding confirmed by the node's certificate is never forgotten to make room for others: forgetting it would let
   * a handle without a certificate (a datagram's header) take the address, which {@link #replace} prevents.
   */
  @Override
  public boolean keepsBinding(TransportLayerNodeHandle<MultiInetSocketAddress> u, @Nullable Map<String, Object> options) {
    return certifies(u, options);
  }

  /** @return true if the options carry a certificate subject that is this handle's id */
  private static boolean certifies(TransportLayerNodeHandle<MultiInetSocketAddress> handle, @Nullable Map<String, Object> options) {
    Object subject = options == null ? null : options.get(SSLTransportLayer.OPTION_CERT_SUBJECT);
    return subject != null && NodeIdCertificate.matches(handle.getNodeId(), subject);
  }

  /** @return true if a TLS certificate whose common name is this id has been presented */
  public synchronized boolean isAuthenticated(Id id) {
    return authenticated.containsKey(id);
  }

  /** @return true if the id was learned without a certificate and no matching certificate has been seen yet */
  public synchronized boolean isPending(Id id) {
    return pending.containsKey(id);
  }
}
