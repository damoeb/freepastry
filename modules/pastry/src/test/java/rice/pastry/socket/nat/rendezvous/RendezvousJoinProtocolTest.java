package rice.pastry.socket.nat.rendezvous;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.rendezvous.IncomingPilotListener;
import org.mpisws.p2p.transport.rendezvous.OutgoingPilotListener;
import org.mpisws.p2p.transport.rendezvous.PilotManager;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.PastryNode;
import rice.pastry.join.JoinRequest;

/**
 * How a NATted node builds its join request: it opens a pilot to a bootstrap it can reach, and fails clearly when the
 * bootstrap is firewalled and it cannot reach it. The node is a firewalled rendezvous socket node that is never booted.
 */
class RendezvousJoinProtocolTest {
  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
  /** Documentation addresses (RFC 5737, RFC 1918): never contacted, the pilot manager below is a fake. */
  private static final InetSocketAddress BOOTSTRAP_EXTERNAL = new InetSocketAddress("203.0.113.7", 9001);
  private static final InetSocketAddress BOOTSTRAP_INTERNAL = new InetSocketAddress("10.11.12.13", 9001);

  /** Opens pilots the way the rendezvous layer does: only to a node that can be contacted directly. */
  private static class RecordingPilotManager implements PilotManager<RendezvousSocketNodeHandle> {
    final List<RendezvousSocketNodeHandle> openedPilots = new ArrayList<>();

    @Override
    public SocketRequestHandle<RendezvousSocketNodeHandle> openPilot(
        RendezvousSocketNodeHandle identifier,
        Continuation<SocketRequestHandle<RendezvousSocketNodeHandle>, Exception> deliverAckToMe) {
      if (!identifier.canContactDirect()) throw new IllegalArgumentException("can't open pilot to natted node:" + identifier);
      openedPilots.add(identifier);
      return null;
    }

    @Override
    public void closePilot(RendezvousSocketNodeHandle identifier) {}

    @Override
    public void addOutgoingPilotListener(OutgoingPilotListener<RendezvousSocketNodeHandle> listener) {}

    @Override
    public void removeOutgoingPilotListener(OutgoingPilotListener<RendezvousSocketNodeHandle> listener) {}

    @Override
    public void addIncomingPilotListener(IncomingPilotListener<RendezvousSocketNodeHandle> listener) {}

    @Override
    public void removeIncomingPilotListener(IncomingPilotListener<RendezvousSocketNodeHandle> listener) {}
  }

  private Environment env;
  private PastryNode node;
  private final RecordingPilotManager pilotManager = new RecordingPilotManager();
  private final List<JoinRequest> joinRequests = new ArrayList<>();
  private final List<Exception> failures = new ArrayList<>();
  private final Continuation<JoinRequest, Exception> recordJoinRequest = new Continuation<>() {
    @Override
    public void receiveResult(JoinRequest result) {
      joinRequests.add(result);
    }

    @Override
    public void receiveException(Exception exception) {
      failures.add(exception);
    }
  };

  @BeforeEach
  void setUp() throws IOException {
    env = new Environment();
    int port;
    try (ServerSocket probe = new ServerSocket(0, 1, LOOPBACK)) {
      port = probe.getLocalPort();
    }
    node = new RendezvousSocketPastryNodeFactory(() -> Id.build("4000"), LOOPBACK, port, env, true).newNode();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /** A handle of the bootstrap behind a NAT, read the way a handle from the wire is read. */
  private RendezvousSocketNodeHandle bootstrap(byte contactStatus) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    new MultiInetSocketAddress(BOOTSTRAP_EXTERNAL, BOOTSTRAP_INTERNAL).serialize(out);
    out.writeLong(1);
    Id.build("8000").serialize(out);
    out.writeByte(contactStatus);
    return assertInstanceOf(RendezvousSocketNodeHandle.class, node.readNodeHandle(new SimpleInputBuffer(out.getBytes())));
  }

  private RendezvousJoinProtocol protocol() {
    return new RendezvousJoinProtocol(node, node.getLocalHandle(), node.getRoutingTable(), node.getLeafSet(), null, pilotManager);
  }

  @Test
  @DisplayName("a NATted joiner opens a pilot to a bootstrap that can be contacted directly")
  void opensPilotToDirectBootstrap() throws Exception {
    RendezvousSocketNodeHandle bootstrap = bootstrap(RendezvousSocketNodeHandle.CONTACT_DIRECT);
    assertFalse(((RendezvousSocketNodeHandle) node.getLocalHandle()).canContactDirect());

    protocol().getJoinRequest(bootstrap, recordJoinRequest);

    assertEquals(List.of(bootstrap), pilotManager.openedPilots);
    assertEquals(List.of(), failures);
  }

  @Test
  @DisplayName("a NATted joiner that cannot reach its firewalled bootstrap fails the join request with NodeIsFirewalledException")
  void failsForUnreachableFirewalledBootstrap() throws Exception {
    RendezvousSocketNodeHandle bootstrap = bootstrap(RendezvousSocketNodeHandle.CONTACT_FIREWALLED);

    protocol().getJoinRequest(bootstrap, recordJoinRequest);

    assertEquals(List.of(), pilotManager.openedPilots);
    assertEquals(List.of(), joinRequests);
    assertEquals(1, failures.size());
    assertEquals(bootstrap, assertInstanceOf(NodeIsFirewalledException.class, failures.get(0)).getHandle());
  }
}
