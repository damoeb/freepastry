package rice.pastry.socket.nat.rendezvous;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.rendezvous.RendezvousStrategy;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.leafset.LeafSet;
import rice.pastry.messaging.PastryRawMessage;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouteSet;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketNodeHandleFactory;
import rice.pastry.socket.SocketNodeHandleIdentitySerializer;
import rice.pastry.socket.nat.probe.ConnectivityProbeApplication;
import rice.pastry.socket.nat.probe.ProbeRequestMessage;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.transport.TransportLayerDeserializer;

/**
 * The rendezvous layer's messages, node handles and identity serializers through the bytes a socket ring carries:
 * read where they arrive, and sent on from there. The node is a rendezvous socket node that is never booted, so its
 * handles read the contact status byte the way a node on a rendezvous ring does.
 */
class RendezvousWireRoundTripTest {
  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
  private static final AtomicInteger NEXT_PORT = new AtomicInteger(2048);

  private static Environment env;
  private static PastryNode node;
  private static TransportLayerDeserializer transportLayer;
  private static RendezvousApplication app;
  private static RendezvousSocketNodeHandle direct;
  private static RendezvousSocketNodeHandle firewalled;

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    int port;
    try (ServerSocket probe = new ServerSocket(0, 1, LOOPBACK)) {
      port = probe.getLocalPort();
    }
    TransportLayerDeserializer[] tls = new TransportLayerDeserializer[1];
    RendezvousApplication[] apps = new RendezvousApplication[1];
    node = new RendezvousSocketPastryNodeFactory(() -> Id.build("4000"), LOOPBACK, port, env, false) {
      @Override
      protected TransportLayerDeserializer getTransportLayerDeserializer(NodeHandleFactory<?> handleFactory, PastryNode pn) {
        tls[0] = super.getTransportLayerDeserializer(handleFactory, pn);
        return tls[0];
      }

      @Override
      protected RendezvousStrategy<RendezvousSocketNodeHandle> getRendezvousStrategy(PastryNode pn) {
        apps[0] = (RendezvousApplication) super.getRendezvousStrategy(pn);
        return apps[0];
      }
    }.newNode();
    transportLayer = tls[0];
    app = apps[0];
    direct = handle("8000", RendezvousSocketNodeHandle.CONTACT_DIRECT);
    firewalled = handle("C000", RendezvousSocketNodeHandle.CONTACT_FIREWALLED);
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  /** A handle of another node, read the way a handle from the wire is read. */
  private static RendezvousSocketNodeHandle handle(String id, byte contactStatus) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    new MultiInetSocketAddress(new InetSocketAddress(LOOPBACK, NEXT_PORT.incrementAndGet())).serialize(out);
    out.writeLong(NEXT_PORT.get());
    Id.build(id).serialize(out);
    out.writeByte(contactStatus);
    return assertInstanceOf(RendezvousSocketNodeHandle.class, node.readNodeHandle(new SimpleInputBuffer(out.getBytes())));
  }

  private static byte[] wire(RawMessage message) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    transportLayer.serialize(message, out);
    return out.getBytes();
  }

  private static RawMessage read(byte[] bytes, NodeHandle from) throws IOException {
    SimpleInputBuffer in = new SimpleInputBuffer(bytes);
    RawMessage read = transportLayer.deserialize(in, from);
    if (!(read instanceof RouteMessage)) { // a route message keeps the rest of the buffer to unwrap later
      assertEquals(0, in.bytesRemaining(), () -> "bytes left after reading " + read);
    }
    return read;
  }

  /** The bytes of a message with the priority byte (after the int address) cleared, which only the sender keeps. */
  private static byte[] withoutPriority(byte[] wire) {
    byte[] copy = wire.clone();
    copy[Integer.BYTES] = 0;
    return copy;
  }

  /** Sends a message to the node through the transport's bytes, reads it, sends the copy on, reads that. */
  @SuppressWarnings("unchecked")
  private static <M extends PastryRawMessage> M hop(M message, NodeHandle from) throws IOException {
    byte[] first = wire(message);
    RawMessage copy = read(first, from);
    assertEquals(message.getClass(), copy.getClass());
    byte[] second = wire(copy);
    assertArrayEquals(withoutPriority(first), withoutPriority(second),
        () -> "the copy of " + message + " is sent on as the same bytes");
    return (M) read(second, from);
  }

  private static byte[] payload(int length) {
    byte[] b = new byte[length];
    for (int i = 0; i < length; i++) {
      b[i] = (byte) (i * 13 + 1);
    }
    return b;
  }

  private static byte[] remaining(ByteBuffer buffer) {
    ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  // ---- node handles

  @ParameterizedTest
  @ValueSource(bytes = {RendezvousSocketNodeHandle.CONTACT_DIRECT, RendezvousSocketNodeHandle.CONTACT_FIREWALLED})
  void rendezvousSocketNodeHandle(byte status) throws IOException {
    RendezvousSocketNodeHandle nodeHandle = handle("2000", status);

    NodeHandle read = WireRoundTrip.roundTrip(nodeHandle, buf -> node.readNodeHandle(buf));

    RendezvousSocketNodeHandle copy = assertInstanceOf(RendezvousSocketNodeHandle.class, read);
    assertEquals(nodeHandle, copy);
    assertEquals(status, copy.getContactStatus());
    assertEquals(status == RendezvousSocketNodeHandle.CONTACT_DIRECT, copy.canContactDirect());
  }

  // ---- messages

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 100, 100_000})
  @DisplayName("ByteBufferMessage carries a datagram of any length, empty to larger than a packet, and is sent on as read")
  void byteBufferMsg(int length) throws IOException {
    ByteBufferMessage msg = new ByteBufferMessage(ByteBuffer.wrap(payload(length)), direct, 3, app.getAddress());

    ByteBufferMessage copy = hop(msg, firewalled);

    assertEquals(direct, copy.getOriginalSender());
    assertArrayEquals(payload(length), remaining(copy.buffer));
  }

  @Test
  @DisplayName("ByteBufferMessage from a firewalled sender keeps the sender's contact status")
  void byteBufferMsgFromFirewalledSender() throws IOException {
    ByteBufferMessage copy = hop(new ByteBufferMessage(ByteBuffer.wrap(payload(5)), firewalled, 0, app.getAddress()), direct);

    RendezvousSocketNodeHandle sender = assertInstanceOf(RendezvousSocketNodeHandle.class, copy.getOriginalSender());
    assertFalse(sender.canContactDirect());
  }

  @Test
  @DisplayName("PilotForwardMessage is read, its datagram is sent on to the target by the pilot, and read there")
  void pilotForwardMsg() throws IOException {
    ByteBufferMessage inner = new ByteBufferMessage(ByteBuffer.wrap(payload(40)), firewalled, 2, app.getAddress());
    PilotForwardMessage msg = new PilotForwardMessage(app.getAddress(), inner, direct);

    PilotForwardMessage copy = hop(msg, direct);

    assertEquals(direct, copy.getTarget());
    assertEquals(firewalled, copy.getByteBufferMessage().getOriginalSender());
    assertArrayEquals(payload(40), remaining(copy.getByteBufferMessage().buffer));
    // the pilot sends the datagram it read on to the target, which reads it again
    ByteBufferMessage atTarget = hop(copy.getByteBufferMessage(), direct);
    assertEquals(firewalled, atTarget.getOriginalSender());
    assertArrayEquals(payload(40), remaining(atTarget.buffer));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE})
  void openChannelMsg(int uid) throws IOException {
    OpenChannelMessage msg = new OpenChannelMessage(app.getAddress(), direct, firewalled, uid);

    OpenChannelMessage copy = hop(msg, direct);

    assertEquals(direct, copy.getRendezvous());
    assertEquals(firewalled, copy.getSource());
    assertEquals(uid, copy.getUid());
    assertFalse(copy.getSource().canContactDirect());
  }

  @Test
  @DisplayName("A rendezvous message routed inside a route message is read at a hop and sent on")
  void rendezvousMessageInARouteMessage() throws IOException {
    OpenChannelMessage msg = new OpenChannelMessage(app.getAddress(), direct, firewalled, 7);
    RouteMessage routed = hop(new RouteMessage(Id.build("9000"), msg, (byte) 1), direct);

    OpenChannelMessage unwrapped = assertInstanceOf(OpenChannelMessage.class, routed.unwrap(app.getDeserializer()));

    assertArrayEquals(WireRoundTrip.bytes(msg), WireRoundTrip.bytes(unwrapped));
    assertSame(OpenChannelMessage.class, unwrapped.getClass());
  }

  @Test
  @DisplayName("RendezvousJoinRequest carries its pilot with the rows and the leaf set of an accepted join")
  void rendezvousJoinRequest() throws IOException {
    RendezvousSocketNodeHandle joiner = handle("1000", RendezvousSocketNodeHandle.CONTACT_FIREWALLED);
    RendezvousJoinRequest request = new RendezvousJoinRequest(joiner, (byte) 4, 1234L, direct);
    RouteSet[] row = new RouteSet[16];
    row[8] = new RouteSet(3, 0, 8, node);
    row[8].put(direct);
    request.pushRow(row);
    LeafSet leafSet = new LeafSet(node.getLocalHandle(), 8, false);
    leafSet.put(direct);
    leafSet.put(firewalled);
    request.acceptJoin(direct, leafSet);

    RendezvousJoinRequest copy = hop(request, joiner);

    assertEquals(joiner, copy.getHandle());
    assertEquals(direct, copy.getPilot());
    assertEquals(direct, copy.getJoinHandle());
    assertEquals(request.lastRow(), copy.lastRow());
    RendezvousSocketNodeHandle pilot = assertInstanceOf(RendezvousSocketNodeHandle.class, copy.getPilot());
    assertTrue(pilot.canContactDirect());
  }

  @Test
  @DisplayName("RendezvousJoinRequest without accepted join and rows")
  void bareRendezvousJoinRequest() throws IOException {
    RendezvousSocketNodeHandle joiner = handle("1000", RendezvousSocketNodeHandle.CONTACT_DIRECT);

    RendezvousJoinRequest copy = hop(new RendezvousJoinRequest(joiner, (byte) 4, Long.MAX_VALUE, firewalled), joiner);

    assertEquals(firewalled, copy.getPilot());
  }

  // ---- probe

  private static MultiInetSocketAddress multi(int count) {
    InetSocketAddress[] addresses = new InetSocketAddress[count];
    for (int i = 0; i < count; i++) {
      addresses[i] = new InetSocketAddress(LOOPBACK, 7000 + i);
    }
    return new MultiInetSocketAddress(addresses);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, MultiInetSocketAddress.HARD_MAX_NUM_ADDRESSES})
  @DisplayName("ProbeRequestMessage carries the address of the requester, one address to the most a node announces")
  void probeRequestMessage(int addresses) throws IOException {
    ConnectivityProbeApplication probeApp = new ConnectivityProbeApplication(node, null, null);
    ProbeRequestMessage msg = new ProbeRequestMessage(multi(addresses), Long.MIN_VALUE, probeApp.getAddress());

    ProbeRequestMessage copy = WireRoundTrip.roundTrip(msg, buf -> ProbeRequestMessage.build(buf, probeApp.getAddress()));

    assertEquals(multi(addresses), copy.getProbeRequester());
    assertEquals(addresses, copy.getProbeRequester().getNumAddresses());
    assertEquals(Long.MIN_VALUE, copy.getUID());
    ProbeRequestMessage viaApp = WireRoundTrip.roundTrip(msg, probeApp.getDeserializer(), direct);
    assertEquals(multi(addresses), viaApp.getProbeRequester());
  }

  // ---- identity serializers

  private static SourceRoute<MultiInetSocketAddress> routeTo(MultiInetSocketAddress last) {
    return new MultiAddressSourceRouteFactory().getSourceRoute(multi(1), last);
  }

  @ParameterizedTest
  @ValueSource(bytes = {RendezvousSocketNodeHandle.CONTACT_DIRECT, RendezvousSocketNodeHandle.CONTACT_FIREWALLED})
  @DisplayName("The rendezvous identity serializer writes the epoch, the id and the contact status, and reads them with the address of the route")
  void rendezvousIdentitySerializer(byte status) throws IOException {
    RendezvousSocketNodeHandle nodeHandle = handle("3000", status);
    RendezvousNodeHandleIdentitySerializer serializer = new RendezvousNodeHandleIdentitySerializer(node,
        new RendezvousSocketNodeHandleFactory(node));
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    serializer.serialize(out, nodeHandle);
    byte[] first = out.getBytes();
    SimpleInputBuffer in = new SimpleInputBuffer(first);

    var read = serializer.deserialize(in, routeTo(nodeHandle.getAddress()));

    assertEquals(0, in.bytesRemaining());
    RendezvousSocketNodeHandle copy = assertInstanceOf(RendezvousSocketNodeHandle.class, read);
    assertEquals(nodeHandle, copy);
    assertEquals(status, copy.getContactStatus());
    SimpleOutputBuffer again = new SimpleOutputBuffer();
    serializer.serialize(again, copy);
    assertArrayEquals(first, again.getBytes());
  }

  @Test
  @DisplayName("The socket identity serializer writes the epoch and the id and reads them with the address of the route")
  void socketIdentitySerializer() throws IOException {
    SocketNodeHandle nodeHandle = handle("3000", RendezvousSocketNodeHandle.CONTACT_DIRECT);
    SocketNodeHandleIdentitySerializer serializer = new SocketNodeHandleIdentitySerializer(node, new SocketNodeHandleFactory(node));
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    serializer.serialize(out, nodeHandle);
    SimpleInputBuffer in = new SimpleInputBuffer(out.getBytes());

    var read = serializer.deserialize(in, routeTo(nodeHandle.getAddress()));

    assertEquals(0, in.bytesRemaining());
    assertEquals(nodeHandle, read);
  }
}
