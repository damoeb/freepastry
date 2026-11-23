package org.mpisws.p2p.transport.rendezvous;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Drives a {@link RendezvousTransportLayerImpl} through fake sockets: the pilot lifecycle, the connector and acceptor
 * headers a rendezvous node reads, and how long it keeps a connector waiting for its acceptor.
 */
class RendezvousPilotTest {
  private record Contact(String name, boolean direct) implements RendezvousContact {
    @Override
    public boolean canContactDirect() {
      return direct;
    }
  }

  private static final Contact LOCAL = new Contact("me", false);
  private static final Contact SUPERNODE = new Contact("super", true);
  private static final Contact NATTED = new Contact("natted", false);
  private static final Contact OPENER = new Contact("opener", false);
  private static final String CONTACT_OPTION = "rendezvous.contact";
  private static final int UID = 42;

  private static class Serializer implements ContactDeserializer<String, Contact> {
    @Override
    public void serialize(Contact contact, OutputBuffer buf) throws IOException {
      buf.writeUTF(contact.name());
      buf.writeBoolean(contact.direct());
    }

    @Override
    public ByteBuffer serialize(Contact contact) throws IOException {
      return bytes(contact);
    }

    @Override
    public Contact deserialize(InputBuffer sib) throws IOException {
      return new Contact(sib.readUTF(), sib.readBoolean());
    }

    @Override
    public String convert(Contact high) {
      return high.name();
    }

    @Override
    public @Nullable Map<String, Object> getOptions(Contact high) {
      return null;
    }
  }

  private static ByteBuffer bytes(Contact contact) throws IOException {
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    sob.writeUTF(contact.name());
    sob.writeBoolean(contact.direct());
    return sob.getByteBuffer();
  }

  /** Records what is written, serves what is queued, and remembers the last registration. */
  private static class FakeSocket implements P2PSocket<String> {
    final ByteArrayOutputStream written = new ByteArrayOutputStream();
    final ByteArrayOutputStream toRead = new ByteArrayOutputStream();
    int readPosition;
    volatile boolean closed;
    @Nullable P2PSocketReceiver<String> receiver;

    void feed(byte... data) {
      toRead.write(data, 0, data.length);
    }

    void feed(ByteBuffer data) {
      byte[] copy = new byte[data.remaining()];
      data.get(copy);
      feed(copy);
    }

    byte[] writtenBytes() {
      return written.toByteArray();
    }

    void fire(boolean canRead, boolean canWrite) throws IOException {
      P2PSocketReceiver<String> p2PSocketReceiver = receiver;
      assertNotNull(p2PSocketReceiver, "nobody is registered on the socket");
      p2PSocketReceiver.receiveSelectResult(this, canRead, canWrite);
    }

    @Override
    public @Nullable Map<String, Object> getOptions() {
      return null;
    }

    @Override
    public String getIdentifier() {
      return "fake";
    }

    @Override
    public long read(ByteBuffer dst) {
      byte[] all = toRead.toByteArray();
      int byteCount = Math.min(dst.remaining(), all.length - readPosition);
      dst.put(all, readPosition, byteCount);
      readPosition += byteCount;
      return byteCount;
    }

    @Override
    public long write(ByteBuffer src) {
      int byteCount = src.remaining();
      byte[] copy = new byte[byteCount];
      src.get(copy);
      written.write(copy, 0, byteCount);
      return byteCount;
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<String> p2PSocketReceiver) {
      receiver = p2PSocketReceiver;
    }

    @Override
    public void shutdownOutput() {}

    @Override
    public void close() {
      closed = true;
    }
  }

  private static class FakeLower implements TransportLayer<String, ByteBuffer> {
    final List<String> opened = new ArrayList<>();
    final List<SocketCallback<String>> callbacks = new ArrayList<>();
    final List<FakeHandle> handles = new ArrayList<>();

    @Override
    public SocketRequestHandle<String> openSocket(
        String identifier, SocketCallback<String> deliverSocketToMe, @Nullable Map<String, Object> options) {
      opened.add(identifier);
      callbacks.add(deliverSocketToMe);
      FakeHandle handle = new FakeHandle(identifier);
      handles.add(handle);
      return handle;
    }

    @Override
    public @Nullable MessageRequestHandle<String, ByteBuffer> sendMessage(
        String identifier,
        ByteBuffer msg,
        @Nullable MessageCallback<String, ByteBuffer> deliverAckToMe,
        @Nullable Map<String, Object> options) {
      return null;
    }

    @Override
    public String getLocalIdentifier() {
      return "lower";
    }

    @Override
    public void acceptSockets(boolean enabled) {}

    @Override
    public void acceptMessages(boolean enabled) {}

    @Override
    public void setCallback(TransportLayerCallback<String, ByteBuffer> callback) {}

    @Override
    public void setErrorHandler(ErrorHandler<String> handler) {}

    @Override
    public void destroy() {}
  }

  private static class FakeHandle implements SocketRequestHandle<String> {
    final String identifier;
    boolean cancelled;

    FakeHandle(String identifier) {
      this.identifier = identifier;
    }

    @Override
    public String getIdentifier() {
      return identifier;
    }

    @Override
    public @Nullable Map<String, Object> getOptions() {
      return null;
    }

    @Override
    public boolean cancel() {
      cancelled = true;
      return true;
    }
  }

  private static class RecordingListener implements OutgoingPilotListener<Contact>, IncomingPilotListener<Contact> {
    final List<String> events = new ArrayList<>();

    @Override
    public void pilotOpening(Contact contact) {
      events.add("open:" + contact.name());
    }

    @Override
    public void pilotClosed(Contact contact) {
      events.add("closed:" + contact.name());
    }
  }

  /** Records the channel requests the layer sends through the overlay as {@code target<-source#uid}. */
  private static class RecordingRendezvousStrategy implements RendezvousStrategy<Contact> {
    final List<String> openedChannels = new ArrayList<>();

    @Override
    public Cancellable openChannel(
        Contact target,
        Contact rendezvous,
        Contact source,
        int uid,
        @Nullable Continuation<Integer, Exception> deliverResultToMe,
        @Nullable Map<String, Object> options) {
      openedChannels.add(target.name() + "<-" + source.name() + "#" + uid);
      return () -> true;
    }

    @Override
    public @Nullable MessageRequestHandle<Contact, ByteBuffer> sendMessage(
        Contact identifier,
        ByteBuffer msg,
        @Nullable MessageCallback<Contact, ByteBuffer> deliverAckToMe,
        @Nullable Map<String, Object> options) {
      return null;
    }

    @Override
    public void setTransportLayer(RendezvousTransportLayer<Contact> transportLayer) {}
  }

  private Environment env;
  private final RecordingRendezvousStrategy rendezvousStrategy = new RecordingRendezvousStrategy();
  private final FakeLower lower = new FakeLower();
  private final RecordingListener outgoingEvents = new RecordingListener();
  private final RecordingListener incomingEvents = new RecordingListener();
  private RendezvousTransportLayerImpl<String, Contact> layer;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    layer = newLayer();
    layer.addOutgoingPilotListener(outgoingEvents);
    layer.addIncomingPilotListener(incomingEvents);
  }

  /** A layer that reads the parameters of {@link #env} as they are now. */
  private RendezvousTransportLayerImpl<String, Contact> newLayer() {
    return new RendezvousTransportLayerImpl<>(
        lower,
        CONTACT_OPTION,
        LOCAL,
        new Serializer(),
        new RendezvousGenerationStrategy<>() {},
        c -> null,
        rendezvousStrategy,
        null,
        c -> false,
        env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private FakeSocket connectOutgoingPilot() {
    layer.openPilot(SUPERNODE, null);
    FakeSocket socket = new FakeSocket();
    lower.callbacks.get(0).receiveResult(lower.handles.get(0), socket);
    return socket;
  }

  @Test
  @DisplayName("opening a pilot announces it, connects to the supernode and introduces the local node")
  void outgoingPilotOpens() throws Exception {
    assertNotNull(layer.openPilot(SUPERNODE, null));

    assertEquals(List.of("super"), lower.opened);
    assertEquals(List.of("open:super"), outgoingEvents.events);

    FakeSocket socket = new FakeSocket();
    lower.callbacks.get(0).receiveResult(lower.handles.get(0), socket);

    ByteBuffer local = bytes(LOCAL);
    byte[] expected = new byte[1 + local.remaining()];
    expected[0] = RendezvousTransportLayerImpl.PILOT_SOCKET;
    local.get(expected, 1, expected.length - 1);
    assertArrayEquals(expected, socket.writtenBytes());
  }

  @Test
  @DisplayName("opening a pilot twice reuses the first one")
  void outgoingPilotIsOpenedOnce() {
    SocketRequestHandle<Contact> first = layer.openPilot(SUPERNODE, null);
    SocketRequestHandle<Contact> second = layer.openPilot(SUPERNODE, null);

    assertEquals(1, lower.opened.size());
    assertEquals(first, second);
  }

  @Test
  @DisplayName("a keepalive tick writes a ping to the pilot socket")
  void keepaliveWritesAPing() throws Exception {
    FakeSocket socket = connectOutgoingPilot();
    int before = socket.writtenBytes().length;

    layer.outgoingPilots.get(SUPERNODE).run();
    socket.fire(false, true);

    byte[] written = socket.writtenBytes();
    assertEquals(before + 1, written.length);
    assertEquals(RendezvousTransportLayerImpl.PILOT_PING, written[written.length - 1]);
  }

  @Test
  @DisplayName("a keepalive tick before the socket is there is a no-op")
  void keepaliveWithoutSocketDoesNothing() {
    layer.openPilot(SUPERNODE, null);

    assertFalse(layer.outgoingPilots.get(SUPERNODE).ping());
  }

  @Test
  @DisplayName("a pong keeps the pilot open")
  void pongKeepsThePilotOpen() throws Exception {
    FakeSocket socket = connectOutgoingPilot();
    socket.feed(RendezvousTransportLayerImpl.PILOT_PONG);

    socket.fire(true, false);

    assertFalse(socket.closed);
    assertTrue(layer.outgoingPilots.containsKey(SUPERNODE));
  }

  @Test
  @DisplayName("closing a pilot closes its socket and forgets it")
  void closePilotClosesTheSocket() {
    FakeSocket socket = connectOutgoingPilot();

    layer.closePilot(SUPERNODE);

    assertTrue(socket.closed);
    assertFalse(layer.outgoingPilots.containsKey(SUPERNODE));
    // closePilot() takes the entry out before cancel(), so cancel() does not announce the removal; pinned as is
    assertEquals(List.of("open:super"), outgoingEvents.events);
  }

  @Test
  @DisplayName("an error on the pilot socket cancels the pilot and announces the removal once")
  void socketErrorCancelsThePilotOnce() {
    FakeSocket socket = connectOutgoingPilot();
    P2PSocketReceiver<String> pilot = socket.receiver;
    assertNotNull(pilot);

    pilot.receiveException(socket, new IOException("reset"));
    pilot.receiveException(socket, new IOException("reset again"));

    assertTrue(socket.closed);
    assertFalse(layer.outgoingPilots.containsKey(SUPERNODE));
    assertEquals(List.of("open:super", "closed:super"), outgoingEvents.events);
  }

  @Test
  @DisplayName("cancelling a pilot that is still connecting cancels the lower socket request")
  void cancelBeforeConnectedCancelsTheLowerRequest() {
    layer.openPilot(SUPERNODE, null);

    layer.closePilot(SUPERNODE);

    assertTrue(lower.handles.get(0).cancelled);
    assertFalse(layer.outgoingPilots.containsKey(SUPERNODE));
  }

  @Test
  @DisplayName("a socket that arrives after the pilot was cancelled is closed")
  void lateSocketForACancelledPilotIsClosed() {
    layer.openPilot(SUPERNODE, null);
    layer.closePilot(SUPERNODE);
    FakeSocket late = new FakeSocket();

    lower.callbacks.get(0).receiveResult(lower.handles.get(0), late);

    assertTrue(late.closed);
    assertEquals(0, late.writtenBytes().length);
  }

  @Test
  @DisplayName("a failure to connect removes the pilot and announces it")
  void failedConnectRemovesThePilot() {
    layer.openPilot(SUPERNODE, null);

    lower.callbacks.get(0).receiveException(lower.handles.get(0), new IOException("refused"));

    assertFalse(layer.outgoingPilots.containsKey(SUPERNODE));
    assertEquals(List.of("open:super", "closed:super"), outgoingEvents.events);
  }

  @Test
  @DisplayName("an incoming pilot socket is registered under the contact it introduces")
  void incomingPilotIsRegistered() throws Exception {
    FakeSocket socket = new FakeSocket();
    socket.feed(RendezvousTransportLayerImpl.PILOT_SOCKET);
    socket.feed(bytes(NATTED));

    layer.incomingSocket(socket);

    assertNotNull(layer.incomingPilots.get(NATTED));
    assertEquals(List.of("open:natted"), incomingEvents.events);
  }

  @Test
  @DisplayName("an incoming pilot answers a ping with a pong")
  void incomingPilotAnswersPing() throws Exception {
    FakeSocket socket = new FakeSocket();
    socket.feed(RendezvousTransportLayerImpl.PILOT_SOCKET);
    socket.feed(bytes(NATTED));
    layer.incomingSocket(socket);
    socket.feed(RendezvousTransportLayerImpl.PILOT_PING);

    socket.fire(true, false);
    socket.fire(false, true);

    assertArrayEquals(new byte[] {RendezvousTransportLayerImpl.PILOT_PONG}, socket.writtenBytes());
  }

  @Test
  @DisplayName("an incoming pilot that fails is forgotten and its socket closed")
  void incomingPilotFailureClosesIt() throws Exception {
    FakeSocket socket = new FakeSocket();
    socket.feed(RendezvousTransportLayerImpl.PILOT_SOCKET);
    socket.feed(bytes(NATTED));
    layer.incomingSocket(socket);

    assertNotNull(socket.receiver);
    socket.receiver.receiveException(socket, new IOException("reset"));

    assertNull(layer.incomingPilots.get(NATTED));
    assertTrue(socket.closed);
    assertEquals(List.of("open:natted", "closed:natted"), incomingEvents.events);
  }

  /** The header a connector or an acceptor sends a rendezvous node: socket type, target, opener and uid. */
  private static byte[] header(byte socketType, Contact target, Contact opener, int uid) throws IOException {
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    sob.writeByte(socketType);
    sob.writeUTF(target.name());
    sob.writeBoolean(target.direct());
    sob.writeUTF(opener.name());
    sob.writeBoolean(opener.direct());
    sob.writeInt(uid);
    ByteBuffer buffer = sob.getByteBuffer();
    byte[] header = new byte[buffer.remaining()];
    buffer.get(header);
    return header;
  }

  /** The socket type byte and the first bytes of the header arrive, the rest is still on the way. */
  private static final int FIRST_CHUNK_BYTES = 4;

  private static byte[] firstChunk(byte[] header) {
    return java.util.Arrays.copyOfRange(header, 0, FIRST_CHUNK_BYTES);
  }

  private static byte[] secondChunk(byte[] header) {
    return java.util.Arrays.copyOfRange(header, FIRST_CHUNK_BYTES, header.length);
  }

  private FakeSocket waitingConnector(int uid) throws IOException {
    FakeSocket connector = new FakeSocket();
    connector.feed(header(RendezvousTransportLayerImpl.CONNECTOR_SOCKET, NATTED, OPENER, uid));
    layer.incomingSocket(connector);
    return connector;
  }

  private static void awaitClosed(FakeSocket socket) throws InterruptedException {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    while (!socket.closed && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
  }

  @Test
  @DisplayName("a connector header that arrives in two chunks is read once it is complete")
  void connectorHeaderInTwoChunks() throws Exception {
    byte[] header = header(RendezvousTransportLayerImpl.CONNECTOR_SOCKET, NATTED, OPENER, UID);
    FakeSocket connector = new FakeSocket();
    connector.feed(firstChunk(header));

    layer.incomingSocket(connector);

    assertFalse(connector.closed);
    assertEquals(List.of(), rendezvousStrategy.openedChannels);

    connector.feed(secondChunk(header));
    connector.fire(true, false);

    assertFalse(connector.closed);
    assertEquals(List.of("natted<-opener#" + UID), rendezvousStrategy.openedChannels);
    assertEquals(connector, layer.removeConnectSocket(OPENER, NATTED, UID));
  }

  @Test
  @DisplayName("an acceptor header that arrives in two chunks is read once it is complete, and both sides get success")
  void acceptorHeaderInTwoChunks() throws Exception {
    FakeSocket connector = new FakeSocket();
    layer.putConnectSocket(OPENER, NATTED, UID, connector);
    byte[] header = header(RendezvousTransportLayerImpl.ACCEPTOR_SOCKET, NATTED, OPENER, UID);
    FakeSocket acceptor = new FakeSocket();
    acceptor.feed(firstChunk(header));

    layer.incomingSocket(acceptor);

    assertFalse(acceptor.closed);
    assertEquals(0, connector.writtenBytes().length);

    acceptor.feed(secondChunk(header));
    acceptor.fire(true, false);

    byte[] success = {RendezvousTransportLayerImpl.CONNECTION_RESPONSE_SUCCESS};
    assertArrayEquals(success, connector.writtenBytes());
    assertArrayEquals(success, acceptor.writtenBytes());
    assertFalse(connector.closed);
    assertFalse(acceptor.closed);
  }

  @Test
  @DisplayName("a connector whose acceptor never comes is closed and forgotten after the connector timeout")
  void waitingConnectorTimesOut() throws Exception {
    env.getParameters().setInt("transport_rendezvous_connector_timeout_millis", 1_000);
    layer = newLayer();
    FakeSocket connector = waitingConnector(UID);
    assertFalse(connector.closed);

    synchronized (env.getSelectorManager()) {
      ((DirectTimeSource) env.getTimeSource()).incrementTime(1_000);
    }
    env.getSelectorManager().invoke(() -> {});
    awaitClosed(connector);

    assertTrue(connector.closed, "the connector is closed after the timeout");
    assertNull(layer.removeConnectSocket(OPENER, NATTED, UID), "the connector is forgotten after the timeout");
  }

  @Test
  @DisplayName("a connector that finds its acceptor in time is not closed by the connector timeout")
  void matchedConnectorOutlivesTheTimeout() throws Exception {
    env.getParameters().setInt("transport_rendezvous_connector_timeout_millis", 1_000);
    layer = newLayer();
    FakeSocket connector = waitingConnector(UID);
    FakeSocket acceptor = new FakeSocket();
    acceptor.feed(header(RendezvousTransportLayerImpl.ACCEPTOR_SOCKET, NATTED, OPENER, UID));
    layer.incomingSocket(acceptor);

    synchronized (env.getSelectorManager()) {
      ((DirectTimeSource) env.getTimeSource()).incrementTime(1_000);
    }
    java.util.concurrent.CountDownLatch selectorRan = new java.util.concurrent.CountDownLatch(2);
    env.getSelectorManager().invoke(() -> {
      selectorRan.countDown();
      env.getSelectorManager().invoke(selectorRan::countDown); // a second round, after the due timers ran
    });
    assertTrue(selectorRan.await(5, java.util.concurrent.TimeUnit.SECONDS));

    assertFalse(connector.closed);
    assertFalse(acceptor.closed);
  }

  @Test
  @DisplayName("a rendezvous node keeps at most the configured number of connectors waiting, and closes the rest at once")
  void waitingConnectorsAreCapped() throws Exception {
    env.getParameters().setInt("transport_rendezvous_max_waiting_connectors", 1);
    layer = newLayer();

    FakeSocket first = waitingConnector(UID);
    FakeSocket second = waitingConnector(UID + 1);

    assertFalse(first.closed);
    assertTrue(second.closed);
    assertNull(layer.removeConnectSocket(OPENER, NATTED, UID + 1));
    assertEquals(first, layer.removeConnectSocket(OPENER, NATTED, UID));
  }

  @Test
  @DisplayName("destroying the layer closes the connectors that still wait for their acceptor")
  void destroyClosesWaitingConnectors() throws Exception {
    FakeSocket connector = waitingConnector(UID);

    layer.destroy();

    assertTrue(connector.closed);
    assertNull(layer.removeConnectSocket(OPENER, NATTED, UID));
  }

  @Test
  @DisplayName("a socket opened without options goes to the lower layer directly, as a normal socket")
  void nullOptionsOpenANormalSocket() throws Exception {
    List<P2PSocket<String>> delivered = new ArrayList<>();
    layer.openSocket("peer", recordingCallback(delivered, new ArrayList<>()), null);

    assertEquals(List.of("peer"), lower.opened);
    FakeSocket socket = new FakeSocket();
    lower.callbacks.get(0).receiveResult(lower.handles.get(0), socket);
    socket.fire(false, true);

    assertArrayEquals(new byte[] {RendezvousTransportLayerImpl.NORMAL_SOCKET}, socket.writtenBytes());
    assertEquals(List.of(socket), delivered);
  }

  @Test
  @DisplayName("a NATted node without outgoing pilots reports one clear failure when it opens a socket to another NATted node")
  void nattedNodeWithoutPilotsFailsClearly() {
    List<Exception> failures = new ArrayList<>();

    layer.openSocket("natted", recordingCallback(new ArrayList<>(), failures), Map.<String, Object>of(CONTACT_OPTION, NATTED));

    assertEquals(1, failures.size());
    assertInstanceOf(IllegalStateException.class, failures.get(0));
    assertEquals(List.of(), lower.opened);
  }

  private static SocketCallback<String> recordingCallback(List<P2PSocket<String>> delivered, List<Exception> failures) {
    return new SocketCallback<>() {
      @Override
      public void receiveResult(SocketRequestHandle<String> cancellable, P2PSocket<String> socket) {
        delivered.add(socket);
      }

      @Override
      public void receiveException(SocketRequestHandle<String> request, Exception exception) {
        failures.add(exception);
      }
    };
  }
}
