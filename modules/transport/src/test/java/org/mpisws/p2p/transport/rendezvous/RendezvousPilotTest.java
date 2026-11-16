package org.mpisws.p2p.transport.rendezvous;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/** Drives the pilot lifecycle of a {@link RendezvousTransportLayerImpl} through fake sockets. */
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

  private static class Serializer implements ContactDeserializer<String, Contact> {
    @Override
    public void serialize(Contact i, OutputBuffer buf) throws IOException {
      buf.writeUTF(i.name());
      buf.writeBoolean(i.direct());
    }

    @Override
    public ByteBuffer serialize(Contact i) throws IOException {
      return bytes(i);
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

  private static ByteBuffer bytes(Contact c) throws IOException {
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    sob.writeUTF(c.name());
    sob.writeBoolean(c.direct());
    return sob.getByteBuffer();
  }

  /** Records what is written, serves what is queued, and remembers the last registration. */
  private static class FakeSocket implements P2PSocket<String> {
    final ByteArrayOutputStream written = new ByteArrayOutputStream();
    final ByteArrayOutputStream toRead = new ByteArrayOutputStream();
    int readPosition;
    boolean closed;
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
      P2PSocketReceiver<String> r = receiver;
      assertNotNull(r, "nobody is registered on the socket");
      r.receiveSelectResult(this, canRead, canWrite);
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
      int n = Math.min(dst.remaining(), all.length - readPosition);
      dst.put(all, readPosition, n);
      readPosition += n;
      return n;
    }

    @Override
    public long write(ByteBuffer src) {
      int n = src.remaining();
      byte[] copy = new byte[n];
      src.get(copy);
      written.write(copy, 0, n);
      return n;
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<String> r) {
      receiver = r;
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
        String i, SocketCallback<String> deliverSocketToMe, @Nullable Map<String, Object> options) {
      opened.add(i);
      callbacks.add(deliverSocketToMe);
      FakeHandle handle = new FakeHandle(i);
      handles.add(handle);
      return handle;
    }

    @Override
    public @Nullable MessageRequestHandle<String, ByteBuffer> sendMessage(
        String i,
        ByteBuffer m,
        @Nullable MessageCallback<String, ByteBuffer> deliverAckToMe,
        @Nullable Map<String, Object> options) {
      return null;
    }

    @Override
    public String getLocalIdentifier() {
      return "lower";
    }

    @Override
    public void acceptSockets(boolean b) {}

    @Override
    public void acceptMessages(boolean b) {}

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
    public void pilotOpening(Contact i) {
      events.add("open:" + i.name());
    }

    @Override
    public void pilotClosed(Contact i) {
      events.add("closed:" + i.name());
    }
  }

  private Environment env;
  private final FakeLower lower = new FakeLower();
  private final RecordingListener outgoingEvents = new RecordingListener();
  private final RecordingListener incomingEvents = new RecordingListener();
  private RendezvousTransportLayerImpl<String, Contact> layer;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    layer = new RendezvousTransportLayerImpl<>(
        lower,
        "rendezvous.contact",
        LOCAL,
        new Serializer(),
        new RendezvousGenerationStrategy<>() {},
        c -> null,
        null,
        null,
        c -> false,
        env);
    layer.addOutgoingPilotListener(outgoingEvents);
    layer.addIncomingPilotListener(incomingEvents);
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
}
