package org.mpisws.p2p.transport.identity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_ALIVE;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD_FOREVER;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityListener;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Drives both roles of an {@link IdentityImpl} through fake neighbouring layers. Upper identifiers look like
 * {@code node@address}; the middle and lower identifier is the address.
 */
class IdentityLayersTest {
  private static final String LOCAL = "self@me";
  private static final String ADDRESS = "peer";
  private static final String NODE_A = "A@" + ADDRESS;
  private static final String NODE_B = "B@" + ADDRESS;
  private static final byte[] PAYLOAD = {7, 8, 9};
  private static final String INDEX = IdentityImpl.NODE_HANDLE_FROM_INDEX;

  private Environment env;
  private final FakeLayer<ByteBuffer> lowerTl = new FakeLayer<>();
  private final FakeLayer<String> middleTl = new FakeLayer<>();
  private final FakeLiveness middleLiveness = new FakeLiveness();
  private final FakeProximity middleProximity = new FakeProximity();
  private final List<String> overrideCalls = new ArrayList<>();
  private final List<String> unexpected = new ArrayList<>();
  private final List<String> lowerReceived = new ArrayList<>();
  private final List<@Nullable Map<String, Object>> lowerReceivedOptions = new ArrayList<>();
  private final List<String> upperReceived = new ArrayList<>();
  private final List<String> upperLiveness = new ArrayList<>();
  private final List<String> upperProximity = new ArrayList<>();
  private IdentityImpl<String, String, String, String> identity;
  private TransportLayerCallback<String, ByteBuffer> lowerInput;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private static byte[] serialized(String upper) throws IOException {
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    sob.writeUTF(upper);
    return sob.getBytes();
  }

  private static class Serializer implements IdentitySerializer<String, String, String> {
    @Override
    public void serialize(OutputBuffer buf, String i) throws IOException {
      buf.writeUTF(i);
    }

    @Override
    public String deserialize(InputBuffer buf, String l) throws IOException {
      return buf.readUTF();
    }

    @Override
    public String translateDown(String i) {
      return i.substring(i.indexOf('@') + 1);
    }

    @Override
    public String translateUp(String i) {
      return i;
    }

    @Override
    public void addSerializerListener(SerializerListener<String> listener) {}

    @Override
    public void removeSerializerListener(SerializerListener<String> listener) {}
  }

  private static class FakeLayer<M> implements TransportLayer<String, M> {
    @Nullable TransportLayerCallback<String, M> callback;
    final List<String> sentTo = new ArrayList<>();
    final List<M> sent = new ArrayList<>();
    final List<@Nullable Map<String, Object>> sentOptions = new ArrayList<>();

    @Override
    public @Nullable SocketRequestHandle<String> openSocket(
        String i, SocketCallback<String> deliverSocketToMe, @Nullable Map<String, Object> options) {
      return null;
    }

    @Override
    public @Nullable MessageRequestHandle<String, M> sendMessage(
        String i, M m, @Nullable MessageCallback<String, M> deliverAckToMe, @Nullable Map<String, Object> options) {
      sentTo.add(i);
      sent.add(m);
      sentOptions.add(options);
      return null;
    }

    @Override
    public String getLocalIdentifier() {
      return "local";
    }

    @Override
    public void acceptSockets(boolean b) {}

    @Override
    public void acceptMessages(boolean b) {}

    @Override
    public void setCallback(TransportLayerCallback<String, M> callback) {
      this.callback = callback;
    }

    @Override
    public void setErrorHandler(ErrorHandler<String> handler) {}

    @Override
    public void destroy() {}
  }

  private static class FakeLiveness implements LivenessProvider<String> {
    @Nullable LivenessListener<String> listener;

    @Override
    public int getLiveness(String i, @Nullable Map<String, Object> options) {
      return LIVENESS_ALIVE;
    }

    @Override
    public boolean checkLiveness(String i, @Nullable Map<String, Object> options) {
      return true;
    }

    @Override
    public void addLivenessListener(LivenessListener<String> name) {
      listener = name;
    }

    @Override
    public boolean removeLivenessListener(LivenessListener<String> name) {
      return true;
    }

    @Override
    public void clearState(String i) {}
  }

  private static class FakeProximity implements ProximityProvider<String> {
    @Nullable ProximityListener<String> listener;

    @Override
    public int proximity(String i, @Nullable Map<String, Object> options) {
      return 5;
    }

    @Override
    public void addProximityListener(ProximityListener<String> l) {
      listener = l;
    }

    @Override
    public boolean removeProximityListener(ProximityListener<String> l) {
      return true;
    }

    @Override
    public void clearState(String i) {}
  }

  private static class AcceptingStrategy implements BindStrategy<String, String> {
    @Override
    public boolean accept(String u, @Nullable String l, @Nullable Map<String, Object> options) {
      return true;
    }
  }

  private void build(NodeChangeStrategy<String> nodeChange, @Nullable BindStrategy<String, String> bindStrategy)
      throws IOException {
    identity = new IdentityImpl<>(serialized(LOCAL), new Serializer(), nodeChange, (u, m) -> true, bindStrategy, env);
    identity.initLowerLayer(lowerTl, new ErrorHandler<String>() {
      @Override
      public void receivedUnexpectedData(String i, byte[] bytes, int location, @Nullable Map<String, Object> options) {
        unexpected.add(i);
      }

      @Override
      public void receivedException(@Nullable String i, Throwable error) {
        throw new AssertionError(error);
      }
    });
    identity.initUpperLayer(
        LOCAL, middleTl, middleLiveness, middleProximity, (i, liveness, options) -> overrideCalls.add(i + ":" + liveness));
    LowerIdentity<String, ByteBuffer> lower = identity.getLowerIdentity();
    assertNotNull(lower);
    lowerInput = (TransportLayerCallback<String, ByteBuffer>) lower;
    lower.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(String i, ByteBuffer m, @Nullable Map<String, Object> options) {
        assertEquals(PAYLOAD.length, m.remaining());
        lowerReceived.add(i);
        lowerReceivedOptions.add(options);
      }

      @Override
      public void incomingSocket(P2PSocket<String> s) {}
    });
    UpperIdentity<String, String> upper = identity.getUpperIdentity();
    assertNotNull(upper);
    upper.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(String i, String m, @Nullable Map<String, Object> options) {
        upperReceived.add(i + ":" + m);
      }

      @Override
      public void incomingSocket(P2PSocket<String> s) {}
    });
    upper.addLivenessListener((i, val, options) -> upperLiveness.add(i + ":" + val));
    upper.addProximityListener((i, prox, options) -> upperProximity.add(i + ":" + prox));
  }

  private <T> T onSelector(Callable<T> task) throws Exception {
    CompletableFuture<T> result = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> {
      try {
        result.complete(task.call());
      } catch (Throwable t) {
        result.completeExceptionally(t);
      }
    });
    return result.get(10, TimeUnit.SECONDS);
  }

  private void receiveFrom(String node) throws Exception {
    byte[] header = serialized(node);
    ByteBuffer message = ByteBuffer.allocate(1 + header.length + PAYLOAD.length);
    message.put(IdentityImpl.NO_ID).put(header).put(PAYLOAD).flip();
    onSelector(() -> {
      lowerInput.messageReceived(ADDRESS, message, null);
      return null;
    });
  }

  private static Map<String, Object> optionsFor(String node) {
    Map<String, Object> options = new HashMap<>();
    options.put(INDEX, node);
    return options;
  }

  @Test
  @DisplayName("a message with a sender id binds that node id to the address and marks it alive")
  void messageBindsTheNodeId() throws Exception {
    build((oldDest, newDest) -> false, null);

    receiveFrom(NODE_A);

    assertEquals(List.of(ADDRESS), lowerReceived);
    assertEquals(NODE_A, lowerReceivedOptions.get(0).get(INDEX));
    assertEquals(List.of(ADDRESS + ":" + LIVENESS_ALIVE), overrideCalls);
    assertTrue(unexpected.isEmpty());
  }

  @Test
  @DisplayName("a message from a changed binding is dropped and the newcomer is declared dead forever")
  void changedBindingIsRejected() throws Exception {
    build((oldDest, newDest) -> false, null);
    receiveFrom(NODE_A);
    overrideCalls.clear();

    receiveFrom(NODE_B);

    assertEquals(1, lowerReceived.size(), "only the first message is delivered");
    assertEquals(List.of(ADDRESS), unexpected);
    assertEquals(List.of(ADDRESS + ":" + LIVENESS_DEAD_FOREVER), overrideCalls);
  }

  @Test
  @DisplayName("a bind strategy that replaces kills the old binding and accepts the new node id")
  void bindStrategyReplacesTheOldBinding() throws Exception {
    build((oldDest, newDest) -> false, new AcceptingStrategy() {
      @Override
      public Replacement replace(String old, String u, @Nullable Map<String, Object> options) {
        return Replacement.REPLACE;
      }
    });
    receiveFrom(NODE_A);
    overrideCalls.clear();

    receiveFrom(NODE_B);

    assertEquals(2, lowerReceived.size());
    assertEquals(NODE_B, lowerReceivedOptions.get(1).get(INDEX));
    assertEquals(List.of(ADDRESS + ":" + LIVENESS_DEAD_FOREVER, ADDRESS + ":" + LIVENESS_ALIVE), overrideCalls);
  }

  @Test
  @DisplayName("a bind strategy that keeps the old binding drops the message of the newcomer")
  void bindStrategyKeepsTheOldBinding() throws Exception {
    build((oldDest, newDest) -> true, new AcceptingStrategy() {
      @Override
      public Replacement replace(String old, String u, @Nullable Map<String, Object> options) {
        return Replacement.KEEP;
      }
    });
    receiveFrom(NODE_A);

    receiveFrom(NODE_B);

    assertEquals(1, lowerReceived.size());
    assertEquals(List.of(ADDRESS), unexpected);
  }

  @Test
  @DisplayName("an outgoing message with a node handle carries the local identity and the payload")
  void outgoingMessageCarriesTheSender() throws Exception {
    build((oldDest, newDest) -> false, null);

    onSelector(() -> identity.getLowerIdentity().sendMessage(ADDRESS, ByteBuffer.wrap(PAYLOAD), null, optionsFor(NODE_A)));

    byte[] local = serialized(LOCAL);
    ByteBuffer sent = lowerTl.sent.get(0);
    assertEquals(IdentityImpl.NO_ID, sent.get());
    byte[] sender = new byte[local.length];
    sent.get(sender);
    assertArrayEquals(local, sender);
    byte[] payload = new byte[sent.remaining()];
    sent.get(payload);
    assertArrayEquals(PAYLOAD, payload);
  }

  @Test
  @DisplayName("a message addressed to another identity is answered with INCORRECT_IDENTITY")
  void messageForWrongNodeIsAnswered() throws Exception {
    build((oldDest, newDest) -> false, null);
    byte[] other = serialized("other@me");
    ByteBuffer message = ByteBuffer.allocate(1 + other.length);
    message.put(IdentityImpl.NORMAL).put(other).flip();

    onSelector(() -> {
      lowerInput.messageReceived(ADDRESS, message, null);
      return null;
    });

    assertTrue(lowerReceived.isEmpty());
    assertEquals(List.of(ADDRESS), lowerTl.sentTo);
    assertEquals(IdentityImpl.INCORRECT_IDENTITY, lowerTl.sent.get(0).get());
  }

  @Test
  @DisplayName("the upper role binds on send, announces the node alive and forwards with the node id")
  void upperSendBindsAndForwards() throws Exception {
    build((oldDest, newDest) -> false, null);

    onSelector(() -> identity.getUpperIdentity().sendMessage(NODE_A, "hello", null, null));

    assertEquals(List.of(ADDRESS), middleTl.sentTo);
    assertEquals(List.of("hello"), middleTl.sent);
    assertEquals(NODE_A, middleTl.sentOptions.get(0).get(INDEX));
    assertEquals(List.of(NODE_A + ":" + LIVENESS_ALIVE), upperLiveness);
  }

  @Test
  @DisplayName("the upper role delivers a middle message to the node id found in the options")
  void upperDeliversMessageFromTheNodeId() throws Exception {
    build((oldDest, newDest) -> false, null);

    middleTl.callback.messageReceived(ADDRESS, "hi", optionsFor(NODE_A));

    assertEquals(List.of(NODE_A + ":hi"), upperReceived);
  }

  @Test
  @DisplayName("the upper role drops a message that the bind strategy does not accept from that channel")
  void upperDropsMessagesTheBindStrategyRejects() throws Exception {
    build((oldDest, newDest) -> false, new AcceptingStrategy() {
      @Override
      public boolean acceptsMessage(String from, @Nullable Map<String, Object> options) {
        return false;
      }
    });

    middleTl.callback.messageReceived(ADDRESS, "hi", optionsFor(NODE_A));

    assertTrue(upperReceived.isEmpty());
  }

  @Test
  @DisplayName("liveness and proximity changes of the middle layer reach the upper listeners by node id")
  void livenessAndProximityAreTranslatedUp() throws Exception {
    build((oldDest, newDest) -> false, null);

    middleLiveness.listener.livenessChanged(ADDRESS, LIVENESS_ALIVE, optionsFor(NODE_A));
    middleLiveness.listener.livenessChanged(ADDRESS, LIVENESS_ALIVE, optionsFor(NODE_A));
    middleProximity.listener.proximityChanged(ADDRESS, 42, optionsFor(NODE_A));

    assertEquals(List.of(NODE_A + ":" + LIVENESS_ALIVE), upperLiveness, "repeated values are reported once");
    assertEquals(List.of(NODE_A + ":42"), upperProximity);
  }
}
