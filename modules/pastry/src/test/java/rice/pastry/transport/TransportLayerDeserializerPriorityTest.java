package rice.pastry.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.NodeHandle;
import rice.pastry.commonapi.PastryEndpointMessage;
import rice.pastry.join.JoinRequest;
import rice.pastry.leafset.BroadcastLeafSet;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.RequestLeafSet;
import rice.pastry.messaging.PastryRawMessage;
import rice.pastry.pns.ProximityNeighborSelectionApplication;
import rice.pastry.pns.messages.LeafSetResponse;
import rice.pastry.pns.messages.ProximityLeafSetRequest;
import rice.pastry.pns.messages.RouteRowRequest;
import rice.pastry.pns.messages.RouteRowResponse;
import rice.pastry.routing.BroadcastRouteRow;
import rice.pastry.routing.RequestRouteRow;
import rice.pastry.routing.RouteSet;
import rice.pastry.standard.ConsistentJoinMessage;
import rice.pastry.standard.StandardAddress;
import rice.pastry.testing.WireRoundTrip.SocketNode;

@DisplayName("a message read from the wire has the priority it was sent with")
class TransportLayerDeserializerPriorityTest {
  private static final int SENT_PRIORITY = rice.p2p.commonapi.Message.LOW_PRIORITY;
  private static final int ENDPOINT_ADDRESS = 8;
  private static final short APPLICATION_TYPE = 3;

  private static Environment env;
  private static SocketNode local;
  private static NodeHandle peer;

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    local = SocketNode.create(env, "4000");
    peer = local.handle("1000");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  private static LeafSet leafSet() {
    LeafSet leafSet = new LeafSet(local.local(), 8, false);
    leafSet.put(peer);
    return leafSet;
  }

  static Stream<Supplier<PastryRawMessage>> pastryMessages() {
    int pns = StandardAddress.getAddress(ProximityNeighborSelectionApplication.class, null, env);
    return Stream.of(
        () -> new JoinRequest(peer, (byte) 4, 1L),
        () -> new BroadcastLeafSet(peer, leafSet(), BroadcastLeafSet.Update, 1L),
        () -> new RequestLeafSet(peer, 1L),
        () -> new BroadcastRouteRow(peer, new RouteSet[0]),
        () -> new RequestRouteRow(peer, (short) 1),
        () -> new ConsistentJoinMessage(leafSet(), new HashSet<>(), true),
        () -> new ProximityLeafSetRequest(peer, pns),
        () -> new LeafSetResponse(leafSet(), pns),
        () -> new RouteRowRequest(peer, (short) 1, pns),
        () -> new RouteRowResponse(peer, (short) 1, new RouteSet[0], pns));
  }

  @ParameterizedTest
  @MethodSource("pastryMessages")
  @DisplayName("a pastry message that is not routed")
  void pastryMessage(Supplier<PastryRawMessage> factory) throws IOException {
    PastryRawMessage sent = factory.get();
    sent.setPriority(SENT_PRIORITY);

    RawMessage read = local.read(local.wire(sent), peer);

    assertEquals(SENT_PRIORITY, read.getPriority(), () -> sent.getClass().getSimpleName());
  }

  /** An application message that keeps the priority it is given, and whose deserializer ignores the wire's. */
  private static final class ApplicationMessage implements RawMessage {
    private int priority;

    ApplicationMessage(int priority) {
      this.priority = priority;
    }

    @Override
    public int getPriority() {
      return priority;
    }

    @Override
    public short getType() {
      return APPLICATION_TYPE;
    }

    @Override
    public void serialize(OutputBuffer buf) {
    }
  }

  @Test
  @DisplayName("an application message sent to a node directly, and the endpoint message around it")
  void endpointMessage() throws IOException {
    TransportLayerDeserializer transport = new TransportLayerDeserializer(null, env);
    transport.setDeserializer(ENDPOINT_ADDRESS, (buf, type, priority, sender) -> new PastryEndpointMessage(
        ENDPOINT_ADDRESS, buf,
        (innerBuf, innerType, innerPriority, innerSender) -> new ApplicationMessage(rice.p2p.commonapi.Message.DEFAULT_PRIORITY),
        type, priority, null));
    PastryEndpointMessage sent = new PastryEndpointMessage(ENDPOINT_ADDRESS, new ApplicationMessage(SENT_PRIORITY), null);
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    transport.serialize(sent, out);

    PastryEndpointMessage read = (PastryEndpointMessage) transport.deserialize(new SimpleInputBuffer(out.getBytes()), null);

    assertEquals(SENT_PRIORITY, read.getPriority(), "the endpoint message");
    assertEquals(SENT_PRIORITY, read.getMessage().getPriority(), "the application message in it");
  }
}
