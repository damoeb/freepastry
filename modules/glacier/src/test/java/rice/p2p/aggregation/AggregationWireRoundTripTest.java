package rice.p2p.aggregation;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.environment.Environment;
import rice.p2p.aggregation.messaging.NonAggregate;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.glacier.v2.DebugContent;
import rice.p2p.past.gc.ExpiringPastContent;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;

/** Aggregates, their handles and descriptors, and the wrapper that carries a single object, through socket bytes. */
@DisplayName("Aggregation values on the wire")
class AggregationWireRoundTripTest {
  private static Environment env;
  private static SocketNode socketNode;
  private static NodeHandle peer;
  private static Endpoint endpoint;
  private static PastryIdFactory idFactory;
  private static final PastContentDeserializer CONTENTS = (buf, ep, type) -> {
    if (type == DebugContent.TYPE) {
      return new DebugContent(buf, ep);
    }
    throw new IOException("Unknown content type: " + type);
  };

  @BeforeAll
  static void setUp() throws IOException {
    env = new Environment();
    socketNode = SocketNode.create(env, "4000");
    peer = socketNode.handle("8000");
    idFactory = new PastryIdFactory(env);
    endpoint = socketNode.node.buildEndpoint(new Application() {
      @Override
      public boolean forward(RouteMessage message) {
        return true;
      }

      @Override
      public void deliver(rice.p2p.commonapi.Id id, Message message) {
      }

      @Override
      public void update(rice.p2p.commonapi.NodeHandle handle, boolean joined) {
      }
    }, "aggregation-values");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  private static DebugContent content(int i) {
    return new DebugContent(Id.build(Integer.toHexString(0x3000 + i)), false, i, new byte[i]);
  }

  private static Aggregate aggregate(int pointers, int components) {
    rice.p2p.commonapi.Id[] pointerIds = new rice.p2p.commonapi.Id[pointers];
    for (int i = 0; i < pointers; i++) {
      pointerIds[i] = Id.build(Integer.toHexString(0x4000 + i));
    }
    ExpiringPastContent[] parts = new ExpiringPastContent[components];
    for (int i = 0; i < components; i++) {
      parts[i] = content(i);
    }
    Aggregate aggregate = new Aggregate(parts, pointerIds);
    aggregate.setId(Id.build("AAAA"));
    return aggregate;
  }

  @ParameterizedTest(name = "{0} pointers")
  @ValueSource(ints = {0, 1, 2, 1000})
  void aggregateWithPointersAndComponents(int pointers) throws IOException {
    for (int components : new int[] {0, 1, 3}) {
      Aggregate original = aggregate(pointers, components);
      Aggregate copy = WireRoundTrip.roundTrip(original, buf -> new Aggregate(buf, endpoint, CONTENTS));
      assertEquals(original.getId(), copy.getId());
      assertArrayEquals(original.getPointers(), copy.getPointers());
      assertEquals(components, copy.numComponents());
      for (int i = 0; i < components; i++) {
        DebugContent expected = (DebugContent) original.getComponent(i);
        DebugContent actual = assertInstanceOf(DebugContent.class, copy.getComponent(i));
        assertEquals(expected.getId(), actual.getId());
        assertArrayEquals(expected.getPayload(), actual.getPayload());
      }
      assertArrayEquals(original.getContentHash(), copy.getContentHash());
    }
  }

  @Test
  void aggregateHandle() throws IOException {
    AggregateHandle original = new AggregateHandle(peer, Id.build("AAAA"), 0L, Long.MAX_VALUE);
    AggregateHandle copy = WireRoundTrip.roundTrip(original, AggregateHandle::serialize,
        buf -> new AggregateHandle(buf, endpoint));
    assertEquals(original.id(), copy.id());
    assertEquals(original.version(), copy.version());
    assertEquals(original.expiration(), copy.expiration());
    assertEquals(original.nodeHandle().getId(), copy.nodeHandle().getId());
  }

  @Test
  void objectDescriptor() throws IOException {
    ObjectDescriptor original = new ObjectDescriptor(Id.build("0123"), 5L, 100L, Long.MAX_VALUE, Integer.MAX_VALUE);
    ObjectDescriptor copy = WireRoundTrip.roundTrip(original, ObjectDescriptor::serialize,
        buf -> new ObjectDescriptor(buf, idFactory));
    assertEquals(0, original.compareTo(copy));
    ObjectDescriptor small = new ObjectDescriptor(Id.build(), 0L, 0L, 0L, 0);
    assertEquals(0, small.compareTo(WireRoundTrip.roundTrip(small, ObjectDescriptor::serialize,
        buf -> new ObjectDescriptor(buf, idFactory))));
  }

  /** The reading side of a NonAggregate is in AggregationImpl, which unwraps it; this reads it the same way. */
  @Test
  void nonAggregateIsReadAsItsContent() throws IOException {
    DebugContent inner = content(12);
    NonAggregate copy = WireRoundTrip.roundTrip(new NonAggregate(inner), NonAggregate::serialize, buf -> {
      assertEquals(0, buf.readByte());
      return new NonAggregate(CONTENTS.deserializePastContent(buf, endpoint, buf.readShort()));
    });
    assertEquals(inner.getId(), copy.getId());
    assertArrayEquals(WireRoundTrip.bytes(new NonAggregate(inner)), WireRoundTrip.bytes(copy));
  }
}
