package rice.p2p.replication;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.replication.messaging.ReminderMessage;
import rice.p2p.replication.messaging.RequestMessage;
import rice.p2p.replication.messaging.ResponseMessage;
import rice.p2p.util.IdBloomFilter;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.Id;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;

/**
 * The messages of replication through the bytes of a socket ring: read where they arrive, sent on, and routed.
 */
class ReplicationWireRoundTripTest {
  private static final int REPLICAS = 3;

  private static Environment env;
  private static SocketNode sn;
  private static NodeHandle local;
  private static Endpoint endpoint;
  private static MessageDeserializer deserializer;

  @BeforeAll
  static void setUp() throws Exception {
    env = new Environment();
    sn = SocketNode.create(env, "4000");
    local = sn.local();
    ReplicationClient client = (ReplicationClient) Proxy.newProxyInstance(
        ReplicationWireRoundTripTest.class.getClassLoader(), new Class<?>[] {ReplicationClient.class},
        (proxy, method, args) -> null);
    ReplicationImpl replication = new ReplicationImpl(sn.node, client, REPLICAS, "replication-wire");
    Field field = ReplicationImpl.class.getDeclaredField("endpoint");
    field.setAccessible(true);
    endpoint = (Endpoint) field.get(replication);
    deserializer = endpoint.getDeserializer();
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  private static <M extends rice.p2p.commonapi.rawserialization.RawMessage> M wire(M message) throws IOException {
    M copy = WireRoundTrip.roundTrip(message, deserializer, local);
    sn.route(endpoint, Id.build("9000"), message, local, false);
    sn.route(endpoint, Id.build("9000"), message, local, true);
    return copy;
  }

  private static IdRange range(String ccw, String cw) {
    return sn.node.getIdFactory().buildIdRange(Id.build(ccw), Id.build(cw));
  }

  private static IdSet set(String... ids) {
    IdSet set = sn.node.getIdFactory().buildIdSet();
    for (String id : ids) {
      set.addId(Id.build(id));
    }
    return set;
  }

  private static IdBloomFilter filter(String... ids) {
    return new IdBloomFilter(set(ids));
  }

  @Test
  @DisplayName("a request with ranges and one bloom filter per range is read and sent on")
  void request() throws IOException {
    IdRange[] ranges = {range("1000", "2000"), range("F000", "1000")};
    IdBloomFilter[] filters = {filter("1100", "1200"), filter()};

    RequestMessage copy = wire(new RequestMessage(local, ranges, filters));

    assertEquals(local, copy.getSource());
    assertEquals(2, copy.getRanges().length);
    assertEquals(ranges[1].getCCWId(), copy.getRanges()[1].getCCWId());
    assertEquals(2, copy.getFilters().length);
    assertTrue(copy.getFilters()[0].check(Id.build("1100")));
  }

  @Test
  void requestWithASingleRange() throws IOException {
    RequestMessage copy = wire(new RequestMessage(local, new IdRange[] {range("1000", "2000")},
        new IdBloomFilter[] {filter("1500")}));

    assertEquals(1, copy.getRanges().length);
    assertTrue(copy.getFilters()[0].check(Id.build("1500")));
  }

  @Test
  void requestWithoutRangesOrFilters() throws IOException {
    RequestMessage copy = wire(new RequestMessage(local, new IdRange[0], new IdBloomFilter[0]));

    assertEquals(0, copy.getRanges().length);
    assertEquals(0, copy.getFilters().length);
  }

  @Test
  @DisplayName("a bloom filter over many ids, as a request carries after a long outage, is read and sent on")
  void requestWithALargeFilter() throws IOException {
    String[] ids = new String[500];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = String.format("%04X", i * 13 + 1);
    }

    RequestMessage copy = wire(new RequestMessage(local, new IdRange[] {range("0000", "FFFF")},
        new IdBloomFilter[] {filter(ids)}));

    for (String id : ids) {
      assertTrue(copy.getFilters()[0].check(Id.build(id)), id);
    }
  }

  @Test
  void response() throws IOException {
    IdRange[] ranges = {range("1000", "2000"), range("3000", "4000"), range("5000", "6000")};
    IdSet[] sets = {set("1100", "1200", "1300"), set("3100"), set("5100", "5200")};

    ResponseMessage copy = wire(new ResponseMessage(local, ranges, sets));

    assertEquals(local, copy.getSource());
    assertEquals(3, copy.getRanges().length);
    assertEquals(3, copy.getIdSets().length);
    assertEquals(3, copy.getIdSets()[0].length);
    assertEquals(1, copy.getIdSets()[1].length);
    assertArrayEquals(sets[2].asArray(), copy.getIdSets()[2]);
  }

  @Test
  @DisplayName("a response with empty id sets between full ones is read and sent on")
  void responseWithEmptyInnerArrays() throws IOException {
    ResponseMessage copy = wire(new ResponseMessage(local, new IdRange[] {range("1000", "2000"), range("3000", "4000")},
        new IdSet[] {set(), set("3100")}));

    assertEquals(0, copy.getIdSets()[0].length);
    assertEquals(1, copy.getIdSets()[1].length);
  }

  @Test
  void responseWithOnlyEmptyIdSets() throws IOException {
    ResponseMessage copy = wire(new ResponseMessage(local, new IdRange[] {range("1000", "2000")}, new IdSet[] {set()}));

    assertEquals(1, copy.getIdSets().length);
    assertEquals(0, copy.getIdSets()[0].length);
  }

  @Test
  void responseWithoutIdsOrRanges() throws IOException {
    ResponseMessage copy = wire(new ResponseMessage(local, new IdRange[0], new IdSet[0]));

    assertEquals(0, copy.getIdSets().length);
    assertEquals(0, copy.getRanges().length);
  }

  @Test
  @DisplayName("the reminder is local: it does not serialize and the deserializer does not read its type")
  void reminder() {
    ReminderMessage reminder = new ReminderMessage(local);

    assertThrows(RuntimeException.class, () -> WireRoundTrip.bytes(reminder));
    assertThrows(IOException.class,
        () -> deserializer.deserialize(new SimpleInputBuffer(new byte[16]), reminder.getType(), 0, local));
  }
}
