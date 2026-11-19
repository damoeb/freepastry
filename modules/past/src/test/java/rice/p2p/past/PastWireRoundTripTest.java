package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.past.gc.ExpiringId;
import rice.p2p.past.gc.ExpiringPastImpl;
import rice.p2p.past.gc.ExpiringPastMetadata;
import rice.p2p.past.gc.GCIdFactory;
import rice.p2p.past.gc.GCIdRange;
import rice.p2p.past.gc.GCIdSet;
import rice.p2p.past.messaging.FetchHandleMessage;
import rice.p2p.past.messaging.FetchMessage;
import rice.p2p.past.messaging.GCCollectMessage;
import rice.p2p.past.messaging.GCInsertMessage;
import rice.p2p.past.messaging.GCLookupHandlesMessage;
import rice.p2p.past.messaging.GCRefreshMessage;
import rice.p2p.past.messaging.InsertMessage;
import rice.p2p.past.messaging.LookupHandlesMessage;
import rice.p2p.past.messaging.LookupMessage;
import rice.p2p.past.messaging.MessageLostMessage;
import rice.p2p.past.messaging.PastMessage;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.testing.DistPastTestContent;
import rice.p2p.util.BloomFilter;
import rice.p2p.util.IdBloomFilter;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.Id;
import rice.pastry.IdSet;
import rice.pastry.NodeSet;
import rice.pastry.testing.WireRoundTrip;
import rice.pastry.testing.WireRoundTrip.SocketNode;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

/**
 * The messages of Past and ExpiringPast, and the handles, ids and metadata they carry, through the bytes of a socket ring:
 * read where they arrive, given a response there (as the replica does) and sent back, and routed hop by hop.
 */
class PastWireRoundTripTest {
  private static final int REPLICAS = 3;
  private static final long COLLECTION_INTERVAL = 100_000;
  private static final long EXPIRATION = 123_456_789L;

  private static Environment env;
  private static SocketNode sn;
  private static IdFactory idf;
  private static NodeHandle local;
  private static NodeHandle remote;
  private static ExpiringPastImpl gcPast;
  private static Endpoint pastEndpoint;
  private static Endpoint gcInner;
  private static MessageDeserializer pastDeserializer;
  private static MessageDeserializer gcDeserializer;
  private static DistPastTestContent content;

  @BeforeAll
  static void setUp() throws Exception {
    env = new Environment();
    sn = SocketNode.create(env, "4000");
    idf = sn.node.getIdFactory();
    local = sn.local();
    remote = sn.handle("8000");

    PastImpl past = new PastImpl(sn.node, storage(), REPLICAS, "past-wire");
    past.setContentDeserializer(TestContentReader.INSTANCE);
    pastEndpoint = past.getEndpoint();
    pastDeserializer = pastEndpoint.getDeserializer();

    gcPast = new ExpiringPastImpl(sn.node, storage(), null, REPLICAS, "gcpast-wire", new PastPolicy.DefaultPastPolicy(),
        COLLECTION_INTERVAL, null);
    gcPast.setContentDeserializer(TestContentReader.INSTANCE);
    // SocketNode.route wraps the message for the pastry endpoint underneath the GC wrapper
    Field inner = gcPast.getEndpoint().getClass().getDeclaredField("endpoint");
    inner.setAccessible(true);
    gcInner = (Endpoint) inner.get(gcPast.getEndpoint());
    gcDeserializer = gcPast.getEndpoint().getDeserializer();

    content = new DistPastTestContent(env, idf, "some content");
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  private static CachedStorageImpl storage() {
    return new CachedStorageImpl(idf, new MemoryStorage(idf), new NoOpCache(idf));
  }

  private enum TestContentReader implements PastContentDeserializer {
    INSTANCE;

    @Override
    public PastContent deserializePastContent(InputBuffer buf, Endpoint endpoint, short contentType)
        throws IOException {
      if (contentType == DistPastTestContent.TYPE) {
        return new DistPastTestContent(buf, endpoint);
      }
      throw new IOException("Unknown content type: " + contentType);
    }
  }

  private static rice.p2p.commonapi.Id dest() {
    return Id.build("9000");
  }

  /** Reads the message as a hop would and sends it on; then the same routed inside a route message. */
  private static <M extends RawMessage> M wire(M message, MessageDeserializer md, Endpoint routeEndpoint)
      throws IOException {
    M copy = WireRoundTrip.roundTrip(message, md, local);
    sn.route(routeEndpoint, dest(), message, local, false);
    sn.route(routeEndpoint, dest(), message, local, true);
    return copy;
  }

  private static <M extends RawMessage> M wire(M message) throws IOException {
    return wire(message, pastDeserializer, pastEndpoint);
  }

  private static <M extends RawMessage> M gcWire(M message) throws IOException {
    return wire(message, gcDeserializer, gcInner);
  }

  /** The replica reads the request and answers it: the response is given and sent back, read again by the requester. */
  private static <M extends PastMessage> M respond(M request, MessageDeserializer md, Endpoint routeEndpoint,
      Consumer<M> answer) throws IOException {
    M atReplica = WireRoundTrip.roundTrip(request, md, local);
    answer.accept(atReplica);
    assertTrue(atReplica.isResponse());
    return wire(atReplica, md, routeEndpoint);
  }

  private static <M extends PastMessage> M respond(M request, Consumer<M> answer) throws IOException {
    return respond(request, pastDeserializer, pastEndpoint, answer);
  }

  private static <M extends PastMessage> M gcRespond(M request, Consumer<M> answer) throws IOException {
    return respond(request, gcDeserializer, gcInner, answer);
  }

  private static void assertHeader(PastMessage expected, PastMessage actual) {
    assertEquals(expected.getClass(), actual.getClass());
    assertEquals(expected.getUID(), actual.getUID());
    assertEquals(expected.getDestination(), actual.getDestination());
    assertEquals(expected.getSource(), actual.getSource());
    assertEquals(expected.isResponse(), actual.isResponse());
  }

  /** What the requester sees as the result of a message: the result and the exception. */
  private static Object[] outcome(PastMessage message) {
    Object[] result = new Object[2];
    message.returnResponse(new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object o) {
        result[0] = o;
      }

      @Override
      public void receiveException(Exception e) {
        result[1] = e;
      }
    }, env, "test");
    return result;
  }

  private static void assertRemoteException(PastMessage message, String className, String text) {
    PastRemoteException e = assertInstanceOf(PastRemoteException.class, outcome(message)[1]);
    assertEquals(className, e.getRemoteClassName());
    assertEquals(text, e.getRemoteMessage());
  }

  private static NodeSet nodes(NodeHandle... handles) {
    NodeSet set = new NodeSet();
    for (NodeHandle h : handles) {
      set.putHandle(h);
    }
    return set;
  }

  // ---- insert ----

  @Test
  @DisplayName("an insert request carries its content")
  void insertRequest() throws IOException {
    InsertMessage request = new InsertMessage(7, content, local, dest());

    InsertMessage copy = wire(request);

    assertHeader(request, copy);
    assertEquals(content.getId(), copy.getContent().getId());
    assertEquals(content.getContent(), ((DistPastTestContent) copy.getContent()).getContent());
  }

  @Test
  @DisplayName("an insert request without content is read and sent on")
  void insertRequestWithoutContent() throws IOException {
    assertNull(wire(new InsertMessage(7, null, local, dest())).getContent());
  }

  @ParameterizedTest(name = "answered {0}")
  @ValueSource(booleans = {true, false})
  @DisplayName("an insert response with a Boolean is read at the requester and sent on")
  void insertResponseBoolean(boolean answer) throws IOException {
    InsertMessage copy = respond(new InsertMessage(7, content, local, dest()), m -> m.receiveResult(answer));

    assertEquals(answer, outcome(copy)[0]);
    assertNull(copy.getContent());
  }

  @Test
  void insertResponseException() throws IOException {
    InsertMessage copy = respond(new InsertMessage(7, content, local, dest()),
        m -> m.receiveException(new IllegalStateException("no room")));

    assertRemoteException(copy, IllegalStateException.class.getName(), "no room");
  }

  @Test
  void insertResponseExceptionWithoutMessage() throws IOException {
    InsertMessage copy = respond(new InsertMessage(7, content, local, dest()),
        m -> m.receiveException(new IllegalStateException()));

    assertRemoteException(copy, IllegalStateException.class.getName(), null);
  }

  @Test
  void insertResponseNull() throws IOException {
    InsertMessage copy = respond(new InsertMessage(7, content, local, dest()), m -> m.receiveResult(null));

    assertNull(outcome(copy)[0]);
    assertNull(outcome(copy)[1]);
  }

  // ---- lookup ----

  @Test
  void lookupRequest() throws IOException {
    LookupMessage request = new LookupMessage(8, content.getId(), local, dest());

    LookupMessage copy = wire(request);

    assertHeader(request, copy);
    assertEquals(content.getId(), copy.getId());
  }

  @Test
  @DisplayName("a lookup response with a found content is read at the requester and sent on")
  void lookupResponseContent() throws IOException {
    LookupMessage copy = respond(new LookupMessage(8, content.getId(), local, dest()), m -> m.receiveResult(content));

    assertEquals(content.getId(), ((PastContent) outcome(copy)[0]).getId());
    assertEquals(content.getId(), copy.getId());
  }

  @Test
  @DisplayName("a lookup response for a content that was not found is read and sent on")
  void lookupResponseNotFound() throws IOException {
    LookupMessage copy = respond(new LookupMessage(8, content.getId(), local, dest()), m -> m.receiveResult(null));

    assertNull(outcome(copy)[0]);
    assertNull(outcome(copy)[1]);
  }

  @Test
  void lookupResponseException() throws IOException {
    LookupMessage copy = respond(new LookupMessage(8, content.getId(), local, dest()),
        m -> m.receiveException(new RuntimeException("boom")));

    assertRemoteException(copy, RuntimeException.class.getName(), "boom");
  }

  // ---- lookup handles ----

  @Test
  void lookupHandlesRequest() throws IOException {
    LookupHandlesMessage request = new LookupHandlesMessage(9, content.getId(), 5, local, dest());

    LookupHandlesMessage copy = wire(request);

    assertHeader(request, copy);
    assertEquals(5, copy.getMax());
    assertEquals(content.getId(), copy.getId());
  }

  @Test
  void lookupHandlesResponse() throws IOException {
    NodeSet three = nodes(local, remote, sn.handle("C000"));
    LookupHandlesMessage copy = respond(new LookupHandlesMessage(9, content.getId(), 5, local, dest()),
        m -> m.receiveResult(three));

    assertEquals(3, ((NodeHandleSet) outcome(copy)[0]).size());
    assertEquals(5, copy.getMax());
  }

  @Test
  void lookupHandlesResponseEmptySet() throws IOException {
    LookupHandlesMessage copy = respond(new LookupHandlesMessage(9, content.getId(), 0, local, dest()),
        m -> m.receiveResult(nodes()));

    assertEquals(0, ((NodeHandleSet) outcome(copy)[0]).size());
  }

  @Test
  void lookupHandlesResponseExceptionAndNull() throws IOException {
    LookupHandlesMessage failed = respond(new LookupHandlesMessage(9, content.getId(), 5, local, dest()),
        m -> m.receiveException(new Exception("down")));
    assertRemoteException(failed, Exception.class.getName(), "down");

    LookupHandlesMessage none = respond(new LookupHandlesMessage(9, content.getId(), 5, local, dest()),
        m -> m.receiveResult(null));
    assertNull(outcome(none)[0]);
  }

  // ---- fetch ----

  private static PastContentHandle handle() {
    return new ContentHashPastContentHandle(remote, content.getId());
  }

  @Test
  void fetchRequest() throws IOException {
    FetchMessage request = new FetchMessage(10, handle(), local, dest());

    FetchMessage copy = wire(request);

    assertHeader(request, copy);
    assertEquals(handle(), copy.getHandle());
  }

  @Test
  @DisplayName("a fetch message whose cached flag is set keeps it")
  void fetchRequestCached() throws Exception {
    FetchMessage request = new FetchMessage(10, handle(), local, dest());
    Field cached = FetchMessage.class.getDeclaredField("cached");
    cached.setAccessible(true);
    cached.setBoolean(request, true);

    FetchMessage copy = wire(request);

    assertTrue(cached.getBoolean(copy));
  }

  @Test
  void fetchResponseContent() throws IOException {
    FetchMessage copy = respond(new FetchMessage(10, handle(), local, dest()), m -> m.receiveResult(content));

    assertEquals(content.getId(), ((PastContent) outcome(copy)[0]).getId());
    assertEquals(handle(), copy.getHandle());
  }

  @Test
  void fetchResponseNullAndException() throws IOException {
    FetchMessage none = respond(new FetchMessage(10, handle(), local, dest()), m -> m.receiveResult(null));
    assertNull(outcome(none)[0]);

    FetchMessage failed = respond(new FetchMessage(10, handle(), local, dest()),
        m -> m.receiveException(new PastException("gone")));
    assertRemoteException(failed, PastException.class.getName(), "gone");
  }

  @Test
  void fetchHandleRequest() throws IOException {
    FetchHandleMessage request = new FetchHandleMessage(11, content.getId(), local, dest());

    FetchHandleMessage copy = wire(request);

    assertHeader(request, copy);
    assertEquals(content.getId(), copy.getId());
  }

  @Test
  void fetchHandleResponse() throws IOException {
    FetchHandleMessage copy = respond(new FetchHandleMessage(11, content.getId(), local, dest()),
        m -> m.receiveResult(handle()));

    assertEquals(handle(), outcome(copy)[0]);
  }

  @Test
  void fetchHandleResponseNullAndException() throws IOException {
    FetchHandleMessage none = respond(new FetchHandleMessage(11, content.getId(), local, dest()),
        m -> m.receiveResult(null));
    assertNull(outcome(none)[0]);

    FetchHandleMessage failed = respond(new FetchHandleMessage(11, content.getId(), local, dest()),
        m -> m.receiveException(new Exception()));
    assertRemoteException(failed, Exception.class.getName(), null);
  }

  // ---- local messages ----

  @Test
  @DisplayName("the local messages are never sent: they do not serialize and no deserializer reads their type")
  void localMessages() {
    MessageLostMessage lost = new MessageLostMessage(1, local, dest(), null, null);
    GCCollectMessage collect = new GCCollectMessage(1, local, dest());

    assertThrows(RuntimeException.class, () -> WireRoundTrip.bytes(lost));
    assertThrows(RuntimeException.class, () -> WireRoundTrip.bytes(collect));
    assertThrows(IOException.class,
        () -> pastDeserializer.deserialize(new SimpleInputBuffer(new byte[16]), lost.getType(), 0, local));
    assertThrows(IOException.class,
        () -> gcDeserializer.deserialize(new SimpleInputBuffer(new byte[16]), collect.getType(), 0, local));
  }

  // ---- GC messages ----

  private static GCIdSet gcIds(long... expirations) {
    GCIdSet set = (GCIdSet) new GCIdFactory(idf).buildIdSet();
    for (int i = 0; i < expirations.length; i++) {
      set.addId(new ExpiringId(Id.build(String.format("%04X", 0x1000 + 0x111 * i)), expirations[i]));
    }
    return set;
  }

  @Test
  void gcInsertRequest() throws IOException {
    GCInsertMessage request = new GCInsertMessage(12, content, EXPIRATION, local, dest());

    GCInsertMessage copy = gcWire(request);

    assertHeader(request, copy);
    assertEquals(EXPIRATION, copy.getExpiration());
    assertEquals(content.getId(), copy.getContent().getId());
  }

  @Test
  void gcInsertResponses() throws IOException {
    GCInsertMessage ok = gcRespond(new GCInsertMessage(12, content, EXPIRATION, local, dest()),
        m -> m.receiveResult(true));
    assertEquals(true, outcome(ok)[0]);
    assertEquals(EXPIRATION, ok.getExpiration());

    GCInsertMessage failed = gcRespond(new GCInsertMessage(12, content, EXPIRATION, local, dest()),
        m -> m.receiveException(new RuntimeException("full")));
    assertRemoteException(failed, RuntimeException.class.getName(), "full");

    GCInsertMessage none = gcRespond(new GCInsertMessage(12, content, EXPIRATION, local, dest()),
        m -> m.receiveResult(null));
    assertNull(outcome(none)[0]);
  }

  @Test
  void gcLookupHandles() throws IOException {
    GCLookupHandlesMessage request = new GCLookupHandlesMessage(13, content.getId(), local, dest());

    GCLookupHandlesMessage copy = gcWire(request);

    assertHeader(request, copy);
    assertEquals(Integer.MAX_VALUE, copy.getMax());

    GCLookupHandlesMessage response = gcRespond(request, m -> m.receiveResult(nodes(local, remote)));
    assertEquals(2, ((NodeHandleSet) outcome(response)[0]).size());

    GCLookupHandlesMessage failed = gcRespond(request, m -> m.receiveException(new Exception("x")));
    assertRemoteException(failed, Exception.class.getName(), "x");
  }

  private static long[] expirations(int count) {
    long[] expirations = new long[count];
    for (int i = 0; i < count; i++) {
      expirations[i] = EXPIRATION + i;
    }
    return expirations;
  }

  @ParameterizedTest(name = "{0} keys")
  @ValueSource(ints = {0, 1, 2, 17})
  @DisplayName("a refresh request carries any number of keys")
  void gcRefreshRequest(int count) throws IOException {
    GCRefreshMessage request = new GCRefreshMessage(14, gcIds(expirations(count)), local, dest());

    GCRefreshMessage copy = gcWire(request);

    assertHeader(request, copy);
    assertEquals(count, copy.getKeys().length);
    for (int i = 0; i < count; i++) {
      assertEquals(request.getKeys()[i], copy.getKeys()[i]);
      assertEquals(request.getKeys()[i].expiration(), copy.getKeys()[i].expiration());
    }
  }

  @ParameterizedTest(name = "{0} answers")
  @ValueSource(ints = {0, 1, 2, 17})
  @DisplayName("a refresh response with one Boolean per key reaches the requester, and is sent on with them")
  void gcRefreshResponse(int count) throws IOException {
    Boolean[] answers = new Boolean[count];
    for (int i = 0; i < count; i++) {
      answers[i] = i % 2 == 0;
    }

    GCRefreshMessage copy = gcRespond(new GCRefreshMessage(14, gcIds(expirations(count)), local, dest()),
        m -> m.receiveResult(answers));

    assertArrayEquals(answers, (Boolean[]) outcome(copy)[0]);
    assertEquals(count, copy.getKeys().length);
  }

  @Test
  void gcRefreshResponseExceptionAndNull() throws IOException {
    GCRefreshMessage failed = gcRespond(new GCRefreshMessage(14, gcIds(EXPIRATION), local, dest()),
        m -> m.receiveException(new RuntimeException("busy")));
    assertRemoteException(failed, RuntimeException.class.getName(), "busy");

    GCRefreshMessage none = gcRespond(new GCRefreshMessage(14, gcIds(), local, dest()), m -> m.receiveResult(null));
    assertNull(outcome(none)[0]);
  }

  // ---- values ----

  @Test
  void contentHashHandle() throws IOException {
    ContentHashPastContentHandle copy = WireRoundTrip.roundTrip(
        new ContentHashPastContentHandle(remote, content.getId()),
        buf -> new ContentHashPastContentHandle(buf, pastEndpoint));

    assertEquals(remote, copy.getNodeHandle());
    assertEquals(content.getId(), copy.getId());
  }

  @Test
  void gcMetadata() throws IOException {
    for (long expiration : new long[] {0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE}) {
      ExpiringPastMetadata metadata = new ExpiringPastMetadata(expiration);
      assertEquals(metadata, WireRoundTrip.roundTrip(metadata, ExpiringPastMetadata::serialize, ExpiringPastMetadata::new));
    }
  }

  @Test
  void gcId() throws IOException {
    ExpiringId id = new ExpiringId(Id.build("5000"), EXPIRATION);

    // a message writes the type of a key, then the key writes its own type again
    ExpiringId copy = WireRoundTrip.roundTrip(id, (key, buf) -> {
      buf.writeShort(key.getType());
      key.serialize(buf);
    }, buf -> (ExpiringId) gcPast.getEndpoint().readId(buf, buf.readShort()));

    assertEquals(id, copy);
    assertEquals(EXPIRATION, copy.expiration());
  }

  @Test
  void gcIdRange() throws IOException {
    for (rice.p2p.commonapi.IdRange range : List.of(idf.buildIdRange(Id.build("1000"), Id.build("2000")),
        idf.buildIdRange(Id.build("F000"), Id.build("1000")), idf.buildIdRange(Id.build("3000"), Id.build("3000")))) {
      GCIdRange gcRange = new GCIdRange(range);

      GCIdRange copy = WireRoundTrip.roundTrip(gcRange, GCIdRange::serialize,
          buf -> (GCIdRange) gcPast.getEndpoint().readIdRange(buf));

      assertEquals(gcRange.isEmpty(), copy.isEmpty());
      assertEquals(range.getCounterClockwiseBound(), copy.range().getCounterClockwiseBound());
      assertEquals(range.getClockwiseBound(), copy.range().getClockwiseBound());
    }
  }

  @Test
  @DisplayName("GCIdSet is not a wire type: only ExpiringId keys are carried, one by one")
  void gcIdSetIsNotSerializable() {
    assertFalse(RawSerializable.class.isAssignableFrom(GCIdSet.class));
  }

  @Test
  void idBloomFilters() throws IOException {
    for (int count : new int[] {0, 1, 63, 64, 65, 200, 1000}) {
      IdSet set = new IdSet();
      for (int i = 0; i < count; i++) {
        set.addId(Id.build(String.format("%04X", i * 37 + 1)));
      }
      IdBloomFilter filter = new IdBloomFilter(set);

      IdBloomFilter copy = WireRoundTrip.roundTrip(filter, IdBloomFilter::serialize, IdBloomFilter::new);

      for (int i = 0; i < count; i++) {
        assertTrue(copy.check(Id.build(String.format("%04X", i * 37 + 1))), count + " ids, id " + i);
      }
    }
  }

  @ParameterizedTest(name = "{0} bits")
  @ValueSource(ints = {0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 255, 256, 257})
  @DisplayName("a bloom filter of any length, with the last bit set or not, is read back")
  void bloomFilterLengths(int length) throws IOException {
    BloomFilter filter = new BloomFilter(2, length);
    for (int i = 0; i < 20; i++) {
      filter.add(new byte[] {(byte) i, (byte) (i * 7)});
    }

    for (BloomFilter f : List.of(filter, new BloomFilter(2, length))) {
      BloomFilter copy = WireRoundTrip.roundTrip(f, BloomFilter::serialize, BloomFilter::new);
      for (int i = 0; i < 20; i++) {
        byte[] probe = {(byte) i, (byte) (i * 7)};
        assertEquals(f.check(probe), copy.check(probe));
      }
    }
  }
}
