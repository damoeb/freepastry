package rice.p2p.multiring;

import java.io.IOException;
import java.lang.reflect.Proxy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RingId ordering and deserialization, MultiringIdFactory parsing, and the listener of a coalesced handle.
 */
class RingIdTest {

  private Environment env;
  private PastryIdFactory idFactory;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    idFactory = new PastryIdFactory(env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void compareToOrdersByRingFirst() {
    Id inner = idFactory.buildId("inner");
    RingId ringA = RingId.build(idFactory.buildId("a"), inner);
    RingId ringB = RingId.build(idFactory.buildId("b"), inner);

    assertNotEquals(0, ringA.compareTo(ringB));
    assertEquals(-Integer.signum(ringA.compareTo(ringB)), Integer.signum(ringB.compareTo(ringA)));
    assertEquals(0, ringA.compareTo(RingId.build(idFactory.buildId("a"), inner)));
  }

  /** A stub endpoint that reads pastry ids and returns the same handle from coalesce(). */
  private Endpoint endpoint(NodeHandle canonical) {
    return (Endpoint) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {Endpoint.class}, (proxy, method, args) -> {
      switch (method.getName()) {
        case "readId":
          return rice.pastry.Id.build((InputBuffer) args[0]);
        case "coalesce":
          return canonical;
        default:
          throw new UnsupportedOperationException(method.getName());
      }
    });
  }

  @Test
  void deserializedRingIdIsTheCanonicalInstance() throws IOException {
    RingId built = RingId.build(idFactory.buildId("ring"), idFactory.buildId("inner"));
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    built.serialize(out);

    RingId read = RingId.build(new SimpleInputBuffer(out.getBytes()), endpoint(null));

    assertSame(built, read);
  }

  @Test
  void buildIdFromToStringHonoursOffsetAndLength() {
    MultiringIdFactory factory = new MultiringIdFactory(idFactory.buildId("ring"), idFactory);
    Id expected = RingId.build(idFactory.buildId("ring"), idFactory.buildId("inner"));
    char[] text = ("junk" + expected.toStringFull() + "more junk").toCharArray();

    Id actual = factory.buildIdFromString(text, 4, expected.toStringFull().length());

    assertEquals(expected, actual);
  }

  static class Handle extends NodeHandle {
    final Id id;

    Handle(Id id) {
      this.id = id;
    }

    @Override
    public Id getId() {
      return id;
    }

    @Override
    public boolean checkLiveness() {
      return true;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
    }
  }

  @Test
  void coalescedHandleListensToTheCanonicalHandle() {
    Id id = idFactory.buildId("node");
    Handle original = new Handle(id);
    Handle canonical = new Handle(id);
    MultiringNodeHandle wrapper = new MultiringNodeHandle(idFactory.buildId("ring"), original);

    wrapper.coalesceHandle(endpoint(canonical));

    assertEquals(0, original.countNodeHandleListeners());
    assertTrue(canonical.countNodeHandleListeners() > 0);
    assertSame(canonical, wrapper.getHandle());
  }
}
